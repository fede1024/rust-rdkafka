//! Stream-based consumer implementation.
extern crate rdkafka_sys as rdkafka;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;
use std::time::Duration;

use futures::{Future, Poll, Sink};
use futures::stream::Stream;
use futures::sync::mpsc;

use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use error::KafkaResult;
use message::Message;

use consumer::{Consumer, ConsumerContext, EmptyConsumerContext};
use consumer::base_consumer::BaseConsumer;


/// A Consumer with an associated polling thread. This consumer doesn't need to
/// be polled and it will return all consumed messages as a `Stream`.
/// Due to the asynchronous nature of the stream, some messages might be consumed by the consumer
/// without being processed on the other end of the stream. If auto commit is used, it might cause
/// message loss after consumer restart. Manual offset commit should be used instead.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static> {
    consumer: Arc<BaseConsumer<C>>,
    paused: Arc<AtomicBool>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.consumer)
    }

    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer<C> {
        Arc::get_mut(&mut self.consumer).expect("Could not get mutable consumer from context")  // TODO add check?
    }
}

impl FromClientConfig for StreamConsumer<EmptyConsumerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer<EmptyConsumerContext>> {
        StreamConsumer::from_config_and_context(config, EmptyConsumerContext)
    }
}

/// Creates a new Consumer starting from a ClientConfig.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(try!(BaseConsumer::from_config_and_context(config, context))),
            paused: Arc::new(AtomicBool::new(false)),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
        };
        Ok(stream_consumer)
    }
}

/// A Stream of Kafka messages. It can be used to receive messages as they are received.
pub struct MessageStream {
    receiver: mpsc::Receiver<KafkaResult<Message>>,
}

impl MessageStream {
    fn new(receiver: mpsc::Receiver<KafkaResult<Message>>) -> MessageStream {
        MessageStream { receiver: receiver }
    }
}

impl Stream for MessageStream {
    type Item = KafkaResult<Message>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.receiver.poll()
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the StreamConsumer, returning a MessageStream.
    pub fn start(&mut self) -> MessageStream {
        let (sender, receiver) = mpsc::channel(0);
        let consumer = self.consumer.clone();
        let paused = self.paused.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("poll".to_string())
            .spawn(move || {
                poll_loop(consumer, sender, paused, should_stop);
            })
            .expect("Failed to start polling thread");
        self.handle = Some(handle);
        MessageStream::new(receiver)
    }

    /// Stops the StreamConsumer, blocking the caller until the internal consumer
    /// has been stopped.
    pub fn stop(&mut self) {
        if self.handle.is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match self.handle.take().expect("no handle present in consumer context").join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }

    /// Pause consuming of this consumer.
    pub fn pause(&mut self) {
        trace!("Pausing consumer");

        // Pause the base consumer
        self.consumer.pause();

        // Indicate we're paused and start a thread that
        // will poll while we're paused.
        self.paused.store(true, Ordering::Relaxed);
        let consumer = self.consumer.clone();
        let paused = self.paused.clone();
        thread::Builder::new()
            .name("pause_poll".to_string())
            .spawn(move || {
                pause_loop(consumer, paused);
            })
            .expect("Failed to start pause polling thread");
    }

    /// Resume consuming of this consumer.
    pub fn resume(&mut self) {
        trace!("Resuming consumer");
        // This will make the pause thread exit
        self.paused.store(false, Ordering::Relaxed);
        // Sleep for more than a tick of the loop to make sure
        // the pause loop does not get a result.
        thread::sleep(Duration::from_millis(150));
        // Resume the base consumer
        self.consumer.resume();
    }
}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy ConsumerPollingThread");
        self.stop();
    }
}

/// Internal consumer loop.
fn poll_loop<C: ConsumerContext>(
    consumer: Arc<BaseConsumer<C>>,
    sender: mpsc::Sender<KafkaResult<Message>>,
    paused: Arc<AtomicBool>,
    should_stop: Arc<AtomicBool>
) {
    trace!("Polling thread loop started");
    let mut curr_sender = sender;
    while !should_stop.load(Ordering::Relaxed) {
        // If we're paused also pause this loop
        while paused.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }

        trace!("Polling base consumer");
        let future_sender = match consumer.poll(100) {
            Ok(None) => continue,   // TODO: check stream closed
            Ok(Some(m)) => curr_sender.send(Ok(m)),
            Err(e) => curr_sender.send(Err(e)),
        };
        match future_sender.wait() {
            Ok(new_sender) => curr_sender = new_sender,
            Err(e) => {
                debug!("Sender not available: {:?}", e);
                break;
            }
        };
    }
    trace!("Polling thread loop terminated");
}

/// Internal loop that polls while the consumer is paused
fn pause_loop<C: ConsumerContext>(
    consumer: Arc<BaseConsumer<C>>,
    paused: Arc<AtomicBool>,
) {
    trace!("Pause polling loop started");
    while paused.load(Ordering::Relaxed) {
        match consumer.poll(100) {
            Ok(None) => continue,
            Ok(Some(_)) => panic!("Received result from poll while paused"),
            Err(e) => error!("Error polling while paused: {}", e)
        }
    }
    trace!("Pause polling thread loop terminated");
}
