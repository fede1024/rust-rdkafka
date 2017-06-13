//! Stream-based consumer implementation.
use futures::{Future, Poll, Sink, Stream};
use futures::sync::mpsc;
use rdsys::types::*;
use rdsys;

use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::base_consumer::BaseConsumer;
use consumer::{Consumer, ConsumerContext, EmptyConsumerContext};
use error::{KafkaError, KafkaResult};
use message::Message;
use util::duration_to_millis;

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;


/// A small wrapper for a message pointer. This wrapper is only used to
/// pass a message between the polling thread and the thread consuming the stream,
/// and transform it from pointer to `Message` with a lifetime that derives from the
/// lifetime of the stream consumer. In general is not safe to pass a struct with an internal
/// reference across threads. However the `StreamConsumer` guarantees that the polling thread
/// will be terminated before the consumer is actually dropped, ensuring that the messages
/// are safe to be used for their entire lifetime.
struct PolledMessagePtr {
    message_ptr: Option<*mut RDKafkaMessage>,
}

impl PolledMessagePtr {
    /// Creates a new PolledPtr from a message pointer.
    fn new(message_ptr: *mut RDKafkaMessage) -> PolledMessagePtr {
        trace!("New polled ptr {:?}", message_ptr);
        PolledMessagePtr {
            message_ptr: Some(message_ptr)
        }
    }

    /// Transforms the `PolledMessagePtr` into a message, that will be bound to the lifetime
    /// of the provided consumer.
    fn into_message_of<'a, C: ConsumerContext>(mut self, consumer: &'a StreamConsumer<C>) -> Message<'a> {
        Message::new(self.message_ptr.take().unwrap(), consumer)
    }
}

impl Drop for PolledMessagePtr {
    /// If the `PolledMessagePtr` is hasn't been transformed into a message and the pointer is
    /// still available, it will free the underlying resources.
    fn drop(&mut self) {
        if let Some(ptr) = self.message_ptr {
            trace!("Destroy PolledPtr {:?}", ptr);
            unsafe { rdsys::rd_kafka_message_destroy(ptr) };
        }
    }
}

/// Allow message pointer to be moved across threads.
unsafe impl Send for PolledMessagePtr {}


/// A Stream of Kafka messages. It can be used to receive messages as they are consumed from Kafka.
/// Note: there might be buffering between the actual Kafka consumer and the receiving end of this
/// stream, so it is not advised to use automatic commit, as some messages might have been consumed
/// by the internal Kafka consumer but not processed.
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: &'a StreamConsumer<C>,
    receiver: mpsc::Receiver<KafkaResult<PolledMessagePtr>>,
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    fn new(consumer: &'a StreamConsumer<C>, receiver: mpsc::Receiver<KafkaResult<PolledMessagePtr>>) -> MessageStream<'a, C> {
        MessageStream {
            consumer: consumer,
            receiver: receiver,
        }
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<Message<'a>>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.receiver.poll()  // There has to be a better way
            .map(|ready|
                ready.map(|option|
                    option.map(|result|
                        result.map(|polled_ptr| polled_ptr.into_message_of(self.consumer)))))
    }
}

/// Internal consumer loop. This is the main body of the thread that will drive the
/// stream consumer.
fn poll_loop<C: ConsumerContext>(
    consumer: Arc<BaseConsumer<C>>,
    sender: mpsc::Sender<KafkaResult<PolledMessagePtr>>,
    should_stop: Arc<AtomicBool>,
    poll_interval: Duration,
    no_message_error: bool,
) {
    trace!("Polling thread loop started");
    let mut curr_sender = sender;
    let poll_interval_ms = duration_to_millis(poll_interval) as i32;
    while !should_stop.load(Ordering::Relaxed) {
        trace!("Polling base consumer");
        let future_sender = match consumer.poll_raw(poll_interval_ms) {
            Ok(None) => {
                if no_message_error {
                    curr_sender.send(Err(KafkaError::NoMessageReceived))
                } else {
                    continue // TODO: check stream closed
                }
            },
            Ok(Some(m_ptr)) => curr_sender.send(Ok(PolledMessagePtr::new(m_ptr))),
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

/// A Consumer with an associated polling thread. This consumer doesn't need to
/// be polled and it will return all consumed messages as a `Stream`.
/// Due to the asynchronous nature of the stream, some messages might be consumed by the consumer
/// without being processed on the other end of the stream. If auto commit is used, it might cause
/// message loss after consumer restart. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static> {
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
    handle: Cell<Option<JoinHandle<()>>>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.consumer)
    }
}

impl FromClientConfig for StreamConsumer<EmptyConsumerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer<EmptyConsumerContext>> {
        StreamConsumer::from_config_and_context(config, EmptyConsumerContext)
    }
}

/// Creates a new `Consumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(BaseConsumer::from_config_and_context(config, context)?),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: Cell::new(None),
        };
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the StreamConsumer with default configuration (100ms polling interval and no
    /// `NoMessageReceived` notifications).
    pub fn start(&self) -> MessageStream<C> {
        self.start_with(Duration::from_millis(100), false)
    }

    /// Starts the StreamConsumer with the specified poll interval. Additionally, if
    /// `no_message_error` is set to true, it will return an error of type
    /// `KafkaError::NoMessageReceived` every time the poll interval is reached and no message
    /// has been received.
    pub fn start_with(&self, poll_interval: Duration, no_message_error: bool) -> MessageStream<C> {
        let (sender, receiver) = mpsc::channel(0);
        let consumer = self.consumer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("poll".to_string())
            .spawn(move || {
                poll_loop(consumer, sender, should_stop, poll_interval, no_message_error);
            })
            .expect("Failed to start polling thread");
        self.handle.set(Some(handle));
        MessageStream::new(self, receiver)
    }

    /// Stops the StreamConsumer, blocking the caller until the internal consumer
    /// has been stopped.
    pub fn stop(&self) {
        if let Some(handle) = self.handle.take() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            match handle.join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }
}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy StreamConsumer");
        // The polling thread must be fully stopped before we can proceed with the actual drop,
        // otherwise it might consume from a destroyed consumer.
        self.stop();
    }
}

