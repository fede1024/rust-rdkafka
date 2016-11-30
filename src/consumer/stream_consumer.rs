//! Consumer implementations.
extern crate rdkafka_sys as rdkafka;
extern crate futures;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;

use self::futures::Future;
use self::futures::stream;
use self::futures::stream::{Receiver, Sender};

use client::{Context};
use config::{FromClientConfig, ClientConfig};
use error::{KafkaError, KafkaResult};
use message::Message;

use consumer::Consumer;
use consumer::base_consumer::BaseConsumer;


/// A Consumer with an associated polling thread. This consumer doesn't need to
/// be polled and it will return all consumed messages as a `Stream`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: Context + 'static> {
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<C: Context> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.consumer)
    }

    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer<C> {
        Arc::get_mut(&mut self.consumer).unwrap()  // TODO add check?
    }
}

/// Creates a new Consumer starting from a ClientConfig.
impl<C: Context> FromClientConfig<C> for StreamConsumer<C> {
    fn from_config(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(try!(BaseConsumer::from_config(config, context))),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
        };
        Ok(stream_consumer)
    }
}

impl<C: Context> StreamConsumer<C> {
    /// Starts the StreamConsumer, returning a Stream.
    pub fn start(&mut self) -> Receiver<Message, KafkaError> {
        let (sender, receiver) = stream::channel();
        let consumer = self.consumer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("poll".to_string())
            .spawn(move || {
                poll_loop(consumer, sender, should_stop);
            })
            .expect("Failed to start polling thread");
        self.handle = Some(handle);
        receiver
    }

    /// Stops the StreamConsumer. It blocks until the internal consumer
    /// has been stopped.
    pub fn stop(&mut self) {
        if self.handle.is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match self.handle.take().unwrap().join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }
}

impl<C: Context> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy ConsumerPollingThread");
        self.stop();
    }
}

fn poll_loop<C: Context>(consumer: Arc<BaseConsumer<C>>, sender: Sender<Message, KafkaError>, should_stop: Arc<AtomicBool>) {
    trace!("Polling thread loop started");
    let mut curr_sender = sender;
    while !should_stop.load(Ordering::Relaxed) {
        let future_sender = match consumer.poll(100) {
            Ok(None) => continue,
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
