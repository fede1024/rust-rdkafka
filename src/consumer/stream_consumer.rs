//! Stream-based consumer implementation.
use crate::rdsys;
use crate::rdsys::types::*;
use futures::channel::mpsc;
use futures::executor::block_on;
use futures::{Poll, SinkExt, Stream, StreamExt};

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::util::duration_to_millis;

use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Default channel size for the stream consumer. The number of context switches
/// seems to decrease exponentially as the channel size is increased, and it stabilizes when
/// the channel size reaches 10 or so.
const CONSUMER_CHANNEL_SIZE: usize = 10;

/// A small wrapper for a message pointer. This wrapper is only used to
/// pass a message between the polling thread and the thread consuming the stream,
/// and to transform it from pointer to `BorrowedMessage` with a lifetime that derives from the
/// lifetime of the stream consumer. In general is not safe to pass a struct with an internal
/// reference across threads. However the `StreamConsumer` guarantees that the polling thread
/// is terminated before the consumer is actually dropped, ensuring that the messages
/// are safe to be used for their entire lifetime.
struct PolledMessagePtr {
    message_ptr: *mut RDKafkaMessage,
}

impl PolledMessagePtr {
    /// Creates a new PolledPtr from a message pointer. It takes the ownership of the message.
    fn new(message_ptr: *mut RDKafkaMessage) -> PolledMessagePtr {
        trace!("New polled ptr {:?}", message_ptr);
        PolledMessagePtr { message_ptr }
    }

    /// Transforms the `PolledMessagePtr` into a message whose lifetime will be bound to the
    /// lifetime of the provided consumer. If the librdkafka message represents an error, the error
    /// will be returned instead.
    fn into_message_of<C: ConsumerContext>(
        mut self,
        consumer: &StreamConsumer<C>,
    ) -> KafkaResult<BorrowedMessage> {
        let msg = unsafe { BorrowedMessage::from_consumer(self.message_ptr, consumer) };
        self.message_ptr = ptr::null_mut();
        msg
    }
}

impl Drop for PolledMessagePtr {
    /// If the `PolledMessagePtr` is hasn't been transformed into a message and the pointer is
    /// still available, it will free the underlying resources.
    fn drop(&mut self) {
        if !self.message_ptr.is_null() {
            trace!("Destroy PolledPtr {:?}", self.message_ptr);
            unsafe { rdsys::rd_kafka_message_destroy(self.message_ptr) };
        }
    }
}

/// Allow message pointer to be moved across threads.
unsafe impl Send for PolledMessagePtr {}

/// A Kafka consumer implementing Stream.
///
/// It can be used to receive messages as they are consumed from Kafka. Note: there might be
/// buffering between the actual Kafka consumer and the receiving end of this stream, so it is not
/// advised to use automatic commit, as some messages might have been consumed by the internal Kafka
/// consumer but not processed. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: &'a StreamConsumer<C>,
    receiver: mpsc::Receiver<Option<PolledMessagePtr>>,
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    fn new(
        consumer: &'a StreamConsumer<C>,
        receiver: mpsc::Receiver<Option<PolledMessagePtr>>,
    ) -> MessageStream<'a, C> {
        MessageStream { consumer, receiver }
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.receiver
            .poll_next_unpin(cx)
            .map(|ready: Option<Option<PolledMessagePtr>>| {
                ready.map(|polled_ptr_opt: Option<PolledMessagePtr>| {
                    polled_ptr_opt.map_or(
                        Err(KafkaError::NoMessageReceived),
                        |polled_ptr: PolledMessagePtr| polled_ptr.into_message_of(self.consumer),
                    )
                })
            })
    }
}

/// Internal consumer loop. This is the main body of the thread that will drive the stream consumer.
/// If `send_none` is true, the loop will send a None into the sender every time the poll times out.
async fn poll_loop<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    sender: mpsc::Sender<Option<PolledMessagePtr>>,
    should_stop: &AtomicBool,
    poll_interval: Duration,
    send_none: bool,
) {
    trace!("Polling thread loop started");
    let mut curr_sender = sender;
    let poll_interval_ms = duration_to_millis(poll_interval) as i32;
    while !should_stop.load(Ordering::Relaxed) {
        trace!("Polling base consumer");
        let future_sender = match consumer.poll_raw(poll_interval_ms) {
            None => {
                if send_none {
                    curr_sender.send(None)
                } else {
                    continue; // TODO: check stream closed
                }
            }
            Some(m_ptr) => curr_sender.send(Some(PolledMessagePtr::new(m_ptr))),
        };
        match future_sender.await {
            Ok(_) => {},
            Err(e) => {
                debug!("Sender not available: {:?}", e);
                break;
            }
        };
    }
    trace!("Polling thread loop terminated");
}

/// A Kafka Consumer providing a `futures::Stream` interface.
///
/// This consumer doesn't need to be polled since it has a separate polling thread. Due to the
/// asynchronous nature of the stream, some messages might be consumed by the consumer without being
/// processed on the other end of the stream. If auto commit is used, it might cause message loss
/// after consumer restart. Manual offset storing should be used, see the `store_offset` function on
/// `Consumer`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.consumer)
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(BaseConsumer::from_config_and_context(config, context)?),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
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
    /// `KafkaError::NoMessageReceived` every time the poll interval is reached and no message has
    /// been received.
    pub fn start_with(&self, poll_interval: Duration, no_message_error: bool) -> MessageStream<C> {
        // TODO: verify called once
        let (sender, receiver) = mpsc::channel(CONSUMER_CHANNEL_SIZE);
        let consumer = self.consumer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("poll".to_string())
            .spawn(move || {
                block_on(poll_loop(
                    consumer.as_ref(),
                    sender,
                    should_stop.as_ref(),
                    poll_interval,
                    no_message_error,
                ));
            })
            .expect("Failed to start polling thread");
        *self.handle.lock().unwrap() = Some(handle);
        MessageStream::new(self, receiver)
    }

    /// Stops the StreamConsumer, blocking the caller until the internal consumer has been stopped.
    pub fn stop(&self) {
        let mut handle = self.handle.lock().unwrap();
        if let Some(handle) = handle.take() {
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
