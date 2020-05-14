//! Stream-based consumer implementation.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use log::{debug, trace};
use tokio::time::{self, Duration};

use rdkafka_sys::types::*;

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::util::{NativePtr, OnDrop, Timeout};

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
    message_ptr: NativePtr<RDKafkaMessage>,
}

impl PolledMessagePtr {
    /// Transforms the `PolledMessagePtr` into a message whose lifetime will be bound to the
    /// lifetime of the provided consumer. If the librdkafka message represents an error, the error
    /// will be returned instead.
    fn into_message_of<C: ConsumerContext>(
        self,
        consumer: &StreamConsumer<C>,
    ) -> KafkaResult<BorrowedMessage> {
        let msg = unsafe { BorrowedMessage::from_consumer(self.message_ptr, consumer) };
        msg
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
        self.receiver.poll_next_unpin(cx).map(|ready| {
            ready.map(|polled_ptr_opt| {
                polled_ptr_opt.map_or(Err(KafkaError::NoMessageReceived), |polled_ptr| {
                    polled_ptr.into_message_of(self.consumer)
                })
            })
        })
    }
}

/// Internal consumer loop. This is the main body of the thread that will drive the stream consumer.
/// If `send_none` is true, the loop will send a None into the sender every time the poll times out.
async fn poll_loop<C: ConsumerContext>(
    consumer: Arc<BaseConsumer<C>>,
    mut sender: mpsc::Sender<Option<PolledMessagePtr>>,
    should_stop: Arc<AtomicBool>,
    poll_interval: Duration,
    send_none: bool,
) {
    trace!("Polling task loop started");
    let _on_drop = OnDrop(|| trace!("Polling task loop terminated"));
    while !should_stop.load(Ordering::Relaxed) {
        let backoff = time::delay_for(poll_interval);
        let message = consumer
            .poll_raw(Timeout::After(Duration::new(0, 0)))
            .map(|message_ptr| PolledMessagePtr { message_ptr });
        match message {
            None => {
                if send_none {
                    if let Err(e) = sender.send(None).await {
                        debug!("Sender not available: {:?}", e);
                        break;
                    }
                }
                backoff.await;
            }
            Some(msg) => {
                if let Err(e) = sender.send(Some(msg)).await {
                    debug!("Sender not available: {:?}", e);
                }
            }
        }
    }
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
        };
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the StreamConsumer with default configuration (100ms polling
    /// interval and no `NoMessageReceived` notifications).
    ///
    /// **Note:** this method must be called from within the context of a Tokio
    /// runtime.
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
        tokio::spawn(poll_loop(
            self.consumer.clone(),
            sender,
            self.should_stop.clone(),
            poll_interval,
            no_message_error,
        ));
        MessageStream::new(self, receiver)
    }

    /// Stops the StreamConsumer, blocking the caller until the internal consumer has been stopped.
    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy StreamConsumer");
        self.stop();
    }
}
