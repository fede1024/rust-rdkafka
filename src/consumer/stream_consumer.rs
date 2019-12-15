//! Stream-based consumer implementation.
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use log::*;
use pin_project::pin_project;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;

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
#[pin_project]
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: &'a StreamConsumer<C>,
    poll_interval: Duration,
    send_none: bool,
    should_stop: Arc<AtomicBool>,
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    fn new(
        consumer: &'a StreamConsumer<C>,
        poll_interval: Duration,
        send_none: bool,
        should_stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            consumer: consumer,
            poll_interval: poll_interval,
            send_none: send_none,
            should_stop: should_stop,
        }
    }
}

impl<'a, C: ConsumerContext> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        debug!("Polling stream for next message");
        loop {
            if this.should_stop.load(Ordering::Relaxed) {
                info!("Stopping");
                return Poll::Ready(None);
            } else {
                let timeout = crate::util::Timeout::After(*this.poll_interval);
                let res = this.consumer.consumer.poll(timeout);
                let opt_ret = match res {
                    None => {
                        if *this.send_none {
                            debug!("No message polled, but forwarding none as requested");
                            Some(Poll::Ready(Some(Err(KafkaError::NoMessageReceived))))
                        } else {
                            None
                        }
                    },
                    Some(Err(e)) => {
                        Some(Poll::Ready(Some(Err(e))))
                    }
                    Some(Ok(msg)) => {
                        Some(Poll::Ready(Some(Ok(msg))))
                    },
                };
                if let Some(ret) = opt_ret {
                    return ret;
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
        MessageStream::new(
            self,
            poll_interval,
            no_message_error,
            self.should_stop.clone(),
        )
    }

    /// Stops the MessageStream
    pub async fn stop(&self) {
        trace!("Stopping polling");
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy StreamConsumer");
        // The polling thread must be fully stopped before we can proceed with the actual drop,
        // otherwise it might consume from a destroyed consumer.
        self.should_stop.store(true, Ordering::Relaxed);
    }
}
