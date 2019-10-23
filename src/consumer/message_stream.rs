use crate::rdsys;
use crate::rdsys::types::*;
use futures::Stream;

use crate::consumer::{ConsumerContext, BaseConsumer};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::util::duration_to_millis;

use log::*;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_executor::blocking::{run as block_on, Blocking};

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
    fn into_message<'a>(
        mut self,
    ) -> KafkaResult<BorrowedMessage<'a>> {
        let msg = unsafe { BorrowedMessage::from_consumer(self.message_ptr) };
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

// A Kafka consumer implementing Stream.
///
/// It can be used to receive messages as they are consumed from Kafka. Note: there might be
/// buffering between the actual Kafka consumer and the receiving end of this stream, so it is not
/// advised to use automatic commit, as some messages might have been consumed by the internal Kafka
/// consumer but not processed. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
#[pin_project]
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
    poll_interval_ms: i32,
    send_none: bool,
    #[pin]
    pending: Option<Blocking<PollConsumerResult>>,
    phantom: &'a std::marker::PhantomData<C>,
}

enum PollConsumerResult {
    Continue,
    Ready(Option<PolledMessagePtr>),
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    /// Create a new message stream from a base consumer
    pub fn new(
        consumer: Arc<BaseConsumer<C>>,
        should_stop: Arc<AtomicBool>,
        poll_interval: Duration,
        send_none: bool,
    ) -> Self {
        let poll_interval_ms = duration_to_millis(poll_interval) as i32;
        Self {
            consumer: consumer,
            should_stop: should_stop,
            poll_interval_ms: poll_interval_ms,
            send_none: send_none,
            pending: None,
            phantom: &std::marker::PhantomData,
        }
    }

    /// Close the message stream
    pub async fn close(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut pending: Pin<&mut Option<Blocking<PollConsumerResult>>> = this.pending.as_mut();
        debug!("Polling stream for next message");
        loop {
            if this.should_stop.load(Ordering::Relaxed) {
                info!("Stopping");
                return Poll::Ready(None);
            } else {
                if let Some(to_poll) = pending.as_mut().as_pin_mut() {
                    debug!("Seeing if poll result is ready");
                    if let PollConsumerResult::Ready(res) = futures::ready!(to_poll.poll(cx)) {
                        debug!("Poll result ready");
                        pending.set(None);
                        let ret = match res {
                            None => {
                                debug!("No message polled, but forwarding none as requested");
                                Poll::Ready(Some(Err(KafkaError::NoMessageReceived)))
                            },
                            Some(polled_ptr) => Poll::Ready(Some(polled_ptr.into_message())),
                        };
                        return ret;
                    }
                }
                debug!("Requesting next poll result");
                let consumer = Arc::clone(&this.consumer);
                let poll_interval_ms = *this.poll_interval_ms;
                let send_none = *this.send_none;
                let f = block_on(move || {
                    match consumer.poll_raw(poll_interval_ms) {
                        None => {
                            if send_none {
                                PollConsumerResult::Ready(None)
                            } else {
                                PollConsumerResult::Continue
                            }
                        }
                        Some(m_ptr) => {
                            PollConsumerResult::Ready(Some(PolledMessagePtr::new(m_ptr)))
                        },
                    }
                });
                pending.replace(f);
            }
        }
    }
}