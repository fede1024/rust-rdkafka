//! Stream-based consumer implementation.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::{ready, Stream};
use log::trace;
use tokio::time::{self, Duration, Instant};

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{ClientContext, NativeClient};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext, Rebalance};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::statistics::Statistics;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::{NativePtr, OnDrop, Timeout};

/// The [`ConsumerContext`] used by the [`StreamConsumer`]. This context will
/// automatically wake up the message stream when new data is available.
///
/// This type is not intended to be used directly.
pub struct StreamConsumerContext<C: ConsumerContext + 'static> {
    inner: C,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<C: ConsumerContext + 'static> StreamConsumerContext<C> {
    fn new(inner: C) -> StreamConsumerContext<C> {
        StreamConsumerContext {
            inner,
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

impl<C: ConsumerContext + 'static> ClientContext for StreamConsumerContext<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }

    fn stats(&self, statistics: Statistics) {
        self.inner.stats(statistics)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }
}

impl<C: ConsumerContext + 'static> ConsumerContext for StreamConsumerContext<C> {
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        self.inner.rebalance(native_client, err, tpl)
    }

    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.pre_rebalance(rebalance)
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.post_rebalance(rebalance)
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        self.inner.commit_callback(result, offsets)
    }

    fn main_queue_min_poll_interval(&self) -> Timeout {
        self.inner.main_queue_min_poll_interval()
    }

    fn message_queue_nonempty_callback(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
        self.inner.message_queue_nonempty_callback()
    }
}

/// A Kafka consumer implementing [`futures::Stream`].
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: &'a StreamConsumer<C>,
    err_interval: Option<Duration>,
    err_delay: Option<time::Delay>,
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    fn new(
        consumer: &'a StreamConsumer<C>,
        err_interval: Option<Duration>,
    ) -> MessageStream<'a, C> {
        MessageStream {
            consumer,
            err_interval,
            err_delay: None,
        }
    }

    fn context(&self) -> &StreamConsumerContext<C> {
        self.consumer.get_base_consumer().context()
    }

    fn client_ptr(&self) -> *mut RDKafka {
        self.consumer.client().native_ptr()
    }

    fn set_waker(&self, waker: Waker) {
        self.context().waker.lock().expect("lock poisoned").replace(waker);
    }

    fn poll(&self) -> Option<KafkaResult<BorrowedMessage<'a>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_poll(self.client_ptr(), 0))
                .map(|p| BorrowedMessage::from_consumer(p, self.consumer))
        }
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.poll() {
            None => {
                // The consumer queue is empty. Arrange to be notified when the
                // queue is no longer empty.
                self.set_waker(cx.waker().clone());
                if let Some(err_interval) = self.err_interval {
                    // We've been asked to periodically report that there are no
                    // new messages. Check if it's time to do so.
                    let mut err_delay = self.err_delay.get_or_insert_with(|| time::delay_for(err_interval));
                    ready!(Pin::new(&mut err_delay).poll(cx));
                    err_delay.reset(Instant::now() + err_interval);
                    Poll::Ready(Some(Err(KafkaError::NoMessageReceived)))
                } else {
                    Poll::Pending
                }
            }
            Some(message) => {
                self.err_delay = None;
                Poll::Ready(Some(message))
            }
        }
    }
}

/// A Kafka Consumer providing a [`futures::Stream`] interface.
///
/// This consumer doesn't need to be polled manually since
/// [`StreamConsumer::start`] will launch a background polling task.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    consumer: Arc<BaseConsumer<StreamConsumerContext<C>>>,
    should_stop: Arc<AtomicBool>,
}

impl<C: ConsumerContext> Consumer<StreamConsumerContext<C>> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<StreamConsumerContext<C>> {
        Arc::as_ref(&self.consumer)
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `StreamConsumer` starting from a [`ClientConfig`].
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let context = StreamConsumerContext::new(context);
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(BaseConsumer::from_config_and_context(config, context)?),
            should_stop: Arc::new(AtomicBool::new(false)),
        };
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the `StreamConsumer` with default configuration (100ms polling
    /// interval and no `NoMessageReceived` notifications).
    ///
    /// **Note:** this method must be called from within the context of a Tokio
    /// runtime.
    pub fn start(&self) -> MessageStream<C> {
        self.start_with(Duration::from_millis(100), false)
    }

    /// Starts the `StreamConsumer` with the specified poll interval.
    ///
    /// If `no_message_error` is set to true, the returned `MessageStream` will
    /// yield an error of type `KafkaError::NoMessageReceived` every time the
    /// poll interval is reached and no message has been received.
    pub fn start_with(&self, poll_interval: Duration, no_message_error: bool) -> MessageStream<C> {
        // TODO: verify called once
        tokio::spawn({
            let consumer = self.consumer.clone();
            let should_stop = self.should_stop.clone();
            async move {
                trace!("Polling task loop started");
                let _on_drop = OnDrop(|| trace!("Polling task loop terminated"));
                while !should_stop.load(Ordering::Relaxed) {
                    unsafe { rdsys::rd_kafka_poll(consumer.client().native_ptr(), 0) };
                    time::delay_for(poll_interval).await;
                }
            }
        });
        let no_message_interval = match no_message_error {
            true => Some(poll_interval),
            false => None,
        };
        MessageStream::new(self, no_message_interval)
    }

    /// Stops the `StreamConsumer`.
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
