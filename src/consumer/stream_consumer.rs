//! Stream-based consumer implementation.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::{ready, Stream};

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
#[cfg(feature = "tokio")]
use crate::util::TokioRuntime;
use crate::util::{AsyncRuntime, NativePtr, Timeout};

/// The [`ConsumerContext`] used by the [`StreamConsumer`]. This context will
/// automatically wake up the message stream when new data is available.
///
/// This type is not intended to be used directly. It will be automatically
/// created by the `StreamConsumer` when necessary.
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
    fn log(&self, id: &str, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(id, level, fac, log_message)
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
pub struct MessageStream<
    'a,
    C,
    // Ugly, but this provides backwards compatibility when the `tokio` feature
    // is enabled, as it is by default.
    #[cfg(feature = "tokio")] R = TokioRuntime,
    #[cfg(not(feature = "tokio"))] R,
> where
    C: ConsumerContext + 'static,
    R: AsyncRuntime,
{
    consumer: &'a StreamConsumer<C>,
    err_interval: Option<Duration>,
    err_delay: Option<R::Delay>,
}

impl<'a, C, R> MessageStream<'a, C, R>
where
    C: ConsumerContext + 'static,
    R: AsyncRuntime,
{
    fn new(
        consumer: &'a StreamConsumer<C>,
        err_interval: Option<Duration>,
    ) -> MessageStream<'a, C, R> {
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
        self.context()
            .waker
            .lock()
            .expect("lock poisoned")
            .replace(waker);
    }

    fn poll(&self) -> Option<KafkaResult<BorrowedMessage<'a>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_poll(self.client_ptr(), 0))
                .map(|p| BorrowedMessage::from_consumer(p, self.consumer))
        }
    }
}

impl<'a, C, R> Stream for MessageStream<'a, C, R>
where
    C: ConsumerContext + 'a,
    R: AsyncRuntime,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // Unconditionally store the waker so that we are woken up if the queue
        // flips from non-empty to empty. We have to store the waker on every
        // call to poll in case this future migrates between tasks. We also need
        // to store the waker *before* calling poll to avoid a race where `poll`
        // returns None to indicate that the queue is empty, but the queue
        // becomes non-empty before we've installed the waker.
        self.set_waker(cx.waker().clone());

        match self.poll() {
            None => match self.err_interval {
                None => Poll::Pending,
                Some(err_interval) => {
                    // We've been asked to periodically report that there are no
                    // new messages. Check if it's time to do so.
                    let mut err_delay = self
                        .err_delay
                        .get_or_insert_with(|| R::delay_for(err_interval));
                    ready!(Pin::new(&mut err_delay).poll(cx));
                    *err_delay = R::delay_for(err_interval);
                    Poll::Ready(Some(Err(KafkaError::NoMessageReceived)))
                }
            },
            Some(message) => {
                self.err_delay = None;
                Poll::Ready(Some(message))
            }
        }
    }
}

/// A Kafka consumer providing a [`futures::Stream`] interface.
///
/// This consumer doesn't need to be polled explicitly since `await`ing the
/// stream returned by [`StreamConsumer::start`] will implicitly poll the
/// consumer.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    consumer: Arc<BaseConsumer<StreamConsumerContext<C>>>,
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
        };
        unsafe {
            rdsys::rd_kafka_poll_set_consumer(stream_consumer.consumer.client().native_ptr())
        };
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the stream consumer with default configuration (100ms polling
    /// interval and no `NoMessageReceived` notifications).
    ///
    /// **Note:** this method must be called from within the context of a Tokio
    /// runtime.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub fn start(&self) -> MessageStream<C, TokioRuntime> {
        self.start_with(Duration::from_millis(100), false)
    }

    /// Starts the stream consumer with the specified poll interval.
    ///
    /// If `no_message_error` is set to true, the returned `MessageStream` will
    /// yield an error of type `KafkaError::NoMessageReceived` every time the
    /// poll interval is reached and no message has been received.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub fn start_with(
        &self,
        poll_interval: Duration,
        no_message_error: bool,
    ) -> MessageStream<C, TokioRuntime> {
        // TODO: verify called once
        self.start_with_runtime(poll_interval, no_message_error)
    }

    /// Like [`StreamConsumer::start_with`], but with a customizable
    /// asynchronous runtime.
    ///
    /// See the [`AsyncRuntime`] trait for the details on the interface the
    /// runtime must satisfy.
    pub fn start_with_runtime<R>(
        &self,
        poll_interval: Duration,
        no_message_error: bool,
    ) -> MessageStream<C, R>
    where
        R: AsyncRuntime,
    {
        let no_message_interval = match no_message_error {
            true => Some(poll_interval),
            false => None,
        };
        MessageStream::new(self, no_message_interval)
    }
}
