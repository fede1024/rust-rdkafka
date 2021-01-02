//! Stream-based consumer implementation.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::{ready, Stream};
use slab::Slab;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{ClientContext, NativeClient};
use crate::config::{ClientConfig, FromClientConfig, RDKafkaLogLevel};
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
    wakers: Arc<Mutex<Slab<Option<Waker>>>>,
}

impl<C: ConsumerContext + 'static> StreamConsumerContext<C> {
    fn new(inner: C) -> StreamConsumerContext<C> {
        StreamConsumerContext {
            inner,
            wakers: Arc::new(Mutex::new(Slab::new())),
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
        let mut wakers = self.wakers.lock().unwrap();
        for (_, waker) in wakers.iter_mut() {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
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
    interval: Duration,
    delay: Pin<Box<Option<R::Delay>>>,
    slot: usize,
}

impl<'a, C, R> MessageStream<'a, C, R>
where
    C: ConsumerContext + 'static,
    R: AsyncRuntime,
{
    fn new(consumer: &'a StreamConsumer<C>, interval: Duration) -> MessageStream<'a, C, R> {
        let slot = {
            let context = consumer.get_base_consumer().context();
            let mut wakers = context.wakers.lock().expect("lock poisoned");
            wakers.insert(None)
        };
        MessageStream {
            consumer,
            interval,
            delay: Box::pin(None),
            slot,
        }
    }

    fn context(&self) -> &StreamConsumerContext<C> {
        self.consumer.get_base_consumer().context()
    }

    fn client_ptr(&self) -> *mut RDKafka {
        self.consumer.client().native_ptr()
    }

    fn set_waker(&self, waker: Waker) {
        let mut wakers = self.context().wakers.lock().expect("lock poisoned");
        wakers[self.slot].replace(waker);
    }

    fn poll(&self) -> Option<KafkaResult<BorrowedMessage<'a>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_poll(self.client_ptr(), 0))
                .map(|p| BorrowedMessage::from_consumer(p, self.consumer))
        }
    }

    // SAFETY: All access to `self.delay` occurs via the following two
    // functions. These functions are careful to never move out of `self.delay`.
    // (They can *drop* the future stored in `self.delay`, but that is
    // permitted.) They never return a non-pinned pointer to the contents of
    // `self.delay`.

    fn ensure_delay(&mut self, delay: R::Delay) -> Pin<&mut R::Delay> {
        unsafe { Pin::new_unchecked(self.delay.as_mut().get_unchecked_mut().get_or_insert(delay)) }
    }

    fn clear_delay(&mut self) {
        unsafe { *self.delay.as_mut().get_unchecked_mut() = None }
    }
}

impl<'a, C, R> Stream for MessageStream<'a, C, R>
where
    C: ConsumerContext + 'a,
    R: AsyncRuntime,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Unconditionally store the waker so that we are woken up if the queue
        // flips from non-empty to empty. We have to store the waker on every
        // call to poll in case this future migrates between tasks. We also need
        // to store the waker *before* calling poll to avoid a race where `poll`
        // returns None to indicate that the queue is empty, but the queue
        // becomes non-empty before we've installed the waker.
        self.set_waker(cx.waker().clone());

        match self.poll() {
            None => loop {
                let delay = R::delay_for(self.interval);
                ready!(self.ensure_delay(delay).poll(cx));
                self.clear_delay();
            },
            Some(message) => {
                self.clear_delay();
                Poll::Ready(Some(message))
            }
        }
    }
}

impl<'a, C, R> Drop for MessageStream<'a, C, R>
where
    C: ConsumerContext + 'static,
    R: AsyncRuntime,
{
    fn drop(&mut self) {
        let mut wakers = self.context().wakers.lock().expect("lock poisoned");
        wakers.remove(self.slot);
    }
}

/// A Kafka consumer providing a [`futures::Stream`] interface.
///
/// This consumer doesn't need to be polled explicitly since `await`ing the
/// stream returned by [`StreamConsumer::start`] will implicitly poll the
/// consumer.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    consumer: BaseConsumer<StreamConsumerContext<C>>,
}

impl<C: ConsumerContext> Consumer<StreamConsumerContext<C>> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<StreamConsumerContext<C>> {
        &self.consumer
    }
}

impl<C> FromClientConfig<C> for StreamConsumer<C>
where
    C: ConsumerContext,
{
    fn from_client_config(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let context = StreamConsumerContext::new(context);
        let stream_consumer = StreamConsumer {
            consumer: BaseConsumer::from_client_config(config, context)?,
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
    pub fn start(&self) -> MessageStream<'_, C, TokioRuntime> {
        self.start_with(Duration::from_millis(100))
    }

    /// Starts the stream consumer with the specified poll interval.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub fn start_with(&self, poll_interval: Duration) -> MessageStream<'_, C, TokioRuntime> {
        // TODO: verify called once
        self.start_with_runtime(poll_interval)
    }

    /// Like [`StreamConsumer::start_with`], but with a customizable
    /// asynchronous runtime.
    ///
    /// See the [`AsyncRuntime`] trait for the details on the interface the
    /// runtime must satisfy.
    pub fn start_with_runtime<R>(&self, poll_interval: Duration) -> MessageStream<'_, C, R>
    where
        R: AsyncRuntime,
    {
        MessageStream::new(self, poll_interval)
    }
}
