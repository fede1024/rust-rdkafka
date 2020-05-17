//! Stream-based consumer implementation.

use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rdkafka_sys::types::*;

use futures::stream::{self, Stream};
use futures::{ready, Future, FutureExt};

use std::sync::Mutex;

use crate::client::{ClientContext, NativeClient};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext, Rebalance};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::statistics::Statistics;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::Timeout;

/// The [`ConsumerContext`] used by the [`StreamConsumer`]. This context will
/// automatically wake up the message stream when new data is available.
///
/// This type is not intended to be used directly.
pub struct StreamConsumerContext<C: ConsumerContext> {
    inner: C,
    waker: Mutex<Option<Waker>>,
}

impl<C: ConsumerContext> StreamConsumerContext<C> {
    fn new(inner: C) -> StreamConsumerContext<C> {
        StreamConsumerContext {
            inner,
            waker: Mutex::new(None),
        }
    }
}

impl<C: ConsumerContext> ClientContext for StreamConsumerContext<C> {
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

impl<C: ConsumerContext> ConsumerContext for StreamConsumerContext<C> {
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
        self.waker.lock().unwrap().take().map(|w| w.wake());
        self.inner.message_queue_nonempty_callback()
    }
}

/// A Kafka Consumer providing a `futures::Stream` interface.
///
/// This consumer doesn't need to be polled manually since
/// [`StreamConsumer::start`] will launch a background polling task.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext = DefaultConsumerContext> {
    consumer: BaseConsumer<StreamConsumerContext<C>>,
}

impl<C: ConsumerContext> Consumer<StreamConsumerContext<C>> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<StreamConsumerContext<C>> {
        return &self.consumer;
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
        let c = StreamConsumerContext::new(context);
        let stream_consumer = StreamConsumer {
            consumer: BaseConsumer::from_config_and_context(config, c)?,
        };
        Ok(stream_consumer)
    }
}

/// Combinator, which forces stream `poll_next` call, if it
/// went Pending and didn't wake up before future returned
// by `make_timer` became ready. If stream had been polled due to heartbeat,
/// optionally produce KafkaError stream item passed in `err_on_heartbeat`
///
/// # Panics
/// If `make_timer` returns immediately ready future it can cause
/// deadlock on low traffic topics, so we panic.
fn heartbeat_poll<Fut, F, T, S: Stream<Item = KafkaResult<T>>>(
    make_timer: F,
    err_on_heartbeat: Option<KafkaError>,
    mut s: S,
) -> impl Stream<Item = S::Item>
where
    F: Fn() -> Fut,
    Fut: Future + Unpin,
{
    let mut timer: Option<Fut> = None;
    stream::poll_fn(move |cx| -> Poll<Option<_>> {
        // SAFETY: as per Pin docs we never move out of s
        let sp = unsafe { Pin::new_unchecked(&mut s) };
        match sp.poll_next(cx) {
            r @ Poll::Ready(_) => {
                timer.take(); //disarm timer
                r
            }
            _ => {
                ready!(timer.get_or_insert_with(|| make_timer()).poll_unpin(cx));
                // we are here only if timer fired. Arm new timer.
                #[allow(unused_must_use)]
                { timer.replace(make_timer()).as_mut().unwrap().poll_unpin(cx); }
                match &err_on_heartbeat {
                    Some(err) => Poll::Ready(Some(Err(err.clone()))),
                    _ => Poll::Pending,
                }
            }
        }
    })
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Creates Stream with default params. Requires
    /// tokio feature to be enabled.
    #[cfg(feature = "tokio")]
    pub fn start(&self) -> impl Stream<Item = StreamItem> {
        self.start_with(Duration::from_secs(1), false)
    }

    /// Starts the Stream. Additionally, if `no_message_error`
    /// is set to true, it will return an error of type
    /// `KafkaError::NoMessageReceived` every
    ///  `empty_queue_interval` when no message has
    /// been received.
    #[cfg(feature = "tokio")]
    pub fn start_with(
        &self,
        empty_queue_interval: Duration,
        no_message_error: bool,
    ) -> impl Stream<Item = StreamItem> {
        self.start_with_heartbeat(
            move || tokio::time::delay_for(empty_queue_interval),
            no_message_error,
        )
    }

    /// Returns stream. Underneath kafka client is polled at least as often
    /// as futures produced by `f` become ready.
    pub fn start_with_heartbeat<Fut, F>(
        &self,
        f: F,
        no_message_error: bool,
    ) -> impl Stream<Item = StreamItem>
    where
        F: Fn() -> Fut,
        Fut: Future + Unpin,
    {
        let err_on_heartbeat = if no_message_error {
            Some(KafkaError::NoMessageReceived)
        } else {
            None
        };
        heartbeat_poll(f, err_on_heartbeat, MessageStream(self))
    }
}

type StreamItem<'a> = KafkaResult<BorrowedMessage<'a>>;
struct MessageStream<'a, C: ConsumerContext>(&'a StreamConsumer<C>);
impl<'a, C: ConsumerContext> Stream for MessageStream<'a, C> {
    type Item = StreamItem<'a>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(m_ptr) = self
            .0
            .consumer
            .poll_raw(Timeout::After(Duration::from_secs(0)))
        {
            let msg = unsafe { BorrowedMessage::from_consumer(m_ptr, &self.0.consumer) };
            return Poll::Ready(Some(msg));
        }

        // Queue is empty, need to arrange our awakening
        {
            let mut waker = self.0.consumer.context().waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
        }

        // While doing previous step, it is possible that old `self.waker` was
        // notified, we don't want to miss this notification
        // so we poll again

        match self
            .0
            .consumer
            .poll_raw(Timeout::After(Duration::from_secs(0)))
        {
            None => {
                // Nope, still nothing, returning Pening as planned
                return Poll::Pending;
            }
            Some(m_ptr) => {
                // There was indeed a notification race, which we've caught
                let msg = unsafe { BorrowedMessage::from_consumer(m_ptr, &self.0.consumer) };
                return Poll::Ready(Some(msg));
            }
        };
    }
}
