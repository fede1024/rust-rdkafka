//! Stream-based consumer implementation.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::Stream;

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
use crate::util::{NativePtr, Timeout};

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
    fn context(&self) -> &StreamConsumerContext<C> {
        self.consumer.get_base_consumer().context()
    }

    fn client_ptr(&self) -> *mut RDKafka {
        self.consumer.client().native_ptr()
    }

    fn poll<'a>(&'a self) -> Option<KafkaResult<BorrowedMessage<'a>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_poll(self.client_ptr(), 0))
                .map(|p| BorrowedMessage::from_consumer(p, self))
        }
    }

    fn set_waker(&self, waker: Waker) {
        self.context()
            .waker
            .lock()
            .expect("lock poisoned")
            .replace(waker);
    }
}

impl<'a, C: ConsumerContext> Stream for &'a StreamConsumer<C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Unconditionally store the waker so that we are woken up if the queue
        // flips from non-empty to empty. We have to store the waker on every
        // call to poll in case this future migrates between tasks. We also need
        // to store the waker *before* calling poll to avoid a race where `poll`
        // returns None to indicate that the queue is empty, but the queue
        // becomes non-empty before we've installed the waker.
        self.set_waker(cx.waker().clone());

        match self.poll() {
            None => Poll::Pending,
            Some(message) => Poll::Ready(Some(message)),
        }
    }
}
