//! High-level consumers with a [`Stream`](futures::Stream) interface.

use std::marker::PhantomData;
use std::os::raw::c_void;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::select;
use futures::stream::{Stream, StreamExt};
use log::trace;
use slab::Slab;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{Client, NativeQueue};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{
    CommitMode, Consumer, ConsumerContext, ConsumerGroupMetadata, DefaultConsumerContext,
    RebalanceProtocol,
};
use crate::error::{KafkaError, KafkaResult};
use crate::groups::GroupList;
use crate::message::BorrowedMessage;
use crate::metadata::Metadata;
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::util::{AsyncRuntime, DefaultRuntime, NativePtr, Timeout};

unsafe extern "C" fn native_message_queue_nonempty_cb(_: *mut RDKafka, opaque_ptr: *mut c_void) {
    let wakers = &*(opaque_ptr as *const WakerSlab);
    wakers.wake_all();
}

unsafe fn enable_nonempty_callback(queue: &NativeQueue, wakers: &Arc<WakerSlab>) {
    rdsys::rd_kafka_queue_cb_event_enable(
        queue.ptr(),
        Some(native_message_queue_nonempty_cb),
        Arc::as_ptr(wakers) as *mut c_void,
    )
}

struct WakerSlab {
    wakers: Mutex<Slab<Option<Waker>>>,
}

impl WakerSlab {
    fn new() -> WakerSlab {
        WakerSlab {
            wakers: Mutex::new(Slab::new()),
        }
    }

    fn wake_all(&self) {
        let mut wakers = self.wakers.lock().unwrap();
        for (_, waker) in wakers.iter_mut() {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
    }

    fn register(&self) -> usize {
        let mut wakers = self.wakers.lock().expect("lock poisoned");
        wakers.insert(None)
    }

    fn unregister(&self, slot: usize) {
        let mut wakers = self.wakers.lock().expect("lock poisoned");
        wakers.remove(slot);
    }

    fn set_waker(&self, slot: usize, waker: Waker) {
        let mut wakers = self.wakers.lock().expect("lock poisoned");
        wakers[slot] = Some(waker);
    }
}

/// A stream of messages from a [`StreamConsumer`].
///
/// See the documentation of [`StreamConsumer::stream`] for details.
pub struct MessageStream<'a, C, R = DefaultRuntime>
where
    C: ConsumerContext + 'static,
{
    consumer: &'a StreamConsumer<C, R>,
    slot: usize,
}

impl<'a, C, R> MessageStream<'a, C, R>
where
    C: ConsumerContext + 'static,
{
    fn new(consumer: &'a StreamConsumer<C, R>) -> MessageStream<'a, C, R> {
        let slot = consumer.wakers.register();
        MessageStream { consumer, slot }
    }

    fn poll(&self) -> Option<KafkaResult<BorrowedMessage<'a>>> {
        let client_ptr = self.consumer.client().native_ptr();
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_poll(client_ptr, 0))
                .map(|p| BorrowedMessage::from_consumer(p, self.consumer))
        }
    }
}

impl<'a, C, R> Stream for MessageStream<'a, C, R>
where
    C: ConsumerContext + 'a,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If there is a message ready, yield it immediately to avoid the
        // taking the lock in `self.set_waker`.
        if let Some(message) = self.poll() {
            return Poll::Ready(Some(message));
        }

        // Otherwise, we need to wait for a message to become available. Store
        // the waker so that we are woken up if the queue flips from non-empty
        // to empty. We have to store the waker repatedly in case this future
        // migrates between tasks.
        self.consumer
            .wakers
            .set_waker(self.slot, cx.waker().clone());

        // Check whether a new message became available after we installed the
        // waker. This avoids a race where `poll` returns None to indicate that
        // the queue is empty, but the queue becomes non-empty before we've
        // installed the waker.
        match self.poll() {
            None => Poll::Pending,
            Some(message) => Poll::Ready(Some(message)),
        }
    }
}

impl<'a, C, R> Drop for MessageStream<'a, C, R>
where
    C: ConsumerContext + 'static,
{
    fn drop(&mut self) {
        self.consumer.wakers.unregister(self.slot);
    }
}

/// A high-level consumer with a [`Stream`](futures::Stream) interface.
///
/// This consumer doesn't need to be polled explicitly. Extracting an item from
/// the stream returned by the [`stream`](StreamConsumer::stream) will
/// implicitly poll the underlying Kafka consumer.
///
/// If you activate the consumer group protocol by calling
/// [`subscribe`](Consumer::subscribe), the stream consumer will integrate with
/// librdkafka's liveness detection as described in [KIP-62]. You must be sure
/// that you attempt to extract a message from the stream consumer at least
/// every `max.poll.interval.ms` milliseconds, or librdkafka will assume that
/// the processing thread is wedged and leave the consumer groups.
///
/// [KIP-62]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C = DefaultConsumerContext, R = DefaultRuntime>
where
    C: ConsumerContext,
{
    base: BaseConsumer<C>,
    wakers: Arc<WakerSlab>,
    _queue: NativeQueue,
    _shutdown_trigger: oneshot::Sender<()>,
    _runtime: PhantomData<R>,
}

impl<R> FromClientConfig for StreamConsumer<DefaultConsumerContext, R>
where
    R: AsyncRuntime,
{
    fn from_config(config: &ClientConfig) -> KafkaResult<Self> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `StreamConsumer` starting from a [`ClientConfig`].
impl<C, R> FromClientConfigAndContext<C> for StreamConsumer<C, R>
where
    C: ConsumerContext + 'static,
    R: AsyncRuntime,
{
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<Self> {
        let native_config = config.create_native_config()?;
        let poll_interval = {
            let millis: u64 = native_config
                .get("max.poll.interval.ms")?
                .parse()
                .expect("librdkafka validated config value is valid u64");
            Duration::from_millis(millis)
        };

        let base = BaseConsumer::new(config, native_config, context)?;
        let native_ptr = base.client().native_ptr() as usize;

        // Redirect rdkafka's main queue to the consumer queue so that we only
        // need to listen to the consumer queue to observe events like
        // rebalancings and stats.
        unsafe { rdsys::rd_kafka_poll_set_consumer(base.client().native_ptr()) };

        let queue = base
            .client()
            .consumer_queue()
            .ok_or_else(|| KafkaError::ClientCreation(
                "librdkafka failed to create consumer queue".into(),
            ))?;
        let wakers = Arc::new(WakerSlab::new());
        unsafe { enable_nonempty_callback(&queue, &wakers) }

        // We need to make sure we poll the consumer at least once every max
        // poll interval, *unless* the processing task has wedged. To accomplish
        // this, we launch a background task that sends spurious wakeup
        // notifications at half the max poll interval. An unwedged processing
        // task will wake up and poll the consumer with plenty of time to spare,
        // while a wedged processing task will not.
        //
        // The default max poll interval is 5m, so there is essentially no
        // performance impact to these spurious wakeups.
        let (shutdown_trigger, shutdown_tripwire) = oneshot::channel();
        let mut shutdown_tripwire = shutdown_tripwire.fuse();
        R::spawn({
            let wakers = wakers.clone();
            async move {
                trace!("Starting stream consumer wake loop: 0x{:x}", native_ptr);
                loop {
                    select! {
                        _ = R::delay_for(poll_interval / 2).fuse() => wakers.wake_all(),
                        _ = shutdown_tripwire => break,
                    }
                }
                trace!("Shut down stream consumer wake loop: 0x{:x}", native_ptr);
            }
        });

        Ok(StreamConsumer {
            base,
            wakers,
            _queue: queue,
            _shutdown_trigger: shutdown_trigger,
            _runtime: PhantomData,
        })
    }
}

impl<C, R> StreamConsumer<C, R>
where
    C: ConsumerContext + 'static,
{
    /// Constructs a stream that yields messages from this consumer.
    ///
    /// It is legal to have multiple live message streams for the same consumer,
    /// and to move those message streams across threads. Note, however, that
    /// the message streams share the same underlying state. A message received
    /// by the consumer will be delivered to only one of the live message
    /// streams. If you seek the underlying consumer, all message streams
    /// created from the consumer will begin to draw messages from the new
    /// position of the consumer.
    ///
    /// If you want multiple independent views of a Kafka topic, create multiple
    /// consumers, not multiple message streams.
    pub fn stream(&self) -> MessageStream<'_, C, R> {
        MessageStream::new(self)
    }

    /// Constructs a stream that yields messages from this consumer.
    #[deprecated = "use the more clearly named \"StreamConsumer::stream\" method instead"]
    pub fn start(&self) -> MessageStream<'_, C, R> {
        self.stream()
    }

    /// Receives the next message from the stream.
    ///
    /// This method will block until the next message is available or an error
    /// occurs. It is legal to call `recv` from multiple threads simultaneously.
    ///
    /// Note that this method is exactly as efficient as constructing a
    /// single-use message stream and extracting one message from it:
    ///
    /// ```
    /// use futures::stream::StreamExt;
    /// # use rdkafka::consumer::StreamConsumer;
    ///
    /// # async fn example(consumer: StreamConsumer) {
    /// consumer.stream().next().await.expect("MessageStream never returns None");
    /// # }
    /// ```
    pub async fn recv(&self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.stream()
            .next()
            .await
            .expect("kafka streams never terminate")
    }
}

impl<C, R> Consumer<C> for StreamConsumer<C, R>
where
    C: ConsumerContext,
{
    fn client(&self) -> &Client<C> {
        self.base.client()
    }

    fn group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        self.base.group_metadata()
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.base.subscribe(topics)
    }

    fn unsubscribe(&self) {
        self.base.unsubscribe();
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.base.assign(assignment)
    }

    fn seek<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: T,
    ) -> KafkaResult<()> {
        self.base.seek(topic, partition, offset, timeout)
    }

    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        self.base.commit(topic_partition_list, mode)
    }

    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        self.base.commit_consumer_state(mode)
    }

    fn commit_message(&self, message: &BorrowedMessage<'_>, mode: CommitMode) -> KafkaResult<()> {
        self.base.commit_message(message, mode)
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        self.base.store_offset(topic, partition, offset)
    }

    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()> {
        self.base.store_offset_from_message(message)
    }

    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        self.base.store_offsets(tpl)
    }

    fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        self.base.subscription()
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        self.base.assignment()
    }

    fn committed<T>(&self, timeout: T) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.committed(timeout)
    }

    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
    {
        self.base.committed_offsets(tpl, timeout)
    }

    fn offsets_for_timestamp<T>(
        &self,
        timestamp: i64,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.offsets_for_timestamp(timestamp, timeout)
    }

    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.offsets_for_times(timestamps, timeout)
    }

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.base.position()
    }

    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.fetch_metadata(topic, timeout)
    }

    fn fetch_watermarks<T>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.fetch_watermarks(topic, partition, timeout)
    }

    fn fetch_group_list<T>(&self, group: Option<&str>, timeout: T) -> KafkaResult<GroupList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.fetch_group_list(group, timeout)
    }

    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.base.pause(partitions)
    }

    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.base.resume(partitions)
    }

    fn rebalance_protocol(&self) -> RebalanceProtocol {
        self.base.rebalance_protocol()
    }
}
