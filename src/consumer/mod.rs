//! Kafka consumers.

use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{Client, ClientContext};
use crate::error::{KafkaError, KafkaResult};
use crate::groups::GroupList;
use crate::log::{error, trace};
use crate::message::BorrowedMessage;
use crate::metadata::Metadata;
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::util::{KafkaDrop, NativePtr, Timeout};

pub mod base_consumer;
pub mod stream_consumer;

// Re-exports.
#[doc(inline)]
pub use self::base_consumer::BaseConsumer;
#[doc(inline)]
pub use self::stream_consumer::{MessageStream, StreamConsumer};

/// Rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance<'a> {
    /// A new partition assignment is received.
    Assign(&'a TopicPartitionList),
    /// A new partition revocation is received.
    Revoke(&'a TopicPartitionList),
    /// Unexpected error from Kafka.
    Error(KafkaError),
}

/// Consumer-specific context.
///
/// This user-defined object can be used to provide custom callbacks for
/// consumer events. Refer to the list of methods to check which callbacks can
/// be specified.
///
/// See also the [`ClientContext`] trait.
pub trait ConsumerContext: ClientContext + Sized {
    /// Implements the default rebalancing strategy and calls the
    /// [`pre_rebalance`](ConsumerContext::pre_rebalance) and
    /// [`post_rebalance`](ConsumerContext::post_rebalance) methods. If this
    /// method is overridden, it will be responsibility of the user to call them
    /// if needed.
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        let rebalance = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => Rebalance::Assign(tpl),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Rebalance::Revoke(tpl),
            _ => {
                let error_code: RDKafkaErrorCode = err.into();
                error!("Error rebalancing: {}", error_code);
                Rebalance::Error(KafkaError::Rebalance(error_code))
            }
        };

        trace!("Running pre-rebalance with {:?}", rebalance);
        self.pre_rebalance(base_consumer, &rebalance);

        trace!("Running rebalance with {:?}", rebalance);
        let native_client = base_consumer.native_client();
        // Execute rebalance
        unsafe {
            match err {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    match native_client.rebalance_protocol() {
                        RebalanceProtocol::Cooperative => {
                            rdsys::rd_kafka_incremental_assign(native_client.ptr(), tpl.ptr());
                        }
                        _ => {
                            rdsys::rd_kafka_assign(native_client.ptr(), tpl.ptr());
                        }
                    }
                }
                _ => match native_client.rebalance_protocol() {
                    RebalanceProtocol::Cooperative => {
                        rdsys::rd_kafka_incremental_unassign(native_client.ptr(), tpl.ptr());
                    }
                    _ => {
                        rdsys::rd_kafka_assign(native_client.ptr(), ptr::null());
                    }
                },
            }
        }
        trace!("Running post-rebalance with {:?}", rebalance);
        self.post_rebalance(base_consumer, &rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance and
    /// should terminate its execution quickly.
    #[allow(unused_variables)]
    fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {}

    /// Post-rebalance callback. This method will run after the rebalance and
    /// should terminate its execution quickly.
    #[allow(unused_variables)]
    fn post_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {}

    // TODO: convert pointer to structure
    /// Post commit callback. This method will run after a group of offsets was
    /// committed to the offset store.
    #[allow(unused_variables)]
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {}

    /// Returns the minimum interval at which to poll the main queue, which
    /// services the logging, stats, and error events.
    ///
    /// The main queue is polled once whenever [`BaseConsumer::poll`] is called.
    /// If `poll` is called with a timeout that is larger than this interval,
    /// then the main queue will be polled at that interval while the consumer
    /// queue is blocked. This allows serving events while there are no messages.
    ///
    /// For example, if the main queue's minimum poll interval is 200ms and
    /// `poll` is called with a timeout of 1s, then `poll` may block for up to
    /// 1s waiting for a message, but it will poll the main queue every 200ms
    /// while it is waiting.
    ///
    /// By default, the minimum poll interval for the main queue is 1s.
    fn main_queue_min_poll_interval(&self) -> Timeout {
        Timeout::After(Duration::from_secs(1))
    }
}

/// An inert [`ConsumerContext`] that can be used when no customizations are
/// needed.
#[derive(Clone, Debug, Default)]
pub struct DefaultConsumerContext;

impl ClientContext for DefaultConsumerContext {}
impl ConsumerContext for DefaultConsumerContext {}

/// Specifies whether a commit should be performed synchronously or
/// asynchronously.
///
/// A commit is performed via [`Consumer::commit`] or one of its variants.
///
/// Regardless of the `CommitMode`, the commit APIs enqueue the commit request
/// in a local work queue. A separate worker thread picks up this commit request
/// and forwards it to the Kafka broker over the network.
///
/// The difference between [`CommitMode::Sync`] and [`CommitMode::Async`] is in
/// whether the caller waits for the Kafka broker to respond that it finished
/// handling the commit request.
///
/// Note that the commit APIs are not async in the Rust sense due to the lack of
/// a callback-based interface exposed by librdkafka. See
/// [librdkafka#3212](https://github.com/edenhill/librdkafka/issues/3212).
#[derive(Clone, Copy, Debug)]
pub enum CommitMode {
    /// In `Sync` mode, the caller blocks until the Kafka broker finishes
    /// processing the commit request.
    Sync = 0,

    /// In `Async` mode, the caller enqueues the commit request in a local
    /// work queue and returns immediately.
    Async = 1,
}

/// Consumer group metadata.
///
/// For use with [`Producer::send_offsets_to_transaction`].
///
/// [`Producer::send_offsets_to_transaction`]: crate::producer::Producer::send_offsets_to_transaction
pub struct ConsumerGroupMetadata(NativePtr<RDKafkaConsumerGroupMetadata>);

impl ConsumerGroupMetadata {
    pub(crate) fn ptr(&self) -> *const RDKafkaConsumerGroupMetadata {
        self.0.ptr()
    }
}

unsafe impl KafkaDrop for RDKafkaConsumerGroupMetadata {
    const TYPE: &'static str = "consumer_group_metadata";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_consumer_group_metadata_destroy;
}

unsafe impl Send for ConsumerGroupMetadata {}
unsafe impl Sync for ConsumerGroupMetadata {}

/// The rebalance protocol for a consumer.
pub enum RebalanceProtocol {
    /// The consumer has not (yet) joined a group.
    None,
    /// Eager rebalance protocol.
    Eager,
    /// Cooperative rebalance protocol.
    Cooperative,
}

/// Common trait for all consumers.
///
/// # Note about object safety
///
/// Doing type erasure on consumers is expected to be rare (eg. `Box<dyn
/// Consumer>`). Therefore, the API is optimised for the case where a concrete
/// type is available. As a result, some methods are not available on trait
/// objects, since they are generic.
pub trait Consumer<C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    /// Returns the [`Client`] underlying this consumer.
    fn client(&self) -> &Client<C>;

    /// Returns a reference to the [`ConsumerContext`] used to create this
    /// consumer.
    fn context(&self) -> &Arc<C> {
        self.client().context()
    }

    /// Returns the current consumer group metadata associated with the
    /// consumer.
    ///
    /// If the consumer was not configured with a `group.id`, returns `None`.
    /// For use with [`Producer::send_offsets_to_transaction`].
    ///
    /// [`Producer::send_offsets_to_transaction`]: crate::producer::Producer::send_offsets_to_transaction
    fn group_metadata(&self) -> Option<ConsumerGroupMetadata>;

    /// Subscribes the consumer to a list of topics.
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;

    /// Unsubscribes the current subscription list.
    fn unsubscribe(&self);

    /// Manually assigns topics and partitions to the consumer. If used,
    /// automatic consumer rebalance won't be activated.
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()>;

    /// Clears all topic and partitions currently assigned to the consumer
    fn unassign(&self) -> KafkaResult<()>;

    /// Incrementally add partitions from the current assignment
    fn incremental_assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()>;

    /// Incrementally remove partitions from the current assignment
    fn incremental_unassign(&self, assignment: &TopicPartitionList) -> KafkaResult<()>;

    /// Seeks to `offset` for the specified `topic` and `partition`. After a
    /// successful call to `seek`, the next poll of the consumer will return the
    /// message with `offset`.
    fn seek<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: T,
    ) -> KafkaResult<()>;

    /// Seeks consumer for partitions in `topic_partition_list` to the per-partition offset
    /// in the `offset` field of `TopicPartitionListElem`.
    /// The offset can be either absolute (>= 0) or a logical offset.
    /// Seek should only be performed on already assigned/consumed partitions.
    /// Individual partition errors are reported in the per-partition `error` field of
    /// `TopicPartitionListElem`.
    fn seek_partitions<T: Into<Timeout>>(
        &self,
        topic_partition_list: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>;

    /// Commits the offset of the specified message. The commit can be sync
    /// (blocking), or async. Notice that when a specific offset is committed,
    /// all the previous offsets are considered committed as well. Use this
    /// method only if you are processing messages in order.
    ///
    /// The highest committed offset is interpreted as the next message to be
    /// consumed in the event that a consumer rehydrates its local state from
    /// the Kafka broker (i.e. consumer server restart). This means that,
    /// in general, the offset of your [`TopicPartitionList`] should equal
    /// 1 plus the offset from your last consumed message.
    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()>;

    /// Commits the current consumer state. Notice that if the consumer fails
    /// after a message has been received, but before the message has been
    /// processed by the user code, this might lead to data loss. Check the
    /// "at-least-once delivery" section in the readme for more information.
    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()>;

    /// Commit the provided message. Note that this will also automatically
    /// commit every message with lower offset within the same partition.
    ///
    /// This method is exactly equivalent to invoking [`Consumer::commit`]
    /// with a [`TopicPartitionList`] which copies the topic and partition
    /// from the message and adds 1 to the offset of the message.
    fn commit_message(&self, message: &BorrowedMessage<'_>, mode: CommitMode) -> KafkaResult<()>;

    /// Stores offset to be used on the next (auto)commit. When
    /// using this `enable.auto.offset.store` should be set to `false` in the
    /// config.
    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()>;

    /// Like [`Consumer::store_offset`], but the offset to store is derived from
    /// the provided message.
    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()>;

    /// Store offsets to be used on the next (auto)commit. When using this
    /// `enable.auto.offset.store` should be set to `false` in the config.
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()>;

    /// Returns the current topic subscription.
    fn subscription(&self) -> KafkaResult<TopicPartitionList>;

    /// Returns the current partition assignment.
    fn assignment(&self) -> KafkaResult<TopicPartitionList>;

    /// Check whether the consumer considers the current assignment to have been lost
    /// involuntarily.
    ///
    /// This method is only applicable for use with a high level subscribing consumer. Assignments
    /// are revoked immediately when determined to have been lost, so this method is only useful
    /// when reacting to a rebalance or from within a rebalance_cb. Partitions
    /// that have been lost may already be owned by other members in the group and therefore
    /// commiting offsets, for example, may fail.
    ///
    /// Calling rd_kafka_assign(), rd_kafka_incremental_assign() or rd_kafka_incremental_unassign()
    /// resets this flag.
    ///
    /// Returns true if the current partition assignment is considered lost, false otherwise.
    fn assignment_lost(&self) -> bool;

    /// Retrieves the committed offsets for topics and partitions.
    fn committed<T>(&self, timeout: T) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Retrieves the committed offsets for specified topics and partitions.
    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>;

    /// Looks up the offsets for this consumer's partitions by timestamp.
    fn offsets_for_timestamp<T>(
        &self,
        timestamp: i64,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Looks up the offsets for the specified partitions by timestamp.
    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Retrieve current positions (offsets) for topics and partitions.
    fn position(&self) -> KafkaResult<TopicPartitionList>;

    /// Returns the metadata information for the specified topic, or for all
    /// topics in the cluster if no topic is specified.
    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Returns the low and high watermarks for a specific topic and partition.
    fn fetch_watermarks<T>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    fn fetch_group_list<T>(&self, group: Option<&str>, timeout: T) -> KafkaResult<GroupList>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Pauses consumption for the provided list of partitions.
    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()>;

    /// Resumes consumption for the provided list of partitions.
    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()>;

    /// Reports the rebalance protocol in use.
    fn rebalance_protocol(&self) -> RebalanceProtocol;
}
