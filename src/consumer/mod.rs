//! Kafka consumers.

use std::ptr;
use std::time::Duration;

use log::{error, trace};

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{Client, ClientContext, NativeClient};
use crate::error::KafkaResult;
use crate::groups::GroupList;
use crate::message::BorrowedMessage;
use crate::metadata::Metadata;
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::util::{cstr_to_owned, Timeout};

pub mod base_consumer;
pub mod stream_consumer;

// Re-exports.
pub use self::base_consumer::BaseConsumer;
pub use self::stream_consumer::StreamConsumer;

/// Rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance<'a> {
    /// A new partition assignment is received.
    Assign(&'a TopicPartitionList),
    /// All partitions are revoked.
    Revoke,
    /// Unexpected error from Kafka.
    Error(String),
}

/// Consumer-specific context.
///
/// This user-defined object can be used to provide custom callbacks for
/// consumer events. Refer to the list of methods to check which callbacks can
/// be specified.
///
/// See also the [`ClientContext`] trait.
pub trait ConsumerContext: ClientContext {
    /// Implements the default rebalancing strategy and calls the
    /// [`pre_rebalance`](ConsumerContext::pre_rebalance) and
    /// [`post_rebalance`](ConsumerContext::post_rebalance) methods. If this
    /// method is overridden, it will be responsibility of the user to call them
    /// if needed.
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        let rebalance = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => Rebalance::Assign(tpl),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Rebalance::Revoke,
            _ => {
                let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(err)) };
                error!("Error rebalancing: {}", error);
                Rebalance::Error(error)
            }
        };

        trace!("Running pre-rebalance with {:?}", rebalance);
        self.pre_rebalance(&rebalance);

        trace!("Running rebalance with {:?}", rebalance);
        // Execute rebalance
        unsafe {
            match err {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    rdsys::rd_kafka_assign(native_client.ptr(), tpl.ptr());
                }
                _ => {
                    // Also for RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
                    rdsys::rd_kafka_assign(native_client.ptr(), ptr::null());
                }
            }
        }
        trace!("Running post-rebalance with {:?}", rebalance);
        self.post_rebalance(&rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance and
    /// should terminate its execution quickly.
    #[allow(unused_variables)]
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {}

    /// Post-rebalance callback. This method will run after the rebalance and
    /// should terminate its execution quickly.
    #[allow(unused_variables)]
    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {}

    // TODO: convert pointer to structure
    /// Post commit callback. This method will run after a group of offsets was
    /// committed to the offset store.
    #[allow(unused_variables)]
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {}

    /// Returns the minimum interval at which to poll the main queue, which
    /// services the logging, stats, and error callbacks.
    ///
    /// The main queue is polled once whenever [`BaseConsumer::poll`] is called.
    /// If `poll` is called with a timeout that is larger than this interval,
    /// then the main queue will be polled at that interval while the consumer
    /// queue is blocked.
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

    /// Message queue nonempty callback. This method will run when the
    /// consumer's message queue switches from empty to nonempty.
    fn message_queue_nonempty_callback(&self) {}

    // NOTE: when adding a new method, remember to add it to the
    // StreamConsumerContext as well.
}

/// An empty consumer context that can be user when no customizations are
/// needed.
#[derive(Clone, Default)]
pub struct DefaultConsumerContext;

impl ClientContext for DefaultConsumerContext {}
impl ConsumerContext for DefaultConsumerContext {}

/// Specifies if the commit should be performed synchronously
/// or asynchronously.
#[derive(Clone, Copy, Debug)]
pub enum CommitMode {
    /// Synchronous commit.
    Sync = 0,
    /// Asynchronous commit.
    Async = 1,
}

/// Common trait for all consumers.
///
/// # Note about object safety
///
/// Doing type erasure on consumers is expected to be rare (eg. `Box<Consumer>`). Therefore, the
/// API is optimised for the case where a concrete type is available. As a result, some methods are
/// not available on trait objects, since they are generic.
///
/// If there's still the need to erase the type, the generic methods can still be reached through
/// the [`get_base_consumer`](#method.get_base_consumer) method.
pub trait Consumer<C: ConsumerContext = DefaultConsumerContext> {
    /// Returns a reference to the BaseConsumer.
    fn get_base_consumer(&self) -> &BaseConsumer<C>;

    // Default implementations

    /// Subscribe the consumer to a list of topics.
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.get_base_consumer().subscribe(topics)
    }

    /// Unsubscribe the current subscription list.
    fn unsubscribe(&self) {
        self.get_base_consumer().unsubscribe();
    }

    /// Manually assign topics and partitions to the consumer. If used, automatic consumer
    /// rebalance won't be activated.
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer().assign(assignment)
    }

    /// Seek to `offset` for the specified `topic` and `partition`. After a
    /// successful call to `seek`, the next poll of the consumer will return the
    /// message with `offset`.
    fn seek<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: T,
    ) -> KafkaResult<()> {
        self.get_base_consumer()
            .seek(topic, partition, offset, timeout)
    }

    /// Commits the offset of the specified message. The commit can be sync (blocking), or async.
    /// Notice that when a specific offset is committed, all the previous offsets are considered
    /// committed as well. Use this method only if you are processing messages in order.
    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        self.get_base_consumer().commit(topic_partition_list, mode)
    }

    /// Commit the current consumer state. Notice that if the consumer fails after a message
    /// has been received, but before the message has been processed by the user code,
    /// this might lead to data loss. Check the "at-least-once delivery" section in the readme
    /// for more information.
    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer().commit_consumer_state(mode)
    }

    /// Commit the provided message. Note that this will also automatically commit every
    /// message with lower offset within the same partition.
    fn commit_message(&self, message: &BorrowedMessage, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer().commit_message(message, mode)
    }

    /// Store offset for this message to be used on the next (auto)commit.
    /// When using this `enable.auto.offset.store` should be set to `false` in the config.
    fn store_offset(&self, message: &BorrowedMessage) -> KafkaResult<()> {
        self.get_base_consumer().store_offset(message)
    }

    /// Store offsets to be used on the next (auto)commit.
    /// When using this `enable.auto.offset.store` should be set to `false` in the config.
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer().store_offsets(tpl)
    }

    /// Returns the current topic subscription.
    fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().subscription()
    }

    /// Returns the current partition assignment.
    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().assignment()
    }

    /// Retrieve committed offsets for topics and partitions.
    fn committed<T>(&self, timeout: T) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.get_base_consumer().committed(timeout)
    }

    /// Retrieve committed offsets for specified topics and partitions.
    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
    {
        self.get_base_consumer().committed_offsets(tpl, timeout)
    }

    /// Lookup the offsets for this consumer's partitions by timestamp.
    fn offsets_for_timestamp<T>(
        &self,
        timestamp: i64,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.get_base_consumer()
            .offsets_for_timestamp(timestamp, timeout)
    }

    /// Look up the offsets for the specified partitions by timestamp.
    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.get_base_consumer()
            .offsets_for_times(timestamps, timeout)
    }

    /// Retrieve current positions (offsets) for topics and partitions.
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().position()
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.get_base_consumer().fetch_metadata(topic, timeout)
    }

    /// Returns the metadata information for all the topics in the cluster.
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
        self.get_base_consumer()
            .fetch_watermarks(topic, partition, timeout)
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    fn fetch_group_list<T>(&self, group: Option<&str>, timeout: T) -> KafkaResult<GroupList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.get_base_consumer().fetch_group_list(group, timeout)
    }

    /// Pause consumption for the provided list of partitions.
    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer().pause(partitions)
    }

    /// Resume consumption for the provided list of partitions.
    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer().resume(partitions)
    }

    /// Returns the [`Client`] underlying this consumer.
    fn client(&self) -> &Client<C> {
        self.get_base_consumer().client()
    }
}
