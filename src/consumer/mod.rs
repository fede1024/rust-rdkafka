//! Base trait and common functionality for all consumers.
pub mod base_consumer;
pub mod stream_consumer;

// Re-export
pub use self::base_consumer::BaseConsumer;
pub use self::stream_consumer::{MessageStream, StreamConsumer};

use rdsys;
use rdsys::types::*;

use client::{Context, NativeClient};
use error::KafkaResult;
use groups::GroupList;
use message::BorrowedMessage;
use metadata::Metadata;
use util::cstr_to_owned;

use std::ptr;

use topic_partition_list::TopicPartitionList;

/// Rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance<'a> {
    Assign(&'a TopicPartitionList),
    Revoke,
    Error(String),
}

/// Consumer specific Context. This user-defined object can be used to provide custom callbacks to
/// consumer events. Refer to the list of methods to check which callbacks can be specified.
pub trait ConsumerContext: Context {
    /// Implements the default rebalancing strategy and calls the `pre_rebalance` and
    /// `post_rebalance` methods. If this method is overridden, it will be responsibility
    /// of the user to call them if needed.
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &TopicPartitionList,
    ) {

        let rebalance = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                Rebalance::Assign(tpl)
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Rebalance::Revoke,
            _ => {
                let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(err)) };
                error!("Error rebalancing: {}", error);
                Rebalance::Error(error)
            }
        };

        self.pre_rebalance(&rebalance);

        // Execute rebalance
        unsafe {
            match err {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    rdsys::rd_kafka_assign(native_client.ptr(), tpl.ptr());
                },
                _ => {  // Also for RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
                    rdsys::rd_kafka_assign(native_client.ptr(), ptr::null());
                }
            }
        }
        self.post_rebalance(&rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance and should
    /// terminate its execution quickly.
    #[allow(unused_variables)]
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {}

    /// Post-rebalance callback. This method will run after the rebalance and should
    /// terminate its execution quickly.
    #[allow(unused_variables)]
    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {}

    /// Post commit callback. This method will run after a group of offsets was committed to the
    /// offset store.
    #[allow(unused_variables)]
    fn commit_callback(&self, result: KafkaResult<()>, offsets: *mut RDKafkaTopicPartitionList) {}
}

/// An empty consumer context that can be user when no context is needed.
#[derive(Clone)]
pub struct EmptyConsumerContext;

impl Context for EmptyConsumerContext {}
impl ConsumerContext for EmptyConsumerContext {}

/// Specifies if the commit should be performed synchronously
/// or asynchronously.
pub enum CommitMode {
    /// Synchronous commit.
    Sync = 0,
    /// Asynchronous commit.
    Async = 1,
}

/// Common trait for all consumers.
pub trait Consumer<C: ConsumerContext> {
    /// Returns a reference to the BaseConsumer.
    fn get_base_consumer(&self) -> &BaseConsumer<C>;

    // Default implementations

    /// Subscribe the consumer to a list of topics.
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.get_base_consumer().subscribe(topics)
    }

    /// Manually assign topics and partitions to the consumer.
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer().assign(assignment)
    }

    /// Commit offsets on broker for the provided list of partitions, or the underlying consumers state if `None`.
    /// If mode is set to CommitMode::Sync, the call will block until
    /// the message has been successfully committed.
    fn commit(&self, topic_partition_list: Option<&TopicPartitionList>, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer()
            .commit(topic_partition_list, mode)
    }

    /// Commit a specific message. If mode is set to CommitMode::Sync,
    /// the call will block until the message has been successfully
    /// committed.
    fn commit_message(&self, message: &BorrowedMessage, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer().commit_message(message, mode)
    }

    /// Store offset for this message to be used on the next (auto)commit.
    /// When using this `enable.auto.offset.store` should be set to `false` in the config.
    fn store_offset(&self, message: &BorrowedMessage) -> KafkaResult<()> {
        self.get_base_consumer().store_offset(message)
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
    fn committed(&self, timeout_ms: i32) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().committed(timeout_ms)
    }

    /// Lookup the offsets for this consumer's partitions by timestamp.
    fn offsets_for_timestamp(&self, timestamp: i64, timeout_ms: i32) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer()
            .offsets_for_timestamp(timestamp, timeout_ms)
    }

    /// Retrieve current positions (offsets) for topics and partitions.
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().position()
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    fn fetch_metadata(&self, topic: Option<&str>, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.get_base_consumer()
            .fetch_metadata(topic, timeout_ms)
    }

    /// Returns the metadata information for all the topics in the cluster.
    fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        self.get_base_consumer()
            .fetch_watermarks(topic, partition, timeout_ms)
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    fn fetch_group_list(&self, group: Option<&str>, timeout_ms: i32) -> KafkaResult<GroupList> {
        self.get_base_consumer()
            .fetch_group_list(group, timeout_ms)
    }
}
