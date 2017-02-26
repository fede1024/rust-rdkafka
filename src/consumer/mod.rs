//! Base trait and common functionality for all consumers.
pub mod base_consumer;
pub mod stream_consumer;

use rdsys;
use rdsys::types::*;

use std::ptr;
use std::os::raw::c_void;
use util::cstr_to_owned;

use client::{Context, NativeClient};
use message::Message;
use metadata::Metadata;
use error::KafkaResult;
use groups::GroupList;

pub use consumer::base_consumer::BaseConsumer;
pub use topic_partition_list::TopicPartitionList;

/// Rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance {
    Assign(TopicPartitionList),
    Revoke,
    Error(String)
}

/// Consumer specific Context. This user-defined object can be used to provide custom callbacks to
/// consumer events. Refer to the list of methods to check which callbacks can be specified.
pub trait ConsumerContext: Context {
    /// Implements the default rebalancing strategy and calls the `pre_rebalance` and
    /// `post_rebalance` methods. If this method is overridden, it will be responsibility
    /// of the user to call them if needed.
    fn rebalance(&self, native_client: &NativeClient, err: RDKafkaRespErr,
                 partitions_ptr: *mut RDKafkaTopicPartitionList) {

        let rebalance = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                // TODO: this might be expensive
                let topic_partition_list = TopicPartitionList::from_rdkafka(partitions_ptr);
                Rebalance::Assign(topic_partition_list)
            },
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                Rebalance::Revoke
            },
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
                    rdsys::rd_kafka_assign(native_client.ptr(), partitions_ptr);
                },
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                    rdsys::rd_kafka_assign(native_client.ptr(), ptr::null());
                },
                _ => {
                    rdsys::rd_kafka_assign(native_client.ptr(), ptr::null());
                }
            }
        }
        self.post_rebalance(&rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance and should
    /// terminate its execution quickly.
    fn pre_rebalance(&self, _rebalance: &Rebalance) { }

    /// Post-rebalance callback. This method will run after the rebalance and should
    /// terminate its execution quickly.
    fn post_rebalance(&self, _rebalance: &Rebalance) { }
}

/// An empty consumer context that can be user when no context is needed.
#[derive(Clone)]
pub struct EmptyConsumerContext;

impl Context for EmptyConsumerContext { }
impl ConsumerContext for EmptyConsumerContext { }

/// Native rebalance callback. This callback will run on every rebalance, and it will call the
/// rebalance method defined in the current `Context`.
unsafe extern "C" fn rebalance_cb<C: ConsumerContext>(rk: *mut RDKafka,
                                                      err: RDKafkaRespErr,
                                                      partitions: *mut RDKafkaTopicPartitionList,
                                                      opaque_ptr: *mut c_void) {
    let context: &C = &*(opaque_ptr as *const C);
    let native_client = NativeClient::new(rk);

    context.rebalance(&native_client, err, partitions);
}

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
    /// Returns a mutable reference to the BaseConsumer.
    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer<C>;

    // Default implementations

    /// Subscribe the consumer to a list of topics.
    fn subscribe(&mut self, topics: &Vec<&str>) -> KafkaResult<()> {
        self.get_base_consumer_mut().subscribe(topics)
    }

    /// Manually assign topics and partitions to the consumer.
    fn assign(&mut self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer_mut().assign(assignment)
    }

    /// Commit offsets on broker for the provided list of partitions.
    /// If mode is set to CommitMode::Sync, the call will block until
    /// the message has been succesfully committed.
    fn commit(&self, topic_partition_list: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer().commit(topic_partition_list, mode)
    }

    /// Commit a specific message. If mode is set to CommitMode::Sync,
    /// the call will block until the message has been successfully
    /// committed.
    fn commit_message(&self, message: &Message, mode: CommitMode) -> KafkaResult<()> {
        self.get_base_consumer().commit_message(message, mode)
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

    /// Retrieve current positions (offsets) for topics and partitions.
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.get_base_consumer().position()
    }

    /// Returns the metadata information for all the topics in the cluster.
    fn fetch_metadata(&self, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.get_base_consumer().fetch_metadata(timeout_ms)
    }

    /// Returns the metadata information for all the topics in the cluster.
    fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        self.get_base_consumer().fetch_watermarks(topic, partition, timeout_ms)
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    fn fetch_group_list(&self, group: Option<&str>, timeout_ms: i32) -> KafkaResult<GroupList> {
        self.get_base_consumer().fetch_group_list(group, timeout_ms)
    }
}
