//! Base trait and common functionalities for all consumers.
extern crate rdkafka_sys as rdkafka;

pub mod base_consumer;
pub mod stream_consumer;

use self::rdkafka::types::*;

use std::ptr;
use std::os::raw::c_void;
use util::cstr_to_owned;

use client::{Context, NativeClient};
use message::Message;
use metadata::Metadata;
use error::KafkaResult;

pub use consumer::base_consumer::BaseConsumer;
pub use topic_partition_list::TopicPartitionList;

/// Consumer specific Context. This user-defined object can be used to provide custom callbacks to
/// consumer events. Refer to the list of methods to check which callbacks can be specified.
pub trait ConsumerContext: Context {
    /// Implements the default rebalancing strategy and calls the pre_rebalance and
    /// post_rebalance methods. If this method is overridden, it will be responsibility
    /// of the user to call them if needed.
    fn rebalance(&self,
                 native_client: &NativeClient,
                 err: RDKafkaRespErr,
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
                let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(err)) };
                error!("Error rebalancing: {}", error);
                Rebalance::Error(error)
            }
        };

        self.pre_rebalance(&rebalance);

        // Execute rebalance
        unsafe {
            match err {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr(), partitions_ptr);
                },
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr(), ptr::null());
                },
                _ => {
                    rdkafka::rd_kafka_assign(native_client.ptr(), ptr::null());
                }
            }
        }
        self.post_rebalance(&rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance, and it will receive the
    /// relabance information. This method is executed as part of the rebalance callback and should
    /// terminate its execution quickly.
    fn pre_rebalance(&self, _rebalance: &Rebalance) { }

    /// Post-rebalance callback. This method will run before the rebalance, and it will receive the
    /// relabance information. This method is executed as part of the rebalance callback and should
    /// terminate its execution quickly.
    fn post_rebalance(&self, _rebalance: &Rebalance) { }
}

#[derive(Clone)]
struct EmptyConsumerContext;

impl Context for EmptyConsumerContext { }
impl ConsumerContext for EmptyConsumerContext { }

/// Contains rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance {
    Assign(TopicPartitionList),
    Revoke,
    Error(String)
}

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
    Sync,
    /// Asynchronous commit.
    Async,
}

/// Common trait for all consumers
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

    /// Commit a specific message. If mode is set to CommitMode::Sync,
    /// the call will block until the message has been succesfully
    /// committed.
    fn commit_message(&self, message: &Message, mode: CommitMode) {
        self.get_base_consumer().commit_message(message, mode);
    }

    fn fetch_metadata(&mut self, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.get_base_consumer().fetch_metadata(timeout_ms)
    }

}
