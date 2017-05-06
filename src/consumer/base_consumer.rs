//! Low level consumer wrapper.
use rdsys;
use rdsys::types::*;

use client::{Client, NativeClient};
use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::{Consumer, ConsumerContext, CommitMode, EmptyConsumerContext};
use error::{KafkaError, KafkaResult, IsError};
use groups::GroupList;
use message::Message;
use metadata::Metadata;
use topic_partition_list::TopicPartitionList;
use util::cstr_to_owned;

use std::os::raw::c_void;
use std::slice;
use std::str;
use std::mem;

pub unsafe extern "C" fn native_commit_cb<C: ConsumerContext>(
    _conf: *mut RDKafka,
    err: RDKafkaRespErr,
    offsets: *mut RDKafkaTopicPartitionList,
    opaque_ptr: *mut c_void,
) {
    let context = Box::from_raw(opaque_ptr as *mut C);

    let commit_error = if err.is_error() {
        Err(KafkaError::ConsumerCommit(err))
    } else {
        Ok(())
    };
    (*context).commit_callback(commit_error, offsets);

    mem::forget(context); // Do not free the context
}

/// Native rebalance callback. This callback will run on every rebalance, and it will call the
/// rebalance method defined in the current `Context`.
unsafe extern "C" fn native_rebalance_cb<C: ConsumerContext>(
    rk: *mut RDKafka,
    err: RDKafkaRespErr,
    partitions: *mut RDKafkaTopicPartitionList,
    opaque_ptr: *mut c_void,
) {
    // let context: &C = &*(opaque_ptr as *const C);
    let context = Box::from_raw(opaque_ptr as *mut C);
    let native_client = NativeClient::from_ptr(rk);

    context.rebalance(&native_client, err, partitions);

    mem::forget(native_client); // Do not free native client
    mem::forget(context); // Do not free the context
}

use std::sync::Arc;

/// Low level wrapper around the librdkafka consumer. This consumer requires to be periodically polled
/// to make progress on rebalance, callbacks and to receive messages. The consumer can be cheaply
/// cloned to create a new reference to the same underlying consumer.
pub struct BaseConsumer<C: ConsumerContext> {
    client: Arc<Client<C>>,
}

impl<C: ConsumerContext> Clone for BaseConsumer<C> {
    fn clone(&self) -> Self {
        BaseConsumer { client: Arc::clone(&self.client) }
    }
}

impl<C: ConsumerContext> Consumer<C> for BaseConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        self
    }
}

impl FromClientConfig for BaseConsumer<EmptyConsumerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer<EmptyConsumerContext>> {
        BaseConsumer::from_config_and_context(config, EmptyConsumerContext)
    }
}

/// Creates a new `BaseConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseConsumer<C>> {
        let native_config = config.create_native_config()?;
        unsafe {
            rdsys::rd_kafka_conf_set_rebalance_cb(native_config.ptr(), Some(native_rebalance_cb::<C>));
            rdsys::rd_kafka_conf_set_offset_commit_cb(native_config.ptr(), Some(native_commit_cb::<C>));
        }
        let client = Client::new(config, native_config, RDKafkaType::RD_KAFKA_CONSUMER, context)?;
        unsafe { rdsys::rd_kafka_poll_set_consumer(client.native_ptr()) };
        Ok(BaseConsumer { client: Arc::new(client) })
    }
}

impl<C: ConsumerContext> BaseConsumer<C> {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        let tp_list = TopicPartitionList::with_topics(topics).create_native_topic_partition_list();
        let ret_code = unsafe { rdsys::rd_kafka_subscribe(self.client.native_ptr(), tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Unsubscribe from previous subscription list.
    pub fn unsubscribe(&self) {
        unsafe { rdsys::rd_kafka_unsubscribe(self.client.native_ptr()) };
    }

    /// Manually assign topics and partitions to consume.
    pub fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let tp_list = assignment.create_native_topic_partition_list();
        let ret_code = unsafe { rdsys::rd_kafka_assign(self.client.native_ptr(), tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Polls the consumer for events. It won't block more than the specified timeout.
    pub fn poll(&self, timeout_ms: i32) -> KafkaResult<Option<Message>> {
        let message_ptr = unsafe { rdsys::rd_kafka_consumer_poll(self.client.native_ptr(), timeout_ms) };
        if message_ptr.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_ptr).err };
        if error.is_error() {
            return Err(
                match error {
                    rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                        KafkaError::PartitionEOF(unsafe { (*message_ptr).partition })
                    }
                    e => KafkaError::MessageConsumption(e),
                },
            );
        }
        let kafka_message = Message::new(message_ptr);
        Ok(Some(kafka_message))
    }

    /// Commits the provided list of partitions. The commit can be sync (blocking), or async.
    pub fn commit(&self, topic_partition_list: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        let tp_list = topic_partition_list.create_native_topic_partition_list();
        let error = unsafe {
            let e = rdsys::rd_kafka_commit(self.client.native_ptr(), tp_list, mode as i32);
            rdsys::rd_kafka_topic_partition_list_destroy(tp_list);
            e
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error))
        } else {
            Ok(())
        }
    }

    /// Commits the specified message. The commit can be sync (blocking), or async.
    pub fn commit_message(&self, message: &Message, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe { rdsys::rd_kafka_commit_message(self.client.native_ptr(), message.ptr(), mode as i32) };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error))
        } else {
            Ok(())
        }
    }

    /// Returns the current topic subscription.
    pub fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        let mut tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(0) };
        let error = unsafe { rdsys::rd_kafka_subscription(self.client.native_ptr(), &mut tp_list) };

        let result = if error.is_error() {
            Err(KafkaError::MetadataFetch(error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Returns the current partition assignment.
    pub fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        let mut tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(0) };
        let error = unsafe {
            rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list)
        };

        let result = if error.is_error() {
            Err(KafkaError::MetadataFetch(error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Retrieve committed offsets for topics and partitions.
    pub fn committed(&self, timeout_ms: i32) -> KafkaResult<TopicPartitionList> {
        let mut tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(0) };
        let assignment_error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list) };
        if assignment_error.is_error() {
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };
            return Err(KafkaError::MetadataFetch(assignment_error));
        }

        let committed_error = unsafe { rdsys::rd_kafka_committed(self.client.native_ptr(), tp_list, timeout_ms) };

        let result = if committed_error.is_error() {
            Err(KafkaError::MetadataFetch(committed_error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Lookup the offsets for this consumer's partitions by timestamp.
    pub fn offsets_for_timestamp(&self, timestamp: i64, timeout_ms: i32) -> KafkaResult<TopicPartitionList> {
        let mut tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(0) };
        let assignment_error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list) };
        if assignment_error.is_error() {
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };
            return Err(KafkaError::MetadataFetch(assignment_error));
        }

        // Set the timestamp we want in the offset field for every partition as librdkafka expects.
        let elements = unsafe { slice::from_raw_parts((*tp_list).elems, (*tp_list).cnt as usize) };
        for tp in elements {
            unsafe {
                // The timestamp is passed here as offset
                rdsys::rd_kafka_topic_partition_list_set_offset(tp_list, tp.topic, tp.partition, timestamp);
            }
        }

        // This call will then put the offset in the offset field of this topic partition list.
        let offset_for_times_error =
            unsafe { rdsys::rd_kafka_offsets_for_times(self.client.native_ptr(), tp_list, timeout_ms) };

        let result = if offset_for_times_error.is_error() {
            Err(KafkaError::MetadataFetch(offset_for_times_error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Retrieve current positions (offsets) for topics and partitions.
    pub fn position(&self) -> KafkaResult<TopicPartitionList> {
        let mut tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(0) };
        let error = unsafe {
            // TODO: improve error handling
            rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list);
            rdsys::rd_kafka_position(self.client.native_ptr(), tp_list)
        };

        let result = if error.is_error() {
            Err(KafkaError::MetadataFetch(error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    pub fn fetch_metadata(&self, topic: Option<&str>, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.client.fetch_metadata(topic, timeout_ms)
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        self.client
            .fetch_watermarks(topic, partition, timeout_ms)
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list(&self, group: Option<&str>, timeout_ms: i32) -> KafkaResult<GroupList> {
        self.client.fetch_group_list(group, timeout_ms)
    }
}

impl<C: ConsumerContext> Drop for BaseConsumer<C> {
    fn drop(&mut self) {
        trace!("Consumer drop");
        if Arc::strong_count(&self.client) == 1 {
            trace!("Destroying consumer");
            unsafe { rdsys::rd_kafka_consumer_close(self.client.native_ptr()) };
        }
    }
}
