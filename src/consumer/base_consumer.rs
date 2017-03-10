//! Low level consumer wrapper.
use rdsys;
use rdsys::types::*;

use std::str;

use client::Client;
use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::{Consumer, ConsumerContext, CommitMode, EmptyConsumerContext};
use consumer::rebalance_cb;  // TODO: reorganize module
use error::{KafkaError, KafkaResult, IsError};
use metadata::Metadata;
use message::Message;
use util::cstr_to_owned;
use topic_partition_list::TopicPartitionList;
use groups::GroupList;

/// Low level wrapper around the librdkafka consumer. This consumer requires to be periodically polled
/// to make progress on rebalance, callbacks and to receive messages.
pub struct BaseConsumer<C: ConsumerContext> {
    client: Client<C>,
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
        let native_config = try!(config.create_native_config());
        unsafe { rdsys::rd_kafka_conf_set_rebalance_cb(native_config.ptr(), Some(rebalance_cb::<C>)) };
        let client = try!(Client::new(config, native_config, RDKafkaType::RD_KAFKA_CONSUMER, context));
        unsafe { rdsys::rd_kafka_poll_set_consumer(client.native_ptr()) };
        Ok(BaseConsumer { client: client })
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
            return Err(KafkaError::Subscription(error))
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
            return Err(KafkaError::Subscription(error))
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
            return Err(match error {
                rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    KafkaError::PartitionEOF(unsafe { (*message_ptr).partition } )
                },
                e => KafkaError::MessageConsumption(e)
            })
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
        let error = unsafe {
            rdsys::rd_kafka_subscription(self.client.native_ptr(), &mut tp_list)
        };

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
        let assignment_error = unsafe {
            rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list)
        };
        if assignment_error.is_error() {
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };
            return Err(KafkaError::MetadataFetch(assignment_error))
        }

        let committed_error = unsafe {
            rdsys::rd_kafka_committed(
                self.client.native_ptr(),
                tp_list,
                timeout_ms
            )
        };

        let result = if committed_error.is_error() {
            Err(KafkaError::MetadataFetch(committed_error))
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
            rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list);
            rdsys::rd_kafka_position(
                self.client.native_ptr(),
                tp_list
            )
        };

        let result = if error.is_error() {
            Err(KafkaError::MetadataFetch(error))
        } else {
            Ok(TopicPartitionList::from_rdkafka(tp_list))
        };
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(tp_list) };

        result
    }

    /// Returns the metadata information for all the topics in the cluster.
    pub fn fetch_metadata(&self, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.client.fetch_metadata(timeout_ms)
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        self.client.fetch_watermarks(topic, partition, timeout_ms)
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list(&self, group: Option<&str>, timeout_ms: i32) -> KafkaResult<GroupList> {
        self.client.fetch_group_list(group, timeout_ms)
    }
}

impl<C: ConsumerContext> Drop for BaseConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdsys::rd_kafka_consumer_close(self.client.native_ptr()) };
    }
}
