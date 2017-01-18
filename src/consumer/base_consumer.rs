//! Low level consumer wrapper.
extern crate rdkafka_sys as rdkafka;
extern crate futures;

use self::rdkafka::types::*;

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

/// Low level wrapper around the librdkafka consumer. This consumer requires to be periodically polled
/// to make progress on rebalance, callbacks and to receive messages.
pub struct BaseConsumer<C: ConsumerContext> {
    client: Client<C>,
}

impl<C: ConsumerContext> Consumer<C> for BaseConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        self
    }

    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer<C> {
        self
    }
}

impl FromClientConfig for BaseConsumer<EmptyConsumerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer<EmptyConsumerContext>> {
        BaseConsumer::from_config_and_context(config, EmptyConsumerContext)
    }
}

/// Creates a new BaseConsumer starting from a ClientConfig.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseConsumer<C>> {
        let config_ptr = try!(config.create_native_config());
        unsafe { rdkafka::rd_kafka_conf_set_rebalance_cb(config_ptr, Some(rebalance_cb::<C>)) };
        let client = try!(Client::new(config_ptr, RDKafkaType::RD_KAFKA_CONSUMER, context));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.native_ptr()) };
        Ok(BaseConsumer { client: client })
    }
}

impl<C: ConsumerContext> BaseConsumer<C> {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&mut self, topics: &Vec<&str>) -> KafkaResult<()> {
        let tp_list = TopicPartitionList::with_topics(topics).create_native_topic_partition_list();
        let ret_code = unsafe { rdkafka::rd_kafka_subscribe(self.client.native_ptr(), tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error))
        };
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Unsubscribe from previous subscription list.
    pub fn unsubscribe(&mut self) {
        unsafe { rdkafka::rd_kafka_unsubscribe(self.client.native_ptr()) };
    }

    /// Manually assign topics and partitions to consume.
    pub fn assign(&mut self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let tp_list = assignment.create_native_topic_partition_list();
        let ret_code = unsafe { rdkafka::rd_kafka_assign(self.client.native_ptr(), tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error))
        };
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Pause consuming of this consumer.
    pub fn pause(&self) {
        unsafe {
            let mut tp_list = rdkafka::rd_kafka_topic_partition_list_new(0);
            rdkafka::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list);
            rdkafka::rd_kafka_pause_partitions(self.client.native_ptr(), tp_list);
            rdkafka::rd_kafka_topic_partition_list_destroy(tp_list);
        }
    }

    /// Resume consuming of this consumer.
    pub fn resume(&self) {
        unsafe {
            let mut tp_list = rdkafka::rd_kafka_topic_partition_list_new(0);
            rdkafka::rd_kafka_assignment(self.client.native_ptr(), &mut tp_list);
            rdkafka::rd_kafka_resume_partitions(self.client.native_ptr(), tp_list);
            rdkafka::rd_kafka_topic_partition_list_destroy(tp_list);
        }
    }

    /// Returns a list of topics or topic patterns the consumer is subscribed to.
    pub fn get_subscriptions(&self) -> TopicPartitionList {
        let mut tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(0) };
        unsafe { rdkafka::rd_kafka_subscription(self.client.native_ptr(), &mut tp_list as *mut *mut RDKafkaTopicPartitionList) };
        TopicPartitionList::from_rdkafka(tp_list)
    }

    /// Polls the consumer for events. It won't block more than the specified timeout.
    pub fn poll(&self, timeout_ms: i32) -> KafkaResult<Option<Message>> {
        let message_ptr = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.native_ptr(), timeout_ms) };
        if message_ptr.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_ptr).err };
        if error.is_error() {
            return Err(match error {
                rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    KafkaError::PartitionEOF(unsafe { (*message_ptr).partition } )
                },
                e => KafkaError::MessageConsumption(e)
            })
        }
        let kafka_message = Message::new(message_ptr);
        Ok(Some(kafka_message))
    }

    /// Commits the provided list of partitions. The commit can be sync (blocking), or async.
    pub fn commit(&self, topic_partition_list: &TopicPartitionList, mode: CommitMode) {
        let tp_list = topic_partition_list.create_native_topic_partition_list();
        unsafe {
            rdkafka::rd_kafka_commit(self.client.native_ptr(), tp_list, mode as i32);
            rdkafka::rd_kafka_topic_partition_list_destroy(tp_list);
        }
    }

    /// Commits the specified message. The commit can be sync (blocking), or async.
    pub fn commit_message(&self, message: &Message, mode: CommitMode) {
        unsafe { rdkafka::rd_kafka_commit_message(self.client.native_ptr(), message.ptr(), mode as i32) };
    }

    /// Returns the metadata information for all the topics in the cluster.
    pub fn fetch_metadata(&self, timeout_ms: i32) -> KafkaResult<Metadata> {
        self.client.fetch_metadata(timeout_ms)
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        self.client.fetch_watermarks(topic, partition, timeout_ms)
    }
}

impl<C: ConsumerContext> Drop for BaseConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.native_ptr()) };
    }
}
