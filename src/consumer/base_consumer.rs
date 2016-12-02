extern crate rdkafka_sys as rdkafka;
extern crate futures;

use self::rdkafka::types::*;

use std::str;

use client::Client;
use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::{Consumer, ConsumerContext, CommitMode, EmptyConsumerContext};
use consumer::rebalance_cb;  // TODO: reorganize module
use error::{KafkaError, KafkaResult, IsError};
use message::Message;
use util::cstr_to_owned;
use topic_partition_list::TopicPartitionList;

/// A BaseConsumer client.
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
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.get_ptr()) };
        Ok(BaseConsumer { client: client })
    }
}

impl<C: ConsumerContext> BaseConsumer<C> {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&mut self, topics: &TopicPartitionList) -> KafkaResult<()> {
        let tp_list = topics.create_native_topic_partition_list();
        let ret_code = unsafe { rdkafka::rd_kafka_subscribe(self.client.get_ptr(), tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error))
        };
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Unsubscribe from previous subscription list.
    pub fn unsubscribe(&mut self) {
        unsafe { rdkafka::rd_kafka_unsubscribe(self.client.get_ptr()) };
    }

    /// Returns a list of topics or topic patterns the consumer is subscribed to.
    pub fn get_subscriptions(&self) -> TopicPartitionList {
        let mut tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(0) };
        unsafe { rdkafka::rd_kafka_subscription(self.client.get_ptr(), &mut tp_list as *mut *mut RDKafkaTopicPartitionList) };
        TopicPartitionList::from_rdkafka(tp_list)
    }

    /// Polls the consumer for events. It won't block more than the specified timeout.
    pub fn poll(&self, timeout_ms: i32) -> KafkaResult<Option<Message>> {
        let message_ptr = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.get_ptr(), timeout_ms) };
        if message_ptr.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_ptr).err };
        if error.is_error() {
            return Err(KafkaError::MessageConsumption(error));
        }
        let kafka_message = Message::new(message_ptr);
        Ok(Some(kafka_message))
    }

    /// Commits the current message. The commit can be synk (blocking), or asynk.
    pub fn commit_message(&self, message: &Message, mode: CommitMode) -> () {
        let async = match mode {
            CommitMode::Sync => 0,
            CommitMode::Async => 1,
        };

        unsafe { rdkafka::rd_kafka_commit_message(self.client.get_ptr(), message.ptr, async) };
    }
}

impl<C: ConsumerContext> Drop for BaseConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.get_ptr()) };
    }
}
