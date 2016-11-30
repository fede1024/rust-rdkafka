extern crate rdkafka_sys as rdkafka;
extern crate futures;

use std::str;

use client::{Client, ClientType, Context, Rebalance};
use config::{FromClientConfig, ClientConfig};
use consumer::{Consumer, CommitMode};
use error::{KafkaError, KafkaResult, IsError};
use message::Message;
use util::cstr_to_owned;
use topic_partition_list::TopicPartitionList;

/// A BaseConsumer client.
pub struct BaseConsumer<'a, C: Context + 'a> {
    client: Client<'a, C>,
}

impl<'a, C: Context + 'a> Consumer<'a, C> for BaseConsumer<'a, C> {
    fn get_base_consumer(&self) -> &BaseConsumer<'a, C> {
        self
    }

    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer<'a, C> {
        self
    }
}

/// Creates a new BaseConsumer starting from a ClientConfig.
impl<'a, C: Context + 'a> FromClientConfig<'a, C> for BaseConsumer<'a, C> {
    fn from_config(config: &ClientConfig, context: &'a C) -> KafkaResult<BaseConsumer<'a, C>> {
        let client = try!(Client::new(config, ClientType::Consumer, context));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(BaseConsumer { client: client })
    }
}

impl<'a, C: Context + 'a> BaseConsumer<'a, C> {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&mut self, topics: &TopicPartitionList) -> KafkaResult<()> {
        let tp_list = topics.create_native_topic_partition_list();
        let ret_code = unsafe { rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error))
        };
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Unsubscribe from previous subscription list.
    pub fn unsubscribe(&mut self) {
        unsafe { rdkafka::rd_kafka_unsubscribe(self.client.ptr) };
    }

    /// Returns a list of topics or topic patterns the consumer is subscribed to.
    pub fn get_subscriptions(&self) -> TopicPartitionList {
        let mut tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(0) };
        unsafe { rdkafka::rd_kafka_subscription(self.client.ptr, &mut tp_list as *mut *mut rdkafka::rd_kafka_topic_partition_list_t) };
        TopicPartitionList::from_rdkafka(tp_list)
    }

    /// Polls the consumer for events. It won't block more than the specified timeout.
    pub fn poll(&self, timeout_ms: i32) -> KafkaResult<Option<Message>> {
        let message_ptr = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.ptr, timeout_ms) };
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

    pub fn commit_message(&self, message: &Message, mode: CommitMode) -> () {
        let async = match mode {
            CommitMode::Sync => 0,
            CommitMode::Async => 1,
        };

        unsafe { rdkafka::rd_kafka_commit_message(self.client.ptr, message.ptr, async) };
    }

    /// Take rebalance events that were recorded. This only returns results if
    /// `track_rebalances` is on in the config for this client.
    pub fn take_rebalances(&mut self) -> Vec<Rebalance> {
        self.client.take_rebalances()
    }
}

impl<'a, C: Context + 'a> Drop for BaseConsumer<'a, C> {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}
