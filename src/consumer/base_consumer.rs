extern crate rdkafka_sys as rdkafka;
extern crate futures;

use std::ffi::CString;
use std::str;

use client::{Client, ClientType};
use config::{FromClientConfig, ClientConfig};
use consumer::{Consumer, CommitMode};
use error::{KafkaError, KafkaResult, IsError};
use message::Message;
use util::cstr_to_owned;


/// A BaseConsumer client.
pub struct BaseConsumer {
    client: Client,
}

impl Consumer for BaseConsumer {
    fn get_base_consumer(&self) -> &BaseConsumer {
        self
    }

    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer {
        self
    }
}

/// Creates a new BaseConsumer starting from a ClientConfig.
impl FromClientConfig for BaseConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer> {
        let client = try!(Client::new(config, ClientType::Consumer));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(BaseConsumer { client: client })
    }
}

impl BaseConsumer {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&mut self, topics: &Vec<&str>) -> KafkaResult<()> {
        let tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(topics.len() as i32) };
        for &topic in topics {
            let topic_c = CString::new(topic).unwrap();
            let ret_code = unsafe {
                rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_c.as_ptr(), -1);
                rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list)
            };
            if ret_code.is_error() {
                return Err(KafkaError::Subscription(topic.to_string()))
            };
        }
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Unsubscribe from previous subscription list.
    pub fn unsubscribe(&mut self) {
        unsafe { rdkafka::rd_kafka_unsubscribe(self.client.ptr) };
    }

    /// Returns a vector of topics or topic patterns the consumer is subscribed to.
    pub fn get_subscriptions(&self) -> Vec<String> {
        let mut tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(0) };
        unsafe { rdkafka::rd_kafka_subscription(self.client.ptr, &mut tp_list as *mut *mut rdkafka::rd_kafka_topic_partition_list_t) };

        let mut tp_res = Vec::new();
        for i in 0..unsafe { (*tp_list).cnt } {
            let elem = unsafe { (*tp_list).elems.offset(i as isize) };
            let topic_name = unsafe { cstr_to_owned((*elem).topic) };
            tp_res.push(topic_name);
        }
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        tp_res
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
}

impl Drop for BaseConsumer {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}
