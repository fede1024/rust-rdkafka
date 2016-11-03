extern crate librdkafka_sys as rdkafka;

use std::ffi::CString;
use std::str;

use client::{KafkaClient, KafkaClientType};
use config::{CreateConsumer, KafkaConfig};
use error::{KafkaError, IsError};
use message::KafkaMessage;


pub struct KafkaConsumer {
    client: KafkaClient,
}

impl CreateConsumer<KafkaConsumer, KafkaError> for KafkaConfig {
    fn create_consumer(&self) -> Result<KafkaConsumer, KafkaError> {
        let client = try!(KafkaClient::new(&self, KafkaClientType::Consumer));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(KafkaConsumer{ client: client })
    }
}

impl KafkaConsumer {
    pub fn subscribe(&mut self, topic_name: &str) -> Result<(), KafkaError> {
        let topic_name_c = CString::new(topic_name).unwrap();
        let ret_code = unsafe {
            let tp_list = rdkafka::rd_kafka_topic_partition_list_new(1);
            rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_name_c.as_ptr(), 0);
            rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list)
        };
        if ret_code.is_error() {
            Err(KafkaError::SubscriptionError(topic_name.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn poll(&self, timeout_ms: i32) -> Result<Option<KafkaMessage>, KafkaError> {
        let message_n = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.ptr, timeout_ms) };
        if message_n.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_n).err };
        if error.is_error() {
            return Err(KafkaError::MessageConsumptionError(error));
        }
        let kafka_message = KafkaMessage { message_n: message_n };
        Ok(Some(kafka_message))
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}
