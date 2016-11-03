extern crate librdkafka_sys as rdkafka;

use std::ffi::CString;
use std::ptr;

use config::KafkaConfig;
use error::KafkaError;
use util::cstr_to_owned;

pub enum KafkaClientType {
    Consumer,
    Producer
}

pub struct KafkaClient {
    pub ptr: *mut rdkafka::rd_kafka_t
}

unsafe impl Sync for KafkaClient {}
unsafe impl Send for KafkaClient {}

impl KafkaClient {
    pub fn new(config: &KafkaConfig, client_type: KafkaClientType) -> Result<KafkaClient, KafkaError> {
        let errstr = [0i8; 1024];
        let config_ptr = try!(config.create_kafka_config());
        let rd_kafka_type = match client_type {
            KafkaClientType::Consumer => rdkafka::rd_kafka_type_t::RD_KAFKA_CONSUMER,
            KafkaClientType::Producer => rdkafka::rd_kafka_type_t::RD_KAFKA_PRODUCER,
        };
        let client_ptr = unsafe {
            rdkafka::rd_kafka_new(rd_kafka_type, config_ptr, errstr.as_ptr() as *mut i8, errstr.len())
        };
        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreationError(cstr_to_owned(&errstr)));
        }
        Ok(KafkaClient { ptr: client_ptr })
    }
}

impl Drop for KafkaClient {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_destroy(self.ptr) };
        unsafe { rdkafka::rd_kafka_wait_destroyed(1000) };
    }
}


pub struct KafkaTopic {  // TODO: link its lifetime to client lifetime
    pub ptr: *mut rdkafka::rd_kafka_topic_t
}

impl KafkaTopic {
    pub fn new(client: &KafkaClient, name: &str) -> Result<KafkaTopic, KafkaError> {
        let name_ptr = CString::new(name).unwrap();
        let topic_ptr = unsafe {
            rdkafka::rd_kafka_topic_new(client.ptr, name_ptr.as_ptr(), ptr::null_mut())
        };
        if topic_ptr.is_null() {
            Err(KafkaError::TopicNameError(name.to_string()))
        } else {
            Ok(KafkaTopic { ptr: topic_ptr })
        }
    }
}

impl Drop for KafkaTopic {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_topic_destroy(self.ptr); }
    }
}
