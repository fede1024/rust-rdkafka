extern crate librdkafka_sys as rdkafka;
extern crate errno;

use error::{KafkaError, IsError};
use util::cstr_to_owned;

use config::KafkaConfig;
use std::ptr;
use std::ffi::CString;

use std::os::raw::c_void;

pub use config::CreateProducer;

pub struct KafkaTopic {
    topic_n: *mut rdkafka::rd_kafka_topic_t
}

pub struct KafkaProducer {
    client_n: *mut rdkafka::rd_kafka_t,
}

impl CreateProducer<KafkaProducer, KafkaError> for KafkaConfig {
    fn create_producer(&self) -> Result<KafkaProducer, KafkaError> {
        let errstr = [0i8; 1024];
        let rd_config = try!(self.create_kafka_config());
        unsafe {
            let client_n = rdkafka::rd_kafka_new(rdkafka::rd_kafka_type_t::RD_KAFKA_PRODUCER,
                                            rd_config,
                                            errstr.as_ptr() as *mut i8,
                                            errstr.len());
            if client_n.is_null() {
                return Err(KafkaError::ProducerCreationError(cstr_to_owned(&errstr)));
            }

            Ok(KafkaProducer { client_n: client_n })
        }
    }
}

impl KafkaProducer {
    pub fn new_topic(&self, name: &str) -> Result<KafkaTopic, KafkaError> {
        let name_n = CString::new(name).unwrap();
        let topic_n = unsafe {
            rdkafka::rd_kafka_topic_new(self.client_n, name_n.as_ptr(), ptr::null_mut())
        };
        if topic_n.is_null() {
            Err(KafkaError::TopicNameError(name.to_string()))
        } else {
            Ok(KafkaTopic { topic_n: topic_n })
        }
    }

    pub fn produce(&self, topic: &KafkaTopic, msg: &[u8]) -> Result<(), KafkaError> {
        let n = unsafe {
            rdkafka::rd_kafka_produce(topic.topic_n, 0, 2, msg.as_ptr() as *mut c_void, msg.len(), ptr::null_mut(), 0, ptr::null_mut())
        };
        if n != 0 {
            let errno = errno::errno().0 as i32;
            let kafka_error = unsafe { rdkafka::rd_kafka_errno2err(errno) };
            Err(KafkaError::MessageProductionError(kafka_error))
        } else {
            Ok(())
        }
    }

    pub fn broker_add(&mut self, brokers: &str) -> i32 {
        let brokers = CString::new(brokers).unwrap();
        unsafe { rdkafka::rd_kafka_brokers_add(self.client_n, brokers.as_ptr()) }
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_destroy(self.client_n) };
        unsafe { rdkafka::rd_kafka_wait_destroyed(1000) };
    }
}
