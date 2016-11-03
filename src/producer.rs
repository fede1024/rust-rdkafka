extern crate librdkafka_sys as rdkafka;
extern crate errno;

use error::{KafkaError, IsError};
use util::cstr_to_owned;

use config::KafkaConfig;
use std::ptr;
use std::ffi::CString;

use std::sync::Arc;

use std::os::raw::c_void;

use message::KafkaMessage;
use message::ToBytes;

pub use config::CreateProducer;


pub struct KafkaTopic {
    topic_n: *mut rdkafka::rd_kafka_topic_t
}

pub struct KafkaNativeClient {
    ptr: *mut rdkafka::rd_kafka_t
}
unsafe impl Sync for KafkaNativeClient {}
unsafe impl Send for KafkaNativeClient {}

#[derive(Clone)]
pub struct KafkaProducer {
    client: Arc<KafkaNativeClient>
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
            let native_client = KafkaNativeClient { ptr: client_n };
            Ok(KafkaProducer { client: Arc::new(native_client) })
        }
    }
}

impl KafkaProducer {
    pub fn new_topic(&self, name: &str) -> Result<KafkaTopic, KafkaError> {
        let name_n = CString::new(name).unwrap();
        let topic_n = unsafe {
            rdkafka::rd_kafka_topic_new(self.client.ptr, name_n.as_ptr(), ptr::null_mut())
        };
        if topic_n.is_null() {
            Err(KafkaError::TopicNameError(name.to_string()))
        } else {
            Ok(KafkaTopic { topic_n: topic_n })
        }
    }

    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.client.ptr, timeout_ms) }

    }

    pub fn send_copy(&self, topic: &KafkaTopic, payload: Option<&[u8]>, key: Option<&[u8]>) -> Result<(), KafkaError> {
        let (payload_n, plen) = match payload {
            None => (ptr::null_mut(), 0),
            Some(p) => (p.as_ptr() as *mut c_void, p.len())
        };
        let (key_n, klen) = match key {
            None => (ptr::null_mut(), 0),
            Some(k) => (k.as_ptr() as *mut c_void, k.len())
        };
        let n = unsafe {
            rdkafka::rd_kafka_produce(topic.topic_n, -1, rdkafka::RD_KAFKA_MSG_F_COPY as i32, payload_n, plen, key_n, klen, ptr::null_mut())
        };
        if n != 0 {
            let errno = errno::errno().0 as i32;
            let kafka_error = unsafe { rdkafka::rd_kafka_errno2err(errno) };
            Err(KafkaError::MessageProductionError(kafka_error))
        } else {
            Ok(())
        }
    }

    pub fn send_test<P, K>(&self, topic: &KafkaTopic, payload: Option<&P>, key: Option<&K>) -> Result<(), KafkaError>
        where K: ToBytes,
              P: ToBytes {
        self.send_copy(topic, payload.map(P::to_bytes), key.map(K::to_bytes))
    }

    pub fn broker_add(&mut self, brokers: &str) -> i32 {
        let brokers = CString::new(brokers).unwrap();
        unsafe { rdkafka::rd_kafka_brokers_add(self.client.ptr, brokers.as_ptr()) }
    }
}

impl Drop for KafkaNativeClient {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_destroy(self.ptr) };
        unsafe { rdkafka::rd_kafka_wait_destroyed(1000) };
    }
}
