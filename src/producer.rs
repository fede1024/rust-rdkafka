extern crate librdkafka_sys as rdkafka;
extern crate errno;

use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use config::CreateProducer;
use config::KafkaConfig;
use error::KafkaError;
use message::ToBytes;
use client::{KafkaClient, KafkaClientType, KafkaTopic};

#[derive(Clone)]
pub struct KafkaProducer {
    client: Arc<KafkaClient>
}

impl CreateProducer<KafkaProducer, KafkaError> for KafkaConfig {
    fn create_producer(&self) -> Result<KafkaProducer, KafkaError> {
        let client = try!(KafkaClient::new(&self, KafkaClientType::Producer));
        Ok(KafkaProducer { client: Arc::new(client) })
    }
}

impl KafkaProducer {
    pub fn get_topic(&self, topic_name: &str) -> Result<KafkaTopic, KafkaError> {
        KafkaTopic::new(&self.client, topic_name)
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
            rdkafka::rd_kafka_produce(topic.ptr, -1, rdkafka::RD_KAFKA_MSG_F_COPY as i32, payload_n, plen, key_n, klen, ptr::null_mut())
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
}

