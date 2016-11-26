//! Common client funcionalities.
extern crate futures;
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
use std::marker::PhantomData;

use config::{ClientConfig, TopicConfig};
use error::{KafkaError, KafkaResult};
use util::bytes_cstr_to_owned;

/// Specifies the type of client.
pub enum ClientType {
    /// A librdkafka consumer
    Consumer,
    /// A librdkafka producer
    Producer,
}

/// A librdkafka client.
pub struct Client {
    pub ptr: *mut rdkafka::rd_kafka_t,
}

unsafe impl Sync for Client {}
unsafe impl Send for Client {}

/// Delivery callback function type.
pub type DeliveryCallback =
    unsafe extern "C" fn(*mut rdkafka::rd_kafka_t,
                         *const rdkafka::rd_kafka_message_t,
                         *mut c_void);

impl Client {
    /// Creates a new Client given a configuration and a client type.
    pub fn new(config: &ClientConfig, client_type: ClientType) -> KafkaResult<Client> {
        let errstr = [0i8; 1024];
        let config_ptr = try!(config.create_native_config());
        let rd_kafka_type = match client_type {
            ClientType::Consumer => rdkafka::rd_kafka_type_t::RD_KAFKA_CONSUMER,
            ClientType::Producer => {
                if config.get_delivery_cb().is_some() {
                    unsafe { rdkafka::rd_kafka_conf_set_dr_msg_cb(config_ptr, config.get_delivery_cb()) };
                }
                rdkafka::rd_kafka_type_t::RD_KAFKA_PRODUCER
            }
        };
        let client_ptr =
            unsafe { rdkafka::rd_kafka_new(rd_kafka_type, config_ptr, errstr.as_ptr() as *mut i8, errstr.len()) };
        if client_ptr.is_null() {
            let descr = unsafe { bytes_cstr_to_owned(&errstr) };
            return Err(KafkaError::ClientCreation(descr));
        }
        Ok(Client { ptr: client_ptr })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka");
        unsafe { rdkafka::rd_kafka_destroy(self.ptr) };
        unsafe { rdkafka::rd_kafka_wait_destroyed(1000) };
    }
}

/// Represents a Kafka topic with an associated producer.
pub struct Topic<'a> {
    ptr: *mut rdkafka::rd_kafka_topic_t,
    client: &'a Client,
}

impl<'a> Topic<'a> {
    /// Creates the Topic.
    pub fn new(client: &'a Client, name: &str, topic_config: &TopicConfig) -> KafkaResult<Topic<'a>> {
        let name_ptr = CString::new(name.to_string()).unwrap();
        let config_ptr = try!(topic_config.create_native_config());
        let topic_ptr = unsafe { rdkafka::rd_kafka_topic_new(client.ptr, name_ptr.as_ptr(), config_ptr) };
        if topic_ptr.is_null() {
            Err(KafkaError::TopicCreation(name.to_string()))
        } else {
            let topic = Topic {
                ptr: topic_ptr,
                client: client,
            };
            Ok(topic)
        }
    }

    /// Returns a pointer to the correspondent rdkafka `rd_kafka_topic_t` stuct.
    pub fn get_ptr(&self) -> *mut rdkafka::rd_kafka_topic_t {
        self.ptr
    }
}

impl<'a> Drop for Topic<'a> {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka_topic");
        unsafe {
            rdkafka::rd_kafka_topic_destroy(self.ptr);
        }
    }
}
