//! Common client funcionalities.
extern crate futures;
extern crate librdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
use std::collections::HashMap;
use std::marker::PhantomData;

use config::{Config, TopicConfig};
use error::{IsError, Error};
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
    pub fn new(config: &Config, client_type: ClientType) -> Result<Client, Error> {
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
            return Err(Error::ClientCreation(descr));
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
    pub ptr: *mut rdkafka::rd_kafka_topic_t,
    phantom: PhantomData<&'a u8>, // Refers to client, should we use the client instead?
}

impl<'a> Topic<'a> {
    /// Creates the Topic.
    pub fn new(client: &'a Client, name: &str, topic_config: &TopicConfig) -> Result<Topic<'a>, Error> {
        let name_ptr = CString::new(name.to_string()).unwrap();
        let config_ptr = try!(topic_config.create_native_config());
        let topic_ptr = unsafe { rdkafka::rd_kafka_topic_new(client.ptr, name_ptr.as_ptr(), config_ptr) };
        if topic_ptr.is_null() {
            Err(Error::TopicName(name.to_string())) //TODO: TopicCreationError
        } else {
            let topic = Topic {
                ptr: topic_ptr,
                phantom: PhantomData,
            };
            Ok(topic)
        }
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
