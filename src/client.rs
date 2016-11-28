//! Common client funcionalities.
extern crate futures;
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
use std::mem;
use std::ptr;

use config::{ClientConfig, TopicConfig};
use error::{KafkaError, KafkaResult};
use topic_partition_list::TopicPartitionList;
use util::{bytes_cstr_to_owned,cstr_to_owned};

/// Specifies the type of client.
pub enum ClientType {
    /// A librdkafka consumer
    Consumer,
    /// A librdkafka producer
    Producer,
}

pub struct Opaque {
    pub rebalances: Vec<Rebalance>
}

impl Opaque {
    pub fn new() -> Opaque {
        Opaque {
            rebalances: Vec::new()
        }
    }

    pub fn rebalance_callback(&mut self, rebalance: Rebalance) {
        self.rebalances.push(rebalance);
    }
}

pub enum Rebalance {
    Assign(TopicPartitionList),
    Revoke,
    Error(String)
}

unsafe extern "C" fn rebalance_cb(rk: *mut rdkafka::rd_kafka_t,
                                  err: rdkafka::rd_kafka_resp_err_t,
                                  partitions: *mut rdkafka::rd_kafka_topic_partition_list_t,
                                  opaque_ptr: *mut c_void) {
    // Get our opaque for callbacks
    let mut opaque = Box::from_raw(opaque_ptr as *mut Opaque);

    // Handle callback
    match err {
        rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
            rdkafka::rd_kafka_assign(rk, partitions);
            let topic_partition_list = TopicPartitionList::from_rdkafka(partitions);
            opaque.rebalance_callback(Rebalance::Assign(topic_partition_list));
        },
        rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
            rdkafka::rd_kafka_assign(rk, ptr::null());
            opaque.rebalance_callback(Rebalance::Revoke);
        },
        _ => {
            let error = cstr_to_owned(rdkafka::rd_kafka_err2str(err));
            error!("Error rebalancing: {}", error);
            rdkafka::rd_kafka_assign(rk, ptr::null());
            opaque.rebalance_callback(Rebalance::Error(error));
        }
    }

    // This will only be cleaned up in the client's drop
    mem::forget(opaque);
}

/// A librdkafka client.
pub struct Client {
    pub ptr: *mut rdkafka::rd_kafka_t,
    pub opaque_ptr: *mut Opaque
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
            ClientType::Consumer => {
                if config.is_rebalance_tracking_enabled() {
                    unsafe { rdkafka::rd_kafka_conf_set_rebalance_cb(config_ptr, Some(rebalance_cb)) };
                }
                rdkafka::rd_kafka_type_t::RD_KAFKA_CONSUMER
            },
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

        // Set opaque to handle callbacks
        let opaque = Box::new(Opaque::new());
        let opaque_ptr = Box::into_raw(opaque);
        unsafe { rdkafka::rd_kafka_conf_set_opaque(config_ptr, opaque_ptr as *mut c_void) };

        Ok(Client {
            ptr: client_ptr,
            opaque_ptr: opaque_ptr
        })
    }

    /// Take rebalance events that were recorded. This only returns results if
    /// rebalance tracking has been enabled in the config for this client.
    pub fn take_rebalances(&mut self) -> Vec<Rebalance> {
        let mut opaque = unsafe { Box::from_raw(self.opaque_ptr as *mut Opaque) };

        let rebalances = opaque.rebalances.drain(0..).collect();

        // This will only be cleaned up in the client's drop
        mem::forget(opaque);

        rebalances
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka");
        unsafe {
            rdkafka::rd_kafka_destroy(self.ptr);
            rdkafka::rd_kafka_wait_destroyed(1000);
            Box::from_raw(self.opaque_ptr);
        }
    }
}

/// Represents a Kafka topic with an associated producer.
pub struct Topic<'a> {
    ptr: *mut rdkafka::rd_kafka_topic_t,
    _client: &'a Client,
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
                _client: client,
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
