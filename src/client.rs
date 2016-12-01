//! Common client funcionalities.
extern crate futures;
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
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

pub trait Context: Send + Sync {
    fn rebalance(&self,
                 native_client: &NativeClient,
                 err: rdkafka::rd_kafka_resp_err_t,
                 partitions_ptr: *mut rdkafka::rd_kafka_topic_partition_list_t) {

        let rebalance = match err {
            rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                // TODO: this might be expensive
                let topic_partition_list = TopicPartitionList::from_rdkafka(partitions_ptr);
                Rebalance::Assign(topic_partition_list)
            },
            rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                Rebalance::Revoke
            },
            _ => {
                let error = unsafe { cstr_to_owned(rdkafka::rd_kafka_err2str(err)) };
                error!("Error rebalancing: {}", error);
                Rebalance::Error(error)
            }
        };

        self.pre_rebalance(&rebalance);

        // Execute rebalance
        unsafe {
            match err {
                rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr, partitions_ptr);
                },
                rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr, ptr::null());
                },
                _ => {
                    rdkafka::rd_kafka_assign(native_client.ptr, ptr::null());
                }
            }
        }
        self.post_rebalance(&rebalance);
    }

    fn pre_rebalance(&self, _rebalance: &Rebalance) { }

    fn post_rebalance(&self, _rebalance: &Rebalance) { }
}

pub struct EmptyContext;

impl Context for EmptyContext {}

impl EmptyContext {
    pub fn new() -> EmptyContext {
        EmptyContext {}
    }
}

#[derive(Debug)]
pub enum Rebalance {
    Assign(TopicPartitionList),
    Revoke,
    Error(String)
}

unsafe extern "C" fn rebalance_cb<C: Context>(rk: *mut rdkafka::rd_kafka_t,
                                              err: rdkafka::rd_kafka_resp_err_t,
                                              partitions: *mut rdkafka::rd_kafka_topic_partition_list_t,
                                              opaque_ptr: *mut c_void) {
    let context: &C = &*(opaque_ptr as *const C);
    let native_client = NativeClient{ptr: rk};

    context.rebalance(&native_client, err, partitions);
}

pub struct NativeClient {
    ptr: *mut rdkafka::rd_kafka_t,
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

/// A librdkafka client.
pub struct Client<C: Context> {
    native: NativeClient,
    context: Box<C>,  // TODO: should be arc?
}

/// Delivery callback function type.
pub type DeliveryCallback =
    unsafe extern "C" fn(*mut rdkafka::rd_kafka_t,
                         *const rdkafka::rd_kafka_message_t,
                         *mut c_void);

impl<C: Context> Client<C> {
    /// Creates a new Client given a configuration and a client type.
    pub fn new(config: &ClientConfig, client_type: ClientType, context: C) -> KafkaResult<Client<C>> {
        let errstr = [0i8; 1024];
        let config_ptr = try!(config.create_native_config());
        let rd_kafka_type = match client_type {
            ClientType::Consumer => {
                if config.is_rebalance_tracking_enabled() {
                    unsafe { rdkafka::rd_kafka_conf_set_rebalance_cb(config_ptr, Some(rebalance_cb::<C>)) };
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

        let mut boxed_context = Box::new(context);
        unsafe { rdkafka::rd_kafka_conf_set_opaque(config_ptr, (&mut *boxed_context) as *mut C as *mut c_void) };

        let client_ptr =
            unsafe { rdkafka::rd_kafka_new(rd_kafka_type, config_ptr, errstr.as_ptr() as *mut i8, errstr.len()) };
        if client_ptr.is_null() {
            let descr = unsafe { bytes_cstr_to_owned(&errstr) };
            return Err(KafkaError::ClientCreation(descr));
        }

        Ok(Client {
            native: NativeClient { ptr: client_ptr },
            context: boxed_context,
        })
    }

    // TODO DOC
    pub fn get_ptr(&self) -> *mut rdkafka::rd_kafka_t {
        self.native.ptr
    }

    pub fn get_context(&self) -> &C {
        self.context.as_ref()
    }
}

impl<C: Context> Drop for Client<C> {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka");
        unsafe {
            rdkafka::rd_kafka_destroy(self.native.ptr);
            rdkafka::rd_kafka_wait_destroyed(1000);
        }
    }
}

/// Represents a Kafka topic with an associated producer.
pub struct Topic<'a, C: Context + 'a> {
    ptr: *mut rdkafka::rd_kafka_topic_t,
    _client: &'a Client<C>,
}

impl<'a, C: Context> Topic<'a, C> {
    /// Creates the Topic.
    pub fn new(client: &'a Client<C>, name: &str, topic_config: &TopicConfig) -> KafkaResult<Topic<'a, C>> {
        let name_ptr = CString::new(name.to_string()).unwrap();
        let config_ptr = try!(topic_config.create_native_config());
        let topic_ptr = unsafe { rdkafka::rd_kafka_topic_new(client.native.ptr, name_ptr.as_ptr(), config_ptr) };
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

impl<'a, C: Context> Drop for Topic<'a, C> {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka_topic");
        unsafe {
            rdkafka::rd_kafka_topic_destroy(self.ptr);
        }
    }
}
