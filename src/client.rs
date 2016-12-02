//! Common client funcionalities.
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;

use self::rdkafka::types::*;

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

/// A Context is an object that can store user-defined data and on which callbacks can be
/// defined. Refer to the list of methods to see which callbacks can currently be overridden.
/// The context must be thread safe. The context might be cloned and passed to other threads.
pub trait Context: Send + Sync {
    /// Implements the default rebalancing strategy, also calling the pre_rebalance and
    /// post_rebalance methods. If this method is overridden, it will be responsibility
    /// of the user to call them if needed.
    fn rebalance(&self,
                 native_client: &NativeClient,
                 err: RDKafkaRespErr,
                 partitions_ptr: *mut RDKafkaTopicPartitionList) {

        let rebalance = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                // TODO: this might be expensive
                let topic_partition_list = TopicPartitionList::from_rdkafka(partitions_ptr);
                Rebalance::Assign(topic_partition_list)
            },
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
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
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr, partitions_ptr);
                },
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                    rdkafka::rd_kafka_assign(native_client.ptr, ptr::null());
                },
                _ => {
                    rdkafka::rd_kafka_assign(native_client.ptr, ptr::null());
                }
            }
        }
        self.post_rebalance(&rebalance);
    }

    /// Pre-rebalance callback. This method will run before the rebalance, and it will receive the
    /// relabance information. This method is executed as part of the rebalance callback and should
    /// terminate its execution quickly.
    fn pre_rebalance(&self, _rebalance: &Rebalance) { }

    /// Post-rebalance callback. This method will run before the rebalance, and it will receive the
    /// relabance information. This method is executed as part of the rebalance callback and should
    /// terminate its execution quickly.
    fn post_rebalance(&self, _rebalance: &Rebalance) { }
}

/// An empty context that can be used when no context is needed.
pub struct EmptyContext;

impl Context for EmptyContext {}

impl EmptyContext {
    pub fn new() -> EmptyContext {
        EmptyContext {}
    }
}

/// Contains rebalance information.
#[derive(Debug)]
pub enum Rebalance {
    Assign(TopicPartitionList),
    Revoke,
    Error(String)
}

/// Native rebalance callback. This callback will run on every rebalance, and it will call the
/// rebalance method defined in the current `Context`.
unsafe extern "C" fn rebalance_cb<C: Context>(rk: *mut RDKafka,
                                              err: RDKafkaRespErr,
                                              partitions: *mut RDKafkaTopicPartitionList,
                                              opaque_ptr: *mut c_void) {
    let context: &C = &*(opaque_ptr as *const C);
    let native_client = NativeClient{ptr: rk};

    context.rebalance(&native_client, err, partitions);
}

/// A native rdkafka-sys client.
pub struct NativeClient {
    ptr: *mut RDKafka,
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

/// An rdkafka client.
pub struct Client<C: Context> {
    native: NativeClient,
    context: Box<C>,
}

/// Delivery callback function type.
pub type DeliveryCallback = unsafe extern "C" fn(*mut RDKafka, *const RDKafkaMessage, *mut c_void);

impl<C: Context> Client<C> {
    /// Creates a new Client given a configuration, a client type and a context.
    pub fn new(config: &ClientConfig, client_type: ClientType, context: C) -> KafkaResult<Client<C>> {
        let errstr = [0i8; 1024];
        let config_ptr = try!(config.create_native_config());
        let rd_kafka_type = match client_type {
            ClientType::Consumer => {
                unsafe { rdkafka::rd_kafka_conf_set_rebalance_cb(config_ptr, Some(rebalance_cb::<C>)) };
                RDKafkaType::RD_KAFKA_CONSUMER
            },
            ClientType::Producer => {
                if config.get_delivery_cb().is_some() {
                    unsafe { rdkafka::rd_kafka_conf_set_dr_msg_cb(config_ptr, config.get_delivery_cb()) };
                }
                RDKafkaType::RD_KAFKA_PRODUCER
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

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn get_ptr(&self) -> *mut RDKafka {
        self.native.ptr
    }

    /// Returns a reference to the context.
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
    ptr: *mut RDKafkaTopic,
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

    /// Get topic's name
    pub fn get_name(&self) -> String {
        unsafe {
            cstr_to_owned(rdkafka::rd_kafka_topic_name(self.ptr))
        }
    }

    /// Returns a pointer to the correspondent rdkafka `rd_kafka_topic_t` stuct.
    pub fn get_ptr(&self) -> *mut RDKafkaTopic {
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

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    use config::{ClientConfig,TopicConfig};
    use super::*;

    #[test]
    fn test_client() {
        let client = Client::new(&ClientConfig::new(), ClientType::Consumer, EmptyContext::new()).unwrap();
        assert!(!client.get_ptr().is_null());
    }

    #[test]
    fn test_topic() {
        let client = Client::new(&ClientConfig::new(), ClientType::Consumer, EmptyContext::new()).unwrap();
        let topic = Topic::new(&client, "topic_name", &TopicConfig::new()).unwrap();
        assert_eq!(topic.get_name(), "topic_name");
        assert!(!topic.get_ptr().is_null());
    }
}
