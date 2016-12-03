//! Common client funcionalities.
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;

use self::rdkafka::types::*;

use config::TopicConfig;
use error::{KafkaError, KafkaResult};
use util::bytes_cstr_to_owned;


pub trait DeliveryContext: Send + Sync + 'static {
    fn received();
}

/// A Context is an object that can store user-defined data and on which callbacks can be
/// defined. Refer to the list of methods to see which callbacks can currently be overridden.
/// The context must be thread safe. The context might be cloned and passed to other threads.
pub trait Context: Send + Sync { }

/// An empty context that can be used when no context is needed.
#[derive(Clone)]
pub struct EmptyDeliveryContext;

impl DeliveryContext for EmptyDeliveryContext {
    fn received() {}
}

/// An empty context that can be used when no context is needed.
#[derive(Clone)]
pub struct EmptyContext;

impl Context for EmptyContext { }

impl EmptyContext {
    pub fn new() -> EmptyContext {
        EmptyContext {}
    }
}

/// A native rdkafka-sys client.
pub struct NativeClient {
    ptr: *mut RDKafka,
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

impl NativeClient {
    pub fn new(ptr: *mut RDKafka) -> NativeClient {
        NativeClient{ptr: ptr}
    }

    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr
    }
}

/// An rdkafka client.
pub struct Client<C: Context> {
    native: NativeClient,
    context: Box<C>,
}

impl<C: Context> Client<C> {
    /// Creates a new Client given a configuration, a client type and a context.
    pub fn new(config_ptr: *mut RDKafkaConf, rd_kafka_type: RDKafkaType, context: C) -> KafkaResult<Client<C>> {
        let errstr = [0i8; 1024];
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

    /// Returns a pointer to the correspondent rdkafka `RDKafkaTopic` stuct.
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
