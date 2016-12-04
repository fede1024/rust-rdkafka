//! Common client funcionalities.
extern crate rdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;

use log::LogLevel;

use self::rdkafka::types::*;

use config::TopicConfig;
use error::{KafkaError, KafkaResult};
use util::{bytes_cstr_to_owned, cstr_to_owned};


/// A Context is an object that can store user-defined data and on which callbacks can be
/// defined. Refer to the list of methods to see which callbacks can currently be overridden.
/// The context must be thread safe, and might be owned by multiple threads.
pub trait Context: Send + Sync { }

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
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    pub fn new(ptr: *mut RDKafka) -> NativeClient {
        NativeClient{ptr: ptr}
    }

    /// Returns the wrapped pointer to RDKafka.
    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr
    }
}

/// A low level rdkafka client. This client shouldn't be used directly. The producer and consumer modules
/// provide different producer and consumer implementations based on top of `Client` that can be
/// used instead.
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

        // Set log level based on log crate's settings
        let log_level = if log_enabled!(LogLevel::Debug) {
            7
        } else if log_enabled!(LogLevel::Info) {
            5
        } else if log_enabled!(LogLevel::Warn) {
            4
        } else {
            3
        };
        unsafe { rdkafka::rd_kafka_set_log_level(client_ptr, log_level) };

        Ok(Client {
            native: NativeClient { ptr: client_ptr },
            context: boxed_context,
        })
    }

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn native_ptr(&self) -> *mut RDKafka {
        self.native.ptr
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &C {
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

    /// Returns a pointer to the correspondent rdkafka `RDKafkaTopic` stuct.
    pub fn ptr(&self) -> *mut RDKafkaTopic {
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
        let config_ptr = ClientConfig::new().create_native_config().unwrap();
        let client = Client::new(config_ptr, RDKafkaType::RD_KAFKA_PRODUCER, EmptyContext::new()).unwrap();
        assert!(!client.native_ptr().is_null());
    }

    #[test]
    fn test_topic() {
        let config_ptr = ClientConfig::new().create_native_config().unwrap();
        let client = Client::new(config_ptr, RDKafkaType::RD_KAFKA_CONSUMER, EmptyContext::new()).unwrap();
        let topic = Topic::new(&client, "topic_name", &TopicConfig::new()).unwrap();
        assert_eq!(topic.get_name(), "topic_name");
        assert!(!topic.ptr().is_null());
    }
}
