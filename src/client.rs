//! Common client funcionalities.
extern crate rdkafka_sys as rdkafka;

use std::os::raw::c_void;
use std::ptr;
use std::ffi::CString;

use log::LogLevel;

use self::rdkafka::types::*;

use metadata::Metadata;
use error::{IsError, KafkaError, KafkaResult};
use util::bytes_cstr_to_owned;


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
            native: NativeClient::new(client_ptr),
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

    /// Returns the metadata information for all the topics in the cluster.
    pub fn fetch_metadata(&self, timeout_ms: i32) -> KafkaResult<Metadata> {
        let mut metadata_ptr: *const RDKafkaMetadata = ptr::null_mut();
        trace!("Starting metadata fetch");
        let ret = unsafe {
            rdkafka::rd_kafka_metadata(
                self.native_ptr(),
                1,   // All topics
                ptr::null::<u8>() as *mut RDKafkaTopic,
                &mut metadata_ptr as *mut *const RDKafkaMetadata,
                timeout_ms)
        };
        trace!("Metadata fetch completed");
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret));
        }

        Ok(Metadata::from_ptr(metadata_ptr))
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks(&self, topic: &str, partition: i32, timeout_ms: i32) -> KafkaResult<(i64, i64)> {
        let mut low = -1;
        let mut high = -1;
        let topic_c = try!(CString::new(topic.to_string()));
        let ret = unsafe {
            rdkafka::rd_kafka_query_watermark_offsets(self.native_ptr(), topic_c.as_ptr(), partition,
                                                      &mut low as *mut i64, &mut high as *mut i64, timeout_ms)
        };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret));
        }
        Ok((low, high))
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


#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    extern crate rdkafka_sys as rdkafka;
    use config::ClientConfig;
    use self::rdkafka::types::*;
    use super::*;

    #[test]
    fn test_client() {
        let config_ptr = ClientConfig::new().create_native_config().unwrap();
        let client = Client::new(config_ptr, RDKafkaType::RD_KAFKA_PRODUCER, EmptyContext::new()).unwrap();
        assert!(!client.native_ptr().is_null());
    }
}
