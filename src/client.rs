//! Common client functionalities.
use rdsys;
use rdsys::types::*;

use std::ffi::{CString, CStr};
use std::mem;
use std::os::raw::c_void;
use std::ptr;

use config::{ClientConfig, NativeClientConfig, RDKafkaLogLevel};
use error::{IsError, KafkaError, KafkaResult};
use groups::GroupList;
use metadata::Metadata;
use util::bytes_cstr_to_owned;


/// A Context is an object that can store user-defined data and on which callbacks can be
/// defined. Refer to the list of methods to see which callbacks can currently be overridden.
/// The context must be thread safe, and might be owned by multiple threads.
pub trait Context: Send + Sync {
    /// Receives log lines from librdkafka.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg => error!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Alert => error!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Critical => error!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Error => error!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Warning => warn!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Notice => info!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Info => info!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Debug => debug!("librdkafka: {} {}", fac, log_message),
        }
    }

    /// Receives the statistics of the librdkafka client in JSON format. To enable, the
    /// "statistics.interval.ms" configuration parameter must be specified.
    fn stats(&self, json: String) {
        println!("Client stats: {}", json);
    }

    // NOTE: when adding a new method, remember to add it to the FutureProducerContext as well.
    // https://github.com/rust-lang/rfcs/pull/1406 will maybe help in the future.
}

/// An empty context that can be used when no context is needed.
#[derive(Clone, Default)]
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
        NativeClient {ptr: ptr}
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
    /// Creates a new `Client` given a configuration, a client type and a context.
    pub fn new(config: &ClientConfig, native_config: NativeClientConfig, rd_kafka_type: RDKafkaType,
               context: C)
            -> KafkaResult<Client<C>> {
        let errstr = [0i8; 1024];
        let mut boxed_context = Box::new(context);
        unsafe { rdsys::rd_kafka_conf_set_opaque(
            native_config.ptr(), (&mut *boxed_context) as *mut C as *mut c_void) };
        unsafe { rdsys::rd_kafka_conf_set_log_cb(native_config.ptr(), Some(native_log_cb::<C>)) };
        unsafe { rdsys::rd_kafka_conf_set_stats_cb(native_config.ptr(), Some(native_stats_cb::<C>)) };

        let client_ptr = unsafe { rdsys::rd_kafka_new(
            rd_kafka_type, native_config.ptr_mut(), errstr.as_ptr() as *mut i8, errstr.len()) };
        if client_ptr.is_null() {
            let descr = unsafe { bytes_cstr_to_owned(&errstr) };
            return Err(KafkaError::ClientCreation(descr));
        }

        unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

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
            rdsys::rd_kafka_metadata(
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
        let topic_c = CString::new(topic.to_string())?;
        let ret = unsafe {
            rdsys::rd_kafka_query_watermark_offsets(self.native_ptr(), topic_c.as_ptr(), partition,
                                                      &mut low as *mut i64, &mut high as *mut i64, timeout_ms)
        };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret));
        }
        Ok((low, high))
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list(&self, group: Option<&str>, timeout_ms: i32) -> KafkaResult<GroupList> {
        // Careful with group_c getting freed before time
        let group_c = CString::new(group.map_or("".to_string(), |g| g.to_string())).unwrap();
        let group_c_ptr = if group.is_some() {
            group_c.as_ptr()
        } else {
            ptr::null_mut()
        };
        let mut group_list_ptr: *const RDKafkaGroupList = ptr::null_mut();
        trace!("Starting group list fetch");
        let ret = unsafe {
            rdsys::rd_kafka_list_groups(
                self.native_ptr(),
                group_c_ptr,
                &mut group_list_ptr as *mut *const RDKafkaGroupList,
                timeout_ms)
        };
        trace!("Group list fetch completed");
        if ret.is_error() {
            return Err(KafkaError::GroupListFetch(ret));
        }

        Ok(GroupList::from_ptr(group_list_ptr))
    }
}

impl<C: Context> Drop for Client<C> {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka");
        unsafe {
            rdsys::rd_kafka_destroy(self.native.ptr);
            rdsys::rd_kafka_wait_destroyed(1000);
        }
    }
}

pub unsafe extern "C" fn native_log_cb<C: Context>(
        client: *const RDKafka, level: i32,
        fac: *const i8, buf: *const i8
) {
    let fac = CStr::from_ptr(fac).to_string_lossy();
    let log_message = CStr::from_ptr(buf).to_string_lossy();

    let context = Box::from_raw(rdsys::rd_kafka_opaque(client) as *mut C);
    (*context).log(RDKafkaLogLevel::from_int(level), fac.trim(), log_message.trim());
    mem::forget(context);   // Do not free the context
}

pub unsafe extern "C" fn native_stats_cb<C: Context>(
        _conf: *mut RDKafka, json: *mut i8, json_len: usize,
        opaque: *mut c_void
) -> i32 {
    let json_string = String::from_raw_parts(json as *mut u8, json_len, json_len);

    let context = Box::from_raw(opaque as *mut C);
    (*context).stats(json_string);
    mem::forget(context);   // Do not free the context

    1 // librdkafka will not free the pointer
}


#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    extern crate rdkafka_sys as rdsys;
    use rdsys::types::*;
    use config::ClientConfig;
    use super::*;

    #[test]
    fn test_client() {
        let config = ClientConfig::new();
        let native_config = config.create_native_config().unwrap();
        let client = Client::new(&config, native_config, RDKafkaType::RD_KAFKA_PRODUCER,
                                 EmptyContext::new()).unwrap();
        assert!(!client.native_ptr().is_null());
    }
}
