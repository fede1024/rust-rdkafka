//! Common client functionalities.
use crate::rdsys;
use crate::rdsys::types::*;

use std::ffi::{CStr, CString};
use std::mem;
use std::os::raw::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::slice;
use std::string::ToString;
use std::time::Duration;

use serde_json;

use crate::config::{ClientConfig, NativeClientConfig, RDKafkaLogLevel};
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::groups::GroupList;
use crate::metadata::Metadata;
use crate::statistics::Statistics;
use crate::util::{timeout_to_ms, ErrBuf};

/// Client-level context
///
/// Each client (consumers and producers included) has a context object that can be used to
/// customize its behavior. Implementing `ClientContext` enables the customization of
/// methods common to all clients, while `ProducerContext` and `ConsumerContext` are specific to
/// producers and consumers. Refer to the list of methods to see which callbacks can currently
/// be overridden. Implementations of `ClientContext` must be thread safe, as they might be owned by
/// multiple threads.
pub trait ClientContext: Send + Sync {
    /// Receives log lines from librdkafka.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Warning => warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Notice => info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Info => info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Debug => debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message),
        }
    }

    /// Receives the statistics of the librdkafka client. To enable, the
    /// "statistics.interval.ms" configuration parameter must be specified.
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:?}", statistics);
    }

    /// Receives global errors from the librdkafka client.
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }

    // NOTE: when adding a new method, remember to add it to the FutureProducerContext as well.
    // https://github.com/rust-lang/rfcs/pull/1406 will maybe help in the future.
}

/// An empty `ClientContext` that can be used when no context is needed. Default
/// callback implementations will be used.
#[derive(Clone, Default)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

//
// ********** CLIENT **********
//

/// A native rdkafka-sys client. This struct shouldn't be used directly. Use higher level `Client`
/// or producers and consumers.
pub struct NativeClient {
    ptr: *mut RDKafka,
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

impl NativeClient {
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafka) -> NativeClient {
        NativeClient { ptr }
    }

    /// Returns the wrapped pointer to RDKafka.
    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr
    }
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        trace!("Destroying client: {:p}", self.ptr);
        unsafe {
            rdsys::rd_kafka_destroy(self.ptr);
        }
        trace!("Client destroyed: {:?}", self.ptr);
    }
}

/// A low level rdkafka client. This client shouldn't be used directly. The producer and consumer modules
/// provide different producer and consumer implementations based on top of `Client` that can be
/// used instead.
pub struct Client<C: ClientContext = DefaultClientContext> {
    native: NativeClient,
    context: Box<C>,
}

impl<C: ClientContext> Client<C> {
    /// Creates a new `Client` given a configuration, a client type and a context.
    pub fn new(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: C,
    ) -> KafkaResult<Client<C>> {
        let mut err_buf = ErrBuf::new();
        let mut boxed_context = Box::new(context);
        unsafe {
            rdsys::rd_kafka_conf_set_opaque(
                native_config.ptr(),
                (&mut *boxed_context) as *mut C as *mut c_void,
            )
        };
        unsafe { rdsys::rd_kafka_conf_set_log_cb(native_config.ptr(), Some(native_log_cb::<C>)) };
        unsafe {
            rdsys::rd_kafka_conf_set_stats_cb(native_config.ptr(), Some(native_stats_cb::<C>))
        };
        unsafe {
            rdsys::rd_kafka_conf_set_error_cb(native_config.ptr(), Some(native_error_cb::<C>))
        };

        let client_ptr = unsafe {
            rdsys::rd_kafka_new(
                rd_kafka_type,
                native_config.ptr_move(),
                err_buf.as_mut_ptr(),
                err_buf.len(),
            )
        };
        trace!("Create new librdkafka client {:p}", client_ptr);

        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

        Ok(Client {
            native: unsafe { NativeClient::from_ptr(client_ptr) },
            context: boxed_context,
        })
    }

    /// Returns a reference to the native rdkafka-sys client.
    pub fn native_client(&self) -> &NativeClient {
        &self.native
    }

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn native_ptr(&self) -> *mut RDKafka {
        self.native.ptr
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &C {
        self.context.as_ref()
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    pub fn fetch_metadata<T: Into<Option<Duration>>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        let mut metadata_ptr: *const RDKafkaMetadata = ptr::null_mut();
        let (flag, native_topic) = if let Some(topic_name) = topic {
            (0, Some(self.native_topic(topic_name)?))
        } else {
            (1, None)
        };
        trace!("Starting metadata fetch");
        let ret = unsafe {
            rdsys::rd_kafka_metadata(
                self.native_ptr(),
                flag,
                native_topic
                    .map(|t| t.ptr())
                    .unwrap_or_else(NativeTopic::null),
                &mut metadata_ptr as *mut *const RDKafkaMetadata,
                timeout_to_ms(timeout),
            )
        };
        trace!("Metadata fetch completed");
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }

        Ok(unsafe { Metadata::from_ptr(metadata_ptr) })
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks<T: Into<Option<Duration>>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        let mut low = -1;
        let mut high = -1;
        let topic_c = CString::new(topic.to_string())?;
        let ret = unsafe {
            rdsys::rd_kafka_query_watermark_offsets(
                self.native_ptr(),
                topic_c.as_ptr(),
                partition,
                &mut low as *mut i64,
                &mut high as *mut i64,
                timeout_to_ms(timeout),
            )
        };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }
        Ok((low, high))
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list<T: Into<Option<Duration>>>(
        &self,
        group: Option<&str>,
        timeout: T,
    ) -> KafkaResult<GroupList> {
        // Careful with group_c getting freed before time
        let group_c = CString::new(group.map_or("".to_string(), ToString::to_string))?;
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
                timeout_to_ms(timeout),
            )
        };
        trace!("Group list fetch completed");
        if ret.is_error() {
            return Err(KafkaError::GroupListFetch(ret.into()));
        }

        Ok(unsafe { GroupList::from_ptr(group_list_ptr) })
    }

    /// Returns a NativeTopic from the current client. The NativeTopic shouldn't outlive the client
    /// it was generated from.
    fn native_topic(&self, topic: &str) -> KafkaResult<NativeTopic> {
        let topic_c = CString::new(topic.to_string())?;
        Ok(unsafe {
            NativeTopic::from_ptr(rdsys::rd_kafka_topic_new(
                self.native_ptr(),
                topic_c.as_ptr(),
                ptr::null_mut(),
            ))
        })
    }

    /// Returns a NativeQueue from the current client. The NativeQueue shouldn't
    /// outlive the client it was generated from.
    pub(crate) fn new_native_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_new(self.native_ptr())) }
    }
}

struct NativeTopic {
    ptr: *mut RDKafkaTopic,
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

impl NativeTopic {
    /// Wraps a pointer to an `RDKafkaTopic` object and returns a new `NativeTopic`.
    unsafe fn from_ptr(ptr: *mut RDKafkaTopic) -> NativeTopic {
        NativeTopic { ptr }
    }

    /// Returns the pointer to the librdkafka RDKafkaTopic structure.
    fn ptr(&self) -> *mut RDKafkaTopic {
        self.ptr
    }

    /// Returns a null pointer.
    fn null() -> *mut RDKafkaTopic {
        ptr::null::<u8>() as *mut RDKafkaTopic
    }
}

impl Drop for NativeTopic {
    fn drop(&mut self) {
        trace!("Destroying NativeTopic: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_topic_destroy(self.ptr);
        }
        trace!("NativeTopic destroyed: {:?}", self.ptr);
    }
}

pub(crate) struct NativeQueue {
    ptr: *mut RDKafkaQueue,
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeQueue {}
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    /// Wraps a pointer to an `RDKafkaQueue` object and returns a new
    /// `NativeQueue`.
    unsafe fn from_ptr(ptr: *mut RDKafkaQueue) -> NativeQueue {
        NativeQueue { ptr }
    }

    /// Returns the pointer to the librdkafka RDKafkaQueue structure.
    pub fn ptr(&self) -> *mut RDKafkaQueue {
        self.ptr
    }

    pub fn poll<T: Into<Option<Duration>>>(&self, t: T) -> *mut RDKafkaEvent {
        unsafe { rdsys::rd_kafka_queue_poll(self.ptr, timeout_to_ms(t)) }
    }
}

impl Drop for NativeQueue {
    fn drop(&mut self) {
        trace!("Destroying queue: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_queue_destroy(self.ptr);
        }
        trace!("Queue destroyed: {:?}", self.ptr);
    }
}

pub(crate) unsafe extern "C" fn native_log_cb<C: ClientContext>(
    client: *const RDKafka,
    level: i32,
    fac: *const c_char,
    buf: *const c_char,
) {
    let fac = CStr::from_ptr(fac).to_string_lossy();
    let log_message = CStr::from_ptr(buf).to_string_lossy();

    let context = Box::from_raw(rdsys::rd_kafka_opaque(client) as *mut C);
    (*context).log(
        RDKafkaLogLevel::from_int(level),
        fac.trim(),
        log_message.trim(),
    );
    mem::forget(context); // Do not free the context
}

pub(crate) unsafe extern "C" fn native_stats_cb<C: ClientContext>(
    _conf: *mut RDKafka,
    json: *mut c_char,
    json_len: usize,
    opaque: *mut c_void,
) -> i32 {
    let context = Box::from_raw(opaque as *mut C);

    let mut bytes_vec = Vec::new();
    bytes_vec.extend_from_slice(slice::from_raw_parts(json as *mut u8, json_len));
    let json_string = CString::from_vec_unchecked(bytes_vec).into_string();
    match json_string {
        Ok(json) => match serde_json::from_str(&json) {
            Ok(stats) => (*context).stats(stats),
            Err(e) => error!("Could not parse statistics JSON: {}", e),
        },
        Err(e) => error!("Statistics JSON string is not UTF-8: {:?}", e),
    }

    mem::forget(context); // Do not free the context

    0 // librdkafka will free the json buffer
}

pub(crate) unsafe extern "C" fn native_error_cb<C: ClientContext>(
    _client: *mut RDKafka,
    err: i32,
    reason: *const c_char,
    opaque: *mut c_void,
) {
    let err = rdsys::primitive_to_rd_kafka_resp_err_t(err)
        .expect("global error not an rd_kafka_resp_err_t");
    let error = KafkaError::Global(err.into());
    let reason = CStr::from_ptr(reason).to_string_lossy();

    let context = Box::from_raw(opaque as *mut C);
    (*context).error(error, reason.trim());
    mem::forget(context); // Do not free the context
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    use super::*;
    use crate::config::ClientConfig;
    use crate::rdsys::types::*;

    #[test]
    fn test_client() {
        let config = ClientConfig::new();
        let native_config = config.create_native_config().unwrap();
        let client = Client::new(
            &config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            DefaultClientContext,
        )
        .unwrap();
        assert!(!client.native_ptr().is_null());
    }
}
