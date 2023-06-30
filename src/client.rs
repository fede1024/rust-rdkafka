//! Common client functionality.
//!
//! In librdkafka parlance, a client is either a consumer or a producer. This
//! module's [`Client`] type provides the functionality that is common to both
//! consumers and producers.
//!
//! Typically you will not want to construct a client directly. Construct one of
//! the consumers in the [`consumer`] module or one of the producers in the
//! [`producer`] modules instead.
//!
//! [`consumer`]: crate::consumer
//! [`producer`]: crate::producer

use std::convert::TryFrom;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::slice;
use std::string::ToString;
use std::sync::Arc;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::config::{ClientConfig, NativeClientConfig, RDKafkaLogLevel};
use crate::consumer::RebalanceProtocol;
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::groups::GroupList;
use crate::log::{debug, error, info, trace, warn};
use crate::metadata::Metadata;
use crate::mocking::MockCluster;
use crate::statistics::Statistics;
use crate::util::{self, ErrBuf, KafkaDrop, NativePtr, Timeout};

/// Client-level context.
///
/// Each client (consumers and producers included) has a context object that can
/// be used to customize its behavior. Implementing `ClientContext` enables the
/// customization of methods common to all clients, while [`ProducerContext`]
/// and [`ConsumerContext`] are specific to producers and consumers. Refer to
/// the list of methods to see which callbacks can currently be overridden.
///
/// **Important**: implementations of `ClientContext` must be thread safe, as
/// they might be shared between multiple threads.
///
/// [`ConsumerContext`]: crate::consumer::ConsumerContext
/// [`ProducerContext`]: crate::producer::ProducerContext
pub trait ClientContext: Send + Sync {
    /// Whether to periodically refresh the SASL `OAUTHBEARER` token
    /// by calling [`ClientContext::generate_oauth_token`].
    ///
    /// If disabled, librdkafka's default token refresh callback is used
    /// instead.
    ///
    /// This parameter is only relevant when using the `OAUTHBEARER` SASL
    /// mechanism.
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;

    /// Receives log lines from librdkafka.
    ///
    /// The default implementation forwards the log lines to the appropriate
    /// [`log`] crate macro. Consult the [`RDKafkaLogLevel`] documentation for
    /// details about the log level mapping.
    ///
    /// [`log`]: https://docs.rs/log
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
        }
    }

    /// Receives the decoded statistics of the librdkafka client. To enable, the
    /// `statistics.interval.ms` configuration parameter must be specified.
    ///
    /// The default implementation logs the statistics at the `info` log level.
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:?}", statistics);
    }

    /// Receives the JSON-encoded statistics of the librdkafka client. To
    /// enable, the `statistics.interval.ms` configuration parameter must be
    /// specified.
    ///
    /// The default implementation calls [`ClientContext::stats`] with the
    /// decoded statistics, logging an error if the decoding fails.
    fn stats_raw(&self, statistics: &[u8]) {
        match serde_json::from_slice(statistics) {
            Ok(stats) => self.stats(stats),
            Err(e) => error!("Could not parse statistics JSON: {}", e),
        }
    }

    /// Receives global errors from the librdkafka client.
    ///
    /// The default implementation logs the error at the `error` log level.
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }

    /// Generates an OAuth token from the provided configuration.
    ///
    /// Override with an appropriate implementation when using the `OAUTHBEARER`
    /// SASL authentication mechanism. For this method to be called, you must
    /// also set [`ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`] to true.
    ///
    /// The `fmt::Display` implementation of the returned error must not
    /// generate a message with an embedded null character.
    ///
    /// The default implementation always returns an error and is meant to
    /// be overridden.
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        Err("Default implementation of generate_oauth_token must be overridden".into())
    }

    // NOTE: when adding a new method, remember to add it to the
    // FutureProducerContext as well.
    // https://github.com/rust-lang/rfcs/pull/1406 will maybe help in the
    // future.
}

/// An empty [`ClientContext`] that can be used when no customizations are
/// needed.
///
/// Uses the default callback implementations provided by `ClientContext`.
#[derive(Clone, Debug, Default)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

//
// ********** CLIENT **********
//

/// A native rdkafka-sys client. This struct shouldn't be used directly. Use
/// higher level `Client` or producers and consumers.
// TODO(benesch): this should be `pub(crate)`.
pub struct NativeClient {
    ptr: NativePtr<RDKafka>,
}

unsafe impl KafkaDrop for RDKafka {
    const TYPE: &'static str = "client";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

impl NativeClient {
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafka) -> NativeClient {
        NativeClient {
            ptr: NativePtr::from_ptr(ptr).unwrap(),
        }
    }

    /// Returns the wrapped pointer to RDKafka.
    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr.ptr()
    }

    pub(crate) fn rebalance_protocol(&self) -> RebalanceProtocol {
        let protocol = unsafe { rdsys::rd_kafka_rebalance_protocol(self.ptr()) };
        if protocol.is_null() {
            RebalanceProtocol::None
        } else {
            let protocol = unsafe { CStr::from_ptr(protocol) };
            match protocol.to_bytes() {
                b"NONE" => RebalanceProtocol::None,
                b"EAGER" => RebalanceProtocol::Eager,
                b"COOPERATIVE" => RebalanceProtocol::Cooperative,
                _ => unreachable!(),
            }
        }
    }
}

/// A low-level rdkafka client.
///
/// This type is the basis of the consumers and producers in the [`consumer`]
/// and [`producer`] modules, respectively.
///
/// Typically you do not want to construct a `Client` directly, but instead
/// construct a consumer or producer. A `Client` can be used, however, when
/// only access to cluster metadata and watermarks is required.
///
/// [`consumer`]: crate::consumer
/// [`producer`]: crate::producer
pub struct Client<C: ClientContext = DefaultClientContext> {
    native: NativeClient,
    context: Arc<C>,
}

impl<C: ClientContext> Client<C> {
    /// Creates a new `Client` given a configuration, a client type and a context.
    pub fn new(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: C,
    ) -> KafkaResult<Client<C>> {
        Self::new_context_arc(config, native_config, rd_kafka_type, Arc::new(context))
    }

    /// Creates a new `Client` given a configuration, a client type and a context.
    pub(crate) fn new_context_arc(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: Arc<C>,
    ) -> KafkaResult<Client<C>> {
        let mut err_buf = ErrBuf::new();
        unsafe {
            rdsys::rd_kafka_conf_set_opaque(
                native_config.ptr(),
                Arc::as_ptr(&context) as *mut c_void,
            )
        };
        unsafe { rdsys::rd_kafka_conf_set_log_cb(native_config.ptr(), Some(native_log_cb::<C>)) };
        unsafe {
            rdsys::rd_kafka_conf_set_stats_cb(native_config.ptr(), Some(native_stats_cb::<C>))
        };
        unsafe {
            rdsys::rd_kafka_conf_set_error_cb(native_config.ptr(), Some(native_error_cb::<C>))
        };
        if C::ENABLE_REFRESH_OAUTH_TOKEN {
            unsafe {
                rdsys::rd_kafka_conf_set_oauthbearer_token_refresh_cb(
                    native_config.ptr(),
                    Some(native_oauth_refresh_cb::<C>),
                )
            };
        }

        let client_ptr = unsafe {
            let native_config = ManuallyDrop::new(native_config);
            rdsys::rd_kafka_new(
                rd_kafka_type,
                native_config.ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        trace!("Create new librdkafka client {:p}", client_ptr);

        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

        Ok(Client {
            native: unsafe { NativeClient::from_ptr(client_ptr) },
            context,
        })
    }

    /// Returns a reference to the native rdkafka-sys client.
    pub fn native_client(&self) -> &NativeClient {
        &self.native
    }

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn native_ptr(&self) -> *mut RDKafka {
        self.native.ptr.ptr()
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &Arc<C> {
        &self.context
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    pub fn fetch_metadata<T: Into<Timeout>>(
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
                native_topic.map(|t| t.ptr()).unwrap_or_else(ptr::null_mut),
                &mut metadata_ptr as *mut *const RDKafkaMetadata,
                timeout.into().as_millis(),
            )
        };
        trace!("Metadata fetch completed");
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }

        Ok(unsafe { Metadata::from_ptr(metadata_ptr) })
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks<T: Into<Timeout>>(
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
                timeout.into().as_millis(),
            )
        };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }
        Ok((low, high))
    }

    /// Returns the cluster identifier option or None if the cluster identifier is null
    pub fn fetch_cluster_id<T: Into<Timeout>>(&self, timeout: T) -> Option<String> {
        let cluster_id =
            unsafe { rdsys::rd_kafka_clusterid(self.native_ptr(), timeout.into().as_millis()) };
        if cluster_id.is_null() {
            return None;
        }
        let buf = unsafe { CStr::from_ptr(cluster_id).to_bytes() };
        String::from_utf8(buf.to_vec()).ok()
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list<T: Into<Timeout>>(
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
                timeout.into().as_millis(),
            )
        };
        trace!("Group list fetch completed");
        if ret.is_error() {
            return Err(KafkaError::GroupListFetch(ret.into()));
        }

        Ok(unsafe { GroupList::from_ptr(group_list_ptr) })
    }

    /// Returns the first fatal error set on this client instance, or `None` if
    /// no fatal error has occurred.
    ///
    /// This function is intended to be used with idempotent producers, where
    /// some errors must logically be considered fatal to retain consistency.
    pub fn fatal_error(&self) -> Option<(RDKafkaErrorCode, String)> {
        let mut err_buf = ErrBuf::new();
        let code = unsafe {
            rdsys::rd_kafka_fatal_error(self.native_ptr(), err_buf.as_mut_ptr(), err_buf.capacity())
        };
        if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            None
        } else {
            Some((code.into(), err_buf.to_string()))
        }
    }

    /// If this client was configured with `test.mock.num.brokers`,
    /// this will return a [`MockCluster`] instance associated with this client,
    /// otherwise `None` is returned.
    ///
    /// [`MockCluster`]: crate::mocking::MockCluster
    pub fn mock_cluster(&self) -> Option<MockCluster<'_, C>> {
        MockCluster::from_client(self)
    }

    /// Returns a NativeTopic from the current client. The NativeTopic shouldn't outlive the client
    /// it was generated from.
    pub(crate) fn native_topic(&self, topic: &str) -> KafkaResult<NativeTopic> {
        let topic_c = CString::new(topic.to_string())?;
        Ok(unsafe {
            NativeTopic::from_ptr(rdsys::rd_kafka_topic_new(
                self.native_ptr(),
                topic_c.as_ptr(),
                ptr::null_mut(),
            ))
            .unwrap()
        })
    }

    /// Returns a NativeQueue from the current client. The NativeQueue shouldn't
    /// outlive the client it was generated from.
    pub(crate) fn new_native_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_new(self.native_ptr())).unwrap() }
    }

    pub(crate) fn consumer_queue(&self) -> Option<NativeQueue> {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_consumer(self.native_ptr())) }
    }
}

pub(crate) type NativeTopic = NativePtr<RDKafkaTopic>;

unsafe impl KafkaDrop for RDKafkaTopic {
    const TYPE: &'static str = "native topic";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_topic_destroy;
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

pub(crate) type NativeQueue = NativePtr<RDKafkaQueue>;

unsafe impl KafkaDrop for RDKafkaQueue {
    const TYPE: &'static str = "queue";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_queue_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeQueue {}
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    pub fn poll<T: Into<Timeout>>(&self, t: T) -> *mut RDKafkaEvent {
        unsafe { rdsys::rd_kafka_queue_poll(self.ptr(), t.into().as_millis()) }
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

    let context = &mut *(rdsys::rd_kafka_opaque(client) as *mut C);
    context.log(
        RDKafkaLogLevel::from_int(level),
        fac.trim(),
        log_message.trim(),
    );
}

pub(crate) unsafe extern "C" fn native_stats_cb<C: ClientContext>(
    _conf: *mut RDKafka,
    json: *mut c_char,
    json_len: usize,
    opaque: *mut c_void,
) -> i32 {
    let context = &mut *(opaque as *mut C);
    context.stats_raw(slice::from_raw_parts(json as *mut u8, json_len));
    0 // librdkafka will free the json buffer
}

pub(crate) unsafe extern "C" fn native_error_cb<C: ClientContext>(
    _client: *mut RDKafka,
    err: i32,
    reason: *const c_char,
    opaque: *mut c_void,
) {
    let err = RDKafkaRespErr::try_from(err).expect("global error not an rd_kafka_resp_err_t");
    let error = KafkaError::Global(err.into());
    let reason = CStr::from_ptr(reason).to_string_lossy();

    let context = &mut *(opaque as *mut C);
    context.error(error, reason.trim());
}

/// A generated OAuth token and its associated metadata.
///
/// When using the `OAUTHBEARER` SASL authentication method, this type is
/// returned from [`ClientContext::generate_oauth_token`]. The token and
/// principal name must not contain embedded null characters.
///
/// Specifying SASL extensions is not currently supported.
pub struct OAuthToken {
    /// The token value to set.
    pub token: String,
    /// The Kafka principal name associated with the token.
    pub principal_name: String,
    /// When the token expires, in number of milliseconds since the Unix epoch.
    pub lifetime_ms: i64,
}

pub(crate) unsafe extern "C" fn native_oauth_refresh_cb<C: ClientContext>(
    client: *mut RDKafka,
    oauthbearer_config: *const c_char,
    opaque: *mut c_void,
) {
    let res: Result<_, Box<dyn Error>> = (|| {
        let context = &mut *(opaque as *mut C);
        let oauthbearer_config = match oauthbearer_config.is_null() {
            true => None,
            false => Some(util::cstr_to_owned(oauthbearer_config)),
        };
        let token_info = context.generate_oauth_token(oauthbearer_config.as_deref())?;
        let token = CString::new(token_info.token)?;
        let principal_name = CString::new(token_info.principal_name)?;
        Ok((token, principal_name, token_info.lifetime_ms))
    })();
    match res {
        Ok((token, principal_name, lifetime_ms)) => {
            let mut err_buf = ErrBuf::new();
            let code = rdkafka_sys::rd_kafka_oauthbearer_set_token(
                client,
                token.as_ptr(),
                lifetime_ms,
                principal_name.as_ptr(),
                ptr::null_mut(),
                0,
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            );
            if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                debug!("successfully set refreshed OAuth token");
            } else {
                debug!(
                    "failed to set refreshed OAuth token (code {:?}): {}",
                    code, err_buf
                );
                rdkafka_sys::rd_kafka_oauthbearer_set_token_failure(client, err_buf.as_mut_ptr());
            }
        }
        Err(e) => {
            debug!("failed to refresh OAuth token: {}", e);
            let message = match CString::new(e.to_string()) {
                Ok(message) => message,
                Err(e) => {
                    error!("error message generated while refreshing OAuth token has embedded null character: {}", e);
                    CString::new("error while refreshing OAuth token has embedded null character")
                        .expect("known to be a valid CString")
                }
            };
            rdkafka_sys::rd_kafka_oauthbearer_set_token_failure(client, message.as_ptr());
        }
    }
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    use super::*;
    use crate::config::ClientConfig;

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
