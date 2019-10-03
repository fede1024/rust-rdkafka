//! Producer and consumer configuration.
//!
//! ## C library configuration
//!
//! The Rust library will forward all the configuration to the C library. The most frequently
//! used parameters are listed here.
//!
//! ### Frequently used parameters
//!
//! For producer-specific and consumer-specific parameters check the producer and consumer modules
//! documentation. The full list of available parameters is available in the  [librdkafka
//! documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
//!
//! - `client.id` (rdkafka): Client identifier.
//! - `bootstrap.servers`: Initial list of brokers as a CSV list of broker host or host:port.
//! - `message.max.bytes` (1000000): Maximum message size.
//! - `debug`: A comma-separated list of debug contexts to enable. Use 'all' to print all the debugging information.
//! - `statistics.interval.ms` (0 - disabled): how often the statistic callback specified in the `Context` will be called.
//!

use log::LogLevel;
use crate::rdsys::types::*;
use crate::rdsys;

use crate::client::ClientContext;
use crate::error::{KafkaError, KafkaResult, IsError};
use crate::util::ErrBuf;

use std::collections::HashMap;
use std::ffi::CString;
use std::mem;


/// The log levels supported by librdkafka.
#[derive(Copy, Clone, Debug)]
pub enum RDKafkaLogLevel {
    /// Higher priority then LogLevel::Error from the log crate.
    Emerg = 0,
    /// Higher priority then LogLevel::Error from the log crate.
    Alert = 1,
    /// Higher priority then LogLevel::Error from the log crate.
    Critical = 2,
    /// Equivalent to LogLevel::Error from the log crate.
    Error = 3,
    /// Equivalent to LogLevel::Warning from the log crate.
    Warning = 4,
    /// Higher priority then LogLevel::Info from the log crate.
    Notice = 5,
    /// Equivalent to LogLevel::Info from the log crate.
    Info = 6,
    /// Equivalent to LogLevel::Debug from the log crate.
    Debug = 7,
}

impl RDKafkaLogLevel {
    pub(crate) fn from_int(level: i32) -> RDKafkaLogLevel {
        match level {
            0 => RDKafkaLogLevel::Emerg,
            1 => RDKafkaLogLevel::Alert,
            2 => RDKafkaLogLevel::Critical,
            3 => RDKafkaLogLevel::Error,
            4 => RDKafkaLogLevel::Warning,
            5 => RDKafkaLogLevel::Notice,
            6 => RDKafkaLogLevel::Info,
            _ => RDKafkaLogLevel::Debug,
        }
    }
}

//
// ********** CLIENT CONFIG **********
//

/// A native rdkafka-sys client config.
pub struct NativeClientConfig {
    ptr: *mut RDKafkaConf,
}

impl NativeClientConfig {
    /// Wraps a pointer to an `RDKafkaConfig` object and returns a new `NativeClientConfig`.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafkaConf) -> NativeClientConfig {
        NativeClientConfig { ptr }
    }

    /// Returns the pointer to the librdkafka RDKafkaConf structure.
    pub fn ptr(&self) -> *mut RDKafkaConf {
        self.ptr
    }

    /// Returns the pointer to the librdkafka RDKafkaConf structure. This method should be used when
    /// the native pointer is intended to be moved. The destructor won't be executed automatically;
    /// the caller should take care of deallocating the resource when no longer needed.
    pub fn ptr_move(self) -> *mut RDKafkaConf {
        let ptr = self.ptr;
        mem::forget(self);
        ptr
    }
}

impl Drop for NativeClientConfig {
    fn drop(&mut self) {
        trace!("Drop NativeClientConfig {:p}", self.ptr());
        unsafe { rdsys::rd_kafka_conf_destroy(self.ptr) }
    }
}

/// Client configuration.
#[derive(Clone)]
pub struct ClientConfig {
    conf_map: HashMap<String, String>,
    /// The librdkafka logging level. Refer to `RDKafkaLogLevel` for the list of available levels.
    pub log_level: RDKafkaLogLevel,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientConfig {
    /// Creates a new empty configuration.
    pub fn new() -> ClientConfig {
        ClientConfig {
            conf_map: HashMap::new(),
            log_level: log_level_from_global_config(),
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut ClientConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets the log level of the client. If not specified, the log level will be calculated based
    /// on the global log level of the log crate.
    pub fn set_log_level(&mut self, log_level: RDKafkaLogLevel) -> &mut ClientConfig {
        self.log_level = log_level;
        self
    }

    /// Returns the native rdkafka-sys configuration.
    pub fn create_native_config(&self) -> KafkaResult<NativeClientConfig> {
        let conf = unsafe { rdsys::rd_kafka_conf_new() };
        let mut err_buf = ErrBuf::new();
        for (key, value) in &self.conf_map {
            let key_c = CString::new(key.to_string())?;
            let value_c = CString::new(value.to_string())?;
            let ret = unsafe {
                rdsys::rd_kafka_conf_set(conf, key_c.as_ptr(), value_c.as_ptr(),
                                         err_buf.as_mut_ptr(), err_buf.len())
            };
            if ret.is_error() {
                return Err(KafkaError::ClientConfig(ret, err_buf.to_string(), key.to_string(), value.to_string()));
            }
        }
        Ok(unsafe {NativeClientConfig::from_ptr(conf)})
    }

    /// Uses the current configuration to create a new Consumer or Producer.
    pub fn create<T: FromClientConfig>(&self) -> KafkaResult<T> {
        T::from_config(self)
    }

    /// Uses the current configuration and the provided context to create a new Consumer or Producer.
    pub fn create_with_context<C, T>(&self, context: C) -> KafkaResult<T>
            where C: ClientContext,
                  T: FromClientConfigAndContext<C> {
        T::from_config_and_context(self, context)
    }
}

/// Return the log level
fn log_level_from_global_config() -> RDKafkaLogLevel {
    if log_enabled!(target: "librdkafka", LogLevel::Debug) {
        RDKafkaLogLevel::Debug
    } else if log_enabled!(target: "librdkafka", LogLevel::Info) {
        RDKafkaLogLevel::Info
    } else if log_enabled!(target: "librdkafka", LogLevel::Warn) {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    }
}

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    /// Create a client from client configuration. The default client context will be used.
    fn from_config(_: &ClientConfig) -> KafkaResult<Self>;
}

/// Create a new client based on the provided configuration and context.
pub trait FromClientConfigAndContext<C: ClientContext>: Sized {
    /// Create a client from client configuration and a client context.
    fn from_config_and_context(_: &ClientConfig, _: C) -> KafkaResult<Self>;
}
