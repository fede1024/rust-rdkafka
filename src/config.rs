//! Configuration to create a Consumer or Producer.
use log::LogLevel;
use rdsys::types::*;
use rdsys;

use client::Context;
use error::{KafkaError, KafkaResult, IsError};
use util::bytes_cstr_to_owned;

use std::collections::HashMap;
use std::ffi::CString;
use std::mem;

const ERR_LEN: usize = 256;


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
    pub fn from_int(level: i32) -> RDKafkaLogLevel {
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
    pub fn from_ptr(ptr: *mut RDKafkaConf) -> NativeClientConfig {
        NativeClientConfig {ptr: ptr}
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
    default_topic_config: Option<TopicConfig>,
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
            default_topic_config: None,
            log_level: log_level_from_global_config(),
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut ClientConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets the default topic configuration to use for automatically subscribed
    /// topics (e.g., through pattern-matched topics).
    pub fn set_default_topic_config(&mut self, default_topic_config: TopicConfig) -> &mut ClientConfig {
        self.default_topic_config = Some(default_topic_config);
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
        let errstr = [0; ERR_LEN];
        for (key, value) in &self.conf_map {
            let key_c = CString::new(key.to_string())?;
            let value_c = CString::new(value.to_string())?;
            let ret = unsafe {
                rdsys::rd_kafka_conf_set(conf, key_c.as_ptr(), value_c.as_ptr(),
                                           errstr.as_ptr() as *mut i8, errstr.len())
            };
            if ret.is_error() {
                let descr = unsafe { bytes_cstr_to_owned(&errstr) };
                return Err(KafkaError::ClientConfig(ret, descr, key.to_string(), value.to_string()));
            }
        }
        if let Some(ref topic_config) = self.default_topic_config {
            let native_topic_config = topic_config.create_native_config()?;
            unsafe { rdsys::rd_kafka_conf_set_default_topic_conf(conf, native_topic_config.ptr_move()) };
        };
        Ok(NativeClientConfig::from_ptr(conf))
    }

    /// Uses the current configuration to create a new Consumer or Producer.
    pub fn create<T: FromClientConfig>(&self) -> KafkaResult<T> {
        T::from_config(self)
    }

    /// Uses the current configuration and the provided context to create a new Consumer or Producer.
    pub fn create_with_context<C, T>(&self, context: C) -> KafkaResult<T>
            where C: Context,
                  T: FromClientConfigAndContext<C> {
        T::from_config_and_context(self, context)
    }
}

/// Return the log level
fn log_level_from_global_config() -> RDKafkaLogLevel {
    if log_enabled!(LogLevel::Debug) {
        RDKafkaLogLevel::Debug
    } else if log_enabled!(LogLevel::Info) {
        RDKafkaLogLevel::Info
    } else if log_enabled!(LogLevel::Warn) {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    }
}

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    fn from_config(&ClientConfig) -> KafkaResult<Self>;
}

/// Create a new client based on the provided configuration and context.
pub trait FromClientConfigAndContext<C: Context>: Sized {
    fn from_config_and_context(&ClientConfig, C) -> KafkaResult<Self>;
}

//
// ********** TOPIC CONFIG **********
//

/// A native rdkafka-sys client config.
pub struct NativeTopicConfig {
    ptr: *mut RDKafkaTopicConf,
}

impl NativeTopicConfig {
    /// Wraps a pointer to an `RDKafkaTopicConf` object and returns a new `NativeTopicConfig`.
    pub fn from_ptr(ptr: *mut RDKafkaTopicConf) -> NativeTopicConfig {
        NativeTopicConfig {ptr: ptr}
    }

    /// Returns the pointer to the librdkafka RDKafkaTopicConf structure.
    pub fn ptr(&self) -> *mut RDKafkaTopicConf {
        self.ptr
    }

    /// Returns the pointer to the librdkafka RDKafkaTopicConf structure. This method should be used
    /// when the native pointer is intended to be moved. The destructor won't be executed
    /// automatically; the caller should take care of deallocating the resource when no longer needed.
    pub fn ptr_move(self) -> *mut RDKafkaTopicConf {
        let ptr = self.ptr;
        mem::forget(self);
        ptr
    }
}

impl Drop for NativeTopicConfig {
    fn drop(&mut self) {
        trace!("Drop NativeTopicConfig {:p}", self.ptr);
        unsafe { rdsys::rd_kafka_topic_conf_destroy(self.ptr) }
    }
}

/// Topic configuration.
#[derive(Clone, Default)]
pub struct TopicConfig {
    conf_map: HashMap<String, String>,
}

impl TopicConfig {
    /// Returns a new TopicConfig.
    pub fn new() -> TopicConfig {
        TopicConfig {
            conf_map: HashMap::new(),
        }
    }

    /// Adds a new key-value pair in the topic configuration.
    pub fn set(&mut self, key: &str, value: &str) -> &mut TopicConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    /// Finalizes the creation of the topic configuration. Useful at the end of a chain of setters,
    /// if you need to store the resulting topic configuration before its use.
    pub fn finalize(&self) -> TopicConfig {
        TopicConfig { conf_map: self.conf_map.clone() }
    }

    /// Creates a native rdkafka-sys topic configuration.
    pub fn create_native_config(&self) -> KafkaResult<NativeTopicConfig> {
        let config_ptr = unsafe { rdsys::rd_kafka_topic_conf_new() };
        let errstr = [0; ERR_LEN];
        for (name, value) in &self.conf_map {
            let name_c = CString::new(name.to_string())?;
            let value_c = CString::new(value.to_string())?;
            let ret = unsafe {
                rdsys::rd_kafka_topic_conf_set(config_ptr, name_c.as_ptr(), value_c.as_ptr(),
                                                 errstr.as_ptr() as *mut i8, errstr.len())
            };
            if ret.is_error() {
                let descr = unsafe { bytes_cstr_to_owned(&errstr) };
                return Err(KafkaError::TopicConfig(ret, descr, name.to_string(), value.to_string()));
            }
        }
        Ok(NativeTopicConfig::from_ptr(config_ptr))
    }
}
