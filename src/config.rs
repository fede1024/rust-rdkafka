//! Configuration to create a Consumer or Producer.
extern crate rdkafka_sys as rdkafka;

use self::rdkafka::types::*;

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use util::bytes_cstr_to_owned;

use error::{KafkaError, KafkaResult, IsError};
use client::Context;

const ERR_LEN: usize = 256;

unsafe extern "C" fn log_cb(_client: *const RDKafkaState, level: i32, _fac: *const i8, buf: *const i8) {
    let buf_str = CStr::from_ptr(buf).to_string_lossy();
    match level {
        0 => error!("librdkafka: {}", buf_str),
        1 => error!("librdkafka: {}", buf_str),
        2 => error!("librdkafka: {}", buf_str),
        3 => error!("librdkafka: {}", buf_str),
        4 => warn!("librdkafka: {}", buf_str),
        5 => info!("librdkafka: {}", buf_str),
        6 => info!("librdkafka: {}", buf_str),
        _ => debug!("librdkafka: {}", buf_str),
    }
}

/// Client configuration.
#[derive(Clone)]
pub struct ClientConfig {
    conf_map: HashMap<String, String>,
    default_topic_config: Option<TopicConfig>,
}

impl ClientConfig {
    /// Creates a new empty configuration.
    pub fn new() -> ClientConfig {
        ClientConfig {
            conf_map: HashMap::new(),
            default_topic_config: None,
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut ClientConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets the default topic configuration to use for automatically subscribed
    /// topics (e.g., through pattern-matched topics).
    pub fn set_default_topic_config<'a>(&'a mut self, default_topic_config: TopicConfig) -> &'a mut ClientConfig {
        self.default_topic_config = Some(default_topic_config);
        self
    }

    /// Returns the native rdkafka-sys configuration.
    pub fn create_native_config(&self) -> KafkaResult<*mut RDKafkaConf> {
        let conf = unsafe { rdkafka::rd_kafka_conf_new() };
        let errstr = [0; ERR_LEN];
        for (key, value) in &self.conf_map {
            let key_c = try!(CString::new(key.to_string()));
            let value_c = try!(CString::new(value.to_string()));
            let ret = unsafe {
                rdkafka::rd_kafka_conf_set(conf, key_c.as_ptr(), value_c.as_ptr(),
                                           errstr.as_ptr() as *mut i8, errstr.len())
            };
            if ret.is_error() {
                let descr = unsafe { bytes_cstr_to_owned(&errstr) };
                return Err(KafkaError::ClientConfig((ret, descr, key.to_string(), value.to_string())));
            }
        }
        if let Some(topic_config) = self.create_native_default_topic_config() {
            if let Err(e) = topic_config {
                return Err(e);
            } else {
                unsafe { rdkafka::rd_kafka_conf_set_default_topic_conf(conf, topic_config.unwrap()) };
            }
        }
        unsafe { rdkafka::rd_kafka_conf_set_log_cb(conf, Some(log_cb)) };
        Ok(conf)
    }

    fn create_native_default_topic_config(&self) -> Option<KafkaResult<*mut RDKafkaTopicConf>> {
        self.default_topic_config.as_ref().map(|c| c.create_native_config())
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

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    fn from_config(&ClientConfig) -> KafkaResult<Self>;
}

/// Create a new client based on the provided configuration and context.
pub trait FromClientConfigAndContext<C: Context>: Sized {
    fn from_config_and_context(&ClientConfig, C) -> KafkaResult<Self>;
}

/// Topic configuration.
#[derive(Clone)]
pub struct TopicConfig {
    conf_map: HashMap<String, String>,
}

impl TopicConfig {
    /// Returns a new TopicConfig for the specified topic name and client.
    pub fn new() -> TopicConfig {
        TopicConfig {
            conf_map: HashMap::new(),
        }
    }

    /// Adds a new key-value pair in the topic configution.
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
    pub fn create_native_config(&self) -> KafkaResult<*mut RDKafkaTopicConf> {
        let config_ptr = unsafe { rdkafka::rd_kafka_topic_conf_new() };
        let errstr = [0; ERR_LEN];
        for (name, value) in &self.conf_map {
            let name_c = try!(CString::new(name.to_string()));
            let value_c = try!(CString::new(value.to_string()));
            let ret = unsafe {
                rdkafka::rd_kafka_topic_conf_set(config_ptr, name_c.as_ptr(), value_c.as_ptr(),
                                                 errstr.as_ptr() as *mut i8, errstr.len())
            };
            if ret.is_error() {
                let descr = unsafe { bytes_cstr_to_owned(&errstr) };
                return Err(KafkaError::TopicConfig((ret, descr, name.to_string(), value.to_string())));
            }
        }
        Ok(config_ptr)
    }
}
