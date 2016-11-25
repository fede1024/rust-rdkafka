//! Configuration to create a Consumer or Producer.
extern crate rdkafka_sys as rdkafka;

use std::collections::HashMap;
use std::ffi::CString;
use util::bytes_cstr_to_owned;

use error::{KafkaError, KafkaResult, IsError};
use client::{DeliveryCallback};

const ERR_LEN: usize = 256;

/// Client configuration.
#[derive(Clone)]
pub struct ClientConfig {
    conf_map: HashMap<String, String>,
    delivery_cb: Option<DeliveryCallback>,
    default_topic_config: Option<TopicConfig>,
}

impl ClientConfig {
    /// Creates a new empty configuration.
    pub fn new() -> ClientConfig {
        ClientConfig {
            conf_map: HashMap::new(),
            delivery_cb: None,
            default_topic_config: None,
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut ClientConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    pub fn set_delivery_cb<'a>(&'a mut self, cb: DeliveryCallback) -> &'a mut ClientConfig {
        self.delivery_cb = Some(cb);
        self
    }

    pub fn get_delivery_cb(&self) -> Option<DeliveryCallback> {
        self.delivery_cb
    }

    pub fn set_default_topic_config<'a>(&'a mut self, default_topic_config: TopicConfig) -> &'a mut ClientConfig {
        self.default_topic_config = Some(default_topic_config);
        self
    }

    pub fn create_native_config(&self) -> KafkaResult<*mut rdkafka::rd_kafka_conf_t> {
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
        Ok(conf)
    }

    fn create_native_default_topic_config(&self)
            -> Option<KafkaResult<*mut rdkafka::rd_kafka_topic_conf_t>> {
        self.default_topic_config.as_ref().map(|c| c.create_native_config())
    }

    pub fn config_clone(&self) -> ClientConfig {
        (*self).clone()
    }

    pub fn create<T: FromClientConfig>(&self) -> KafkaResult<T> {
        T::from_config(self)
    }
}

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    fn from_config(&ClientConfig) -> KafkaResult<Self>;
}

/// Topic configuration
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

    pub fn create_native_config(&self) -> KafkaResult<*mut rdkafka::rd_kafka_topic_conf_t> {
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
