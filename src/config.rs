//! Configuration to create a Consumer or Producer.
extern crate librdkafka_sys as rdkafka;

use std::collections::HashMap;
use std::ffi::CString;
use util::bytes_cstr_to_owned;

use error::{Error, IsError};
use client::{DeliveryCallback};

/// Client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    conf_map: HashMap<String, String>,
    delivery_cb: Option<DeliveryCallback>,
    default_topic_config: Option<DefaultTopicConfig>,
}

impl Config {
    /// Creates a new empty configuration.
    pub fn new() -> Config {
        Config {
            conf_map: HashMap::new(),
            delivery_cb: None,
            default_topic_config: None,
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut Config {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    pub fn set_delivery_cb<'a>(&'a mut self, cb: DeliveryCallback) -> &'a mut Config {
        self.delivery_cb = Some(cb);
        self
    }

    pub fn get_delivery_cb(&self) -> Option<DeliveryCallback> {
        self.delivery_cb
    }

    pub fn set_default_topic_config<'a>(&'a mut self, default_topic_config: DefaultTopicConfig) -> &'a mut Config {
        self.default_topic_config = Some(default_topic_config);
        self
    }

    pub fn create_native_config(&self) -> Result<*mut rdkafka::rd_kafka_conf_t, Error> { 
        // let mut c_config = try!(map_to_c_config(&self.conf_map));
        // match self.default_topic_config {
        //     Some(topic_config) => try!(map_to_c_config(topic_config.conf_map)),
        //     None
        // }

        // map_to_c_config(&self.conf_map)
        //     .map(|c_config| {
        //         self.default_topic_config.as_ref().map(|topic_config| {
        //             map_to_c_config(&topic_config.conf_map)
        //                 .map(|c_topic_config|
        //                      unsafe { rdkafka::rd_kafka_conf_set_default_topic_conf(c_config, c_topic_config) }
        //                 )


        //             c_topic_config
        //         });
        //         c_config
        //     })
        //
        map_to_c_config(&self.conf_map)
    }

    pub fn create_native_default_topic_config(&self)
            -> Option<Result<*mut rdkafka::rd_kafka_conf_t, Error>> { 
        self.default_topic_config.as_ref().map(|c| map_to_c_config(&c.conf_map))
    }

    pub fn config_clone(&self) -> Config {
        (*self).clone()
    }

    pub fn create<T: FromConfig>(&self) -> Result<T, Error> {
        T::from_config(self)
    }
}

/// Create a new client based on the provided configuration.
pub trait FromConfig: Sized {
    fn from_config(&Config) -> Result<Self, Error>;
}

/// Default topic configuration.
#[derive(Debug, Clone)]
pub struct DefaultTopicConfig {
    conf_map: HashMap<String, String>,
}

impl DefaultTopicConfig {
    /// Creates a new empty configuration.
    pub fn new() -> DefaultTopicConfig {
        DefaultTopicConfig {
            conf_map: HashMap::new(),
        }
    }

    /// Sets a new parameter in the configuration.
    pub fn set(mut self, key: &str, value: &str) -> DefaultTopicConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }
}

fn map_to_c_config(conf_map: &HashMap<String, String>) -> Result<*mut rdkafka::rd_kafka_conf_t, Error> { 
    let conf = unsafe { rdkafka::rd_kafka_conf_new() };
    let errstr = [0; 1024];
    for (key, value) in conf_map {
        let key_c = try!(CString::new(key.to_string()));
        let value_c = try!(CString::new(value.to_string()));
        let ret = unsafe {
            rdkafka::rd_kafka_conf_set(conf,
                                        key_c.as_ptr(),
                                        value_c.as_ptr(),
                                        errstr.as_ptr() as *mut i8,
                                        errstr.len())
        };
        if ret.is_error() {
            let descr = unsafe { bytes_cstr_to_owned(&errstr) };
            return Err(Error::Config((ret, descr, key.to_string(), value.to_string())));
        }
    }
    Ok(conf)
}

/// Topic configuration
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
    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut TopicConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Adds a new key-value pair in the topic configution.
    pub fn seta(mut self, key: &str, value: &str) -> TopicConfig {
        self.conf_map.insert(key.to_string(), value.to_string());
        self
    }

    pub fn create_native_config(&self) -> Result<*mut rdkafka::rd_kafka_topic_conf_t, Error> {
        let config_ptr = unsafe { rdkafka::rd_kafka_topic_conf_new() };
        let errstr = [0; 1024];
        for (name, value) in &self.conf_map {
            let name_c = try!(CString::new(name.to_string()));
            let value_c = try!(CString::new(value.to_string()));
            let ret = unsafe {
                rdkafka::rd_kafka_topic_conf_set(config_ptr,
                                                 name_c.as_ptr(),
                                                 value_c.as_ptr(),
                                                 errstr.as_ptr() as *mut i8,
                                                 errstr.len())
            };
            if ret.is_error() {
                let descr = unsafe { bytes_cstr_to_owned(&errstr) };
                return Err(Error::Config((ret, descr, name.to_string(), value.to_string())));
            }
        }
        Ok(config_ptr)
    }
}
