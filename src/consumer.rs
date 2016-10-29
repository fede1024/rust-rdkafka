extern crate libc;
extern crate librdkafka_sys as rdkafka;
extern crate std;

use std::ffi::CStr;
use std::ffi::CString;
use std::str;
use std::collections::HashMap;

use error;

pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdkafka::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdkafka::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

#[derive(Debug)]
pub struct KafkaConfig {
    conf: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new() -> KafkaConfig {
        KafkaConfig { conf: HashMap::new() }
    }

    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut KafkaConfig {
        self.conf.insert(key.to_string(), value.to_string());
        self
    }

    fn create_kafka_config(&self) -> Result<*mut rdkafka::rd_kafka_conf_t, error::KafkaError> {
        let conf = unsafe { rdkafka::rd_kafka_conf_new() };
        let errstr = [0; 1024];
        for (key, value) in &self.conf {
            let key_c = try!(CString::new(key.to_string()));
            let value_c = try!(CString::new(value.to_string()));
            let ret = unsafe {
                rdkafka::rd_kafka_conf_set(conf,
                                           key_c.as_ptr(),
                                           value_c.as_ptr(),
                                           errstr.as_ptr() as *mut i8,
                                           errstr.len())
            };
            if error::is_config_error(ret) {
                let descr = unsafe { cstr_to_owned(&errstr) };
                return Err(error::KafkaError::ConfigError((ret, descr, key.to_string(), value.to_string())));
            }
        }
        Ok(conf)
    }

    pub fn create_consumer(&self) -> Result<KafkaConsumer, error::KafkaError> {
        let errstr = [0i8; 1024];
        let rd_config = try!(self.create_kafka_config());
        unsafe {
            let ret = rdkafka::rd_kafka_new(rdkafka::rd_kafka_type_t::RD_KAFKA_CONSUMER,
                                            rd_config,
                                            errstr.as_ptr() as *mut i8,
                                            errstr.len());
            if ret.is_null() {
                return Err(error::KafkaError::ConsumerCreationError(cstr_to_owned(&errstr)));
            }

            rdkafka::rd_kafka_poll_set_consumer(ret);
            Ok(KafkaConsumer { client_n: ret })
        }
    }
}

unsafe fn cstr_to_owned(cstr: &[i8]) -> String {
    CStr::from_ptr(cstr.as_ptr()).to_string_lossy().into_owned()
}

pub struct KafkaConsumer {
    client_n: *mut rdkafka::rd_kafka_t,
}

impl KafkaConsumer {
    pub fn broker_add(&mut self, brokers: &str) -> i32 {
        let brokers = CString::new(brokers).unwrap();
        unsafe { rdkafka::rd_kafka_brokers_add(self.client_n, brokers.as_ptr()) }
    }

    pub fn subscribe(&mut self, topic_name: &str) -> Result<(), error::KafkaError> {
        let topic_name_c = CString::new(topic_name).unwrap();
        let ret_code = unsafe {
            let tp_list = rdkafka::rd_kafka_topic_partition_list_new(1);
            rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_name_c.as_ptr(), 0);
            rdkafka::rd_kafka_subscribe(self.client_n, tp_list)
        };
        if error::is_kafka_error(ret_code) {
            Err(error::KafkaError::SubscriptionError(topic_name.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn poll(&self, timeout_ms: i32) -> Result<Option<Message>, error::KafkaError> {
        let message_n = unsafe { rdkafka::rd_kafka_consumer_poll(self.client_n, timeout_ms) };
        if message_n.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_n).err };
        if error::is_kafka_error(error) {
            return Err(error::KafkaError::MessageConsumptionError(error));
        }
        let payload = unsafe {
            if (*message_n).payload.is_null() {
                None
            } else {
                Some(std::slice::from_raw_parts::<u8>((*message_n).payload as *const u8, (*message_n).len))
            }
        };
        let key = unsafe {
            if (*message_n).key.is_null() {
                None
            } else {
                Some(std::slice::from_raw_parts::<u8>((*message_n).key as *const u8, (*message_n).key_len))
            }
        };
        let message = Message {
            payload: payload,
            key: key,
            partition: unsafe { (*message_n).partition },
            offset: unsafe { (*message_n).offset },
            message_n: message_n,
        };
        Ok(Some(message))
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_consumer_close(self.client_n) };
        unsafe { rdkafka::rd_kafka_destroy(self.client_n) };
    }
}

#[derive(Debug)]
pub struct Message<'a> {
    pub payload: Option<&'a [u8]>,
    pub key: Option<&'a [u8]>,
    pub partition: i32,
    pub offset: i64,
    pub message_n: *mut rdkafka::rd_kafka_message_s,
}

impl<'a> Drop for Message<'a> {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_message_destroy(self.message_n) };
    }
}
