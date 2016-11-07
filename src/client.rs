extern crate futures;
extern crate librdkafka_sys as rdkafka;

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;

use self::futures::Complete;

use config::KafkaConfig;
use error::KafkaError;
use util::cstr_to_owned;

pub enum ClientType {
    Consumer,
    Producer
}

pub struct Client {
    pub ptr: *mut rdkafka::rd_kafka_t
}

unsafe impl Sync for Client {}
unsafe impl Send for Client {}

#[derive(Debug)]
pub struct DeliveryStatus {
    error: rdkafka::rd_kafka_resp_err_t,
    partition: i32,
    offset: i64
}

unsafe extern fn prod_callback(_client: *mut rdkafka::rd_kafka_t, msg: *const rdkafka::rd_kafka_message_t, _opaque: *mut c_void) {
    let tx = Box::from_raw((*msg)._private as *mut Complete<DeliveryStatus>);
    let delivery_status = DeliveryStatus {
        error: (*msg).err,
        partition: (*msg).partition,
        offset: (*msg).offset
    };
    // TODO: add topic name?
    trace!("Delivery event received: {:?}", delivery_status);
    tx.complete(delivery_status);
}

impl Client {
    pub fn new(config: &KafkaConfig, client_type: ClientType) -> Result<Client, KafkaError> {
        let errstr = [0i8; 1024];
        let config_ptr = try!(config.create_kafka_config());
        let rd_kafka_type = match client_type {
            ClientType::Consumer => { rdkafka::rd_kafka_type_t::RD_KAFKA_CONSUMER }
            ClientType::Producer => {
                unsafe { rdkafka::rd_kafka_conf_set_dr_msg_cb(config_ptr, Some(prod_callback)) };
                rdkafka::rd_kafka_type_t::RD_KAFKA_PRODUCER
            }
        };
        let client_ptr = unsafe {
            rdkafka::rd_kafka_new(rd_kafka_type, config_ptr, errstr.as_ptr() as *mut i8, errstr.len())
        };
        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreationError(cstr_to_owned(&errstr)));
        }
        Ok(Client { ptr: client_ptr })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka");
        unsafe { rdkafka::rd_kafka_destroy(self.ptr) };
        unsafe { rdkafka::rd_kafka_wait_destroyed(1000) };
    }
}


pub struct KafkaTopic {  // TODO: link its lifetime to client lifetime
    pub ptr: *mut rdkafka::rd_kafka_topic_t
}

impl KafkaTopic {
    pub fn new(client: &Client, name: &str) -> Result<KafkaTopic, KafkaError> {
        let name_ptr = CString::new(name).unwrap();
        let topic_ptr = unsafe {
            rdkafka::rd_kafka_topic_new(client.ptr, name_ptr.as_ptr(), ptr::null_mut())
        };
        if topic_ptr.is_null() {
            Err(KafkaError::TopicNameError(name.to_string()))
        } else {
            Ok(KafkaTopic { ptr: topic_ptr })
        }
    }
}

impl Drop for KafkaTopic {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka_topic");
        unsafe { rdkafka::rd_kafka_topic_destroy(self.ptr); }
    }
}
