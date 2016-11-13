//! Consumer implementations.
extern crate librdkafka_sys as rdkafka;
extern crate futures;

use std::ffi::CString;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;

use self::futures::Future;
use self::futures::stream;
use self::futures::stream::{Receiver, Sender};

use events::{Event, EventQueue};
use client::{Client, ClientType};
use config::{FromConfig, Config};
use error::{Error, IsError};
use message::Message;


/// A Consumer client.
#[derive(Clone)]
pub struct EventConsumer {
    client: Arc<Client>,
}

/// Creates a new EventConsumer starting from a Config.
impl FromConfig for EventConsumer {
    fn from_config(config: &Config) -> Result<EventConsumer, Error> {
        //let flags = rdkafka::RD_KAFKA_EVENT_FETCH as i32 | rdkafka::RD_KAFKA_EVENT_REBALANCE as i32 | rdkafka::RD_KAFKA_EVENT_ERROR as i32 | rdkafka::RD_KAFKA_EVENT_LOG as i32;
        //let flags = rdkafka::RD_KAFKA_EVENT_FETCH as i32 | rdkafka::RD_KAFKA_EVENT_ERROR as i32 | rdkafka::RD_KAFKA_EVENT_LOG as i32 | rdkafka::RD_KAFKA_EVENT_OFFSET_COMMIT as i32;
        //let flags = rdkafka::RD_KAFKA_EVENT_OFFSET_COMMIT as i32;
        let flags = rdkafka::RD_KAFKA_EVENT_ERROR as i32 | rdkafka::RD_KAFKA_EVENT_FETCH as i32;
        println!(">> {}", flags);
        let config_ptr = config.create_kafka_config().expect("Can't create config");
        unsafe { rdkafka::rd_kafka_conf_set_events(config_ptr, 0) };
        let client = try!(Client::new2(config_ptr, ClientType::Consumer));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(EventConsumer { client: Arc::new(client) })
    }
}

impl EventConsumer {
    pub fn subscribe(&mut self, topic_name: &str) -> Result<(), Error> {
        let topic_name_c = CString::new(topic_name).unwrap();
        let ret_code = unsafe {
            let tp_list = rdkafka::rd_kafka_topic_partition_list_new(1);
            rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_name_c.as_ptr(), 0);
            rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list)
        };
        if ret_code.is_error() {
            Err(Error::Subscription(topic_name.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn poll_consumer(&self, timeout_ms: i32) -> Option<Event> {
        let queue = self.client.get_consumer_queue();
        queue.poll(timeout_ms)
    }

    pub fn poll_main(&self, timeout_ms: i32) -> Option<Event> {
        let queue = self.client.get_main_queue();
        queue.poll(timeout_ms)
    }
}

impl Drop for EventConsumer {
    fn drop(&mut self) {
        trace!("Destroying consumer");
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}
