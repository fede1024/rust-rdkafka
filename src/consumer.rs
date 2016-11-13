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

use client::{Client, ClientType};
use config::{FromConfig, Config};
use error::{Error, IsError};
use message::Message;
use util::cstr_to_owned;


/// A Consumer client.
#[derive(Clone)]
pub struct Consumer {
    client: Arc<Client>,
}

/// Creates a new Consumer starting from a Config.
impl FromConfig for Consumer {
    fn from_config(config: &Config) -> Result<Consumer, Error> {
        let client = try!(Client::new(config, ClientType::Consumer));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(Consumer { client: Arc::new(client) })
    }
}

impl Consumer {
    /// Subscribes the consumer to a list of topics and/or topic sets (using regex).
    /// Strings starting with `^` will be regex-matched to the full list of topics in
    /// the cluster and matching topics will be added to the subscription list.
    pub fn subscribe(&mut self, topics: &Vec<&str>) -> Result<(), Error> {
        let tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(topics.len() as i32) };
        for &topic in topics {
            let topic_c = CString::new(topic).unwrap();
            let ret_code = unsafe {
                rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_c.as_ptr(), 0);
                rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list)
            };
            if ret_code.is_error() {
                return Err(Error::Subscription(topic.to_string()))
            };
        }
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        Ok(())
    }

    /// Returns a vector of topics or topic patterns the consumer is subscribed to.
    pub fn get_subscriptions(&self) -> Vec<String> {
        let mut tp_list = unsafe { rdkafka::rd_kafka_topic_partition_list_new(0) };
        unsafe { rdkafka::rd_kafka_subscription(self.client.ptr, &mut tp_list as *mut *mut rdkafka::rd_kafka_topic_partition_list_t) };

        let mut tp_res = Vec::new();
        for i in 0..unsafe { (*tp_list).cnt } {
            let elem = unsafe { (*tp_list).elems.offset(i as isize) };
            let topic_name = unsafe { cstr_to_owned((*elem).topic) };
            tp_res.push(topic_name);
        }
        unsafe { rdkafka::rd_kafka_topic_partition_list_destroy(tp_list) };
        tp_res
    }

    pub fn poll(&self, timeout_ms: i32) -> Result<Option<Message>, Error> {
        let message_n = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.ptr, timeout_ms) };
        if message_n.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_n).err };
        if error.is_error() {
            return Err(Error::MessageConsumption(error));
        }
        let kafka_message = Message { message_n: message_n };
        Ok(Some(kafka_message))
    }

    pub fn start_thread(&self) -> (ConsumerPollingThread, Receiver<Message, Error>) {
        let (sender, receiver) = stream::channel();
        let mut consumer_thread = ConsumerPollingThread::new(self, sender);
        consumer_thread.start();
        (consumer_thread, receiver)
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        trace!("Destroying consumer");  // TODO: fix me (multiple executions)
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}

/// A Consumer with an associated polling thread. This consumer doesn't need to
/// be polled and it will return all consumed messages as a `Stream`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct ConsumerPollingThread {
    consumer: Consumer,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<Message, Error>>,
}

impl ConsumerPollingThread {
    fn new(consumer: &Consumer, sender: Sender<Message, Error>) -> ConsumerPollingThread {
        ConsumerPollingThread {
            consumer: consumer.clone(),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
            sender: Some(sender),
        }
    }

    fn start(&mut self) {
        let consumer = self.consumer.clone();
        let should_stop = self.should_stop.clone();
        let mut sender = self.sender.take().expect("Sender is missing");
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                trace!("Polling thread loop started");
                while !should_stop.load(Ordering::Relaxed) {
                    // TODO: while stream alive?
                    match consumer.poll(100) {
                        Ok(None) => {}
                        Ok(Some(m)) => {
                            let future_sender = sender.send(Ok(m));
                            match future_sender.wait() {
                                Ok(new_sender) => sender = new_sender,
                                Err(e) => {
                                    debug!("Sender not available: {:?}", e);
                                    break;
                                }
                            };
                        }
                        Err(e) => warn!("E: {:?}", e),
                    }
                }
                trace!("Polling thread loop terminated");
            })
            .expect("Failed to start polling thread");
        self.handle = Some(handle);
    }

    pub fn stop(&mut self) {
        if self.handle.is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match self.handle.take().unwrap().join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }
}

impl Drop for ConsumerPollingThread {
    fn drop(&mut self) {
        trace!("Destroy ConsumerPollingThread");
        self.stop();
    }
}
