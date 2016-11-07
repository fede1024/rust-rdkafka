extern crate librdkafka_sys as rdkafka;
extern crate futures;

use std::ffi::CString;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;

use self::futures::{Future};
use self::futures::stream;
use self::futures::stream::{Receiver, Sender};

use client::{Client, ClientType};
use config::{CreateConsumer, KafkaConfig};
use error::{KafkaError, IsError};
use message::Message;


#[derive(Clone)]
pub struct Consumer {
    client: Arc<Client>,
}

impl CreateConsumer<Consumer, KafkaError> for KafkaConfig {
    fn create_consumer(&self) -> Result<Consumer, KafkaError> {
        let client = try!(Client::new(&self, ClientType::Consumer));
        unsafe { rdkafka::rd_kafka_poll_set_consumer(client.ptr) };
        Ok(Consumer{ client: Arc::new(client) })
    }
}

impl Consumer {
    pub fn subscribe(&mut self, topic_name: &str) -> Result<(), KafkaError> {
        let topic_name_c = CString::new(topic_name).unwrap();
        let ret_code = unsafe {
            let tp_list = rdkafka::rd_kafka_topic_partition_list_new(1);
            rdkafka::rd_kafka_topic_partition_list_add(tp_list, topic_name_c.as_ptr(), 0);
            rdkafka::rd_kafka_subscribe(self.client.ptr, tp_list)
        };
        if ret_code.is_error() {
            Err(KafkaError::SubscriptionError(topic_name.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn poll(&self, timeout_ms: i32) -> Result<Option<Message>, KafkaError> {
        let message_n = unsafe { rdkafka::rd_kafka_consumer_poll(self.client.ptr, timeout_ms) };
        if message_n.is_null() {
            return Ok(None);
        }
        let error = unsafe { (*message_n).err };
        if error.is_error() {
            return Err(KafkaError::MessageConsumptionError(error));
        }
        let kafka_message = Message { message_n: message_n };
        Ok(Some(kafka_message))
    }

    pub fn start_thread(&self) -> (ConsumerPollingThread, Receiver<Message, KafkaError>) {
        let (sender, receiver) = stream::channel();
        let mut consumer_thread = ConsumerPollingThread::new(self, sender);
        consumer_thread.start();
        (consumer_thread, receiver)
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_consumer_close(self.client.ptr) };
    }
}

#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct ConsumerPollingThread {
    consumer: Consumer,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<Message, KafkaError>>
}

impl ConsumerPollingThread {
    fn new(consumer: &Consumer, sender: Sender<Message, KafkaError>) -> ConsumerPollingThread {
        ConsumerPollingThread {
            consumer: consumer.clone(),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
            sender: Some(sender)
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
                while !should_stop.load(Ordering::Relaxed) { // TODO: while stream alive?
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
            }).expect("Failed to start polling thread");
        self.handle = Some(handle);
    }

    pub fn stop(&mut self) {
        if self.handle.is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match self.handle.take().unwrap().join() {
                Ok(()) => { trace!("Polling stopped"); },
                Err(e) => { warn!("Failure while terminating thread: {:?}", e) }
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
