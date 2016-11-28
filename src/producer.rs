//! Producer implementations.
extern crate rdkafka_sys as rdkafka;
extern crate errno;
extern crate futures;

use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;

use self::futures::{Canceled, Complete, Future, Poll, Oneshot};

use config::{ClientConfig, FromClientConfig, TopicConfig};
use error::{KafkaError, KafkaResult};
use message::{ToBytes, ToBytesBox};
use client::{Client, ClientType, Topic};
use message::{Message};


/// Contains a reference counted producer client. It can be safely cloned to
/// create another reference to the same producer.
#[derive(Clone)]
pub struct Producer {
    client: Arc<Client>,
}

#[derive(Debug)]
/// Information returned by the producer after a message has been delivered
/// or failed to be delivered.
pub struct DeliveryStatus {
    error: rdkafka::rd_kafka_resp_err_t,
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

/// A future that will receive a `DeliveryStatus` containing information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    receiver: Oneshot<DeliveryStatus>,
}

impl Future for DeliveryFuture {
    type Item = DeliveryStatus;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<DeliveryStatus, Canceled> {
        self.receiver.poll()
    }
}

struct MessageContext {
    copy: bool,
    sender: Complete<DeliveryStatus>,
}

impl MessageContext {
    fn new_boxed(copy: bool, sender: Complete<DeliveryStatus>) -> Box<MessageContext> {
        Box::new(MessageContext {
            copy: copy,
            sender: sender,
        })
    }

    fn into_raw(boxed_context: Box<MessageContext>) -> *mut c_void {
        Box::into_raw(boxed_context) as *mut c_void
    }

    unsafe fn from_raw(raw_ptr: *mut c_void) -> Box<MessageContext> {
        Box::from_raw(raw_ptr as *mut MessageContext)
    }
}

/// Callback that gets called from librdkafka every time a message succeeds
/// or fails to be delivered.
unsafe extern "C" fn delivery_cb(_client: *mut rdkafka::rd_kafka_t,
                                 message_ptr: *const rdkafka::rd_kafka_message_t,
                                 _opaque: *mut c_void) {
    let message_context = MessageContext::from_raw((*message_ptr)._private);
    // (*(message_ptr as *mut rdkafka::rd_kafka_message_t))._private = ptr::null_mut();
    let payload = if !message_context.copy {
        Some(Vec::from_raw_parts((*message_ptr).payload as *mut u8, (*message_ptr).len, (*message_ptr).len))
    } else {
        None
    };
    let key = if !message_context.copy {
        Some(Vec::from_raw_parts((*message_ptr).key as *mut u8, (*message_ptr).key_len, (*message_ptr).key_len))
    } else {
        None
    };
    let delivery_status = DeliveryStatus {
        error: (*message_ptr).err,
        partition: (*message_ptr).partition,
        offset: (*message_ptr).offset,
        payload: payload,
        key: key,
    };
    trace!("Delivery event received: {:?}, copy: {}", delivery_status, message_context.copy);
    message_context.sender.complete(delivery_status);
}

/// Creates a new Producer starting from a ClientConfig.
impl FromClientConfig for Producer {
    fn from_config(config: &ClientConfig) -> KafkaResult<Producer> {
        let mut producer_config = config.config_clone();
        producer_config.set_delivery_cb(delivery_cb);
        let client = try!(Client::new(&producer_config, ClientType::Producer));
        let producer = Producer { client: Arc::new(client) };
        Ok(producer)
    }
}

impl Producer {
    /// Returns a topic associated to the producer
    pub fn get_topic<'a>(&'a self, name: &str, config: &TopicConfig) -> KafkaResult<Topic<'a>> {
        Topic::new(&self.client, name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.client.ptr, timeout_ms) }
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(&self, topic: &Topic, partition: Option<i32>, payload: Option<&P>, key: Option<&K>) -> KafkaResult<DeliveryFuture>
        where K: ToBytes,
              P: ToBytes {
        let (payload_len, payload_ptr) = match payload {
            None => (0, ptr::null_mut()),
            Some(p) => {
                let p = P::to_bytes(p);
                (p.len(), p.as_ptr() as *mut c_void)
            }
        };
        let (key_len, key_ptr) = match key {
            None => (0, ptr::null_mut()),
            Some(k) => {
                let k = K::to_bytes(k);
                (k.len(), k.as_ptr() as *mut c_void)
            }
        };
        produce(topic, partition, payload_ptr, payload_len, key_ptr, key_len, true)
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send<P, K>(&self, topic: &Topic, partition: Option<i32>, payload: Option<P>, key: Option<K>) -> KafkaResult<DeliveryFuture>
        where K: ToBytesBox,
              P: ToBytesBox {
        let (payload_len, payload_ptr) = match payload {
            None => (0, ptr::null_mut()),
            Some(p) => {
                let p = P::to_bytes(p);
                (p.len(), Box::into_raw(p) as *mut c_void)
            }
        };
        let (key_len, key_ptr) = match key {
            None => (0, ptr::null_mut()),
            Some(k) => {
                let k = K::to_bytes(k);
                (k.len(), Box::into_raw(k) as *mut c_void)
            }
        };
        produce(topic, partition, payload_ptr, payload_len, key_ptr, key_len, false)
    }

    /// Starts the polling thread for the producer. It returns a `ProducerPollingThread` that will
    /// process all the events. Calling `poll` is not required if the `ProducerPollingThread`
    /// thread is running.
    pub fn start_polling_thread(&self) -> ProducerPollingThread {
        let mut threaded_producer = ProducerPollingThread::new(self);
        threaded_producer.start();
        threaded_producer
    }
}

fn produce(topic: &Topic, partition: Option<i32>, payload_ptr: *mut c_void, payload_len: usize,
           key_ptr: *mut c_void, key_len: usize, copy: bool) -> KafkaResult<DeliveryFuture> {
    let (sender, receiver) = futures::oneshot();
    let message_context = MessageContext::new_boxed(copy, sender);
    let producer_flags = if copy {
        rdkafka::RD_KAFKA_MSG_F_COPY as i32
    } else {
        0
    };
    let produce_response = unsafe {
        rdkafka::rd_kafka_produce(
            topic.get_ptr(),
            partition.unwrap_or(-1),
            producer_flags,
            payload_ptr,
            payload_len,
            key_ptr,
            key_len,
            MessageContext::into_raw(message_context),
        )
    };
    if produce_response != 0 {
        let errno = errno::errno().0 as i32;
        let kafka_error = unsafe { rdkafka::rd_kafka_errno2err(errno) };
        Err(KafkaError::MessageProduction(kafka_error))
    } else {
        Ok(DeliveryFuture { receiver: receiver })
    }
}

/// A producer with an internal running thread. This producer doesn't neeed to be polled.
/// The internal thread can be terminated with the `stop` method or moving the
/// `ProducerPollingThread` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct ProducerPollingThread {
    producer: Producer,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ProducerPollingThread {
    /// Creates a new producer. The internal thread will not be running yet.
    pub fn new(producer: &Producer) -> ProducerPollingThread {
        ProducerPollingThread {
            producer: producer.clone(),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Starts the internal polling thread.
    pub fn start(&mut self) {
        let producer = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                trace!("Polling thread loop started");
                while !should_stop.load(Ordering::Relaxed) {
                    let n = producer.poll(100);
                    if n != 0 {
                        trace!("Receved {} events", n);
                    }
                }
                trace!("Polling thread loop terminated");
            })
            .expect("Failed to start polling thread");
        self.handle = Some(handle);
    }

    /// Stops the internal polling thread. The thread can also be stopped by moving
    /// the ProducerPollingThread out of scope.
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

impl Drop for ProducerPollingThread {
    fn drop(&mut self) {
        trace!("Destroy ProducerPollingThread");
        self.stop();
    }
}
