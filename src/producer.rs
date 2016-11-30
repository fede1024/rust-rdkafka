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

use client::{Context};
use config::{ClientConfig, FromClientConfig, TopicConfig};
use error::{KafkaError, KafkaResult};
use message::ToBytes;
use client::{Client, ClientType, Topic};


/// Contains a reference counted producer client. It can be safely cloned to
/// create another reference to the same producer.
#[derive(Clone)]
pub struct Producer<'a, C: Context + 'a> {
    client: Arc<Client<'a, C>>,
}

#[derive(Debug)]
/// Information returned by the producer after a message has been delivered
/// or failed to be delivered.
pub struct DeliveryStatus {
    error: rdkafka::rd_kafka_resp_err_t,
    partition: i32,
    offset: i64,
}


/// Callback that gets called from librdkafka every time a message succeeds
/// or fails to be delivered.
unsafe extern "C" fn delivery_cb(_client: *mut rdkafka::rd_kafka_t,
                                 msg: *const rdkafka::rd_kafka_message_t,
                                 _opaque: *mut c_void) {
    let tx = Box::from_raw((*msg)._private as *mut Complete<DeliveryStatus>);
    let delivery_status = DeliveryStatus {
        error: (*msg).err,
        partition: (*msg).partition,
        offset: (*msg).offset,
    };
    // TODO: add topic name?
    trace!("Delivery event received: {:?}", delivery_status);
    tx.complete(delivery_status);
}

/// Creates a new Producer starting from a ClientConfig.
impl<'a, C: Context + 'a> FromClientConfig<'a, C> for Producer<'a, C> {
    fn from_config(config: &ClientConfig, context: &'a C) -> KafkaResult<Producer<'a, C>> {
        let mut producer_config = config.config_clone();
        producer_config.set_delivery_cb(delivery_cb);
        let client = try!(Client::new(&producer_config, ClientType::Producer, context));
        let producer = Producer { client: Arc::new(client) };
        Ok(producer)
    }
}

/// A future that will receive a `DeliveryStatus` containing information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Oneshot<DeliveryStatus>,
}

impl Future for DeliveryFuture {
    type Item = DeliveryStatus;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<DeliveryStatus, Canceled> {
        self.rx.poll()
    }
}

impl<'a, C: Context + 'a> Producer<'a, C> {
    /// Returns a topic associated to the producer
    pub fn get_topic<'b>(&'b self, name: &str, config: &TopicConfig) -> KafkaResult<Topic<'b, C>> {
        Topic::new(&self.client, name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.client.ptr, timeout_ms) }
    }

    fn _send_copy(&self, topic: &Topic<C>, partition: Option<i32>, payload: Option<&[u8]>, key: Option<&[u8]>) -> KafkaResult<DeliveryFuture> {
        let (payload_ptr, payload_len) = match payload {
            None => (ptr::null_mut(), 0),
            Some(p) => (p.as_ptr() as *mut c_void, p.len()),
        };
        let (key_ptr, key_len) = match key {
            None => (ptr::null_mut(), 0),
            Some(k) => (k.as_ptr() as *mut c_void, k.len()),
        };
        let (tx, rx) = futures::oneshot();
        let boxed_tx = Box::new(tx);
        let partition_arg = partition.unwrap_or(-1);
        let produce_response = unsafe {
            rdkafka::rd_kafka_produce(
                topic.get_ptr(),
                partition_arg,
                rdkafka::RD_KAFKA_MSG_F_COPY as i32,
                payload_ptr,
                payload_len,
                key_ptr,
                key_len,
                Box::into_raw(boxed_tx) as *mut c_void
            )
        };
        if produce_response != 0 {
            let errno = errno::errno().0 as i32;
            let kafka_error = unsafe { rdkafka::rd_kafka_errno2err(errno) };
            Err(KafkaError::MessageProduction(kafka_error))
        } else {
            Ok(DeliveryFuture { rx: rx })
        }
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(&self, topic: &Topic<C>, partition: Option<i32>, payload: Option<&P>, key: Option<&K>) -> KafkaResult<DeliveryFuture>
        where K: ToBytes,
              P: ToBytes {
        self._send_copy(topic, partition, payload.map(P::to_bytes), key.map(K::to_bytes))
    }

    /// Starts the polling thread for the producer. It returns a `ProducerPollingThread` that will
    /// process all the events. Calling `poll` is not required if the `ProducerPollingThread`
    /// thread is running.
    pub fn start_polling_thread(&self) -> ProducerPollingThread<'a, C> {
        let mut threaded_producer = ProducerPollingThread::new(self);
        threaded_producer.start();
        threaded_producer
    }
}


/// A producer with an internal running thread. This producer doesn't neeed to be polled.
/// The internal thread can be terminated with the `stop` method or moving the
/// `ProducerPollingThread` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct ProducerPollingThread<'a, C: Context + 'a> {
    producer: Producer<'a, C>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<'a, C: Context + 'a> ProducerPollingThread<'a, C> {
    /// Creates a new producer. The internal thread will not be running yet.
    pub fn new(producer: &Producer<'a, C>) -> ProducerPollingThread<'a, C> {
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

impl<'a, C: Context + 'a> Drop for ProducerPollingThread<'a, C> {
    fn drop(&mut self) {
        trace!("Destroy ProducerPollingThread");
        self.stop();
    }
}
