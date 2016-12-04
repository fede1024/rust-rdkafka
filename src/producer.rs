//! Producer implementations.
extern crate rdkafka_sys as rdkafka;
extern crate errno;
extern crate futures;

use self::rdkafka::types::*;

use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::thread;

use self::futures::{Canceled, Complete, Future, Poll, Oneshot};

use client::{Client, Context, Topic};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, TopicConfig};
use error::{KafkaError, KafkaResult};
use message::ToBytes;


pub trait ProducerContext: Context {
    /// A DeliveryContext is a user-defined structure that will be passed to the base producer when
    /// producing a message. The base producer will call the `received` method on this data once the
    /// associated message has been processed correctly.
    type DeliveryContext: Send + Sync;

    /// This method will be called once the message has beed delivered (or failed to). The
    /// DeliveryContext will be the one provided by the user when calling send.
    fn delivery(&self, DeliveryStatus, Self::DeliveryContext);
}

#[derive(Clone)]
struct EmptyProducerContext;

impl Context for EmptyProducerContext { }
impl ProducerContext for EmptyProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, _: DeliveryStatus, _: Self::DeliveryContext) { }
}

/// Contains a reference counted producer client. It can be safely cloned to
/// create another reference to the same producer.
pub struct BaseProducer<C: ProducerContext> {
    client: Client<C>,
}

#[derive(Debug)]
/// Information returned by the producer after a message has been delivered
/// or failed to be delivered.
pub struct DeliveryStatus {
    error: RDKafkaRespErr,
    partition: i32,
    offset: i64,
}

/// Callback that gets called from librdkafka every time a message succeeds
/// or fails to be delivered.
unsafe extern "C" fn delivery_cb<C: ProducerContext>(_client: *mut RDKafka, msg: *const RDKafkaMessage, _opaque: *mut c_void) {
    let context = Box::from_raw(_opaque as *mut C);
    let delivery_context = Box::from_raw((*msg)._private as *mut C::DeliveryContext);
    let delivery_status = DeliveryStatus {
        error: (*msg).err,
        partition: (*msg).partition,
        offset: (*msg).offset,
    };
    trace!("Delivery event received: {:?}", delivery_status);
    (*context).delivery(delivery_status, (*delivery_context))
}

impl FromClientConfig for BaseProducer<EmptyProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseProducer<EmptyProducerContext>> {
        BaseProducer::from_config_and_context(config, EmptyProducerContext)
    }
}

/// Creates a new `BaseProducer` starting from a ClientConfig.
impl<C: ProducerContext> FromClientConfigAndContext<C> for BaseProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseProducer<C>> {
        let config_ptr = try!(config.create_native_config());
        unsafe { rdkafka::rd_kafka_conf_set_dr_msg_cb(config_ptr, Some(delivery_cb::<C>)) };
        let client = try!(Client::new(config_ptr, RDKafkaType::RD_KAFKA_PRODUCER, context));
        let producer = BaseProducer { client: client };
        Ok(producer)
    }
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Returns a topic associated to the producer
    pub fn get_topic<'b>(&'b self, name: &str, config: &TopicConfig) -> KafkaResult<Topic<'b, C>> {
        Topic::new(&self.client, name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.client.native_ptr(), timeout_ms) }
    }

    fn _send_copy(&self, topic: &Topic<C>, partition: Option<i32>, payload: Option<&[u8]>, key: Option<&[u8]>,   delivery_context: Option<Box<C::DeliveryContext>>) -> KafkaResult<()> {
        let (payload_ptr, payload_len) = match payload {
            None => (ptr::null_mut(), 0),
            Some(p) => (p.as_ptr() as *mut c_void, p.len()),
        };
        let (key_ptr, key_len) = match key {
            None => (ptr::null_mut(), 0),
            Some(k) => (k.as_ptr() as *mut c_void, k.len()),
        };
        let delivery_context_ptr = match delivery_context {
            Some(context) => Box::into_raw(context) as *mut c_void,
            None => ptr::null_mut(),
        };
        let produce_response = unsafe {
            rdkafka::rd_kafka_produce(
                topic.ptr(),
                partition.unwrap_or(-1),
                rdkafka::RD_KAFKA_MSG_F_COPY as i32,
                payload_ptr,
                payload_len,
                key_ptr,
                key_len,
                delivery_context_ptr,
            )
        };
        if produce_response != 0 {
            let errno = errno::errno().0 as i32;
            let kafka_error = unsafe { rdkafka::rd_kafka_errno2err(errno) };
            Err(KafkaError::MessageProduction(kafka_error))
        } else {
            Ok(())
        }
    }

    pub fn send_copy<P, K>(&self, topic: &Topic<C>, partition: Option<i32>, payload: Option<&P>, key: Option<&K>, delivery_context: Option<Box<C::DeliveryContext>>) -> KafkaResult<()>
        where K: ToBytes,
              P: ToBytes {
        self._send_copy(topic, partition, payload.map(P::to_bytes), key.map(K::to_bytes), delivery_context)
    }
}

/// The ProducerContext used by the FutureProducer. This context will use a Future as its
/// DeliveryContext and will complete the future when the message is delivered (or failed to). All
/// other callbacks will be delegated to the wrapped ProducerContext.
#[derive(Clone)]
pub struct FutureProducerContext<C: ProducerContext> {
    _inner_context: C
}

impl<C: ProducerContext> Context for FutureProducerContext<C> {}

impl<C: ProducerContext> ProducerContext for FutureProducerContext<C> {
    type DeliveryContext = Complete<DeliveryStatus>;

    fn delivery(&self, status: DeliveryStatus, tx: Complete<DeliveryStatus>) {
        tx.complete(status);
    }
}

/// A producer with an internal running thread. This producer doesn't neeed to be polled.
/// The internal thread can be terminated with the `stop` method or moving the
/// `ProducerPollingThread` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: ProducerContext + 'static> {
    producer: Arc<BaseProducer<FutureProducerContext<C>>>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FromClientConfig for FutureProducer<EmptyProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer<EmptyProducerContext>> {
        FutureProducer::from_config_and_context(config, EmptyProducerContext)
    }
}

impl<C: ProducerContext> FromClientConfigAndContext<C> for FutureProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<FutureProducer<C>> {
        let future_producer_context = FutureProducerContext{_inner_context: context} ;
        let producer = try!(BaseProducer::from_config_and_context(config, future_producer_context));
        let future_producer = FutureProducer {
            producer: Arc::new(producer),
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: None,
        };
        Ok(future_producer)
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

impl<C: ProducerContext> FutureProducer<C> {
    /// Starts the internal polling thread.
    pub fn start(&mut self) {
        let producer_clone = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                 trace!("Polling thread loop started");
                 while !should_stop.load(Ordering::Relaxed) {
                     let n = producer_clone.poll(100);
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

    /// Returns a topic associated to the current producer.
    pub fn get_topic<'a>(&'a self, name: &str, config: &TopicConfig) -> KafkaResult<Topic<'a, FutureProducerContext<C>>> {
        self.producer.get_topic(name, config)
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(&self, topic: &Topic<FutureProducerContext<C>>, partition: Option<i32>, payload: Option<&P>, key: Option<&K>) -> KafkaResult<DeliveryFuture>
        where K: ToBytes,
              P: ToBytes {
        let (tx, rx) = futures::oneshot();
        try!(self.producer.send_copy(topic, partition, payload, key, Some(Box::new(tx))));
        Ok(DeliveryFuture{rx: rx})
    }
}

impl<C: ProducerContext> Drop for FutureProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy ProducerPollingThread");
        self.stop();
    }
}
