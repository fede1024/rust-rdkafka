//! Producer implementations.
extern crate rdkafka_sys as rdkafka;
extern crate errno;
extern crate futures;

use self::rdkafka::types::*;

use std::ffi::CString;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::thread;

use self::futures::{Canceled, Complete, Future, Poll, Oneshot};

use client::{Client, Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel, TopicConfig};
use error::{KafkaError, KafkaResult, IsError};
use message::ToBytes;
use util::cstr_to_owned;

//
// ********** NATIVE TOPIC **********
//

/// A native rdkafka-sys topic, used for message production.
struct NativeTopic {
    ptr: *mut RDKafkaTopic,
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

impl NativeTopic {
    // TODO: this is not safe, since the called needs to know that the returned value cannot outlive
    // the client. Should we add a reference to the producer?
    /// Given a pointer to a Kafka client, a topic name and a topic configuration, returns a new
    /// RDKafkaTopic created by the given client. The returned RDKafkaTopic should not outlive
    /// the client it was created from.
    unsafe fn new(client: *mut RDKafka, name: &str, topic_config_ptr: *mut RDKafkaTopicConf)
            -> KafkaResult<NativeTopic> {
        let name_ptr = CString::new(name.to_string()).expect("could not create name CString"); // TODO: remove expect
        let topic_ptr = rdkafka::rd_kafka_topic_new(client, name_ptr.as_ptr(), topic_config_ptr);
        if topic_ptr.is_null() {
            Err(KafkaError::TopicCreation(name.to_owned()))
        } else {
            Ok(NativeTopic { ptr: topic_ptr })
        }
    }

    fn ptr(&self) -> *mut RDKafkaTopic {
        self.ptr
    }

    fn name(&self) -> String {
        unsafe {
            cstr_to_owned(rdkafka::rd_kafka_topic_name(self.ptr))
        }
    }

    /// Sends a copy of the provided data to the topic.
    fn send_copy<T>(&self, partition: Option<i32>, payload: Option<&[u8]>, key: Option<&[u8]>,
                    delivery_context: Option<Box<T>>)
            -> KafkaResult<()> {
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
                self.ptr(),
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
}

impl Drop for NativeTopic {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka_topic");
        unsafe {
            rdkafka::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

//
// ********** PRODUCER CONTEXT **********
//

/// A `ProducerContext` is a `Context` specific for producers. It can be used to store user-specified
/// callbacks, such as `delivery`.
pub trait ProducerContext: Context {
    /// A DeliveryContext is a user-defined structure that will be passed to the producer when
    /// producing a message, and returned to the `delivery` method once the message has been
    /// delivered, or failed to.
    type DeliveryContext: Send + Sync;

    /// This method will be called once the message has been delivered (or failed to). The
    /// `DeliveryContext` will be the one provided by the user when calling send.
    fn delivery(&self, DeliveryReport, Self::DeliveryContext);
}

/// Simple empty producer context that can be use when the producer context is not required.
pub struct EmptyProducerContext;

impl Context for EmptyProducerContext { }
impl ProducerContext for EmptyProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, _: DeliveryReport, _: Self::DeliveryContext) { }
}

#[derive(Debug)]
/// Information returned by the producer after a message has been delivered
/// or failed to be delivered.
pub struct DeliveryReport {
    error: RDKafkaRespErr,
    partition: i32,
    offset: i64,
}

impl DeliveryReport {
    /// Creates a new `DeliveryReport`. This should only be used in the delivery_cb.
    fn new(err: RDKafkaRespErr, partition: i32, offset: i64) -> DeliveryReport {
        DeliveryReport {
            error: err,
            partition: partition,
            offset: offset,
        }
    }

    /// Returns the result of the production of the message.
    pub fn result(&self) -> KafkaResult<(i32, i64)> {
        if self.error.is_error() {
            Err(KafkaError::MessageProduction(self.error))
        } else {
            Ok((self.partition, self.offset))
        }
    }

    /// Returns the partition of the message.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset of the message.
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

/// Callback that gets called from librdkafka every time a message succeeds
/// or fails to be delivered.
unsafe extern "C" fn delivery_cb<C: ProducerContext>(
        _client: *mut RDKafka, msg: *const RDKafkaMessage, _opaque: *mut c_void) {
    let context = Box::from_raw(_opaque as *mut C);
    let delivery_context = Box::from_raw((*msg)._private as *mut C::DeliveryContext);
    let delivery_status = DeliveryReport::new((*msg).err, (*msg).partition, (*msg).offset);
    trace!("Delivery event received: {:?}", delivery_status);
    (*context).delivery(delivery_status, (*delivery_context));
    mem::forget(context);   // Do not free the context
}

//
// ********** BASE PRODUCER **********
//

/// The `BaseProducer` implementation.
struct _BaseProducer<C: ProducerContext> {
    client: Client<C>,
}

impl<C: ProducerContext> _BaseProducer<C> {
    fn new(config: &ClientConfig, context: C) -> KafkaResult<_BaseProducer<C>> {
        let native_config = try!(config.create_native_config());
        unsafe { rdkafka::rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(delivery_cb::<C>)) };
        let client = try!(Client::new(config, native_config, RDKafkaType::RD_KAFKA_PRODUCER, context));
        Ok(_BaseProducer { client: client })
    }
}

/// Simple Kafka producer. This producer needs to be `poll`ed at regular intervals in order to
/// serve queued delivery report callbacks. This producer can be cheaply cloned to
/// create a new reference to the same underlying producer. Data production should be done using the
/// `BaseProducerTopic`, that can be created from this producer.
pub struct BaseProducer<C: ProducerContext> {
    inner: Arc<_BaseProducer<C>>,
}

// TODO: use alias instead
impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> BaseProducer<C> {
        BaseProducer { inner: self.inner.clone() }
    }
}

impl FromClientConfig for BaseProducer<EmptyProducerContext> {
    /// Creates a new `BaseProducer` starting from a configuration.
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseProducer<EmptyProducerContext>> {
        BaseProducer::from_config_and_context(config, EmptyProducerContext)
    }
}

impl<C: ProducerContext> FromClientConfigAndContext<C> for BaseProducer<C> {
    /// Creates a new `BaseProducer` starting from a configuration and a context.
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseProducer<C>> {
        let inner = try!(_BaseProducer::new(config, context));
        let producer = BaseProducer {
            inner: Arc::new(inner),
        };
        Ok(producer)
    }
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Returns a topic associated to the producer
    pub fn get_topic(&self, name: &str, config: &TopicConfig) -> KafkaResult<BaseProducerTopic<C>> {
        BaseProducerTopic::new(self.clone(), name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.inner.client.native_ptr(), timeout_ms) }
    }

    /// Returns a pointer to the native Kafka client
    fn native_ptr(&self) -> *mut RDKafka {
        self.inner.client.native_ptr()
    }
}

/// Represents a Kafka topic with an associated `BaseProducer`.
pub struct BaseProducerTopic<C: ProducerContext> {
    native_topic: NativeTopic,
    // A producer topic cannot outlive the client it was created from.
    #[allow(dead_code)]
    producer: BaseProducer<C>,
}

impl<C: ProducerContext> BaseProducerTopic<C> {
    /// Creates the BaseProducerTopic.
    pub fn new(producer: BaseProducer<C>, name: &str, topic_config: &TopicConfig)
            -> KafkaResult<BaseProducerTopic<C>> {
        let config_ptr = try!(topic_config.create_native_config());
        let native_topic = try!(unsafe { NativeTopic::new(producer.inner.client.native_ptr(), name, config_ptr) });
        let producer_topic = BaseProducerTopic {
            native_topic: native_topic,
            producer: producer.clone(),
        };
        Ok(producer_topic)
    }

    /// Get topic's name
    pub fn get_name(&self) -> String {
        self.native_topic.name()
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key. If no key is
    /// specified, a random partition will be used.
    pub fn send_copy<P, K>(&self, partition: Option<i32>, payload: Option<&P>, key: Option<&K>,
                           delivery_context: Option<Box<C::DeliveryContext>>) -> KafkaResult<()>
            where K: ToBytes,
                  P: ToBytes {
        self.native_topic.send_copy::<C::DeliveryContext>(partition, payload.map(P::to_bytes),
                                                          key.map(K::to_bytes), delivery_context)
        }
}

//
// ********** FUTURE PRODUCER **********
//

/// The `ProducerContext` used by the `FutureProducer`. This context will use a Future as its
/// `DeliveryContext` and will complete the future when the message is delivered (or failed to).
pub struct FutureProducerContext<C: Context + 'static> {
    wrapped_context: C
}

// Delegates all the methods calls to the wrapped context.
impl<C: Context + 'static> Context for FutureProducerContext<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, json: String) {
        self.wrapped_context.stats(json);
    }
}

impl<C: Context + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryContext = Complete<DeliveryReport>;

    fn delivery(&self, status: DeliveryReport, tx: Complete<DeliveryReport>) {
        tx.complete(status);
    }
}

/// `FutureProducer` implementation.
#[must_use = "Producer polling thread will stop immediately if unused"]
struct _FutureProducer<C: Context + 'static> {
    producer: BaseProducer<FutureProducerContext<C>>,
    should_stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,  // TODO: is the lock actually needed?
}

impl<C: Context + 'static> _FutureProducer<C> {
    fn start(&self) {
        let producer_clone = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                 trace!("Polling thread loop started");
                 while !should_stop.load(Ordering::Relaxed) {
                     let n = producer_clone.poll(100);
                     if n != 0 {
                         trace!("Received {} events", n);
                     }
                 }
                 trace!("Polling thread loop terminated");
            })
            .expect("Failed to start polling thread");
        match self.handle.write() {
            Ok(mut handle_ref) => *handle_ref = Some(handle),
            Err(_) => panic!("Poison error"),
        };
    }

    fn stop(&self) {
        match self.handle.write() {
            Ok(mut handle) => {
                if handle.is_some() {
                    trace!("Stopping polling");
                    self.should_stop.store(true, Ordering::Relaxed);
                    trace!("Waiting for polling thread termination");
                    match handle.take().expect("no handle present in producer context").join() {
                        Ok(()) => trace!("Polling stopped"),
                        Err(e) => warn!("Failure while terminating thread: {:?}", e),
                    };
                }
            },
            Err(_) => panic!("Poison error"),
        };
    }
}

impl<C: Context + 'static> Drop for _FutureProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy _FutureProducer");
        self.stop();
    }
}

/// A producer with an internal running thread. This producer doesn't need to be polled.
/// The internal thread can be terminated with the `stop` method or moving the
/// `FutureProducer` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: Context + 'static> {
    inner: Arc<_FutureProducer<C>>,
}

impl<C: Context + 'static> Clone for FutureProducer<C> {
    fn clone(&self) -> FutureProducer<C> {
        FutureProducer { inner: self.inner.clone() }
    }
}

impl FromClientConfig for FutureProducer<EmptyContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer<EmptyContext>> {
        FutureProducer::from_config_and_context(config, EmptyContext)
    }
}

impl<C: Context + 'static> FromClientConfigAndContext<C> for FutureProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<FutureProducer<C>> {
        let future_context = FutureProducerContext { wrapped_context: context};
        let inner = _FutureProducer {
            producer: BaseProducer::from_config_and_context(config, future_context)?,
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: RwLock::new(None),
        };
        Ok(FutureProducer { inner: Arc::new(inner) })
    }
}

/// A future that will receive a `DeliveryReport` containing information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Oneshot<DeliveryReport>,
}

impl Future for DeliveryFuture {
    type Item = DeliveryReport;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<DeliveryReport, Canceled> {
        self.rx.poll()
    }
}

impl<C: Context + 'static> FutureProducer<C> {
    /// Returns a topic associated to the producer
    pub fn get_topic(&self, name: &str, config: &TopicConfig) -> KafkaResult<FutureProducerTopic<C>> {
        FutureProducerTopic::new(self.clone(), name, config)
    }

    /// Starts the internal polling thread.
    pub fn start(&self) {
        self.inner.start();
    }

    /// Stops the internal polling thread. The thread can also be stopped by moving
    /// the `ProducerPollingThread` out of scope.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Returns a pointer to the native Kafka client
    fn native_ptr(&self) -> *mut RDKafka {
        self.inner.producer.native_ptr()
    }
}

/// Represents a Kafka topic with an associated `FutureProducer`.
pub struct FutureProducerTopic<C: Context + 'static> {
    native_topic: NativeTopic,
    // A producer topic cannot outlive the client it was created from.
    #[allow(dead_code)]
    producer: FutureProducer<C>,
}

impl<C: Context + 'static> FutureProducerTopic<C> {
    /// Creates the ProducerTopic. TODO: update doc
    pub fn new(producer: FutureProducer<C>, name: &str, topic_config: &TopicConfig)
            -> KafkaResult<FutureProducerTopic<C>> {
        let config_ptr = try!(topic_config.create_native_config());
        let native_topic = try!(unsafe { NativeTopic::new(producer.native_ptr(), name, config_ptr) });
        let producer_topic = FutureProducerTopic {
            native_topic: native_topic,
            producer: producer.clone(),
        };
        Ok(producer_topic)
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(&self, partition: Option<i32>, payload: Option<&P>, key: Option<&K>)
            -> KafkaResult<DeliveryFuture>
            where K: ToBytes,
                  P: ToBytes {
        let (tx, rx) = futures::oneshot();
        try!(self.native_topic.send_copy::<Complete<DeliveryReport>>(partition, payload.map(P::to_bytes),
                                                                     key.map(K::to_bytes), Some(Box::new(tx))));
        Ok(DeliveryFuture{rx: rx})
    }

    /// Get topic's name
    pub fn get_name(&self) -> String {
        self.native_topic.name()
    }
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.
    use super::*;
    use config::{ClientConfig, TopicConfig};

    #[test]
    fn test_base_producer_topic() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();
        assert_eq!(producer_topic.get_name(), "topic_name");
    }

    #[test]
    fn test_future_producer_topic() {
        let producer = ClientConfig::new().create::<FutureProducer<_>>().unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();
        assert_eq!(producer_topic.get_name(), "topic_name");
    }
}
