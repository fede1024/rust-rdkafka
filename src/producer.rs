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
use std::ffi::CString;

use self::futures::{Canceled, Complete, Future, Poll, Oneshot};

use client::{Client, Context};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, TopicConfig};
use error::{KafkaError, KafkaResult};
use message::ToBytes;
use util::cstr_to_owned;


/// A ProducerContext is a Context specific for producers. It can be used to store user-specified
/// callbacks, such as `delivery`.
pub trait ProducerContext: Context {
    /// A DeliveryContext is a user-defined structure that will be passed to the base producer when
    /// producing a message. The base producer will call the `received` method on this data once the
    /// associated message has been processed correctly.
    type DeliveryContext: Send + Sync;

    /// This method will be called once the message has beed delivered (or failed to). The
    /// DeliveryContext will be the one provided by the user when calling send.
    fn delivery(&self, DeliveryStatus, Self::DeliveryContext);
}

pub struct EmptyProducerContext;

impl Context for EmptyProducerContext { }
impl ProducerContext for EmptyProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, _: DeliveryStatus, _: Self::DeliveryContext) { }
}

/// Contains a reference counted producer client.
pub struct BaseProducer<C: ProducerContext> {
    client: Arc<Client<C>>,
}

#[derive(Debug)]
/// Information returned by the producer after a message has been delivered
/// or failed to be delivered.
pub struct DeliveryStatus {
    error: RDKafkaRespErr,
    partition: i32,
    offset: i64,
}

impl DeliveryStatus {
    /// Creates a new DeliveryStatus. This should only be used in the delivery_cb.
    fn new(err: RDKafkaRespErr, partition: i32, offset: i64) -> DeliveryStatus {
        DeliveryStatus {
            error: err,
            partition: partition,
            offset: offset,
        }
    }

    /// Returns the error associated with the production of the message.
    pub fn error(&self) -> RDKafkaRespErr {
        self.error
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
unsafe extern "C" fn delivery_cb<C: ProducerContext>(_client: *mut RDKafka, msg: *const RDKafkaMessage, _opaque: *mut c_void) {
    let context = Box::from_raw(_opaque as *mut C);
    let delivery_context = Box::from_raw((*msg)._private as *mut C::DeliveryContext);
    let delivery_status = DeliveryStatus::new((*msg).err, (*msg).partition, (*msg).offset);
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
        let producer = BaseProducer { client: Arc::new(client) };
        Ok(producer)
    }
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Returns a topic associated to the producer
    pub fn get_topic(&self, name: &str, config: &TopicConfig) -> KafkaResult<ProducerTopic<C>> {
        ProducerTopic::new(&self, name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdkafka::rd_kafka_poll(self.client.native_ptr(), timeout_ms) }
    }

    fn client(&self) -> &Arc<Client<C>> {
        &self.client
    }
}

/// Represents a Kafka topic with an associated BaseProducer.
pub struct ProducerTopic<C: ProducerContext> {
    ptr: *mut RDKafkaTopic,
    // A producer client cannot outlive the client it was created from.
    #[allow(dead_code)]
    client: Arc<Client<C>>,
}

impl<C: ProducerContext> ProducerTopic<C> {
    /// Creates the ProducerTopic.
    pub fn new(producer: &BaseProducer<C>, name: &str, topic_config: &TopicConfig) -> KafkaResult<ProducerTopic<C>> {
        let name_ptr = CString::new(name.to_string()).unwrap();
        let config_ptr = try!(topic_config.create_native_config());
        let topic_ptr = unsafe { rdkafka::rd_kafka_topic_new(producer.client().native_ptr(), name_ptr.as_ptr(), config_ptr) };
        if topic_ptr.is_null() {
            Err(KafkaError::TopicCreation(name.to_string()))
        } else {
            let topic = ProducerTopic {
                ptr: topic_ptr,
                client: producer.client().clone(),
            };
            Ok(topic)
        }
    }

    /// Get topic's name
    pub fn get_name(&self) -> String {
        unsafe {
            cstr_to_owned(rdkafka::rd_kafka_topic_name(self.ptr))
        }
    }

    /// Returns a pointer to the correspondent rdkafka `RDKafkaTopic` struct.
    pub fn ptr(&self) -> *mut RDKafkaTopic {
        self.ptr
    }

    fn _send_copy(&self, partition: Option<i32>, payload: Option<&[u8]>, key: Option<&[u8]>,   delivery_context: Option<Box<C::DeliveryContext>>) -> KafkaResult<()> {
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

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key. If no key is
    /// specified, a random partition will be used.
    pub fn send_copy<P, K>(&self, partition: Option<i32>, payload: Option<&P>, key: Option<&K>, delivery_context: Option<Box<C::DeliveryContext>>) -> KafkaResult<()>
        where K: ToBytes,
              P: ToBytes {
        self._send_copy(partition, payload.map(P::to_bytes), key.map(K::to_bytes), delivery_context)
    }
}

impl<C: ProducerContext> Drop for ProducerTopic<C> {
    fn drop(&mut self) {
        trace!("Destroy rd_kafka_topic");
        unsafe {
            rdkafka::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

/// The ProducerContext used by the FutureProducer. This context will use a Future as its
/// DeliveryContext and will complete the future when the message is delivered (or failed to). All
/// other callbacks will be delegated to the wrapped ProducerContext.
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
        let base_producer = try!(BaseProducer::from_config_and_context(config, future_producer_context));
        let future_producer = FutureProducer {
            producer: Arc::new(base_producer),
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
    /// Returns a topic associated to the producer
    pub fn get_topic(&self, name: &str, config: &TopicConfig)
          -> KafkaResult<FutureProducerTopic<C>> {
        FutureProducerTopic::new(self, name, config)
    }

    pub fn base_producer(&self) -> &BaseProducer<FutureProducerContext<C>> {
        &self.producer
    }

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
}

/// A topic with associated FutureProducer.
pub struct FutureProducerTopic<C: ProducerContext> {
    inner: ProducerTopic<FutureProducerContext<C>>
}

impl<C: ProducerContext> FutureProducerTopic<C> {
    /// Creates the ProducerTopic.
    pub fn new(producer: &FutureProducer<C>, name: &str, topic_config: &TopicConfig) -> KafkaResult<FutureProducerTopic<C>> {
        let future_producer_topic = FutureProducerTopic {
            inner: try!(ProducerTopic::new(producer.base_producer(), name, topic_config))
        };
        Ok(future_producer_topic)
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(&self, partition: Option<i32>, payload: Option<&P>, key: Option<&K>) -> KafkaResult<DeliveryFuture>
        where K: ToBytes,
              P: ToBytes {
        let (tx, rx) = futures::oneshot();
        try!(self.inner.send_copy(partition, payload, key, Some(Box::new(tx))));
        Ok(DeliveryFuture{rx: rx})
    }
}

impl<C: ProducerContext> Drop for FutureProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy ProducerPollingThread");
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.
    use super::*;
    use config::{ClientConfig, TopicConfig};

    #[test]
    fn test_topic() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();
        assert_eq!(producer_topic.get_name(), "topic_name");
        assert!(!producer_topic.ptr().is_null());
    }
}
