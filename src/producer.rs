//! Producer implementations.
use futures::{self, Canceled, Complete, Future, Poll, Oneshot};
use rdsys::rd_kafka_vtype_t::*;
use rdsys::types::*;
use rdsys;

use client::{Client, Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel, TopicConfig};
use error::{KafkaError, KafkaResult, IsError};
use message::ToBytes;
use statistics::Statistics;
use util::cstr_to_owned;

use std::ffi::CString;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

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
#[derive(Clone)]
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

/// Simple Kafka producer. This producer needs to be `poll`ed at regular intervals in order to
/// serve queued delivery report callbacks. This producer can be cheaply cloned to
/// create a new reference to the same underlying producer. Data production should be done using the
/// `BaseProducerTopic`, that can be created from this producer.
pub struct BaseProducer<C: ProducerContext> {
    client_arc: Arc<Client<C>>,
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Creates a base producer starting from a Client.
    fn from_client(client: Client<C>) -> BaseProducer<C> {
        BaseProducer { client_arc: Arc::new(client) }
    }

    /// Returns a topic associated to the producer.
    pub fn get_topic(&self, name: &str, config: &TopicConfig) -> KafkaResult<BaseProducerTopic<C>> {
        BaseProducerTopic::new(self.clone(), name, config)
    }

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdsys::rd_kafka_poll(self.client_arc.native_ptr(), timeout_ms) }
    }

    /// Returns a pointer to the native Kafka client.
    fn native_ptr(&self) -> *mut RDKafka {
        self.client_arc.native_ptr()
    }
}

impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> BaseProducer<C> {
        BaseProducer { client_arc: self.client_arc.clone() }
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
        let native_config = config.create_native_config()?;
        unsafe { rdsys::rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(delivery_cb::<C>)) };
        let client = Client::new(config, native_config, RDKafkaType::RD_KAFKA_PRODUCER, context)?;
        Ok(BaseProducer::from_client(client))
    }
}

struct NativeTopic {
    ptr: *mut RDKafkaTopic,
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

impl NativeTopic {
    /// Wraps a pointer to an `RDKafkaTopic` object and returns a new `Native `.
    pub fn from_ptr(ptr: *mut RDKafkaTopic) -> NativeTopic {
        NativeTopic { ptr: ptr }
    }

    /// Returns the pointer to the librdkafka RDKafkaTopic structure.
    pub fn ptr(&self) -> *mut RDKafkaTopic {
        self.ptr
    }
}

impl Drop for NativeTopic {
    fn drop(&mut self) {
        trace!("Destroy NativeTopic");
        unsafe {
            rdsys::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

/// Represents a Kafka topic with an associated `BaseProducer`. This struct can be cheaply
/// cloned, generating a new reference to the same underlying resource, and can be used
/// across different threads.
#[derive(Clone)]
pub struct BaseProducerTopic<C: ProducerContext> {
    native_topic: Arc<NativeTopic>,
    // A producer topic cannot outlive the client it was created from.
    producer: BaseProducer<C>,
}

impl<C: ProducerContext> BaseProducerTopic<C> {
    /// Creates the BaseProducerTopic.
    pub fn new(producer: BaseProducer<C>, name: &str, topic_config: &TopicConfig)
            -> KafkaResult<BaseProducerTopic<C>> {
        let name_cstring = CString::new(name.to_string())?;
        let native_topic_config = topic_config.create_native_config()?;
        let topic_ptr = unsafe {
            rdsys::rd_kafka_topic_new(producer.native_ptr(), name_cstring.as_ptr(), native_topic_config.ptr_move())
        };
        if topic_ptr.is_null() {
            Err(KafkaError::TopicCreation(name.to_owned()))
        } else {
            let native_topic = NativeTopic::from_ptr(topic_ptr);
            Ok(BaseProducerTopic {
                native_topic: Arc::new(native_topic),
                producer: producer
            })
        }
    }

    /// Get topic's name
    pub fn name(&self) -> String {
        unsafe {
            cstr_to_owned(rdsys::rd_kafka_topic_name(self.native_topic.ptr()))
        }
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key. If no key is
    /// specified, a random partition will be used.
    pub fn send_copy<P, K>(
        &self,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        delivery_context: Option<Box<C::DeliveryContext>>,
        timestamp: Option<i64>
    ) -> KafkaResult<()>
            where K: ToBytes,
                  P: ToBytes {
        let (payload_ptr, payload_len) = match payload.map(P::to_bytes) {
            None => (ptr::null_mut(), 0),
            Some(p) => (p.as_ptr() as *mut c_void, p.len()),
        };
        let (key_ptr, key_len) = match key.map(K::to_bytes) {
            None => (ptr::null_mut(), 0),
            Some(k) => (k.as_ptr() as *mut c_void, k.len()),
        };
        let delivery_context_ptr = match delivery_context {
            Some(context) => Box::into_raw(context) as *mut c_void,
            None => ptr::null_mut(),
        };
        let timestamp = match timestamp {
            Some(t) => t,
            None => 0
        };
        let produce_error = unsafe {
            rdsys::rd_kafka_producev(
                self.producer.native_ptr(),
                RD_KAFKA_VTYPE_RKT, self.native_topic.ptr(),
                RD_KAFKA_VTYPE_PARTITION, partition.unwrap_or(-1),
                RD_KAFKA_VTYPE_MSGFLAGS, rdsys::RD_KAFKA_MSG_F_COPY as i32,
                RD_KAFKA_VTYPE_VALUE, payload_ptr, payload_len,
                RD_KAFKA_VTYPE_KEY, key_ptr, key_len,
                RD_KAFKA_VTYPE_OPAQUE, delivery_context_ptr,
                RD_KAFKA_VTYPE_TIMESTAMP, timestamp,
                RD_KAFKA_VTYPE_END
            )
        };
        if produce_error.is_error() {
            Err(KafkaError::MessageProduction(produce_error))
        } else {
            Ok(())
        }
    }
}

//
// ********** FUTURE PRODUCER **********
//

/// The `ProducerContext` used by the `FutureProducer`. This context will use a Future as its
/// `DeliveryContext` and will complete the future when the message is delivered (or failed to).
#[derive(Clone)]
pub struct FutureProducerContext<C: Context + 'static> {
    wrapped_context: C
}

// Delegates all the methods calls to the wrapped context.
impl<C: Context + 'static> Context for FutureProducerContext<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.wrapped_context.stats(statistics);
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
                    match handle.take().expect("No handle present in producer context").join() {
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

impl<C: Context + 'static> FutureProducer<C> {
    fn base_producer(&self) -> BaseProducer<FutureProducerContext<C>> {
        self.inner.producer.clone()
    }
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
}

/// Represents a Kafka topic created by a `FutureProducer`. This struct can be cheaply
/// cloned, generating a new reference to the same underlying resource, and can be used
/// across different threads.
#[derive(Clone)]
pub struct FutureProducerTopic<C: Context + 'static> {
    topic: BaseProducerTopic<FutureProducerContext<C>>,
}

impl<C: Context + 'static> FutureProducerTopic<C> {
    /// Creates the `FutureProducerTopic`. The `FutureProducerTopic` can be used to produce data to
    /// the specified topic.
    pub fn new(producer: FutureProducer<C>, name: &str, topic_config: &TopicConfig)
            -> KafkaResult<FutureProducerTopic<C>> {
        let producer_topic = BaseProducerTopic::new(producer.base_producer(), name, topic_config)?;
        Ok(FutureProducerTopic {topic : producer_topic})
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(
        &self,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>
    ) -> KafkaResult<DeliveryFuture>
            where K: ToBytes,
                  P: ToBytes {
        let (tx, rx) = futures::oneshot();
        self.topic.send_copy(partition, payload, key, Some(Box::new(tx)), timestamp)?;
        Ok(DeliveryFuture{rx: rx})
    }

    /// Get topic's name.
    pub fn name(&self) -> String {
        self.topic.name()
    }
}

#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use config::{ClientConfig, TopicConfig};
    use std::thread;

    struct TestContext;

    impl Context for TestContext {}
    impl ProducerContext for TestContext {
        type DeliveryContext = i32;

        fn delivery(&self, _: DeliveryReport, _: Self::DeliveryContext) {
            unimplemented!()
        }
    }

    // Test that the base producer topic is clone, send and sync.
    #[test]
    fn test_base_producer_topic_clone_send_sync() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();

        let producer_topic_clone = producer_topic.clone();
        let handle = thread::spawn(move || {
            assert_eq!(producer_topic_clone.name(), "topic_name");
        });
        assert_eq!(producer_topic.name(), "topic_name");

        handle.join().unwrap();
    }

    // Test that the base producer topic can be used even if the context is not Clone.
    #[test]
    fn test_base_producer_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<TestContext, BaseProducer<_>>(test_context).unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();

        let handle = thread::spawn(move || {
            assert_eq!(producer_topic.name(), "topic_name");
        });

        handle.join().unwrap();
    }

    // Test that the future producer topic is clone, send and sync.
    #[test]
    fn test_future_producer_topic_clone_send_sync() {
        let producer = ClientConfig::new().create::<FutureProducer<_>>().unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();

        let producer_topic_clone = producer_topic.clone();
        let handle = thread::spawn(move || {
            assert_eq!(producer_topic_clone.name(), "topic_name");
        });
        assert_eq!(producer_topic.name(), "topic_name");

        handle.join().unwrap();
    }

    // Test that the future producer topic can be used even if the context is not Clone.
    #[test]
    fn test_base_future_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<TestContext, FutureProducer<_>>(test_context).unwrap();
        let producer_topic = producer.get_topic("topic_name", &TopicConfig::new()).unwrap();

        let handle = thread::spawn(move || {
            assert_eq!(producer_topic.name(), "topic_name");
        });

        handle.join().unwrap();
    }
}
