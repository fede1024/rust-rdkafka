//! Producer implementations.
use futures::{self, Canceled, Complete, Future, Poll, Oneshot, Async};
use rdsys::rd_kafka_vtype_t::*;
use rdsys::types::*;
use rdsys;

use client::{Client, Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use error::{KafkaError, KafkaResult, IsError};
use message::ToBytes;
use statistics::Statistics;

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
            Err(KafkaError::MessageProduction(self.error.into()))
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

    /// Polls the producer. Regular calls to `poll` are required to process the evens
    /// and execute the message delivery callbacks.
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        unsafe { rdsys::rd_kafka_poll(self.native_ptr(), timeout_ms) }
    }

    /// Returns a pointer to the native Kafka client.
    fn native_ptr(&self) -> *mut RDKafka {
        self.client_arc.native_ptr()
    }

    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key. If no key is
    /// specified, a random partition will be used.
    pub fn send_copy<P, K>(
        &self,
        topic_name: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        delivery_context: Option<Box<C::DeliveryContext>>,
        timestamp: Option<i64>
    ) -> KafkaResult<()>
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
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
        let topic_name_c = CString::new(topic_name.to_owned())?;
        let produce_error = unsafe {
            rdsys::rd_kafka_producev(
                self.native_ptr(),
                RD_KAFKA_VTYPE_TOPIC, topic_name_c.as_ptr(),
                RD_KAFKA_VTYPE_PARTITION, partition.unwrap_or(-1),
                RD_KAFKA_VTYPE_MSGFLAGS, rdsys::RD_KAFKA_MSG_F_COPY as i32,
                RD_KAFKA_VTYPE_VALUE, payload_ptr, payload_len,
                RD_KAFKA_VTYPE_KEY, key_ptr, key_len,
                RD_KAFKA_VTYPE_OPAQUE, delivery_context_ptr,
                RD_KAFKA_VTYPE_TIMESTAMP, timestamp.unwrap_or(0),
                RD_KAFKA_VTYPE_END
            )
        };
        if produce_error.is_error() {
            Err(KafkaError::MessageProduction(produce_error.into()))
        } else {
            Ok(())
        }
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

//
// ********** FUTURE PRODUCER **********
//

/// The `ProducerContext` used by the `FutureProducer`. This context will use a Future as its
/// `DeliveryContext` and will complete the future when the message is delivered (or failed to).
#[derive(Clone)]
struct FutureProducerContext<C: Context + 'static> {
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

    /// Receives global errors from the librdkafka client.
    fn error(&self, error: KafkaError, reason: &str) {
        self.wrapped_context.error(error, reason);
    }
}

impl<C: Context + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryContext = Complete<KafkaResult<DeliveryReport>>;

    fn delivery(&self, status: DeliveryReport, tx: Complete<KafkaResult<DeliveryReport>>) {
        let _ = tx.send(Ok(status));
    }
}

/// `FutureProducer` implementation.
#[must_use = "Producer polling thread will stop immediately if unused"]
struct _FutureProducer<C: Context + 'static> {
    producer: BaseProducer<FutureProducerContext<C>>,
    should_stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,
}

impl<C: Context + 'static> _FutureProducer<C> {
    /// Starts the polling thread that will drive the producer.
    fn start(&self) {
        let producer_clone = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                 trace!("Polling thread loop started");
                 loop {
                     let n = producer_clone.poll(100);
                     if n == 0 {
                         if should_stop.load(Ordering::Relaxed) {
                             // We received nothing and the thread should
                             // stop, so break the loop.
                             break
                         }
                     } else {
                         trace!("Received {} events", n);
                     }
                 }
                 trace!("Polling thread loop terminated");
            })
            .expect("Failed to start polling thread");
        let mut handle_store = self.handle.write().expect("poison error");
        *handle_store = Some(handle);
    }

    // See documentation in FutureProducer
    fn stop(&self) {
        let mut handle_store = self.handle.write().expect("poison error");
        if (*handle_store).is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match (*handle_store).take().expect("No handle present in producer context").join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }

    // See documentation in FutureProducer
    fn send_copy<P, K>(
        &self,
        topic_name: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>
    ) -> DeliveryFuture
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
        let (tx, rx) = futures::oneshot();

        match self.producer.send_copy(topic_name, partition, payload, key, Some(Box::new(tx)), timestamp) {
            Ok(_) => DeliveryFuture{ rx },
            Err(e) => {
                let (tx, rx) = futures::oneshot();
                let _ = tx.send(Err(e));
                DeliveryFuture { rx }
            }
        }
    }
}

impl<C: Context + 'static> Drop for _FutureProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy _FutureProducer");
        self.stop();
    }
}

/// A producer with an internal running thread. This producer doesn't need to be polled.
/// It can be cheaply cloned to get a reference to the same underlying producer.
/// The internal thread can be terminated with the `stop` method or moving the
/// `FutureProducer` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: Context + 'static> {
    inner: Arc<_FutureProducer<C>>,
}

impl<C: Context + 'static> FutureProducer<C> {}

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
        inner.start();
        Ok(FutureProducer { inner: Arc::new(inner) })
    }
}

/// A future that will receive a `DeliveryReport` containing information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Oneshot<KafkaResult<DeliveryReport>>,
}

impl DeliveryFuture {
    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl Future for DeliveryFuture {
    type Item = DeliveryReport;
    type Error = KafkaError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(Canceled) => Err(KafkaError::FutureCanceled),

            Ok(Async::Ready(Ok(delivery_report))) => Ok(Async::Ready(delivery_report)),
            Ok(Async::Ready(Err(e))) => Err(e)
        }
    }
}

impl<C: Context + 'static> FutureProducer<C> {
    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(
        &self,
        topic_name: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>
    ) -> DeliveryFuture
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
        self.inner.send_copy(topic_name, partition, payload, key, timestamp)
    }

    /// Stops the internal polling thread. The thread can also be stopped by moving
    /// the `FutureProducer` out of scope.
    pub fn stop(&self) {
        self.inner.stop();
    }
}

#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use config::ClientConfig;

    struct TestContext;

    impl Context for TestContext {}
    impl ProducerContext for TestContext {
        type DeliveryContext = i32;

        fn delivery(&self, _: DeliveryReport, _: Self::DeliveryContext) {
            unimplemented!()
        }
    }

    // Verify that the producer is clone, according to documentation.
    #[test]
    fn test_base_producer_clone() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let _producer_clone = producer.clone();
    }

    // Verify that the future producer is clone, according to documentation.
    #[test]
    fn test_future_producer_clone() {
        let producer = ClientConfig::new().create::<FutureProducer<_>>().unwrap();
        let _producer_clone = producer.clone();
    }

    // Test that the future producer can be cloned even if the context is not Clone.
    #[test]
    fn test_base_future_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<TestContext, FutureProducer<_>>(test_context).unwrap();
        let _producer_clone = producer.clone();
    }
}
