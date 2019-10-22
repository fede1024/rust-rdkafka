//! Low level Kafka producers.
//!
//! For more information about the producers provided in rdkafka, refer to the module level documentation.
//!
//! ## `BaseProducer`
//!
//! The `BaseProducer` is a low level Kafka producer designed to be as similar as possible to
//! the underlying C librdkafka producer, while maintaining a safe Rust interface.
//!
//! Production of messages is fully asynchronous. The librdkafka producer will take care of buffering
//! requests together according to configuration, and to send them efficiently. Once a message has
//! been produced, or the retry count reached, a callback function called delivery callback will be
//! called.
//!
//! The `BaseProducer` requires a `ProducerContext` which will be used to specify the delivery callback
//! and the `DeliveryOpaque`. The `DeliveryOpaque` is a user-defined type that the user can pass to the
//! `send` method of the producer, and that the producer will then forward to the delivery
//! callback of the corresponding message. The `DeliveryOpaque` is useful in case the delivery
//! callback requires additional information about the message (such as message id for example).
//!
//! ### Calling poll
//!
//! To execute delivery callbacks the `poll` method of the producer should be called regularly.
//! If `poll` is not called, or not often enough, a `RDKafkaError::QueueFull` error will be returned.
//!
//! ## `ThreadedProducer`
//! The `ThreadedProducer` is a wrapper around the `BaseProducer` which spawns a thread
//! dedicated to calling `poll` on the producer at regular intervals, so that the user doesn't
//! have to. The thread is started when the producer is created, and it will be terminated
//! once the producer goes out of scope.
//!
//! A `RDKafkaError::QueueFull` error can still be returned in case the polling thread is not
//! fast enough or Kafka is not able to receive data and acknowledge messages quickly enough.
//! If this error is returned, the producer should wait and try again.
//!

use crate::rdsys;
use crate::rdsys::rd_kafka_vtype_t::*;
use crate::rdsys::types::*;

use crate::client::{Client, ClientContext};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::message::{BorrowedMessage, OwnedHeaders, ToBytes};
use crate::util::{timeout_to_ms, IntoOpaque};

use std::ffi::CString;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub use crate::message::DeliveryResult;

//
// ********** PRODUCER CONTEXT **********
//

/// A `ProducerContext` is an object that can be used during the creation of a producer to
/// customizer its behavior. In particular, it can be used to specify the `delivery` callback
/// that will be called when the ack from a delivered message is received.
pub trait ProducerContext: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to the producer when
    /// producing a message, and returned to the `delivery` method once the message has been
    /// delivered, or failed to.
    type DeliveryOpaque: IntoOpaque;

    /// This method will be called once the message has been delivered (or failed to). The
    /// `DeliveryOpaque` will be the one provided by the user when calling send.
    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque);
}

/// Default producer context that can be use when a custom context is not required.
#[derive(Clone)]
pub struct DefaultProducerContext;

impl ClientContext for DefaultProducerContext {}
impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult, _: Self::DeliveryOpaque) {}
}

/// Callback that gets called from librdkafka every time a message succeeds or fails to be
/// delivered.
unsafe extern "C" fn delivery_cb<C: ProducerContext>(
    _client: *mut RDKafka,
    msg: *const RDKafkaMessage,
    _opaque: *mut c_void,
) {
    let producer_context = Box::from_raw(_opaque as *mut C);
    let delivery_opaque = C::DeliveryOpaque::from_ptr((*msg)._private);
    let owner = 42u8;
    // Wrap the message pointer into a BorrowedMessage that will only live for the body of this
    // function.
    let delivery_result = BorrowedMessage::from_dr_callback(msg as *mut RDKafkaMessage, &owner);
    trace!("Delivery event received: {:?}", delivery_result);
    (*producer_context).delivery(&delivery_result, delivery_opaque);
    mem::forget(producer_context); // Do not free the producer context
    match delivery_result {
        // Do not free the message, librdkafka will do it for us
        Ok(message) | Err((_, message)) => mem::forget(message),
    }
}

//
// ********** BASE PRODUCER **********
//

/// Producer record for the base producer
///
/// The `BaseRecord` is a structure that can be used to provide a new record to
/// [BaseProducer::send]. Since most fields are optional, a `BaseRecord` can be constructed
/// using the builder pattern.
///
/// # Examples
///
/// This example will create a `BaseRecord` with no [DeliveryOpaque](ProducerContext::DeliveryOpaque):
///
/// ```rust,no_run
/// # use rdkafka::producer::BaseRecord;
/// # use rdkafka::message::ToBytes;
/// let record = BaseRecord::to("topic_name")  // destination topic
///     .key(&[1, 2, 3, 4])                    // message key
///     .payload("content")                    // message payload
///     .partition(5);                         // target partition
/// ```
///
/// The following example will build a similar record, but it will use a number as `DeliveryOpaque`
/// for the message:
///
/// ```rust,no_run
/// # use rdkafka::producer::BaseRecord;
/// # use rdkafka::message::ToBytes;
/// let record = BaseRecord::with_opaque_to("topic_name", 123) // destination topic and message id
///     .key(&[1, 2, 3, 4])                    // message key
///     .payload("content")                    // message payload
///     .partition(5);                         // target partition
/// ```
#[derive(Debug)]
pub struct BaseRecord<
    'a,
    K: ToBytes + ?Sized + 'a = (),
    P: ToBytes + ?Sized + 'a = (),
    D: IntoOpaque = (),
> {
    /// Required destination topic
    pub topic: &'a str,
    /// Optional destination partition
    pub partition: Option<i32>,
    /// Optional payload
    pub payload: Option<&'a P>,
    /// Optional key
    pub key: Option<&'a K>,
    /// Optional timestamp
    pub timestamp: Option<i64>,
    /// Optional message headers
    pub headers: Option<OwnedHeaders>,
    /// Required delivery opaque (defaults to `()` if not required)
    pub delivery_opaque: D,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized, D: IntoOpaque> BaseRecord<'a, K, P, D> {
    /// Create a new record with the specified topic name and delivery opaque.
    pub fn with_opaque_to(topic: &'a str, delivery_opaque: D) -> BaseRecord<'a, K, P, D> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque,
        }
    }

    /// Set the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> BaseRecord<'a, K, P, D> {
        self.partition = Some(partition);
        self
    }

    /// Set the payload of the record.
    pub fn payload(mut self, payload: &'a P) -> BaseRecord<'a, K, P, D> {
        self.payload = Some(payload);
        self
    }

    /// Set the key of the record.
    pub fn key(mut self, key: &'a K) -> BaseRecord<'a, K, P, D> {
        self.key = Some(key);
        self
    }

    /// Set the timestamp of the record.
    pub fn timestamp(mut self, timestamp: i64) -> BaseRecord<'a, K, P, D> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> BaseRecord<'a, K, P, D> {
        self.headers = Some(headers);
        self
    }
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> BaseRecord<'a, K, P, ()> {
    /// Create a new record with the specified topic name.
    pub fn to(topic: &'a str) -> BaseRecord<'a, K, P, ()> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque: (),
        }
    }
}

impl FromClientConfig for BaseProducer<DefaultProducerContext> {
    /// Creates a new `BaseProducer` starting from a configuration.
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseProducer<DefaultProducerContext>> {
        BaseProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C: ProducerContext> FromClientConfigAndContext<C> for BaseProducer<C> {
    /// Creates a new `BaseProducer` starting from a configuration and a context.
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseProducer<C>> {
        let native_config = config.create_native_config()?;
        unsafe { rdsys::rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(delivery_cb::<C>)) };
        let client = Client::new(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            context,
        )?;
        Ok(BaseProducer::from_client(client))
    }
}

/// Low level Kafka producer.
///
/// The `BaseProducer` needs to be polled at regular intervals in order to serve queued delivery
/// report callbacks (for more information, refer to the module-level documentation). This producer
/// can be cheaply cloned to create a new reference to the same underlying producer.
///
/// # Example usage
///
/// This code will send a message to Kafka. No custom [ProducerContext] is specified, so the
/// [DefaultProducerContext] will be used. To see how to use a producer context, refer to the
/// examples in the examples folder.
/// ```rust
/// use rdkafka::config::ClientConfig;
/// use rdkafka::producer::{BaseProducer, BaseRecord};
/// use std::time::Duration;
///
/// let producer: BaseProducer = ClientConfig::new()
///     .set("bootstrap.servers", "kafka:9092")
///     .create()
///     .expect("Producer creation error");
///
/// producer.send(
///     BaseRecord::to("destination_topic")
///         .payload("this is the payload")
///         .key("and this is a key"),
/// ).expect("Failed to enqueue");
///
/// // Poll at regular intervals to process all the asynchronous delivery events.
/// for _ in 0..10 {
///     producer.poll(Duration::from_millis(100));
/// }
///
/// // And/or flush the producer before dropping it.
/// producer.flush(Duration::from_secs(1));
/// ```
pub struct BaseProducer<C: ProducerContext = DefaultProducerContext> {
    client_arc: Arc<Client<C>>,
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Creates a base producer starting from a Client.
    fn from_client(client: Client<C>) -> BaseProducer<C> {
        BaseProducer {
            client_arc: Arc::new(client),
        }
    }

    /// Polls the producer. Regular calls to `poll` are required to process the events
    /// and execute the message delivery callbacks. Returns the number of events served.
    pub fn poll<T: Into<Option<Duration>>>(&self, timeout: T) -> i32 {
        unsafe { rdsys::rd_kafka_poll(self.native_ptr(), timeout_to_ms(timeout)) }
    }

    /// Returns a pointer to the native Kafka client.
    fn native_ptr(&self) -> *mut RDKafka {
        self.client_arc.native_ptr()
    }

    /// Produce a message to Kafka. Message fields such as key, payload, partition, timestamp etc.
    /// are provided to this method via a [BaseRecord]. If the message is correctly enqueued in the
    /// producer's memory buffer, the method will take ownership of the record and return
    /// immediately; in case of failure to enqueue, the original record is returned, alongside an
    /// error code. If the message fails to be produced after being enqueued in the buffer, the
    /// [ProducerContext::delivery] method will be called asynchronously, with the provided
    /// [ProducerContext::DeliveryOpaque].
    ///
    /// When no partition is specified the underlying Kafka library picks a partition based on a
    /// hash of the key. If no key is specified, a random partition will be used. To correctly
    /// handle errors, the delivery callback should be implemented.
    ///
    /// Note that this method will never block.
    // Simplifying the return type requires generic associated types, which are
    // unstable.
    #[allow(clippy::type_complexity)]
    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let (payload_ptr, payload_len) = match record.payload.map(P::to_bytes) {
            None => (ptr::null_mut(), 0),
            Some(p) => (p.as_ptr() as *mut c_void, p.len()),
        };
        let (key_ptr, key_len) = match record.key.map(K::to_bytes) {
            None => (ptr::null_mut(), 0),
            Some(k) => (k.as_ptr() as *mut c_void, k.len()),
        };
        let topic_cstring = CString::new(record.topic.to_owned()).unwrap();
        let produce_error = unsafe {
            rdsys::rd_kafka_producev(
                self.native_ptr(),
                RD_KAFKA_VTYPE_TOPIC,
                topic_cstring.as_ptr(),
                RD_KAFKA_VTYPE_PARTITION,
                record.partition.unwrap_or(-1),
                RD_KAFKA_VTYPE_MSGFLAGS,
                rdsys::RD_KAFKA_MSG_F_COPY as i32,
                RD_KAFKA_VTYPE_VALUE,
                payload_ptr,
                payload_len,
                RD_KAFKA_VTYPE_KEY,
                key_ptr,
                key_len,
                RD_KAFKA_VTYPE_OPAQUE,
                record.delivery_opaque.as_ptr(),
                RD_KAFKA_VTYPE_TIMESTAMP,
                record.timestamp.unwrap_or(0),
                RD_KAFKA_VTYPE_HEADERS,
                record
                    .headers
                    .as_ref()
                    .map_or(ptr::null_mut(), OwnedHeaders::ptr),
                RD_KAFKA_VTYPE_END,
            )
        };
        if produce_error.is_error() {
            Err((KafkaError::MessageProduction(produce_error.into()), record))
        } else {
            // The kafka producer now owns the delivery opaque and the headers
            mem::forget(record.delivery_opaque);
            mem::forget(record.headers);
            Ok(())
        }
    }

    /// Flushes the producer. Should be called before termination. This method will call `poll()`
    /// internally.
    pub fn flush<T: Into<Option<Duration>>>(&self, timeout: T) {
        unsafe { rdsys::rd_kafka_flush(self.native_ptr(), timeout_to_ms(timeout)) };
    }

    /// Returns the number of messages waiting to be sent, or sent but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        unsafe { rdsys::rd_kafka_outq_len(self.native_ptr()) }
    }
}

impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> BaseProducer<C> {
        BaseProducer {
            client_arc: self.client_arc.clone(),
        }
    }
}

//
// ********** THREADED PRODUCER **********
//

/// A producer with a separate thread for event handling.
///
/// The `ThreadedProducer` is a `BaseProducer` with a separate thread dedicated to calling `poll` at
/// regular intervals in order to execute any queued event, such as delivery notifications. The
/// thread will be automatically stopped when the producer is dropped.
#[must_use = "The threaded producer will stop immediately if unused"]
pub struct ThreadedProducer<C: ProducerContext + 'static> {
    producer: BaseProducer<C>,
    should_stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,
}

impl FromClientConfig for ThreadedProducer<DefaultProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<ThreadedProducer<DefaultProducerContext>> {
        ThreadedProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C: ProducerContext + 'static> FromClientConfigAndContext<C> for ThreadedProducer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<ThreadedProducer<C>> {
        let threaded_producer = ThreadedProducer {
            producer: BaseProducer::from_config_and_context(config, context)?,
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: RwLock::new(None),
        };
        threaded_producer.start();
        Ok(threaded_producer)
    }
}

impl<C: ProducerContext + 'static> ThreadedProducer<C> {
    /// Starts the polling thread that will drive the producer. The thread is already started by
    /// default.
    fn start(&self) {
        let producer_clone = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("producer polling thread".to_string())
            .spawn(move || {
                trace!("Polling thread loop started");
                loop {
                    let n = producer_clone.poll(Duration::from_millis(100));
                    if n == 0 {
                        if should_stop.load(Ordering::Relaxed) {
                            // We received nothing and the thread should
                            // stop, so break the loop.
                            break;
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

    /// Stops the polling thread. This method will be called during destruction.
    fn stop(&self) {
        let mut handle_store = self.handle.write().expect("poison error");
        if (*handle_store).is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match (*handle_store).take().unwrap().join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }

    /// Sends a message to Kafka. See the documentation in `BaseProducer`.
    // Simplifying the return type requires generic associated types, which are
    // unstable.
    #[allow(clippy::type_complexity)]
    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.producer.send(record)
    }

    /// Polls the internal producer. This is not normally required since the `ThreadedProducer` had
    /// a thread dedicated to calling `poll` regularly.
    pub fn poll<T: Into<Option<Duration>>>(&self, timeout: T) {
        self.producer.poll(timeout);
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush<T: Into<Option<Duration>>>(&self, timeout: T) {
        self.producer.flush(timeout);
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

impl<C: ProducerContext + 'static> Drop for ThreadedProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy ThreadedProducer");
        self.stop();
        trace!("ThreadedProducer destroyed");
    }
}

#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use crate::config::ClientConfig;

    // Verify that the producer is clone, according to documentation.
    #[test]
    fn test_base_producer_clone() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let _producer_clone = producer.clone();
    }
}
