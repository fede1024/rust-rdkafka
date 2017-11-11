//! Low level Kafka producer.
//!
//! For more information about the producers provided in rdkafka, refer to the module level documentation.
//!
//! ## BaseProducer
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
//! If `poll` is not called, or not often enough, a RDKafkaError::QueueFull error will be returned.
//!

use rdsys::rd_kafka_vtype_t::*;
use rdsys::types::*;
use rdsys;

use client::{Client, Context};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use error::{KafkaError, KafkaResult, IsError};
use message::{BorrowedMessage, ToBytes};
use util::IntoOpaque;

use std::ffi::CString;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

pub use message::DeliveryResult;

//
// ********** PRODUCER CONTEXT **********
//

/// A `ProducerContext` is a `Context` specific for producers. It can be used to store
/// user-specified callbacks, such as `delivery`.
pub trait ProducerContext: Context {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to the producer when
    /// producing a message, and returned to the `delivery` method once the message has been
    /// delivered, or failed to.
    type DeliveryOpaque: IntoOpaque;

    /// This method will be called once the message has been delivered (or failed to). The
    /// `DeliveryOpaque` will be the one provided by the user when calling send.
    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque);
}


/// Simple empty producer context that can be use when the producer context is not required.
#[derive(Clone)]
pub struct EmptyProducerContext;

impl Context for EmptyProducerContext {}
impl ProducerContext for EmptyProducerContext {
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
    match delivery_result {        // Do not free the message, librdkafka will do it for us
        Ok(message) => mem::forget(message),
        Err((_, message)) => mem::forget(message),
    }
}

//
// ********** BASE PRODUCER **********
//

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

/// Simple Kafka producer. This producer needs to be `poll`ed at regular intervals in order to
/// serve queued delivery report callbacks. This producer can be cheaply cloned to
/// create a new reference to the same underlying producer.
pub struct BaseProducer<C: ProducerContext> {
    client_arc: Arc<Client<C>>,
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Creates a base producer starting from a Client.
    fn from_client(client: Client<C>) -> BaseProducer<C> {
        BaseProducer { client_arc: Arc::new(client) }
    }

    /// Polls the producer. Regular calls to `poll` are required to process the events
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
    /// specified, a random partition will be used. Note that some errors will cause an error to be
    /// returned straight-away, such as partition not defined, while others will be returned in the
    /// delivery callback. To correctly handle errors, the delivery callback should be implemented.
    pub fn send_copy<P, K>(
        &self,
        topic_name: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        delivery_opaque: C::DeliveryOpaque,
        timestamp: Option<i64>,
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
        let delivery_opaque_ptr = delivery_opaque.into_ptr();
        let topic_name_c = CString::new(topic_name.to_owned())?;
        let produce_error = unsafe {
            rdsys::rd_kafka_producev(
                self.native_ptr(),
                RD_KAFKA_VTYPE_TOPIC, topic_name_c.as_ptr(),
                RD_KAFKA_VTYPE_PARTITION, partition.unwrap_or(-1),
                RD_KAFKA_VTYPE_MSGFLAGS, rdsys::RD_KAFKA_MSG_F_COPY as i32,
                RD_KAFKA_VTYPE_VALUE, payload_ptr, payload_len,
                RD_KAFKA_VTYPE_KEY, key_ptr, key_len,
                RD_KAFKA_VTYPE_OPAQUE, delivery_opaque_ptr,
                RD_KAFKA_VTYPE_TIMESTAMP, timestamp.unwrap_or(0),
                RD_KAFKA_VTYPE_END
            )
        };
        if produce_error.is_error() {
            if !delivery_opaque_ptr.is_null() { // Drop delivery opaque if provided
                unsafe { C::DeliveryOpaque::from_ptr(delivery_opaque_ptr) };
            }
            Err(KafkaError::MessageProduction(produce_error.into()))
        } else {
            Ok(())
        }
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush(&self, timeout_ms: i32) {
        unsafe { rdsys::rd_kafka_flush(self.native_ptr(), timeout_ms) };
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        unsafe { rdsys::rd_kafka_outq_len(self.native_ptr()) }
    }
}

impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> BaseProducer<C> {
        BaseProducer { client_arc: self.client_arc.clone() }
    }
}

//
// ********** POLLING PRODUCER **********
//

/// A producer with a separate thread for event handling.
///
/// The `PollingProducer` is a `BaseProducer` with a separate thread dedicated to calling `poll` at
/// regular intervals in order to execute any queued event, such as delivery notifications. The
/// thread will be automatically stopped when the producer is dropped.
#[must_use = "The polling producer will stop immediately if unused"]
pub struct PollingProducer<C: ProducerContext + 'static> {
    producer: BaseProducer<C>,
    should_stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,
}

impl FromClientConfig for PollingProducer<EmptyProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<PollingProducer<EmptyProducerContext>> {
        PollingProducer::from_config_and_context(config, EmptyProducerContext)
    }
}

impl<C: ProducerContext + 'static> FromClientConfigAndContext<C> for PollingProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<PollingProducer<C>> {
        let polling_producer = PollingProducer {
            producer: BaseProducer::from_config_and_context(config, context)?,
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: RwLock::new(None),
        };
        polling_producer.start();
        Ok(polling_producer)
    }
}

impl<C: ProducerContext + 'static> PollingProducer<C> {
    /// Starts the polling thread that will drive the producer. The thread is already started by
    /// default.
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
    pub fn send_copy<P, K>(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>,
        delivery_opaque: C::DeliveryOpaque,
    ) -> KafkaResult<()>
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
        self.producer.send_copy(topic, partition, payload, key, delivery_opaque, timestamp)
    }

    /// Polls the internal producer. This is not normally required since the `PollingProducer` had
    /// a thread dedicated to calling `poll` regularly.
    pub fn poll(&self, timeout_ms: i32) {
        self.producer.poll(timeout_ms);
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush(&self, timeout_ms: i32) {
        self.producer.flush(timeout_ms);
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

impl<C: ProducerContext + 'static> Drop for PollingProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy PollingProducer");
        self.stop();
        trace!("PollingProducer destroyed");
    }
}



#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use config::ClientConfig;

    // Verify that the producer is clone, according to documentation.
    #[test]
    fn test_base_producer_clone() {
        let producer = ClientConfig::new().create::<BaseProducer<_>>().unwrap();
        let _producer_clone = producer.clone();
    }
}
