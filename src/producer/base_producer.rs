use rdsys::rd_kafka_vtype_t::*;
use rdsys::types::*;
use rdsys;

use client::{Client, Context};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use error::{KafkaError, KafkaResult, IsError};
use message::{BorrowedMessage, ToBytes};

use std::ffi::CString;
use std::os::raw::c_void;
use std::mem;
use std::ptr;
use std::sync::Arc;

pub use message::DeliveryResult;

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
    fn delivery(&self, &DeliveryResult, Self::DeliveryContext);
}

/// Simple empty producer context that can be use when the producer context is not required.
#[derive(Clone)]
pub struct EmptyProducerContext;

impl Context for EmptyProducerContext { }
impl ProducerContext for EmptyProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, _: &DeliveryResult, _: Self::DeliveryContext) { }
}

/// Callback that gets called from librdkafka every time a message succeeds
/// or fails to be delivered.
// TODO: grep for DeliveryReport and remove
unsafe extern "C" fn delivery_cb<C: ProducerContext>(
        _client: *mut RDKafka, msg: *const RDKafkaMessage, _opaque: *mut c_void) {
    let producer_context = Box::from_raw(_opaque as *mut C);
    let delivery_context = Box::from_raw((*msg)._private as *mut C::DeliveryContext);
    let owner: u8 = 42;
    let delivery_result = BorrowedMessage::from_producer(msg as *mut RDKafkaMessage, &owner);
    trace!("Delivery event received: {:?}", delivery_result);
    (*producer_context).delivery(&delivery_result, (*delivery_context));
    mem::forget(producer_context); // Do not free the producer context
    match delivery_result {  // Do not free the message, librdkafka will do it for us
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

    /// Flushes the producer. Should be called before termination.
    pub fn flush(&self, timeout_ms: i32) {
        unsafe { rdsys::rd_kafka_flush(self.native_ptr(), timeout_ms) };
    }
}

impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> BaseProducer<C> {
        BaseProducer { client_arc: self.client_arc.clone() }
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
