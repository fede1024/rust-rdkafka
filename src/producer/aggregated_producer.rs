use std::sync::Arc;

use futures::channel::mpsc;
#[allow(unused_imports)]

use crate::client::{ClientContext, DefaultClientContext};
use crate::config::{
    ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel,
};
use crate::error::{KafkaError, KafkaResult};
use crate::message::{Headers, Message, OwnedHeaders, OwnedMessage, ToBytes};
use crate::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use crate::statistics::Statistics;
use crate::util::{IntoOpaque, Timeout};
use std::convert::TryInto;

//
// ********** AGGREGATED PRODUCER **********
//

/// Same as [BaseRecord] but specific to the [AggregatedProducer]. The only difference is that
/// the [AggregatedRecord] doesn't provide custom delivery opaque object.
#[derive(Debug)]
pub struct AggregatedRecord<'a, K: ToBytes + ?Sized + 'a, P: ToBytes + ?Sized + 'a> {
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
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> AggregatedRecord<'a, K, P> {
    /// Create a new record with the specified topic name.
    pub fn to(topic: &'a str) -> AggregatedRecord<'a, K, P> {
        AggregatedRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
        }
    }

    #[allow(dead_code)]
    fn from_base_record<D: IntoOpaque>(
        base_record: BaseRecord<'a, K, P, D>,
    ) -> AggregatedRecord<'a, K, P> {
        AggregatedRecord {
            topic: base_record.topic,
            partition: base_record.partition,
            key: base_record.key,
            payload: base_record.payload,
            timestamp: base_record.timestamp,
            headers: base_record.headers,
        }
    }

    /// Set the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> AggregatedRecord<'a, K, P> {
        self.partition = Some(partition);
        self
    }

    /// Set the destination payload of the record.
    pub fn payload(mut self, payload: &'a P) -> AggregatedRecord<'a, K, P> {
        self.payload = Some(payload);
        self
    }

    /// Set the destination key of the record.
    pub fn key(mut self, key: &'a K) -> AggregatedRecord<'a, K, P> {
        self.key = Some(key);
        self
    }

    /// Set the destination timestamp of the record.
    pub fn timestamp(mut self, timestamp: i64) -> AggregatedRecord<'a, K, P> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> AggregatedRecord<'a, K, P> {
        self.headers = Some(headers);
        self
    }

    fn into_base_record<D: IntoOpaque>(self, delivery_opaque: D) -> BaseRecord<'a, K, P, D> {
        BaseRecord {
            topic: self.topic,
            partition: self.partition,
            key: self.key,
            payload: self.payload,
            timestamp: self.timestamp,
            headers: self.headers,
            delivery_opaque,
        }
    }
}

/// The `ProducerContext` used by the `AggregatedProducer`. This context will use a Future as its
/// `DeliveryOpaque` and will complete the future when the message is delivered (or failed to).
#[derive(Clone)]
struct AggregatedProducerContex<C: ClientContext + 'static> {
    wrapped_context: C,
}

/// Represents the result of message production as performed from the `AggregatedProducer`.
///
/// If message delivery was successful, `OwnedDeliveryResult` will return the partition, offset and
/// producer-processed-messages-counter number of the message. If the message failed to be delivered
/// an error will be returned, together with an owned copy of the original message.
pub type AggregatedDeliveryResult = Result<(i32, i64, usize), (KafkaError, OwnedMessage)>;

// Delegates all the methods calls to the wrapped context.
impl<C: ClientContext + 'static> ClientContext for AggregatedProducerContex<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.wrapped_context.stats(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.wrapped_context.error(error, reason);
    }
}

impl<C: ClientContext + 'static> ProducerContext for AggregatedProducerContex<C> {
    type DeliveryOpaque = Box<mpsc::UnboundedSender<AggregatedDeliveryResult>>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult,
        tx: Box<mpsc::UnboundedSender<AggregatedDeliveryResult>>,
    ) {
        let owned_delivery_result = match *delivery_result {
            Ok(ref message) => {
                let kfk_msg_internal_ctr: usize = message.headers().map_or(0, |h| {
                    if let Some(("id", id_bytes)) = h.get(0) {
                        if let Ok(id_bytes_array) = id_bytes.try_into() {
                            usize::from_le_bytes(id_bytes_array)
                        } else {
                            0 // id couldn't be converted into i64
                        }
                    } else {
                        0 // no id key-value found in header
                    }
                });
                Ok((message.partition(), message.offset(), kfk_msg_internal_ctr))
            }
            Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        if let Err(_err) = tx.unbounded_send(owned_delivery_result) {
            unimplemented!(); // TODO:
        }
    }
}

/// A producer that returns a `Result` for every message being produced.
///
/// Since message production in rdkafka is asynchronous, the caller cannot immediately know if the
/// delivery of the message was successful or not. The `AggregatedProducer` provides this information
/// via `futures::channel::mpsc::Sender` that is cloned for each sent message. This producer has an
/// internal polling thread and as such it doesn't need to be polled. It can be cheaply cloned to
/// get a reference to the same underlying producer. The internal will be terminated once the
/// the `AggregatedProducer` goes out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct AggregatedProducer<C: ClientContext + 'static = DefaultClientContext> {
    producer: Arc<ThreadedProducer<AggregatedProducerContex<C>>>,
}

impl<C: ClientContext + 'static> Clone for AggregatedProducer<C> {
    fn clone(&self) -> AggregatedProducer<C> {
        AggregatedProducer {
            producer: self.producer.clone(),
        }
    }
}

impl FromClientConfig for AggregatedProducer {
    fn from_config(config: &ClientConfig) -> KafkaResult<AggregatedProducer> {
        AggregatedProducer::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C: ClientContext + 'static> FromClientConfigAndContext<C> for AggregatedProducer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<AggregatedProducer<C>> {
        let future_context = AggregatedProducerContex {
            wrapped_context: context,
        };
        let threaded_producer = ThreadedProducer::from_config_and_context(config, future_context)?;
        Ok(AggregatedProducer {
            producer: Arc::new(threaded_producer),
        })
    }
}

impl<C: ClientContext + 'static> AggregatedProducer<C> {
    /// Sends the provided `AggregatedRecord`.
    /// Returns `Result<(),KafkaError::MessageProduction>` if successfully submitted to librdkafka's
    /// queue, or if it fails respectively.
    pub fn submit_to_send<'a, K, P>(
        &self,
        record: AggregatedRecord<'a, K, P>,
        tx: mpsc::UnboundedSender<AggregatedDeliveryResult>,
    ) -> Result<(), KafkaError>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let base_record = record.into_base_record(Box::new(tx));
        self.producer
            .send(base_record)
            .map(|()| ())
            .map_err(|(e, _record)| e)
    }

    /// Polls the internal producer. This is not normally required since the `ThreadedProducer` had
    /// a thread dedicated to calling `poll` regularly.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.poll(timeout);
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.flush(timeout);
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

// TODO: this code is copy-pasted from rust-rdkafka, may possibly need to be modified
#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use crate::ClientConfig;

    struct TestContext;

    impl ClientContext for TestContext {}
    impl ProducerContext for TestContext {
        type DeliveryOpaque = Box<i32>;

        fn delivery(&self, _: &DeliveryResult, _: Self::DeliveryOpaque) {
            unimplemented!()
        }
    }

    // Verify that the aggregated producer is clone, according to documentation.
    #[test]
    fn test_aggregated_producer_clone() {
        let producer = ClientConfig::new().create::<AggregatedProducer>().unwrap();
        let _producer_clone = producer.clone();
    }

    // Test that the future producer can be cloned even if the context is not Clone.
    #[test]
    fn test_base_aggregated_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<_, AggregatedProducer<TestContext>>(test_context)
            .unwrap();
        let _producer_clone = producer.clone();
    }
}

