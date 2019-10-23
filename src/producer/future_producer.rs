//! Future producer
//!
//! A high level producer that returns a Future for every produced message.
// TODO: extend docs

use crate::client::{ClientContext, DefaultClientContext};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::error::{KafkaError, KafkaResult, RDKafkaError};
use crate::message::{Message, OwnedHeaders, OwnedMessage, Timestamp, ToBytes};
use crate::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use crate::statistics::Statistics;
use crate::util::IntoOpaque;

use futures::channel::oneshot::{channel, Sender};
use log::*;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_executor::blocking::{run as block_on};

//
// ********** FUTURE PRODUCER **********
//

/// Same as [BaseRecord] but specific to the [FutureProducer]. The only difference is that
/// the [FutureRecord] doesn't provide custom delivery opaque object.
#[derive(Debug)]
pub struct FutureRecord<'a, K: ToBytes + ?Sized + 'a, P: ToBytes + ?Sized + 'a> {
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

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> FutureRecord<'a, K, P> {
    /// Create a new record with the specified topic name.
    pub fn to(topic: &'a str) -> FutureRecord<'a, K, P> {
        FutureRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
        }
    }

    /// Set the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> FutureRecord<'a, K, P> {
        self.partition = Some(partition);
        self
    }

    /// Set the destination payload of the record.
    pub fn payload(mut self, payload: &'a P) -> FutureRecord<'a, K, P> {
        self.payload = Some(payload);
        self
    }

    /// Set the destination key of the record.
    pub fn key(mut self, key: &'a K) -> FutureRecord<'a, K, P> {
        self.key = Some(key);
        self
    }

    /// Set the destination timestamp of the record.
    pub fn timestamp(mut self, timestamp: i64) -> FutureRecord<'a, K, P> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> FutureRecord<'a, K, P> {
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

/// The `ProducerContext` used by the `FutureProducer`. This context will use a Future as its
/// `DeliveryOpaque` and will complete the future when the message is delivered (or failed to).
#[derive(Clone)]
struct FutureProducerContext<C: ClientContext + 'static> {
    wrapped_context: C,
}

/// Represents the result of message production as performed from the `FutureProducer`.
///
/// If message delivery was successful, `OwnedDeliveryResult` will return the partition and offset
/// of the message. If the message failed to be delivered an error will be returned, together with
/// an owned copy of the original message.
pub type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

// Delegates all the methods calls to the wrapped context.
impl<C: ClientContext + 'static> ClientContext for FutureProducerContext<C> {
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

impl<C: ClientContext + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryOpaque = Box<Sender<OwnedDeliveryResult>>;

    fn delivery(&self, delivery_result: &DeliveryResult, tx: Box<Sender<OwnedDeliveryResult>>) {
        let owned_delivery_result = match *delivery_result {
            Ok(ref message) => Ok((message.partition(), message.offset())),
            Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        let _ = tx.send(owned_delivery_result); // TODO: handle error
    }
}

/// A producer that returns a `Future` for every message being produced.
///
/// Since message production in rdkafka is asynchronous, the caller cannot immediately know if the
/// delivery of the message was successful or not. The `FutureProducer` provides this information in
/// a `Future`, that will be completed once the information becomes available. This producer has an
/// internal polling thread and as such it doesn't need to be polled. It can be cheaply cloned to
/// get a reference to the same underlying producer. The internal will be terminated once the
/// the `FutureProducer` goes out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: ClientContext + 'static = DefaultClientContext> {
    producer: Arc<ThreadedProducer<FutureProducerContext<C>>>,
}

impl<C: ClientContext + 'static> Clone for FutureProducer<C> {
    fn clone(&self) -> FutureProducer<C> {
        FutureProducer {
            producer: self.producer.clone(),
        }
    }
}

impl FromClientConfig for FutureProducer {
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer> {
        FutureProducer::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C: ClientContext + 'static> FromClientConfigAndContext<C> for FutureProducer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<FutureProducer<C>> {
        let future_context = FutureProducerContext {
            wrapped_context: context,
        };
        let threaded_producer = ThreadedProducer::from_config_and_context(config, future_context)?;
        Ok(FutureProducer {
            producer: Arc::new(threaded_producer),
        })
    }
}

enum ProducerPollResult<'a, K, P>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
{
    Produce(BaseRecord<'a, K, P, Box<Sender<OwnedDeliveryResult>>>),
    Delivered,
}

impl<C: ClientContext + 'static> FutureProducer<C> {
    /// Sends the provided [FutureRecord]. Returns a [DeliveryFuture] that will eventually contain the
    /// result of the send. The `block_ms` parameter will control for how long the producer
    /// is allowed to block if the queue is full. Set it to -1 to block forever, or 0 to never block.
    /// If `block_ms` is reached and the queue is still full, a [RDKafkaError::QueueFull] will be
    /// reported in the [DeliveryFuture].
    pub async fn send<'a, K, P>(&self, record: FutureRecord<'a, K, P>, block_ms: i64) -> KafkaResult<OwnedDeliveryResult>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let start_time = Instant::now();
        let timeout_dur = if block_ms > 0 {
            Some(Duration::from_millis(block_ms as u64))
        } else {
            None
        };

        let (tx, rx) = channel();
        let record = record.into_base_record(Box::new(tx));

        let mut poll_result = ProducerPollResult::Produce(record);

        while let ProducerPollResult::Produce(to_produce) = std::mem::replace(&mut poll_result, ProducerPollResult::Delivered) {
            poll_result = match self.producer.send(to_produce) {
                Ok(_) => {
                    trace!("Record successfully delivered");
                    ProducerPollResult::Delivered
                },
                Err((KafkaError::MessageProduction(RDKafkaError::QueueFull), record)) => {
                    if let Some(to) = timeout_dur {
                        let timed_out = start_time.elapsed() >= to;
                        if timed_out {
                            debug!("Queue full and timeout reached");
                            return Err(KafkaError::MessageProduction(RDKafkaError::QueueFull));
                        } else {
                            debug!("Queue full, polling for 100ms before trying again");
                            let producer = Arc::clone(&self.producer);
                            block_on(move || producer.poll(Duration::from_millis(100))).await;
                            ProducerPollResult::Produce(record)
                        }
                    } else {
                        ProducerPollResult::Produce(record)
                    }
                }
                Err((e, record)) => {
                    let owned_message = OwnedMessage::new(
                        record.payload.map(|p| p.to_bytes().to_vec()),
                        record.key.map(|k| k.to_bytes().to_vec()),
                        record.topic.to_owned(),
                        record
                            .timestamp
                            .map_or(Timestamp::NotAvailable, Timestamp::CreateTime),
                        record.partition.unwrap_or(-1),
                        0,
                        record.headers,
                    );
                    let _ = record.delivery_opaque.send(Err((e, owned_message)));
                    ProducerPollResult::Delivered
                }
            };
        }

        rx.await.map_err(|_| KafkaError::Canceled)
    }

    /// Same as [FutureProducer::send], with the only difference that if enqueuing fails, an
    /// error will be returned immediately, alongside the [FutureRecord] provided.
    pub async fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> KafkaResult<OwnedDeliveryResult>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let (tx, rx) = channel();

        let record = record.into_base_record(Box::new(tx));
        if let Err((e, record)) = self.producer.send(record) {
            let owned_message = OwnedMessage::new(
                record.payload.map(|p| p.to_bytes().to_vec()),
                record.key.map(|k| k.to_bytes().to_vec()),
                record.topic.to_owned(),
                record
                    .timestamp
                    .map_or(Timestamp::NotAvailable, Timestamp::CreateTime),
                record.partition.unwrap_or(-1),
                0,
                record.headers,
            );
            let _ = record.delivery_opaque.send(Err((e, owned_message)));
        }

        rx.await.map_err(|_| KafkaError::Canceled)
    }

    /// Flushes the producer. Should be called before termination.
    pub async fn flush<T: Into<Option<Duration>> + Send + 'static>(&self, timeout: T) {
        let producer = Arc::clone(&self.producer);
        block_on(move || producer.flush(timeout)).await
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use crate::config::ClientConfig;

    struct TestContext;

    impl ClientContext for TestContext {}
    impl ProducerContext for TestContext {
        type DeliveryOpaque = Box<i32>;

        fn delivery(&self, _: &DeliveryResult, _: Self::DeliveryOpaque) {
            unimplemented!()
        }
    }

    // Verify that the future producer is clone, according to documentation.
    #[test]
    fn test_future_producer_clone() {
        let producer = ClientConfig::new().create::<FutureProducer>().unwrap();
        let _producer_clone = producer.clone();
    }

    // Test that the future producer can be cloned even if the context is not Clone.
    #[test]
    fn test_base_future_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<_, FutureProducer<TestContext>>(test_context)
            .unwrap();
        let _producer_clone = producer.clone();
    }
}
