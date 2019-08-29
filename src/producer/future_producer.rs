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

use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::FutureExt;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

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

    fn from_base_record<D: IntoOpaque>(
        base_record: BaseRecord<'a, K, P, D>,
    ) -> FutureRecord<'a, K, P> {
        FutureRecord {
            topic: base_record.topic,
            partition: base_record.partition,
            key: base_record.key,
            payload: base_record.payload,
            timestamp: base_record.timestamp,
            headers: base_record.headers,
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
type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

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

/// A [Future] wrapping the result of the message production.
///
/// Once completed, the future will contain an `OwnedDeliveryResult` with information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Receiver<OwnedDeliveryResult>,
}

impl Future for DeliveryFuture {
    type Output = KafkaResult<OwnedDeliveryResult>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.rx.poll_unpin(cx) {
            Poll::Ready(Ok(Ok(inner))) => Poll::Ready(Ok(Ok(inner))),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Ok(Err(e))),
            Poll::Ready(Err(_)) => Poll::Ready(Err(KafkaError::NoMessageReceived)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<C: ClientContext + 'static> FutureProducer<C> {
    /// Sends the provided [FutureRecord]. Returns a [DeliveryFuture] that will eventually contain the
    /// result of the send. The `block_ms` parameter will control for how long the producer
    /// is allowed to block if the queue is full. Set it to -1 to block forever, or 0 to never block.
    /// If `block_ms` is reached and the queue is still full, a [RDKafkaError::QueueFull] will be
    /// reported in the [DeliveryFuture].
    pub fn send<K, P>(&self, record: FutureRecord<K, P>, block_ms: i64) -> DeliveryFuture
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let start_time = Instant::now();

        let (tx, rx) = futures::channel::oneshot::channel();
        let mut base_record = record.into_base_record(Box::new(tx));

        loop {
            match self.producer.send(base_record) {
                Ok(_) => break DeliveryFuture { rx },
                Err((KafkaError::MessageProduction(RDKafkaError::QueueFull), record)) => {
                    base_record = record;
                    if block_ms == -1 {
                        continue;
                    } else if block_ms > 0
                        && start_time.elapsed() < Duration::from_millis(block_ms as u64)
                    {
                        self.poll(Duration::from_millis(100));
                        continue;
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
                    break DeliveryFuture { rx };
                }
            }
        }
    }

    /// Same as [FutureProducer::send], with the only difference that if enqueuing fails, an
    /// error will be returned immediately, alongside the [FutureRecord] provided.
    pub fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<DeliveryFuture, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let (tx, rx) = channel();
        let base_record = record.into_base_record(Box::new(tx));
        self.producer
            .send(base_record)
            .map(|()| DeliveryFuture { rx })
            .map_err(|(e, record)| (e, FutureRecord::from_base_record(record)))
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
