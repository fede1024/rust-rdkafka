//! High-level, futures-enabled Kafka producer.
//!
//! See the [`FutureProducer`] for details.
// TODO: extend docs

use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use futures_util::FutureExt;

use crate::client::{Client, ClientContext, DefaultClientContext, OAuthToken};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::consumer::ConsumerGroupMetadata;
use crate::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use crate::message::{Message, OwnedHeaders, OwnedMessage, Timestamp, ToBytes};
use crate::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use crate::statistics::Statistics;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::{AsyncRuntime, DefaultRuntime, IntoOpaque, Timeout};

//
// ********** FUTURE PRODUCER **********
//

/// A record for the future producer.
///
/// Like [`BaseRecord`], but specific to the [`FutureProducer`]. The only
/// difference is that the [FutureRecord] doesn't provide custom delivery opaque
/// object.
#[derive(Debug)]
pub struct FutureRecord<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> {
    /// Required destination topic.
    pub topic: &'a str,
    /// Optional destination partition.
    pub partition: Option<i32>,
    /// Optional payload.
    pub payload: Option<&'a P>,
    /// Optional key.
    pub key: Option<&'a K>,
    /// Optional timestamp.
    pub timestamp: Option<i64>,
    /// Optional message headers.
    pub headers: Option<OwnedHeaders>,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> FutureRecord<'a, K, P> {
    /// Creates a new record with the specified topic name.
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

    /// Sets the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> FutureRecord<'a, K, P> {
        self.partition = Some(partition);
        self
    }

    /// Sets the destination payload of the record.
    pub fn payload(mut self, payload: &'a P) -> FutureRecord<'a, K, P> {
        self.payload = Some(payload);
        self
    }

    /// Sets the destination key of the record.
    pub fn key(mut self, key: &'a K) -> FutureRecord<'a, K, P> {
        self.key = Some(key);
        self
    }

    /// Sets the destination timestamp of the record.
    pub fn timestamp(mut self, timestamp: i64) -> FutureRecord<'a, K, P> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the headers of the record.
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

/// The [`ProducerContext`] used by the [`FutureProducer`].
///
/// This context will use a [`Future`] as its `DeliveryOpaque` and will complete
/// the future when the message is delivered (or failed to).
#[derive(Clone)]
pub struct FutureProducerContext<C: ClientContext + 'static> {
    wrapped_context: C,
}

/// Represents the result of message production as performed from the
/// `FutureProducer`.
///
/// If message delivery was successful, `OwnedDeliveryResult` will return the
/// partition and offset of the message. If the message failed to be delivered
/// an error will be returned, together with an owned copy of the original
/// message.
pub type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

// Delegates all the methods calls to the wrapped context.
impl<C: ClientContext + 'static> ClientContext for FutureProducerContext<C> {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.wrapped_context.stats(statistics);
    }

    fn stats_raw(&self, statistics: &[u8]) {
        self.wrapped_context.stats_raw(statistics)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.wrapped_context.error(error, reason);
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.wrapped_context
            .generate_oauth_token(oauthbearer_config)
    }
}

impl<C: ClientContext + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryOpaque = Box<oneshot::Sender<OwnedDeliveryResult>>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        tx: Box<oneshot::Sender<OwnedDeliveryResult>>,
    ) {
        let owned_delivery_result = match *delivery_result {
            Ok(ref message) => Ok((message.partition(), message.offset())),
            Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        let _ = tx.send(owned_delivery_result); // TODO: handle error
    }
}

/// A producer that returns a [`Future`] for every message being produced.
///
/// Since message production in rdkafka is asynchronous, the caller cannot
/// immediately know if the delivery of the message was successful or not. The
/// FutureProducer provides this information in a [`Future`], which will be
/// completed once the information becomes available.
///
/// This producer has an internal polling thread and as such it doesn't need to
/// be polled. It can be cheaply cloned to get a reference to the same
/// underlying producer. The internal polling thread will be terminated when the
/// `FutureProducer` goes out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C = DefaultClientContext, R = DefaultRuntime>
where
    C: ClientContext + 'static,
{
    producer: Arc<ThreadedProducer<FutureProducerContext<C>>>,
    _runtime: PhantomData<R>,
}

impl<C, R> Clone for FutureProducer<C, R>
where
    C: ClientContext + 'static,
{
    fn clone(&self) -> FutureProducer<C, R> {
        FutureProducer {
            producer: self.producer.clone(),
            _runtime: PhantomData,
        }
    }
}

impl<R> FromClientConfig for FutureProducer<DefaultClientContext, R>
where
    R: AsyncRuntime,
{
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer<DefaultClientContext, R>> {
        FutureProducer::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C, R> FromClientConfigAndContext<C> for FutureProducer<C, R>
where
    C: ClientContext + 'static,
    R: AsyncRuntime,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<FutureProducer<C, R>> {
        let future_context = FutureProducerContext {
            wrapped_context: context,
        };
        let threaded_producer = ThreadedProducer::from_config_and_context(config, future_context)?;
        Ok(FutureProducer {
            producer: Arc::new(threaded_producer),
            _runtime: PhantomData,
        })
    }
}

/// A [`Future`] wrapping the result of the message production.
///
/// Once completed, the future will contain an `OwnedDeliveryResult` with
/// information on the delivery status of the message. If the producer is
/// dropped before the delivery status is received, the future will instead
/// resolve with [`oneshot::Canceled`].
pub struct DeliveryFuture {
    rx: oneshot::Receiver<OwnedDeliveryResult>,
}

impl Future for DeliveryFuture {
    type Output = Result<OwnedDeliveryResult, oneshot::Canceled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

impl<C, R> FutureProducer<C, R>
where
    C: ClientContext + 'static,
    R: AsyncRuntime,
{
    /// Sends a message to Kafka, returning the result of the send.
    ///
    /// The `queue_timeout` parameter controls how long to retry for if the
    /// librdkafka producer queue is full. Set it to `Timeout::Never` to retry
    /// forever or `Timeout::After(0)` to never block. If the timeout is reached
    /// and the queue is still full, an [`RDKafkaErrorCode::QueueFull`] error will
    /// be reported in the [`OwnedDeliveryResult`].
    ///
    /// Keep in mind that `queue_timeout` only applies to the first phase of the
    /// send operation. Once the message is queued, the underlying librdkafka
    /// client has separate timeout parameters that apply, like
    /// `delivery.timeout.ms`.
    ///
    /// See also the [`FutureProducer::send_result`] method, which will not
    /// retry the queue operation if the queue is full.
    pub async fn send<K, P, T>(
        &self,
        record: FutureRecord<'_, K, P>,
        queue_timeout: T,
    ) -> OwnedDeliveryResult
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
        T: Into<Timeout>,
    {
        let start_time = Instant::now();
        let queue_timeout = queue_timeout.into();
        let can_retry = || match queue_timeout {
            Timeout::Never => true,
            Timeout::After(t) if start_time.elapsed() < t => true,
            _ => false,
        };

        let (tx, rx) = oneshot::channel();
        let mut base_record = record.into_base_record(Box::new(tx));

        loop {
            match self.producer.send(base_record) {
                Err((e, record))
                    if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        && can_retry() =>
                {
                    base_record = record;
                    R::delay_for(Duration::from_millis(100)).await;
                }
                Ok(_) => {
                    // We hold a reference to the producer, so it should not be
                    // possible for the producer to vanish and cancel the
                    // oneshot.
                    break rx.await.expect("producer unexpectedly dropped");
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
                    break Err((e, owned_message));
                }
            }
        }
    }

    /// Like [`FutureProducer::send`], but if enqueuing fails, an error will be
    /// returned immediately, alongside the [`FutureRecord`] provided.
    pub fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<DeliveryFuture, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let (tx, rx) = oneshot::channel();
        let base_record = record.into_base_record(Box::new(tx));
        self.producer
            .send(base_record)
            .map(|()| DeliveryFuture { rx })
            .map_err(|(e, record)| (e, FutureRecord::from_base_record(record)))
    }

    /// Polls the internal producer.
    ///
    /// This is not normally required since the `FutureProducer` has a thread
    /// dedicated to calling `poll` regularly.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.poll(timeout);
    }
}

impl<C, R> Producer<FutureProducerContext<C>> for FutureProducer<C, R>
where
    C: ClientContext + 'static,
    R: AsyncRuntime,
{
    fn client(&self) -> &Client<FutureProducerContext<C>> {
        self.producer.client()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.flush(timeout)
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.producer.begin_transaction()
    }

    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()> {
        self.producer
            .send_offsets_to_transaction(offsets, cgm, timeout)
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.abort_transaction(timeout)
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

        fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
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
