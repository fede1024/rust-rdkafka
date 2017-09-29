use client::{Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use producer::{BaseProducer, DeliveryResult, EmptyProducerContext, ProducerContext};
use statistics::Statistics;
use error::{KafkaError, KafkaResult};
use message::{Message, OwnedMessage, Timestamp, ToBytes};

use futures::{self, Canceled, Complete, Future, Poll, Oneshot, Async};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

//
// ********** POLLING PRODUCER **********
//

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

    fn send_copy<P, K>(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>,
        delivery_context: Option<Box<C::DeliveryContext>>,
    ) -> KafkaResult<()>
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
        self.producer.send_copy(topic, partition, payload, key, delivery_context, timestamp)
    }
}

impl<C: ProducerContext + 'static> Drop for PollingProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy PollingProducer");
        self.stop();
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

// TODO: documentation
type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

// Delegates all the methods calls to the wrapped context.
impl<C: Context + 'static> Context for FutureProducerContext<C> {
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

impl<C: Context + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryContext = Complete<OwnedDeliveryResult>;

    fn delivery(&self, delivery_result: &DeliveryResult, tx: Complete<OwnedDeliveryResult>) {
        let owned_delivery_result = match delivery_result {
            &Ok(ref message) => Ok((message.partition(), message.offset())),
            &Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        let _ = tx.send(owned_delivery_result);   // TODO: handle error
    }
}


/// A producer with an internal running thread. This producer doesn't need to be polled.
/// It can be cheaply cloned to get a reference to the same underlying producer.
/// The internal thread can be terminated with the `stop` method or moving the
/// `FutureProducer` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: Context + 'static> {
    inner: Arc<PollingProducer<FutureProducerContext<C>>>,
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
        let polling_producer = PollingProducer::from_config_and_context(config, future_context)?;
        Ok(FutureProducer { inner: Arc::new(polling_producer) })
    }
}

/// A future that will receive a `DeliveryResult` containing information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Oneshot<OwnedDeliveryResult>,
}

impl DeliveryFuture {
    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl Future for DeliveryFuture {
    type Item = OwnedDeliveryResult;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(owned_delivery_result)) => Ok(Async::Ready(owned_delivery_result)),
            Err(Canceled) => Err(Canceled),
        }
    }
}

impl<C: Context + 'static> FutureProducer<C> {
    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key.
    /// Returns a `DeliveryFuture` or an error.
    pub fn send_copy<P, K>(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>
    ) -> DeliveryFuture
        where K: ToBytes + ?Sized,
              P: ToBytes + ?Sized {
        let (tx, rx) = futures::oneshot();

        // TODO: catch and retry on QueueFull
        match self.inner.send_copy(topic, partition, payload, key, timestamp, Some(Box::new(tx))) {
            Ok(_) => DeliveryFuture{ rx },
            Err(e) => {
                let (tx, rx) = futures::oneshot();
                let owned_message = OwnedMessage::new(
                    payload.map(|p| p.to_bytes().to_vec()),
                    key.map(|k| k.to_bytes().to_vec()),
                    topic.to_owned(),
                    timestamp.map(|millis| Timestamp::CreateTime(millis)).unwrap_or(Timestamp::NotAvailable),
                    -1,
                    0
                );
                let _ = tx.send(Err((e, owned_message)));
                DeliveryFuture { rx }
            }
        }
    }

    /// Stops the internal polling thread. The thread can also be stopped by moving
    /// the `FutureProducer` out of scope.
    pub fn stop(&self) {
        self.inner.stop();
    }

    // TODO: add poll and flush
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

        fn delivery(&self, _: DeliveryResult, _: Self::DeliveryContext) {
            unimplemented!()
        }
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
