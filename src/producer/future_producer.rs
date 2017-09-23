use client::{Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use producer::{BaseProducer, DeliveryReport, ProducerContext};
use statistics::Statistics;
use error::{KafkaError, KafkaResult};
use message::ToBytes;

use futures::{self, Canceled, Complete, Future, Poll, Oneshot, Async};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

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
        let _ = tx.send(Ok(status));   // TODO: handle error
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
