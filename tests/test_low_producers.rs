//! Test data production using low level producers.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::ffi::CString;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use maplit::hashmap;

use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{
    BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer,
};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::current_time_millis;
use rdkafka::{ClientContext, Statistics};

use crate::utils::*;

mod utils;

struct PrintingContext {
    _n: i64, // Add data for memory access validation
}

impl ClientContext for PrintingContext {
    // Access and use all stats.
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ProducerContext for PrintingContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        println!("Delivery: {:?} {:?}", delivery_result, delivery_opaque);
    }
}

type TestProducerDeliveryResult = (OwnedMessage, Option<KafkaError>, usize);

#[derive(Clone)]
struct CollectingContext {
    stats: Arc<Mutex<Vec<Statistics>>>,
    results: Arc<Mutex<Vec<TestProducerDeliveryResult>>>,
}

impl CollectingContext {
    fn new() -> CollectingContext {
        CollectingContext {
            stats: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ClientContext for CollectingContext {
    // Access and use all stats.
    fn stats(&self, stats: Statistics) {
        let mut stats_vec = self.stats.lock().unwrap();
        (*stats_vec).push(stats);
    }
}

impl ProducerContext for CollectingContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        let mut results = self.results.lock().unwrap();
        match *delivery_result {
            Ok(ref message) => (*results).push((message.detach(), None, delivery_opaque)),
            Err((ref err, ref message)) => {
                (*results).push((message.detach(), Some(err.clone()), delivery_opaque))
            }
        }
    }
}

fn default_config(config_overrides: HashMap<&str, &str>) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &get_bootstrap_server())
        .set("message.timeout.ms", "5000");

    for (key, value) in config_overrides {
        config.set(key, value);
    }
    config
}

fn base_producer(config_overrides: HashMap<&str, &str>) -> BaseProducer<PrintingContext> {
    base_producer_with_context(PrintingContext { _n: 123 }, config_overrides)
}

fn base_producer_with_context<C: ProducerContext>(
    context: C,
    config_overrides: HashMap<&str, &str>,
) -> BaseProducer<C> {
    default_config(config_overrides)
        .create_with_context::<C, BaseProducer<_>>(context)
        .unwrap()
}

#[allow(dead_code)]
fn threaded_producer(config_overrides: HashMap<&str, &str>) -> ThreadedProducer<PrintingContext> {
    threaded_producer_with_context(PrintingContext { _n: 123 }, config_overrides)
}

fn threaded_producer_with_context<C: ProducerContext>(
    context: C,
    config_overrides: HashMap<&str, &str>,
) -> ThreadedProducer<C> {
    default_config(config_overrides)
        .create_with_context::<C, ThreadedProducer<_>>(context)
        .unwrap()
}

// TESTS

#[test]
fn test_base_producer_queue_full() {
    let producer = base_producer(hashmap! { "queue.buffering.max.messages" => "10" });
    let topic_name = rand_test_topic();

    let results = (0..30)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("payload")
                    .key("key")
                    .timestamp(current_time_millis()),
            )
        })
        .collect::<Vec<_>>();
    while producer.in_flight_count() > 0 {
        producer.poll(Duration::from_millis(100));
    }

    let errors = results
        .iter()
        .filter(|&e| {
            if let &Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) = e {
                true
            } else {
                false
            }
        })
        .count();

    let success = results.iter().filter(|&r| r.is_ok()).count();

    assert_eq!(results.len(), 30);
    assert_eq!(success, 10);
    assert_eq!(errors, 20);
}

#[test]
fn test_base_producer_timeout() {
    let context = CollectingContext::new();
    let producer = base_producer_with_context(
        context.clone(),
        hashmap! {
            "message.timeout.ms" => "1000",
            "bootstrap.servers" => "1.2.3.4"
        },
    );
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("A")
                    .key("B"),
            )
        })
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10));

    assert_eq!(results_count, 10);

    let delivery_results = context.results.lock().unwrap();
    let mut ids = HashSet::new();
    for &(ref message, ref error, id) in &(*delivery_results) {
        assert_eq!(message.payload_view::<str>(), Some(Ok("A")));
        assert_eq!(message.key_view::<str>(), Some(Ok("B")));
        assert_eq!(
            error,
            &Some(KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageTimedOut
            ))
        );
        ids.insert(id);
    }
    assert_eq!(ids.len(), 10);
}

struct HeaderCheckContext {
    ids: Arc<Mutex<HashSet<usize>>>,
}

impl ClientContext for HeaderCheckContext {}

impl ProducerContext for HeaderCheckContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, message_id: usize) {
        let message = delivery_result.as_ref().unwrap();
        if message_id % 2 == 0 {
            let headers = message.headers().unwrap();
            assert_eq!(headers.count(), 3);
            assert_eq!(headers.get(0), Some(("header1", &[1, 2, 3, 4][..])));
            assert_eq!(headers.get_as::<str>(1), Some(("header2", Ok("value2"))));
            assert_eq!(headers.get_as::<[u8]>(2), Some(("header3", Ok(&[][..]))));
        } else {
            assert!(message.headers().is_none());
        }
        (*self.ids.lock().unwrap()).insert(message_id);
    }
}

#[test]
fn test_base_producer_headers() {
    let ids_set = Arc::new(Mutex::new(HashSet::new()));
    let context = HeaderCheckContext {
        ids: ids_set.clone(),
    };
    let producer = base_producer_with_context(context, HashMap::new());
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|id| {
            let mut record = BaseRecord::with_opaque_to(&topic_name, id).payload("A");
            if id % 2 == 0 {
                record = record.headers(
                    OwnedHeaders::new()
                        .add("header1", &[1, 2, 3, 4])
                        .add("header2", "value2")
                        .add("header3", &[]),
                );
            }
            producer.send::<str, str>(record)
        })
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10));

    assert_eq!(results_count, 10);
    assert_eq!((*ids_set.lock().unwrap()).len(), 10);
}

#[test]
fn test_threaded_producer_send() {
    let context = CollectingContext::new();
    let producer = threaded_producer_with_context(context.clone(), HashMap::new());
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("A")
                    .key("B"),
            )
        })
        .filter(|r| r.is_ok())
        .count();

    assert_eq!(results_count, 10);
    producer.flush(Duration::from_secs(10));

    let delivery_results = context.results.lock().unwrap();
    let mut ids = HashSet::new();
    for &(ref message, ref error, id) in &(*delivery_results) {
        assert_eq!(message.payload_view::<str>(), Some(Ok("A")));
        assert_eq!(message.key_view::<str>(), Some(Ok("B")));
        assert_eq!(error, &None);
        ids.insert(id);
    }
}

#[test]
fn test_base_producer_opaque_arc() -> Result<(), Box<dyn Error>> {
    struct OpaqueArcContext {}

    impl ClientContext for OpaqueArcContext {}

    impl ProducerContext for OpaqueArcContext {
        type DeliveryOpaque = Arc<Mutex<usize>>;

        fn delivery(&self, _: &DeliveryResult, opaque: Self::DeliveryOpaque) {
            let mut shared_count = opaque.lock().unwrap();
            *shared_count += 1;
        }
    }

    let shared_count = Arc::new(Mutex::new(0));
    let context = OpaqueArcContext {};
    let producer = base_producer_with_context(context, HashMap::new());
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|_| {
            let record = BaseRecord::with_opaque_to(&topic_name, shared_count.clone()).payload("A");
            producer.send::<str, str>(record)
        })
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10));

    let shared_count = Arc::try_unwrap(shared_count).unwrap().into_inner()?;
    assert_eq!(results_count, shared_count);
    Ok(())
}

#[test]
fn test_fatal_errors() {
    let producer = base_producer(HashMap::new());

    assert_eq!(producer.client().fatal_error(), None);

    let msg = CString::new("fake error").unwrap();
    unsafe {
        rdkafka_sys::rd_kafka_test_fatal_error(
            producer.client().native_ptr(),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
            msg.as_ptr(),
        );
    }

    assert_eq!(
        producer.client().fatal_error(),
        Some((
            RDKafkaErrorCode::OutOfOrderSequenceNumber,
            "test_fatal_error: fake error".into()
        ))
    )
}
