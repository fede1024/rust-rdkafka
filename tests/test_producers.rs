//! Test data production using low level and high level producers.
extern crate futures;
extern crate rand;
extern crate rdkafka;

use rdkafka::{ClientContext, Statistics};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, ThreadedProducer, ProducerContext};
use rdkafka::util::current_time_millis;

#[macro_use] mod utils;
use utils::*;

use std::sync::Mutex;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::time::Duration;


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
            results: Arc::new(Mutex::new(Vec::new()))
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
            Err((ref err, ref message)) => (*results).push((message.detach(), Some(err.clone()), delivery_opaque)),
        }
    }
}

fn default_config(config_overrides: HashMap<&str, &str>) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &get_bootstrap_server())
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000");

    for (key, value) in config_overrides {
        config.set(key, value);
    }
    config
}

fn base_producer(config_overrides: HashMap<&str, &str>) -> BaseProducer<PrintingContext> {
    base_producer_with_context(PrintingContext { _n: 123 }, config_overrides)
}

fn base_producer_with_context<C: ProducerContext>(context: C, config_overrides: HashMap<&str, &str>) -> BaseProducer<C> {
    default_config(config_overrides)
        .create_with_context::<C, BaseProducer<_>>(context).unwrap()
}

#[allow(dead_code)]
fn threaded_producer(config_overrides: HashMap<&str, &str>) -> ThreadedProducer<PrintingContext> {
    threaded_producer_with_context(PrintingContext { _n: 123 }, config_overrides)
}

fn threaded_producer_with_context<C: ProducerContext>(context: C, config_overrides: HashMap<&str, &str>) -> ThreadedProducer<C> {
    default_config(config_overrides)
        .create_with_context::<C, ThreadedProducer<_>>(context).unwrap()
}

// TESTS

#[test]
fn test_base_producer_queue_full() {
    let producer = base_producer(map!("queue.buffering.max.messages" => "10"));
    let topic_name = rand_test_topic();

    let results = (0..30)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("payload")
                    .key("key")
                    .timestamp(current_time_millis())
            )
        }).collect::<Vec<_>>();
    while producer.in_flight_count() > 0 {
        producer.poll(Duration::from_millis(100));
    }

    let errors = results.iter()
        .filter(|&e| {
            if let &Err((KafkaError::MessageProduction(RDKafkaError::QueueFull), _)) = e {
                true
            } else {
                false
            }
        })
        .count();

    let success = results.iter()
        .filter(|&r| r.is_ok())
        .count();

    assert_eq!(results.len(), 30);
    assert_eq!(success, 10);
    assert_eq!(errors, 20);
}

#[test]
fn test_base_producer_timeout() {
    let context = CollectingContext::new();
    let producer = base_producer_with_context(
        context.clone(),
        map!("message.timeout.ms" => "1000",
             "bootstrap.servers" => "1.2.3.4"));
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|id| producer.send(
            BaseRecord::with_opaque_to(&topic_name, id)
                .payload("A")
                .key("B")))
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10));

    assert_eq!(results_count, 10);

    let delivery_results = context.results.lock().unwrap();
    let mut ids = HashSet::new();
    for &(ref message, ref error, id) in &(*delivery_results) {
        assert_eq!(message.payload_view::<str>(), Some(Ok("A")));
        assert_eq!(message.key_view::<str>(), Some(Ok("B")));
        assert_eq!(error, &Some(KafkaError::MessageProduction(RDKafkaError::MessageTimedOut)));
        ids.insert(id);
    }
    assert_eq!(ids.len(), 10);
}

#[test]
fn test_threaded_producer_send() {
    let context = CollectingContext::new();
    let producer = threaded_producer_with_context(context.clone(), HashMap::new());
    let topic_name = rand_test_topic();

    let results_count = (0..10)
        .map(|id| producer.send(BaseRecord::with_opaque_to(&topic_name, id).payload("A").key("B")))
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
