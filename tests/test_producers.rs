//! Test data production using low level and high level producers.
extern crate futures;
extern crate rand;
extern crate rdkafka;

use rdkafka::{Context, Statistics};
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::producer::DeliveryResult;
use rdkafka::producer::{BaseProducer, ProducerContext};
use rdkafka::util::current_time_millis;

#[macro_use] mod utils;
use utils::*;

use std::collections::HashMap;


struct TestContext {
    _n: i64, // Add data for memory access validation
}

impl Context for TestContext {
    // Access and use all stats.
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ProducerContext for TestContext {
    type DeliveryContext = u32;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_context: Self::DeliveryContext) {
        println!("Delivery: {:?} {:?}", delivery_result, delivery_context);
    }
}

fn base_producer(config_overrides: Option<HashMap<&str, &str>>) -> BaseProducer<TestContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &get_bootstrap_server());
    config.set_default_topic_config(
        TopicConfig::new()
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .finalize(),
    );

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config.create_with_context::<TestContext, BaseProducer<_>>(TestContext { _n: 123 }).unwrap()
}


#[test]
fn test_produce_queue_full() {
    let producer = base_producer(Some(map!("queue.buffering.max.messages" => "10")));
    let topic_name = rand_test_topic();

    let results = (0..30u32)
        .map(|id| {
            producer.send_copy(
               &topic_name,
               None,
               Some(&format!("Message {}", id)),
               Some(&format!("Key {}", id)),
               Some(Box::new(id)),
               Some(current_time_millis()),
            )
        }).collect::<Vec<_>>();
    while producer.in_flight_count() > 0 {
        producer.poll(100);
    }

    let errors = results.iter()
        .filter(|&r| r == &Err(KafkaError::MessageProduction(RDKafkaError::QueueFull)))
        .count();

    let success = results.iter()
        .filter(|&r| r == &Ok(()))
        .count();

    assert_eq!(results.len(), 30);
    assert_eq!(success, 10);
    assert_eq!(errors, 20);
}
