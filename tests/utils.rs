extern crate rdkafka;
extern crate rdkafka_sys;
extern crate rand;
extern crate futures;

use rand::Rng;
use futures::*;

use rdkafka::client::Context;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::message::ToBytes;
use rdkafka::statistics::Statistics;
use rdkafka::error::KafkaResult;

use std::collections::HashMap;
use std::env;

pub fn rand_test_topic() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_group() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

fn get_bootstrap_server() -> String {
    env::var("KAFKA_HOST").unwrap_or_else(|_| "localhost:9092".to_owned())
}

pub struct TestContext {
    _some_data: i64, // Add some data so that valgrind can check proper allocation
}

impl Context for TestContext {
    fn stats(&self, _: Statistics) { }  // Don't print stats
}

impl ConsumerContext for TestContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

pub fn produce_messages<P, K, J, Q>(topic_name: &str, count: i32, value_fn: &P, key_fn: &K,
                                    partition: Option<i32>, timestamp: Option<i64>)
        -> HashMap<(i32, i64), i32>
    where P: Fn(i32) -> J,
          K: Fn(i32) -> Q,
          J: ToBytes,
          Q: ToBytes {

    let prod_context = TestContext { _some_data: 1234 };

    // Produce some messages
    let producer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("debug", "all")
        .set_default_topic_config(TopicConfig::new()
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "30000")
            .finalize())
        .create_with_context::<TestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|id| {
            let future = producer.send_copy(topic_name, partition, Some(&value_fn(id)), Some(&key_fn(id)), timestamp, 1000);
            (id, future)
        }).collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (id, future) in futures {
        match future.wait() {
            Ok(Ok((partition, offset))) => message_map.insert((partition, offset), id),
            Ok(Err((kafka_error, _message))) => panic!("Delivery failed: {}", kafka_error),
            Err(e) => panic!("Waiting for future failed: {}", e)
        };
    }

    message_map
}


// Create consumer
pub fn create_stream_consumer(group_id: &str, config_overrides: Option<HashMap<&'static str, &'static str>>) -> StreamConsumer<TestContext> {
    let cons_context = TestContext { _some_data: 64 };

    create_stream_consumer_with_context(group_id, config_overrides, cons_context)
}

pub fn create_stream_consumer_with_context<C: ConsumerContext>(
    group_id: &str,
    config_overrides: Option<HashMap<&'static str, &'static str>>,
    context: C,
) -> StreamConsumer<C> {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config.set("enable.partition.eof", "false");
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("statistics.interval.ms", "500");
    config.set("api.version.request", "true");
    config.set("debug", "all");
    config.set_default_topic_config(
        TopicConfig::new()
            .set("auto.offset.reset", "earliest")
            .finalize()
    );

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
        .create_with_context::<C, StreamConsumer<C>>(context)
        .expect("Consumer creation failed")
}

pub fn value_fn(id: i32) -> String {
    format!("Message {}", id)
}

pub fn key_fn(id: i32) -> String {
    format!("Key {}", id)
}
