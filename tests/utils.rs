#![allow(dead_code)]
extern crate rdkafka;
extern crate rand;
extern crate futures;

use rand::Rng;
use futures::*;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::message::ToBytes;
use rdkafka::statistics::Statistics;

use std::collections::HashMap;
use std::env;

#[macro_export]
macro_rules! map(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = HashMap::new();
            $( m.insert($key, $value); )+
            m
        }
    };
);

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

pub fn get_bootstrap_server() -> String {
    env::var("KAFKA_HOST").unwrap_or_else(|_| "localhost:9092".to_owned())
}

pub struct TestContext {
    _some_data: i64, // Add some data so that valgrind can check proper allocation
}

impl ClientContext for TestContext {
    fn stats(&self, _: Statistics) {} // Don't print stats
}

/// Produce the specified count of messages to the topic and partition specified. A map
/// of (partition, offset) -> message id will be returned. It panics if any error is encountered
/// while populating the topic.
pub fn populate_topic<P, K, J, Q>(
    topic_name: &str,
    count: i32,
    value_fn: &P,
    key_fn: &K,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32>
where
    P: Fn(i32) -> J,
    K: Fn(i32) -> Q,
    J: ToBytes,
    Q: ToBytes,
{
    let prod_context = TestContext { _some_data: 1234 };

    // Produce some messages
    let producer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("debug", "all")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "30000")
        .create_with_context::<TestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|id| {
            let future = producer.send(
                FutureRecord {
                    topic: topic_name,
                    payload: Some(&value_fn(id)),
                    key: Some(&key_fn(id)),
                    partition,
                    timestamp,
                    headers: None,
                },
                1000);
            (id, future)
        })
        .collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (id, future) in futures {
        match future.wait() {
            Ok(Ok((partition, offset))) => message_map.insert((partition, offset), id),
            Ok(Err((kafka_error, _message))) => panic!("Delivery failed: {}", kafka_error),
            Err(e) => panic!("Waiting for future failed: {}", e),
        };
    }

    message_map
}


pub fn value_fn(id: i32) -> String {
    format!("Message {}", id)
}

pub fn key_fn(id: i32) -> String {
    format!("Key {}", id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_populate_topic() {
        let topic_name = rand_test_topic();
        let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), None);

        let total_messages = message_map
            .iter()
            .filter(|&(&(partition, _), _)| partition == 0)
            .count();
        assert_eq!(total_messages, 100);

        let mut ids = message_map.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, (0..100).collect::<Vec<_>>());
    }
}
