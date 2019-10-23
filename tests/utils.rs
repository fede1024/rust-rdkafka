#![allow(dead_code)]
use lazy_static::lazy_static;
use log::*;
use rand::Rng;
use regex::Regex;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::statistics::Statistics;

use std::collections::HashMap;
use std::env::{self, VarError};

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

pub fn get_broker_version() -> KafkaVersion {
    // librdkafka doesn't expose this directly, sadly.
    match env::var("KAFKA_VERSION") {
        Ok(v) => {
            let regex = Regex::new(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.(\d+))?$").unwrap();
            match regex.captures(&v) {
                Some(captures) => {
                    let extract = |i| {
                        captures
                            .get(i)
                            .map(|m| m.as_str().parse().unwrap())
                            .unwrap_or(0)
                    };
                    KafkaVersion(extract(1), extract(2), extract(3), extract(4))
                }
                None => panic!("KAFKA_VERSION env var was not in expected [n[.n[.n[.n]]]] format"),
            }
        }
        Err(VarError::NotUnicode(_)) => {
            panic!("KAFKA_VERSION env var contained non-unicode characters")
        }
        // If the environment variable is unset, assume we're running the latest version.
        Err(VarError::NotPresent) => {
            KafkaVersion(std::u32::MAX, std::u32::MAX, std::u32::MAX, std::u32::MAX)
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct KafkaVersion(pub u32, pub u32, pub u32, pub u32);

pub struct TestContext {
    _some_data: i64, // Add some data so that valgrind can check proper allocation
}

impl ClientContext for TestContext {
    fn stats(&self, _: Statistics) {} // Don't print stats
}

/// Produce the specified count of messages to the topic and partition specified. A map
/// of (partition, offset) -> message id will be returned. It panics if any error is encountered
/// while populating the topic.
pub async fn populate_topic<'a, P, K, J, Q>(
    topic_name: &str,
    count: i32,
    value_fn: &P,
    key_fn: &K,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32>
where
    P: Fn(i32) -> &'a J,
    K: Fn(i32) -> &'a Q,
    J: ToBytes + 'a,
    Q: ToBytes + 'a,
{
    let prod_context = TestContext { _some_data: 1234 };

    // Produce some messages
    let bootstrap_servers = get_bootstrap_server();
    info!("Connecting to kafka at {}", bootstrap_servers);
    let producer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers.as_str())
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("debug", "all")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "30000")
        .create_with_context::<TestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    let results = (0..count)
        .map(|id| {
            (id, producer.send(
                FutureRecord {
                    topic: topic_name,
                    payload: Some(value_fn(id)),
                    key: Some(key_fn(id)),
                    partition,
                    timestamp,
                    headers: None,
                },
                1000,
            ))
        })
        .collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (id, f) in results {
        match f.await {
            Ok(Ok((partition, offset))) => {
                debug!("Successfully produced record {} to partition {} with offset {}", id, partition, offset);
                message_map.insert((partition, offset), id)
            },
            Ok(Err((kafka_error, _message))) => panic!("Delivery failed for record {}: {}", id, kafka_error),
            Err(e) => panic!("Waiting for future failed for record {}: {}", id, e),
        };
    }

    message_map
}

lazy_static! {
    static ref VALUES: Vec<String> = (0..100).map(|i| format!("Message {}", i)).collect();
    static ref KEYS: Vec<String> = (0..100).map(|i| format!("Key {}", i)).collect();
}

pub fn value_fn(id: i32) -> &'static String {
    VALUES.get(id as usize).unwrap()
}

pub fn key_fn(id: i32) -> &'static String {
    KEYS.get(id as usize).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_populate_topic() {
        let _ = env_logger::try_init();

        let topic_name = rand_test_topic();
        let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), None).await;

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
