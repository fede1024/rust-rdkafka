//! Test data production using high level producers.

use std::collections::HashMap;

use futures::stream::{FuturesUnordered, StreamExt};

use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::future_producer::FutureRecord;
use rdkafka::producer::FutureProducer;

use crate::utils::*;

mod utils;

fn future_producer(config_overrides: HashMap<&str, &str>) -> FutureProducer<DefaultClientContext> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "localhost")
        .set("message.timeout.ms", "5000");
    for (key, value) in config_overrides {
        config.set(key, value);
    }
    config.create().expect("Failed to create producer")
}

#[tokio::test]
async fn test_future_producer_send() {
    let producer = future_producer(HashMap::new());
    let topic_name = rand_test_topic();

    let results: FuturesUnordered<_> = (0..10)
        .map(|_| {
            producer.send(
                FutureRecord::to(&topic_name).payload("A").key("B"),
                0,
            )
        })
        .collect();

    let results: Vec<_> = results.collect().await;
    assert!(results.len() == 10);
    for (i, result) in results.into_iter().enumerate() {
        let (partition, offset) = result.unwrap().unwrap();
        assert_eq!(partition, 1);
        assert_eq!(offset, i as i64);
    }
}

#[tokio::test]
async fn test_future_producer_send_fail() {
    let producer = future_producer(HashMap::new());

    let future = producer.send(
        FutureRecord::to("topic")
            .payload("payload")
            .key("key")
            .partition(100) // Fail
            .headers(
                OwnedHeaders::new()
                    .add("0", "A")
                    .add("1", "B")
                    .add("2", "C"),
            ),
        10000,
    );

    match future.await {
        Ok(Err((kafka_error, owned_message))) => {
            assert_eq!(
                kafka_error.to_string(),
                "Message production error: UnknownPartition (Local: Unknown partition)"
            );
            assert_eq!(owned_message.topic(), "topic");
            let headers = owned_message.headers().unwrap();
            assert_eq!(headers.count(), 3);
            assert_eq!(headers.get_as::<str>(0).unwrap(), ("0", Ok("A")));
            assert_eq!(headers.get_as::<str>(1).unwrap(), ("1", Ok("B")));
            assert_eq!(headers.get_as::<str>(2).unwrap(), ("2", Ok("C")));
        }
        e => {
            panic!("Unexpected return value: {:?}", e);
        }
    }
}
