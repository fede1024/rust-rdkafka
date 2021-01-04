//! Test data production using high level producers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};

use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

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
                Duration::from_secs(0),
            )
        })
        .collect();

    let results: Vec<_> = results.collect().await;
    assert!(results.len() == 10);
    for (i, result) in results.into_iter().enumerate() {
        let (partition, offset) = result.unwrap();
        assert_eq!(partition, 1);
        assert_eq!(offset, i as i64);
    }
}

#[tokio::test]
async fn test_future_producer_send_full() {
    // Connect to a nonexistent Kafka broker with a long message timeout and a
    // tiny producer queue, so we can fill up the queue for a while by sending a
    // single message.
    let mut config = HashMap::new();
    config.insert("bootstrap.servers", "");
    config.insert("message.timeout.ms", "5000");
    config.insert("queue.buffering.max.messages", "1");
    let producer = &future_producer(config);
    let topic_name = &rand_test_topic();

    // Fill up the queue.
    producer
        .send_result(FutureRecord::to(&topic_name).payload("A").key("B"))
        .unwrap();

    let send_message = |timeout| async move {
        let start = Instant::now();
        let res = producer
            .send(FutureRecord::to(&topic_name).payload("A").key("B"), timeout)
            .await;
        match res {
            Ok(_) => panic!("send unexpectedly succeeded"),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => start.elapsed(),
            Err((e, _)) => panic!("got incorrect error: {}", e),
        }
    };

    // Sending a message with no timeout should return a `QueueFull` error
    // approximately immediately.
    let elapsed = send_message(Duration::from_secs(0)).await;
    assert!(elapsed < Duration::from_millis(20));

    // Sending a message with a 1s timeout should return a `QueueFull` error
    // in about 1s.
    let elapsed = send_message(Duration::from_secs(1)).await;
    assert!(elapsed > Duration::from_millis(800));
    assert!(elapsed < Duration::from_millis(1200));

    producer.flush(Timeout::Never);
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
        Duration::from_secs(10),
    );

    match future.await {
        Err((kafka_error, owned_message)) => {
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
