//! This example is similar to the roundtrip one but uses the mock API.

use std::convert::TryInto;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};

#[tokio::main]
async fn main() {
    const TOPIC: &str = "test_topic";
    let mock_cluster = MockCluster::new(3).unwrap();
    mock_cluster
        .create_topic(TOPIC, 32, 3)
        .expect("Failed to create topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", mock_cluster.bootstrap_servers())
        .create()
        .expect("Producer creation error");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", mock_cluster.bootstrap_servers())
        .set("group.id", "rust-rdkafka-mock-example")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[TOPIC]).unwrap();

    tokio::spawn(async move {
        let mut i = 0_usize;
        loop {
            producer
                .send_result(
                    FutureRecord::to(TOPIC)
                        .key(&i.to_string())
                        .payload("dummy")
                        .timestamp(now()),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            i += 1;
        }
    });

    let start = Instant::now();
    let mut latencies = Histogram::<u64>::new(5).unwrap();
    println!("Warming up for 10s...");
    loop {
        let message = consumer.recv().await.unwrap();
        let then = message.timestamp().to_millis().unwrap();
        if start.elapsed() < Duration::from_secs(10) {
            // Warming up.
        } else if start.elapsed() < Duration::from_secs(20) {
            if latencies.is_empty() {
                println!("Recording for 10s...");
            }
            latencies += (now() - then) as u64;
        } else {
            break;
        }
    }

    println!("measurements: {}", latencies.len());
    println!("mean latency: {}ms", latencies.mean());
    println!("p50 latency:  {}ms", latencies.value_at_quantile(0.50));
    println!("p90 latency:  {}ms", latencies.value_at_quantile(0.90));
    println!("p99 latency:  {}ms", latencies.value_at_quantile(0.99));
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
