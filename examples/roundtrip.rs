use std::convert::TryInto;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{App, Arg};
use hdrhistogram::Histogram;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::example_utils::setup_logger;

mod example_utils;

#[tokio::main]
async fn main() {
    let matches = App::new("Roundtrip example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Measures latency between producer and consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .help("topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let topic = matches.value_of("topic").unwrap().to_owned();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("group.id", "rust-rdkafka-roundtrip-example")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[&topic]).unwrap();

    tokio::spawn(async move {
        let mut i = 0_usize;
        loop {
            producer
                .send_result(
                    FutureRecord::to(&topic)
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
            if latencies.len() == 0 {
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
