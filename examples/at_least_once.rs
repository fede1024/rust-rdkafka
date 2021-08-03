//! This example shows how to achieve at-least-once message delivery semantics. This stream
//! processing code will simply read from an input topic, and duplicate the content to any number of
//! output topics. In case of failure (client or server side), messages might be duplicated,
//! but they won't be lost.
//!
//! The key point is committing the offset only once the message has been fully processed.
//! Note that this technique only works when messages are processed in order. If a message with
//! offset `i+n` is processed and committed before message `i`, in case of failure messages in
//! the interval `[i, i+n)` might be lost.
//!
//! For a simpler example of consumers and producers, check the `simple_consumer` and
//! `simple_producer` files in the example folder.

use std::time::Duration;

use clap::{App, Arg};
use futures::future;
use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use rdkafka::Message;

use crate::example_utils::setup_logger;

mod example_utils;

// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

// Define a new type for convenience
type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> LoggingConsumer {
    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}

fn create_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.ms", "0") // Do not buffer
        .create()
        .expect("Producer creation failed")
}

#[tokio::main]
async fn main() {
    let matches = App::new("at-least-once")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("At-least-once delivery example")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic name")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-topics")
                .long("output-topics")
                .help("Output topics names")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (_, version) = get_rdkafka_version();
    info!("rd_kafka_version: {}", version);

    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topics = matches
        .values_of("output-topics")
        .unwrap()
        .collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let consumer = create_consumer(brokers, group_id, input_topic);
    let producer = create_producer(brokers);

    loop {
        match consumer.recv().await {
            Err(e) => {
                warn!("Kafka error: {}", e);
            }
            Ok(m) => {
                // Send a copy to the message to every output topic in parallel, and wait for the
                // delivery report to be received.
                future::try_join_all(output_topics.iter().map(|output_topic| {
                    let mut record = FutureRecord::to(output_topic);
                    if let Some(p) = m.payload() {
                        record = record.payload(p);
                    }
                    if let Some(k) = m.key() {
                        record = record.key(k);
                    }
                    producer.send(record, Duration::from_secs(1))
                }))
                .await
                .expect("Message delivery failed for some topic");
                // Now that the message is completely processed, add it's position to the offset
                // store. The actual offset will be committed every 5 seconds.
                if let Err(e) = consumer.store_offset_from_message(&m) {
                    warn!("Error while storing offset: {}", e);
                }
            }
        }
    }
}
