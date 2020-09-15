use std::time::Duration;

use clap::{App, Arg};
use futures::StreamExt;
use log::{info, warn};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::future_producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

use crate::example_utils::setup_logger;

mod example_utils;

const TIMEOUT: Duration = Duration::from_secs(5);

// Start a consumer and a transactional producer.
// The consumer will subscribe to all in topics and the producer will forward
// all incoming messages to the out topic, committing a transaction for every
// 100 consumed and produced (processed) messages.
async fn process(
    brokers: &str,
    group_id: &str,
    in_topics: &[&str],
    transactional_id: &str,
    out_topic: &str,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("transactional.id", transactional_id)
        .set("message.timeout.ms", "5000")
        .set("transaction.timeout.ms", "900000")
        .create()
        .expect("Producer creation failed");

    consumer
        .subscribe(in_topics)
        .expect("Can't subscribe to in topics");

    producer
        .init_transactions(TIMEOUT)
        .expect("Can't init transactions");

    producer
        .begin_transaction()
        .expect("Can't begin transaction");

    let mut message_stream = consumer.start();

    let mut processed = 0;

    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => {
                warn!("Kafka error consuming messages: {}", e);
                reset_transaction(&producer, &consumer);
            }

            Ok(m) => {
                let record = FutureRecord::to(out_topic)
                    .key(m.key().unwrap_or(b""))
                    .payload(m.payload().unwrap_or(b""));

                match producer.send(record, TIMEOUT).await {
                    Err((e, _)) => {
                        warn!("Kafka error producing message: {}", e);
                        reset_transaction(&producer, &consumer);
                    }
                    Ok(_) => {
                        processed += 1;
                        println!("sent message to {} ", out_topic);
                    }
                }
            }
        }

        if processed >= 100 {
            // send current consumer position to transaction
            let position = &consumer.position().expect("Can't get consumer position");

            producer
                .send_offsets_to_transaction(position, &consumer.group_metadata(), TIMEOUT)
                .expect("Can't send consumer offsets to transaction");

            producer
                .commit_transaction(TIMEOUT)
                .expect("Can't commit transaction");

            // start a new transaction
            producer
                .begin_transaction()
                .expect("Can't begin transaction");

            processed = 0;
        }
    }
}

// Abort the current transaction, seek consumer to last committed offsets and
// start a new transaction.
fn reset_transaction(producer: &FutureProducer, consumer: &StreamConsumer) {
    producer
        .abort_transaction(TIMEOUT)
        .expect("Can't abort transaction");

    seek_to_committed(consumer);

    producer
        .begin_transaction()
        .expect("Can't begin transaction");
}

fn seek_to_committed(consumer: &StreamConsumer) {
    let committed = consumer
        .committed(TIMEOUT)
        .expect("Can't get consumer committed offsets");

    for e in committed.elements().iter() {
        consumer
            .seek(e.topic(), e.partition(), e.offset(), TIMEOUT)
            .expect(&format!(
                "Can't seek consumer to {}, {}: {:?}",
                e.topic(),
                e.partition(),
                e.offset()
            ));
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("transactions example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Command line transactional message processor")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_transaction_consumer_group_id"),
        )
        .arg(
            Arg::with_name("in-topics")
                .short("i")
                .long("in-topics")
                .help("Topic list to consume from")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .arg(
            Arg::with_name("transactional-id")
                .short("t")
                .long("transactional-id")
                .help("Producer transactional id")
                .takes_value(true)
                .default_value("example_transaction_producer_id"),
        )
        .arg(
            Arg::with_name("out-topic")
                .short("o")
                .long("out-topic")
                .help("Topic to produce to")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let in_topics = matches
        .values_of("in-topics")
        .unwrap()
        .collect::<Vec<&str>>();
    let transactional_id = matches.value_of("transactional-id").unwrap();
    let out_topic = matches.value_of("out-topic").unwrap();

    process(brokers, group_id, &in_topics, transactional_id, out_topic).await;
}
