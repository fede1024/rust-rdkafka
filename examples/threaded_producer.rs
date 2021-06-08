use std::{
    thread,
    time::{Duration, Instant},
};

use rdkafka::{
    error::KafkaError,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    util::get_rdkafka_version,
    ClientConfig,
};

use clap::{App, Arg};
use log::{error, info};

use crate::example_utils::setup_logger;

mod example_utils;

fn main() {
    let matches = App::new("producer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line producer")
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
            Arg::with_name("topic")
                .short("t")
                .long("topic")
                .help("Destination topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("amount")
                .short("a")
                .long("amount")
                .help("Amount of message to be sent")
                .takes_value(true)
                .default_value("1000000"),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();

    let producer: &ThreadedProducer<DefaultProducerContext> = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Kafka Producer creation error");

    let start = Instant::now();

    let amount = matches.value_of("amount").unwrap().parse::<i32>().unwrap();

    let mut errors = 0;
    for i in 0..amount {
        let msg = format!("Message {}", i);
        let key = format!("Key {}", i);
        let mut record = BaseRecord::to(topic).payload(&msg).key(&key);

        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                    // Retry after 500ms
                    record = rec;
                    thread::sleep(Duration::from_millis(500));
                }
                Err((e, _)) => {
                    error!("Failed to publish on kafka {:?}", e);
                    errors += 1;
                    break;
                }
            }
        }
    }

    info!(
        "Took {}ms to publish {} msg, errors {:?}",
        start.elapsed().as_millis(),
        amount,
        errors
    );
}
