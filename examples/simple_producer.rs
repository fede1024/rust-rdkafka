#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;

use clap::{App, Arg};
use futures::*;

use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;

mod example_utils;
use example_utils::setup_logger;


fn produce(brokers: &str, topic_name: &str) {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create::<FutureProducer<_>>()
        .expect("Producer creation error");

    let topic_config = TopicConfig::new()
        .set("produce.offset.report", "true")
        .finalize();

    producer.start();

    // The ProducerTopic represents a topic ready for production.
    let topic = producer.get_topic(topic_name, &topic_config)
        .expect("Topic creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| {
            let value = format!("Message {}", i);
            // The send operation on the topic returns a future, that will be completed once the
            // result or failure from Kafka will be received.
            topic.send_copy(None, Some(&value), Some(&vec![0, 1, 2, 3]))
                .expect("Production failed")
                .map(move |delivery_status| {   // This will be executed onw the result is received
                    info!("Delivery status for message {} received", i);
                    delivery_status
                })
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.wait());
    }
}

fn main() {
    let matches = App::new("producer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line producer")
        .arg(Arg::with_name("brokers")
             .short("b")
             .long("brokers")
             .help("Broker list in kafka format")
             .takes_value(true)
             .default_value("localhost:9092"))
        .arg(Arg::with_name("log-conf")
             .long("log-conf")
             .help("Configure the logging format (example: 'rdkafka=trace')")
             .takes_value(true))
        .arg(Arg::with_name("topic")
             .short("t")
             .long("topic")
             .help("Destination topic")
             .takes_value(true)
             .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();

    produce(brokers, topic);
}
