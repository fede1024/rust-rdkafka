use std::time::Duration;

use clap::{Arg, Command};
use log::trace;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::example_utils::setup_logger;

mod example_utils;

fn print_metadata(
    brokers: &String,
    topic: Option<&String>,
    timeout: Duration,
    fetch_offsets: bool,
) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    trace!("Consumer created");

    let metadata = consumer
        .fetch_metadata(topic.map(|x| x.as_str()), timeout)
        .expect("Failed to fetch metadata");

    let mut message_count = 0;

    println!("Cluster information:");
    println!("  Broker count: {}", metadata.brokers().len());
    println!("  Topics count: {}", metadata.topics().len());
    println!("  Metadata broker name: {}", metadata.orig_broker_name());
    println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    println!("Brokers:");
    for broker in metadata.brokers() {
        println!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    println!("\nTopics:");
    for topic in metadata.topics() {
        println!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            println!(
                "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            if fetch_offsets {
                let (low, high) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));
                println!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                );
                message_count += high - low;
            }
        }
        if fetch_offsets {
            println!("     Total message count: {}", message_count);
        }
    }
}

fn main() {
    let matches = Command::new("metadata fetch example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Fetch and print the cluster metadata")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .num_args(1)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("offsets")
                .long("offsets")
                .help("Enables offset fetching"),
        )
        .arg(
            Arg::new("topic")
                .long("topic")
                .help("Only fetch the metadata of the specified topic")
                .num_args(1),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .default_value("trace")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .num_args(1),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_parser(clap::value_parser!(u64))
                .help("Metadata fetch timeout in milliseconds")
                .num_args(1)
                .default_value("60000"),
        )
        .get_matches();

    setup_logger(true, matches.get_one::<String>("log-conf"));

    let brokers = matches.get_one("brokers").unwrap();
    let timeout = matches.get_one::<u64>("timeout").unwrap();
    let topic = matches.get_one::<String>("topic");
    let fetch_offsets = matches.contains_id("offsets");

    print_metadata(
        brokers,
        topic,
        Duration::from_millis(*timeout),
        fetch_offsets,
    );
}
