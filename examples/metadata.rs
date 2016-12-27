#[macro_use] extern crate log;
#[macro_use] extern crate clap;
extern crate rdkafka;

use clap::{App, Arg};

use rdkafka::consumer::BaseConsumer;
use rdkafka::config::ClientConfig;

mod example_utils;
use example_utils::setup_logger;

fn print_metadata(brokers: &str, timeout_ms: i32) {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create::<BaseConsumer<_>>()
        .expect("Consumer creation failed");

    trace!("Consumer created");

    let metadata = consumer.fetch_metadata(timeout_ms)
        .expect("Failed to fetch metadata");

    println!("Cluster information:");
    println!("  Broker count: {}", metadata.brokers().len());
    println!("  Topics count: {}", metadata.topics().len());
    println!("  Metadata broker name: {}", metadata.orig_broker_name());
    println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    println!("Brokers:");
    for broker in metadata.brokers() {
        println!("  Id: {}  Host: {}:{}  ", broker.id(), broker.host(), broker.port());
    }

    println!("\nTopics:");
    for topic in metadata.topics() {
        println!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            println!("     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                     partition.id(),
                     partition.leader(),
                     partition.replicas(),
                     partition.isr(),
                     partition.error());
        }
    }
}

fn main() {
    let matches = App::new("metadata fetch example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Fetch and print the cluster metadata")
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
        .arg(Arg::with_name("timeout")
             .long("Timeout")
             .help("Metadata fetch timeout in seconds")
             .takes_value(true)
             .default_value("60.0"))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let timeout = value_t!(matches, "timeout", f32).unwrap();

    print_metadata(brokers, (timeout * 1000f32) as i32);
}
