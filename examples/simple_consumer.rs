#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;

use clap::{App, Arg};
use futures::stream::Stream;

use rdkafka::consumer::Consumer;
use rdkafka::config::Config;
use rdkafka::util::get_rdkafka_version;

mod example_utils;
use example_utils::setup_logger;


fn consume_and_print(brokers: &str, group_id: &str, topics: &Vec<&str>) {
    let mut consumer = Config::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("session.timeout.ms", "6000")
        .create::<Consumer>()
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topics");

    let (_consumer_thread, message_stream) = consumer.start_thread();
    info!("Consumer initialized: {:?}", topics);

    let message_stream = message_stream.map(|m| {
        info!("Processing message");
        m
    });

    for message in message_stream.take(5).wait() {
        info!("Result: {:?}", message);
    }
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(Arg::with_name("brokers")
             .short("b")
             .long("brokers")
             .help("Broker list in kafka format")
             .takes_value(true)
             .default_value("localhost:9092"))
        .arg(Arg::with_name("group-id")
             .short("g")
             .long("group-id")
             .help("Consumer group id")
             .takes_value(true)
             .default_value("example_consumer_group_id"))
        .arg(Arg::with_name("log-conf")
             .long("log-conf")
             .help("Configure the logging format")
             .default_value("rdkafka=trace"))
        .arg(Arg::with_name("topics")
             .short("t")
             .long("topics")
             .help("Topic list")
             .takes_value(true)
             .multiple(true)
             .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, &topics);
}


