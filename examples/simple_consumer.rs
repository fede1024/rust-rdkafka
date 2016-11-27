#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;

use clap::{App, Arg};
use futures::stream::Stream;

use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::util::get_rdkafka_version;

mod example_utils;
use example_utils::setup_logger;


fn consume_and_print(brokers: &str, group_id: &str, topics: &Vec<&str>) {
    let mut consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(
             TopicConfig::new()
             .set("auto.offset.reset", "smallest")
             .finalize())
        .create::<StreamConsumer>()
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topics");

    let message_stream = consumer.start();

    info!("Consumer initialized: {:?}", topics);

    for message in message_stream.take(5).wait() {
        match message {
            Err(e) => {
                warn!("Can't receive message: {:?}", e);
            },
            Ok(m) => {
                let key = match m.get_key_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message key: {:?}", e);
                        &[]
                    },
                };
                let payload = match m.get_payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    },
                };
                info!("key: '{:?}', payload: '{}', partition: {}, offset: {}",
                      key, payload, m.get_partition(), m.get_offset());
                consumer.commit_message(&m, CommitMode::Async);
            },
        };
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
             .help("Configure the logging format (example: 'rdkafka=trace')")
             .takes_value(true))
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
