#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate rdkafka_sys;

use clap::{App, Arg};
use futures::stream::Stream;

use rdkafka::client::{Context};
use rdkafka::consumer::{Consumer, ConsumerContext, CommitMode, Rebalance};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig, RDKafkaLogLevel};
use rdkafka::util::get_rdkafka_version;
use rdkafka::error::KafkaResult;

mod example_utils;
use example_utils::setup_logger;

// The Context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular ConsumerContext sets up custom callbacks to log rebalancing events.
struct ConsumerContextExample;

impl Context for ConsumerContextExample {}

impl ConsumerContext for ConsumerContextExample {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList) {
        info!("Committing offsets");
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<ConsumerContextExample>;

fn consume_and_print(brokers: &str, group_id: &str, topics: &Vec<&str>) {
    let context = ConsumerContextExample;

    let mut consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("statistics.interval.ms", "5000")
        .set_default_topic_config(TopicConfig::new()
            //.set("auto.offset.reset", "smallest")
            .finalize())
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context::<_, LoggingConsumer>(context)
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topics");

    // consumer.start() returns a stream. The stream can be used ot chain together expensive steps,
    // such as complex computations on a thread pool or asynchronous IO.
    let message_stream = consumer.start();

    for message in message_stream.wait() {
        match message {
            Err(_) => {
                warn!("Error while reading from stream.");
            },
            Ok(Ok(m)) => {
                let key = match m.key_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message key: {:?}", e);
                        &[]
                    },
                };
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    },
                };
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}",
                      key, payload, m.topic_name(), m.partition(), m.offset());
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            },
            Ok(Err(e)) => {
                warn!("Kafka error: {}", e);
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
