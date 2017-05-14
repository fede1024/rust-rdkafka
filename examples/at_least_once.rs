/// This example shows how to achieve at-least-once message delivery semantics. This stream
/// processing code will simply read from an input topic, and duplicate the content to two output
/// topics. In case of failure (both client or server side), messages might be duplicated, but they
/// won't be lost.
///
/// The key point is committing the offset only once the message has been fully processed.
/// Note that this technique only works when messages are processed in order. If a message with
/// offset `i+n` is processed and committed before message `i`, in case of failure messages in
/// the interval `[i, i+n)` might be lost.
///
/// For a simpler example of consumers and producers, check the `simple_consumer` and
/// `simple_producer` files in the example folder.
///
#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate rdkafka_sys;

use clap::{App, Arg};
use futures::Future;
use futures::future::join_all;
use futures::stream::Stream;

use rdkafka::client::{Context, EmptyContext};
use rdkafka::config::{ClientConfig, TopicConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::{MessageStream, StreamConsumer};
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;

mod example_utils;
use example_utils::setup_logger;


// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
struct LoggingConsumerContext;

impl Context for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

// Define a type for convenience
type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn create_message_stream(brokers: &str, group_id: &str, topic: &str) -> (MessageStream, LoggingConsumer) {
    let context = LoggingConsumerContext;

    let mut consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically: our code won't have to call `consumer.commit*()`. The commit
        // will be done in the background, every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offset that I give you explicitly via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_default_topic_config(TopicConfig::new()
            .finalize())
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context::<_, LoggingConsumer>(context)
        .expect("Consumer creation failed");

    consumer.subscribe(&vec![topic]).expect("Can't subscribe to specified topic");

    (consumer.start(), consumer)
}


fn create_producer(brokers: &str) -> FutureProducer<EmptyContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        //.set("queue.buffering.max.messages", "2")
        .set("queue.buffering.max.ms", "0")
        .set_default_topic_config(TopicConfig::new()
            .finalize())
        .create::<FutureProducer<EmptyContext>>()
        .expect("Producer creation failed")
}

fn main() {
    let matches = App::new("at-least-once")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("At-least-once delivery example")
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
        .arg(Arg::with_name("input-topic")
             .long("input-topic")
             .help("Input topic name")
             .takes_value(true)
             .required(true))
        .arg(Arg::with_name("output-topics")
            .long("output-topics")
            .help("Output topics names")
            .takes_value(true)
            .multiple(true)
            .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (_, version) = get_rdkafka_version();
    info!("rd_kafka_version: {}", version);

    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topics = matches.values_of("output-topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let (message_stream, consumer) = create_message_stream(brokers, group_id, input_topic);
    let producer = create_producer(brokers);

    for message in message_stream.take(10).wait() {
        match message {
            Err(()) => {
                warn!("Error while reading from stream");
            }
            Ok(Err(e)) => {
                warn!("Kafka error: {}", e);
            }
            Ok(Ok(m)) => {
                let delivery_reports = join_all(
                    output_topics.iter()
                        .map(|output_topic|
                            producer.send_copy(output_topic, None, m.payload(), m.key(), None)  // TODO: fix timestamp
                                .expect("Failed to produce message")));
                delivery_reports.wait()
                    .expect("Message delivery failed for some topic");
                if let Err(e) = consumer.store_offset(&m) {
                    warn!("Error while storing offset: {}", e);
                }
            }
        }
    }
}
