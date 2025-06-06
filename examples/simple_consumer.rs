use clap::{Arg, Command};
use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;

use crate::example_utils::setup_logger;

mod example_utils;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(
    brokers: &str,
    group_id: &str,
    topics: &[&str],
    assignor: Option<&String>,
) {
    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug);

    if let Some(assignor) = assignor {
        config
            .set("group.remote.assignor", assignor)
            .set("group.protocol", "consumer")
            .remove("session.timeout.ms");
    }

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                info!(
                    "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    m.key(),
                    payload,
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    m.timestamp()
                );
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let matches = Command::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("group-id")
                .short('g')
                .long("group-id")
                .help("Consumer group id")
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')"),
        )
        .arg(
            Arg::new("topics")
                .short('t')
                .long("topics")
                .help("Topic list")
                .num_args(0..)
                .required(true),
        )
        .arg(
            Arg::new("assignor")
                .short('a')
                .long("assignor")
                .help("Partition assignor"),
        )
        .get_matches();

    setup_logger(true, matches.get_one("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches
        .get_many::<String>("topics")
        .into_iter()
        .flatten()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();

    let brokers = matches.get_one::<String>("brokers").unwrap();
    let group_id = matches.get_one::<String>("group-id").unwrap();
    let assignor = matches.get_one::<String>("assignor");

    consume_and_print(brokers, group_id, &topics, assignor).await
}
