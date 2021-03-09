use std::sync::Arc;

use clap::{App, Arg};
use futures::stream::StreamExt;
use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
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
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topic: &str) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let consumer = Arc::new(consumer);
    let topic_metadata = {
        let topic = String::from(topic);
        let consumer = consumer.clone();
        tokio::task::spawn_blocking(move || {
            consumer
                .client()
                .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
        })
        .await
        .unwrap()
        .expect("Failed to get metadata about topic")
    };
    let topic_metadata = topic_metadata
        .topics()
        .get(0)
        .expect("Requested topic does not exist.");
    let partitions = topic_metadata
        .partitions()
        .iter()
        .map(|x| x.id())
        .collect::<Vec<i32>>();

    consumer
        .subscribe(&vec![topic])
        .expect("Can't subscribe to specified topics");

    let mut streams = partitions
        .into_iter()
        .map(|p| {
            consumer
                .split_partition_queue(topic, p)
                .expect("Failed to get partition")
                .map(move |msg| (p, msg))
        })
        .collect::<Vec<_>>();

    let mut mine = streams.pop().unwrap();
    tokio::spawn(async move {
        while let Some((p, _msg)) = mine.next().await {
            info!("hello from the other side of partition: {}", p);
        }
    });

    let mut streams = futures::stream::select_all(streams);
    loop {
        while let Some((p, msg)) = streams.next().await {
            match msg {
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
                        "key: '{:?}', payload: '{}', partition: {} == {}, offset: {}",
                        m.key(),
                        payload,
                        p,
                        m.partition(),
                        m.offset()
                    );
                    if let Some(headers) = m.headers() {
                        for i in 0..headers.count() {
                            let header = headers.get(i).unwrap();
                            info!("  Header {:#?}: {:?}", header.0, header.1);
                        }
                    }
                    // consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
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
                .help("Topic")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, topic).await
}
