#[macro_use] extern crate log;
extern crate clap;
extern crate rand;
extern crate rdkafka;

use clap::{App, Arg};
use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::BaseProducer;
use rdkafka::config::ClientConfig;

use std::sync::Arc;
use std::time::Duration;

mod example_utils;
use example_utils::setup_logger;

use rdkafka::ClientContext;
use rdkafka::TopicPartitionList;
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::ProducerContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaResult;


struct LoggingContext;

impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?} {:?}", offsets, result);
    }
}

type LoggingConsumer = BaseConsumer<LoggingContext>;


fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> LoggingConsumer {
    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set("debug", "consumer,cgrp")
        .create_with_context::<LoggingContext, LoggingConsumer>(LoggingContext)
        .expect("Consumer creation failed");

    consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");

    consumer
}

struct MirrorProducerContext {
    source_consumer: Arc<LoggingConsumer>
}

impl ClientContext for MirrorProducerContext {}
impl ProducerContext for MirrorProducerContext {
    type DeliveryOpaque = TopicPartitionList;

    fn delivery(&self, delivery_result: &Result<BorrowedMessage, (KafkaError, BorrowedMessage)>, delivery_opaque: TopicPartitionList) {
        match delivery_result {
            Ok(m) => {
                info!("Delivered copy of {:?} to {}", delivery_opaque, m.topic());
                self.source_consumer.store_offset_list(&delivery_opaque);
            },
            Err((e, m)) => error!("Failed to deliver to {}: {:?}", m.topic(), e)
        };
    }
}

type MirrorProducer = BaseProducer<MirrorProducerContext>;

fn create_producer(brokers: &str, consumer: Arc<LoggingConsumer>) -> MirrorProducer {
    let mirror_context = MirrorProducerContext {source_consumer: consumer};
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create_with_context(mirror_context)
        .expect("Producer creation failed")
}

fn mirroring(consumer: &LoggingConsumer, producer: &MirrorProducer, output_topic: &str) {
    loop {
        while producer.poll(Duration::from_secs(0)) > 0 {};
        let message = match consumer.poll(Duration::from_millis(1000)) {
            Some(Ok(message)) => message,
            Some(Err(err)) => {
                error!("Error {:?}", err);
                continue;
            },
            None => continue,
        };

        let mut tpl = TopicPartitionList::new();
        tpl.add_message_offset(&message);
        info!("Received {:?}", tpl);

        let mut record = BaseRecord::with_opaque_to(output_topic, tpl);
        if message.key().is_some() {
            record = record.key(message.key().unwrap());
        }
        if message.payload().is_some() {
            record = record.payload(message.payload().unwrap());
        }

        if let Err((e, _)) = producer.send(record) {
            error!("Failed to enqueue message for production: {}", e);
        }
    }
}

fn main() {
    let matches = App::new("topic-mirror")
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
            .default_value("topic_mirror_example"))
        .arg(Arg::with_name("log-conf")
            .long("log-conf")
            .help("Configure the logging format (example: 'rdkafka=trace')")
            .takes_value(true))
        .arg(Arg::with_name("input-topic")
            .long("input-topic")
            .help("Input topic name")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("output-topic")
            .long("output-topic")
            .help("Output topic name")
            .takes_value(true)
            .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let consumer = Arc::new(create_consumer(brokers, group_id, input_topic));
    let producer = create_producer(brokers, Arc::clone(&consumer));

    mirroring(consumer.as_ref(), &producer, &output_topic);
}
