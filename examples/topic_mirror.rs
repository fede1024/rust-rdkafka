#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rand;
extern crate rdkafka;
extern crate tokio;

use clap::{App, Arg};
use futures::{Future, lazy, Stream};
use tokio::executor::current_thread;
use tokio::executor::thread_pool;

use rdkafka::Message;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::CommitMode;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod example_utils;
use example_utils::setup_logger;
use rdkafka::TopicPartitionList;
use rdkafka::Offset;

fn mirroring(consumer: StreamConsumer, producer: FutureProducer, output_topic: &str) {
    let mut io_thread = current_thread::CurrentThread::new();
    let io_thread_handle = io_thread.handle();

    let (input_stream, consumer) = consumer.owned_stream();

    let stream_processor = input_stream
        .filter_map(|result| {  // Filter out errors
            match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            }
        }).for_each(move |message| {     // Process each message
            info!("Message received: {}:{}", message.partition(), message.offset());
            let consumer = Arc::clone(&consumer);
            let producer_future = producer.send(
                FutureRecord::to(&output_topic)
                    .key("some key")
                    .payload("some payload"),
                0)
                .then(move |result| {
                    match result {
                        Ok(Ok(delivery)) => {
                            println!("Sent: {:?}", delivery);
                            let mut tp = TopicPartitionList::new();
                            tp.add_partition_offset(message.topic(), message.partition(), Offset::Offset(message.offset()));
                            consumer.commit(&tp, CommitMode::Async);
                        },
                        Ok(Err((e, _))) => println!("Error: {:?}", e),
                        Err(_) => println!("Future cancelled")
                    }
                    Ok(())
                });
            let _ = io_thread_handle.spawn(producer_future);
            Ok(())
        });

    info!("Starting event loop");
    let _ = io_thread.block_on(stream_processor);
    info!("Stream processing terminated");
}

fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .create::<StreamConsumer>()
        .expect("Consumer creation failed");

    consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");

    consumer
}

fn create_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.ms", "0")  // Do not buffer
        .create()
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

    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topics = matches.values_of("output-topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let consumer = create_consumer(brokers, group_id, input_topic);
    let producer = create_producer(brokers);

    mirroring(consumer, producer, output_topics[0]);
}
