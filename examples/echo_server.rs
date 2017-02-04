#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate futures_cpupool;
extern crate rdkafka;
extern crate tokio_core;
extern crate rand;
extern crate tokio_timer;

use clap::{App, Arg};
use futures::stream::Stream;
use futures::Future;
use futures_cpupool::{Builder, CpuPool};

use rdkafka::client::{Context};
use rdkafka::consumer::{Consumer, ConsumerContext, EmptyConsumerContext, CommitMode, Rebalance};
use rdkafka::consumer::stream_consumer::{StreamConsumer, MessageStream};
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::util::get_rdkafka_version;
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::cell::RefCell;

use tokio_core::reactor::{Core, Timeout};

use tokio_timer::Timer;

mod example_utils;
use example_utils::setup_logger;

fn message_stream(brokers: &str, group_id: &str, topics: &Vec<&str>)
        -> (StreamConsumer<EmptyConsumerContext>, MessageStream) {
    let mut consumer = ClientConfig::new()
        .set("group.id", "example_group_id")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer.subscribe(&vec!["topic1"]).expect("Can't subscribe to specified topic");
    let message_stream = consumer.start();

    (consumer, message_stream)
}

fn expensive_computation(msg: Message) -> String {
    info!("Starting expensive computation on message");
    thread::sleep(Duration::from_secs(rand::random::<u64>() % 5));
    info!("Expensive computation completed");
    format!("Result for: {:?}", msg.payload_view::<str>().unwrap())
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

    let mut core = Core::new().unwrap();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create::<FutureProducer>()
        .expect("Producer creation error");

    let topic_config = TopicConfig::new()
        .set("produce.offset.report", "true")
        .finalize();

    producer.start();

    // The ProducerTopic represents a topic ready for production.
    let topic = Arc::new(producer.get_topic("topic2", &topic_config)
        .expect("Topic creation error"));

    let (_consumer, input_stream) = message_stream(brokers, group_id, &topics);
    let cpu_pool = Builder::new().pool_size(4).create();
    let timer = Timer::default();

    let handle = core.handle();
    let processed_stream = input_stream.filter_map(|result| {
        match result {
            Ok(msg) => Some(msg),
            Err(kafka_error) => {
                warn!("Error while receiving from Kafka: {:?}", kafka_error);
                None
            }
        }
    }).for_each(|msg| {
        info!("Enqueuing message for computation");
        let topic_handle = topic.clone();
        // a future that resolves to Err after a timeout
        let timeout = timer.sleep(Duration::from_millis(3000)).then(|_| Err(()));
        let process_message = cpu_pool.spawn_fn(move || {
            // Takes ownership of the message, and run an expensive computation on it
            Ok(expensive_computation(msg))
        }).and_then(move |computation_result| {
            info!("Sending result message");
            topic_handle.send_copy::<String, ()>(None, Some(&computation_result), None).unwrap()
        }).and_then(|d_report| {
            info!("Delivery report: {:?}", d_report);
            Ok(())
        }).or_else(|err| {
            info!("Error while processing message: {:?}", err);
            Ok(())
        });
        handle.spawn(process_message);
        Ok(())
    });

    println!("starting the reactor");
    core.run(processed_stream).unwrap();
    println!("DONE");
}
