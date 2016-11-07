#[macro_use] extern crate log;
extern crate env_logger;
extern crate futures;
extern crate rdkafka;

use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::thread;

use futures::stream::Stream;

use rdkafka::config::{CreateConsumer, KafkaConfig};
use rdkafka::util::get_rdkafka_version;

fn consume_and_print(topic: &str) {
    let mut consumer = KafkaConfig::new()
        .set("group.id", "my_group_id")
        .set("bootstrap.servers", "localhost:9092")
        .create_consumer()
        .expect("Consumer creation failed");

    consumer.subscribe(topic).expect("Can't subscribe to topic");

    let (_consumer_thread, message_stream) = consumer.start_thread();
    info!("Consumer initialized");

    let message_stream = message_stream.map(|m| {
        info!("Processing message");
        m
    });

    for message in message_stream.take(5).wait() {
        info!("Result: {:?}", message);
    }
}

fn setup_logger() {
    let print_thread = env::var("LOG_THREAD").is_ok();

    let format = move |record: &LogRecord| {
        let thread_name = if print_thread {
            format!("({}) ", match thread::current().name() {
                Some(name) => name,
                None => "unknown"
            })
        } else {
            "".to_string()
        };
        format!("{}{} - {} - {}", thread_name, record.level(), record.target(), record.args())
    };

    let mut builder = LogBuilder::new();
    builder.format(format).filter(None, LogLevelFilter::Info);

    if env::var("RUST_LOG").is_ok() {
       builder.parse(&env::var("RUST_LOG").unwrap());
    }

    builder.init().unwrap();
}

fn main() {
    setup_logger();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    consume_and_print("topic1");
}
