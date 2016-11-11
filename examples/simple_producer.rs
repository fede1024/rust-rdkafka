#[macro_use] extern crate log;
extern crate env_logger;

extern crate futures;
extern crate rdkafka;

use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::thread;

use rdkafka::config::Config;
use rdkafka::producer::Producer;
use rdkafka::util::get_rdkafka_version;

use futures::*;


fn produce(topic_name: &str) {
    let producer = Config::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<Producer>()
        .unwrap();

    let _producer_thread = producer.start_polling_thread();

    let topic = producer.get_topic(topic_name)
        .set("produce.offset.report", "true")
        .create()
        .expect("Topic creation error");

    let futures = (0..10)
        .map(|i| {
            let value = format!("Message {}", i);
            producer.send_copy(&topic, Some(&value), Some(&"key"))
                .expect("Production failed")
                .map(move |m| {
                    info!("Delivery status for message {} received", i);
                    m
                })
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("Future completed. Result: {:?}", future.wait());
    }
}

fn setup_logger() {
    let print_thread = env::var("LOG_THREAD").is_ok();

    let format = move |record: &LogRecord| {
        let thread_name = if print_thread {
            format!("({}) ", thread::current().name().unwrap_or("unknown"))
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

    produce("topic1");
}
