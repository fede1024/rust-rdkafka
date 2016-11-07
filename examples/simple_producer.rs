#[macro_use] extern crate log;
extern crate env_logger;

extern crate futures;
extern crate rdkafka;

use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::thread;

use rdkafka::config::{CreateProducer, KafkaConfig};
use rdkafka::util::get_rdkafka_version;

use futures::*;


fn produce(topic_name: &str) {
    let producer = KafkaConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create_producer()
        .unwrap();

    let _producer_thread = producer.start_polling_thread();

    let mut futures = Vec::new();
    let topic = producer.get_topic(topic_name).expect("Topic creation error");
    for i in 0..10 {
        let value = format!("Message {}", i);
        let p = producer
           .send_copy(&topic, Some(&value), Some(&"key"))
           .expect("Production failed")
           .map(move |m| {
                info!("Delivery status for message {} received", i);
                m
           });
        futures.push(p);
    }

    for future in futures {
       let result = future.wait();
       info!("Future completed. Result: {:?}", result);
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

    produce("topic1");
}
