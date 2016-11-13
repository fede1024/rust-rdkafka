extern crate librdkafka_sys;

#[macro_use] extern crate log;
extern crate env_logger;
extern crate futures;
extern crate rdkafka;

use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::thread;

use futures::stream::Stream;

use rdkafka::event_consumer::EventConsumer;
use rdkafka::config::Config;
use rdkafka::util::get_rdkafka_version;

fn consume_and_print(topic: &str) {
    let mut consumer = Config::new()
        .set("group.id", "my_group_id2")
        .set("bootstrap.servers", "localhost:9092")
        .create::<EventConsumer>()
        .expect("Consumer creation failed");

    consumer.subscribe(topic).expect("Can't subscribe to topic");

    loop {
        // match consumer.poll_main(1000) {
        //     None => {},
        //     Some(e) => {
        //         match unsafe { librdkafka_sys::rd_kafka_event_type(e.ptr) } {
        //             8 => println!("M Error: {:?}", unsafe { librdkafka_sys::rd_kafka_event_error(e.ptr) }),
        //             16 => println!("M Group rebalance"),
        //             n => println!("M Some event: {}", n),
        //         };
        //     }
        // };
        match consumer.poll_consumer(1000) {
            None => {},
            Some(e) => {
                match unsafe { librdkafka_sys::rd_kafka_event_type(e.ptr) } {
                    8 => println!("C Error: {:?}", unsafe { librdkafka_sys::rd_kafka_event_error(e.ptr) }),
                    16 => println!("C Group rebalance"),
                    n => println!("C Some event: {}", n),
                };
            }
        };
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

    consume_and_print("topic1");
}
