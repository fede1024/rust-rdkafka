#![feature(alloc_system)]
extern crate alloc_system;

extern crate futures;
extern crate rdkafka;

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
                println!("Future for message {} done", i);
                (m, i)
           });
        futures.push(p);
    }

    for future in futures {
       let x = future.wait();
       println!(">> {:?}", x);
    }
}

fn main() {
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    produce("topic1");
}
