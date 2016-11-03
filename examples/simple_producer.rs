extern crate rdkafka;

use rdkafka::config::KafkaConfig;
use rdkafka::util::get_rdkafka_version;
use rdkafka::producer::CreateProducer;

use std::{thread, time};

fn produce(topic: &str) {
    let mut producer = KafkaConfig::new()
        .set("metadata.request.timeout.ms", "20000")
        .create_producer()
        .unwrap();


    producer.broker_add("localhost:9092");

    let loop_producer = producer.clone();
    let handle = thread::spawn(move || {
        loop {
            let n = loop_producer.poll(1000);
            println!("Receved {} events", n);
        }
    });

    let key = vec![69, 70, 71, 72];

    let topic = producer.new_topic(topic).expect("Topic creation error");
    let p = producer.send_test(&topic, Some(&"Payload"), Some(&key));

    println!(">> {:?}", p);

    handle.join();

    println!("DONE");
}

fn main() {
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    produce("topic1");
}
