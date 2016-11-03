extern crate rdkafka;

use rdkafka::config::{CreateProducer, KafkaConfig};
use rdkafka::util::get_rdkafka_version;

use std::thread;

fn produce(topic_name: &str) {
    let producer = KafkaConfig::new()
        .set("metadata.request.timeout.ms", "20000")
        .set("bootstrap.servers", "localhost:9092")
        .create_producer()
        .unwrap();

    let loop_producer = producer.clone();
    let handle = thread::spawn(move || {
        loop {
            let n = loop_producer.poll(1000);
            println!("Receved {} events", n);
        }
    });

    let key = vec![69, 70, 71, 72];

    let topic = producer.get_topic(topic_name).expect("Topic creation error");
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
