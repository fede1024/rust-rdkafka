extern crate rdkafka;

use rdkafka::config::KafkaConfig;
use rdkafka::util::get_rdkafka_version;
use rdkafka::producer::CreateProducer;

use std::{thread, time};


fn produce(topic: &str) {
    let mut producer = KafkaConfig::new()
        //.set("group.id", "marmellata")
        .set("metadata.request.timeout.ms", "20000")
        .create_producer()
        .unwrap();

    producer.broker_add("localhost:9092");

    println!("Producer initialized");

    let topic = producer.new_topic(topic).expect("Topic??");
    let v: Vec<u8> = vec![65, 66, 67, 68];
    let p = producer.produce(&topic, v.as_slice());

    println!(">> {:?}", p);

    thread::sleep(time::Duration::from_millis(3000));

    println!("DONE");
}

fn main() {
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    produce("topic1");
}
