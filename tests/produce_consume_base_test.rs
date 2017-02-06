extern crate rdkafka;

extern crate futures;
extern crate rand;

mod test_utils;

use futures::*;

use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::topic_partition_list::TopicPartitionList;

use test_utils::{rand_test_group, rand_test_topic};

use std::collections::HashMap;

static NUMBER_OF_MESSAGES: u64 = 100;

#[test]
fn test_produce_consume_base() {
    let topic_name = rand_test_topic();

    // Produce some messages
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()
        .expect("Producer creation error");

    producer.start();

    let topic_config = TopicConfig::new()
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .finalize();

    let topic = producer.get_topic(&topic_name, &topic_config)
        .expect("Topic creation error");

    let futures = (0..NUMBER_OF_MESSAGES)
        .map(|i| {
            let value = format!("Message {}", i);
            let key = &(value.as_bytes()[7..]);
            let future = topic.send_copy(None, Some(&value), Some(&key))
                .expect("Production failed");
            (value.clone(), future)
        }).collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (value, future) in futures {
        match future.wait() {
            Ok(report) => match report.result() {
                Err(e) => panic!("Delivery failed: {}", e),
                Ok((p, o)) => message_map.insert((p, o), value),
            },
            Err(e) => panic!("Waiting for future failed: {}", e)
        };
    }

    // Create consumer
    let mut consumer = ClientConfig::new()
        .set("group.id", &rand_test_group())
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(
             TopicConfig::new()
                 .set("auto.offset.reset", "earliest")
                 .finalize()
        )
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");
    consumer.subscribe(&vec![&topic_name]).unwrap();

    // Consume the messages
    let _consumer_future = consumer.start()
        .take(NUMBER_OF_MESSAGES)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let value = message_map.get(&(m.partition(), m.offset())).unwrap();
                    let key = &(value.as_bytes()[7..]);
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value);
                    assert_eq!(m.key_view::<[u8]>().unwrap().unwrap(), key);
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                },
                e => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    // Test that committing separately does not crash
    let mut tpl = TopicPartitionList::new();
    tpl.add_topic_with_partitions_and_offsets("produce_consume_base", &vec![(1, 1)]);
    consumer.commit(&tpl, CommitMode::Async).unwrap();

    // Fetching various metadata should not fail
    consumer.subscription().unwrap();
    consumer.assignment().unwrap();
    consumer.committed(500).unwrap();
    consumer.position().unwrap();
    consumer.fetch_metadata(500).unwrap();
    consumer.fetch_watermarks("produce_consume_base", 1, 500).unwrap();
}
