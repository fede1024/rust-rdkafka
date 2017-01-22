extern crate futures;
extern crate rdkafka;

use futures::*;

use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;
use rdkafka::topic_partition_list::TopicPartitionList;

static NUMBER_OF_MESSAGES: u64 = 100;

#[test]
fn test_produce_consume_base() {
    // Create consumer
    let mut consumer = ClientConfig::new()
        .set("group.id", "produce_consume_base")
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
    consumer.subscribe(&vec!["produce_consume_base"]).expect("Can't subscribe to specified topics");
    let message_stream = consumer.start();

    // Produce some messages
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()
        .expect("Producer creation error");

    producer.start();

    let topic_config = TopicConfig::new()
        .set("produce.offset.report", "true")
        .finalize();

    let topic = producer.get_topic("produce_consume_base", &topic_config)  // TODO: randomize topic name
        .expect("Topic creation error");

    let futures = (0..NUMBER_OF_MESSAGES)
        .map(|i| {
            let value = format!("Message {}", i);
            topic.send_copy(None, Some(&value), Some(&vec![0, 1, 2, 3]))
                .expect("Production failed")
        })
        .collect::<Vec<_>>();

    for future in futures {
        match future.wait() {
            Ok(report) => match report.result() {
                Err(e) => panic!("Delivery failed: {}", e),
                Ok(_) => ()
            },
            Err(e) => panic!("Waiting for future failed: {}", e)
        }
    }

    // Consume the messages
    let messages: Vec<Message> = message_stream.take(NUMBER_OF_MESSAGES).wait().map({ |message|
        match message {
            Ok(Ok(m)) => {
                consumer.commit_message(&m, CommitMode::Async).unwrap();
                m
            },
            Ok(Err(e)) => panic!("Error receiving message: {:?}", e),
            Err(e) => panic!("Error receiving message: {:?}", e)
        }
    }).collect();

    // Test that committing separately does not crash
    let mut tpl = TopicPartitionList::new();
    tpl.add_topic_with_partitions_and_offsets("produce_consume_base", &vec![(1, 1)]);
    consumer.commit(&tpl, CommitMode::Async).unwrap();

    for i in 0..NUMBER_OF_MESSAGES {
        match messages.get(i as usize) {
            Some(ref message) => {
                assert_eq!(message.key_view::<[u8]>().unwrap().unwrap(), [0, 1, 2, 3]);
                assert_eq!(message.payload_view::<str>().unwrap().unwrap(), format!("Message {}", i));
            }
            None => panic!("Message expected")
        }
    }
}
