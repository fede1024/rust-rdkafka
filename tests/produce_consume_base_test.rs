extern crate futures;
extern crate rdkafka;

use futures::*;
use futures::stream::Stream;

use rdkafka::client::EmptyContext;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;
use rdkafka::topic_partition_list::TopicPartitionList;

#[test]
fn test_produce_consume_base() {
    let context = EmptyContext::new();

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
        .create_with_context::<_, StreamConsumer<EmptyContext>>(context)
        .expect("Consumer creation failed");
    consumer.subscribe(&TopicPartitionList::with_topics(&vec!["produce_consume_base"])).expect("Can't subscribe to specified topics");
    let message_stream = consumer.start();

    // Produce some messages
    let mut producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer<_>>()
        .expect("Producer creation error");

    producer.start();

    let topic_config = TopicConfig::new()
        .set("produce.offset.report", "true")
        .finalize();

    let topic = producer.get_topic("produce_consume_base", &topic_config)
        .expect("Topic creation error");

    let futures = (0..5)
        .map(|i| {
            let value = format!("Message {}", i);
            producer.send_copy(&topic, None, Some(&value), Some(&vec![0, 1, 2, 3]))
                .expect("Production failed")
        })
        .collect::<Vec<_>>();

    for future in futures {
        future.wait().expect("Waiting for future failed");
    }

    // Consume the messages
    let messages: Vec<Message> = message_stream.take(5).wait().map({ |message|
        match message {
            Ok(m) => {
                consumer.commit_message(&m, CommitMode::Async);
                m
            },
            Err(e) => panic!("Error receiving message: {:?}", e)
        }
    }).collect();

    for i in 0..5 {
        match messages.get(i) {
            Some(ref message) => {
                assert_eq!(message.get_key_view::<[u8]>().unwrap().unwrap(), [0, 1, 2, 3]);
                assert_eq!(message.get_payload_view::<str>().unwrap().unwrap(), format!("Message {}", i));
            }
            None => panic!("Message expected")
        }
    }
}
