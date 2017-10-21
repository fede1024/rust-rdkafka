//! Test data consumption using low level and high level consumers.
extern crate env_logger;
extern crate futures;
extern crate rand;
extern crate rdkafka;

use futures::*;

use rdkafka::{Message, Timestamp};
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::error::KafkaError;

mod utils;
use utils::*;

use std::time::{Duration, Instant};
use std::collections::HashMap;

// All produced messages should be consumed.
#[test]
fn test_produce_consume_base() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, None, None);
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map[&(m.partition(), m.offset())];
                    match m.timestamp() {
                        Timestamp::CreateTime(timestamp) => assert!(timestamp > 1489495183000),
                        _ => panic!("Expected createtime for message timestamp")
                    };
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(id));
                    assert_eq!(m.topic(), topic_name.as_str());
                },
                Err(e) => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();
}

// All produced messages should be consumed.
#[test]
fn test_produce_consume_base_assign() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None);
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(1), None);
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(2), None);
    let consumer = create_stream_consumer(&rand_test_group(), None);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning);
    tpl.add_partition_offset(&topic_name, 1, Offset::Offset(2));
    tpl.add_partition_offset(&topic_name, 2, Offset::Offset(9));
    consumer.assign(&tpl).unwrap();

    let mut partition_count = vec![0, 0, 0];

    let _consumer_future = consumer.start()
        .take(19)
        .for_each(|message| {
            match message {
                Ok(m) => partition_count[m.partition() as usize] += 1,
                Err(e) => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    assert_eq!(partition_count, vec![10, 8, 1]);
}

// All produced messages should be consumed.
#[test]
fn test_produce_consume_with_timestamp() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), Some(1111));
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map[&(m.partition(), m.offset())];
                    assert_eq!(m.timestamp(), Timestamp::CreateTime(1111));
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(id));
                },
                Err(e) => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), Some(999999));

    // Lookup the offsets
    let tpl = consumer.offsets_for_timestamp(999999, 10000).unwrap();
    let tp = tpl.find_partition(&topic_name, 0).unwrap();
    assert_eq!(tp.topic(), topic_name);
    assert_eq!(tp.offset(), Offset::Offset(100));
    assert_eq!(tp.partition(), 0);
    assert_eq!(tp.error(), Ok(()));
}

#[test]
fn test_consume_with_no_message_error() {
    let _r = env_logger::init();

    let consumer = create_stream_consumer(&rand_test_group(), None);

    let message_stream = consumer.start_with(Duration::from_millis(200), true);

    let mut first_poll_time = None;
    let mut timeouts_count = 0;
    for message in message_stream.wait() {
        match message {
            Ok(Err(KafkaError::NoMessageReceived)) => {
                // TODO: use entry interface for Options once available
                if first_poll_time.is_none() {
                    first_poll_time = Some(Instant::now());
                }
                timeouts_count += 1;
                if timeouts_count == 26 {
                    break;
                }
            }
            Ok(m) => panic!("A message was actually received: {:?}", m),
            Err(e) => panic!("Unexpected error while receiving message: {:?}", e)
        };
    }

    assert_eq!(timeouts_count, 26);
    // It should take 5000ms
    println!("Duration: {:?}", first_poll_time.unwrap().elapsed());
    assert!(first_poll_time.unwrap().elapsed() < Duration::from_millis(7000));
    assert!(first_poll_time.unwrap().elapsed() > Duration::from_millis(4500));
}



// TODO: add check that commit cb gets called correctly
#[test]
fn test_consumer_commit_message() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None);
    populate_topic(&topic_name, 11, &value_fn, &key_fn, Some(1), None);
    populate_topic(&topic_name, 12, &value_fn, &key_fn, Some(2), None);
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(33)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                },
                Err(e) => panic!("error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    assert_eq!(consumer.fetch_watermarks(&topic_name, 0, 5000).unwrap(), (0, 10));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 1, 5000).unwrap(), (0, 11));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 2, 5000).unwrap(), (0, 12));

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic_name, 0, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 1, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed.add_partition_offset(&topic_name, 0, Offset::Invalid);
    committed.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    committed.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(committed, consumer.committed(5000).unwrap());

    let mut position = TopicPartitionList::new();
    position.add_partition_offset(&topic_name, 0, Offset::Offset(10));
    position.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    position.add_partition_offset(&topic_name, 2, Offset::Offset(12));
    assert_eq!(position, consumer.position().unwrap());
}

#[test]
fn test_consumer_store_offset_commit() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None);
    populate_topic(&topic_name, 11, &value_fn, &key_fn, Some(1), None);
    populate_topic(&topic_name, 12, &value_fn, &key_fn, Some(2), None);
    let mut config = HashMap::new();
    config.insert("enable.auto.offset.store", "false");
    config.insert("enable.partition.eof", "true");
    let consumer = create_stream_consumer(&rand_test_group(), Some(config));
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(36)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.store_offset(&m).unwrap();
                    }
                },
                Err(KafkaError::PartitionEOF(_)) => {},
                Err(e) => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    // Commit the whole current state
    consumer.commit_consumer_state(CommitMode::Sync).unwrap();

    assert_eq!(consumer.fetch_watermarks(&topic_name, 0, 5000).unwrap(), (0, 10));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 1, 5000).unwrap(), (0, 11));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 2, 5000).unwrap(), (0, 12));

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic_name, 0, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 1, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed.add_partition_offset(&topic_name, 0, Offset::Invalid);
    committed.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    committed.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(committed, consumer.committed(5000).unwrap());

    let mut position = TopicPartitionList::new();
    position.add_partition_offset(&topic_name, 0, Offset::Offset(10));
    position.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    position.add_partition_offset(&topic_name, 2, Offset::Offset(12));
    assert_eq!(position, consumer.position().unwrap());
}
