#![feature(alloc_system)]
extern crate alloc_system;

extern crate env_logger;
extern crate futures;
extern crate rand;
extern crate rdkafka;

mod test_utils;

use futures::*;

use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::message::Timestamp;
use rdkafka::topic_partition_list::{OFFSET_BEGINNING, TopicPartitionList};

use test_utils::*;


// All messages should go to the same partition.
#[test]
fn test_produce_partition() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = produce_messages(&topic_name, 100, &value_fn, &key_fn, Some(0), None);

    let res = message_map.iter()
        .filter(|&(&(partition, _), _)| partition == 0)
        .count();

    assert_eq!(res, 100);
}

// All produced messages should be consumed.
#[test]
fn test_produce_consume_base() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = produce_messages(&topic_name, 100, &value_fn, &key_fn, None, None);
    let mut consumer = create_stream_consumer(&rand_test_group());
    consumer.subscribe(&vec![topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map.get(&(m.partition(), m.offset())).unwrap();
                    match m.timestamp() {
                        Timestamp::CreateTime(timestamp) => assert!(timestamp > 1489495183000),
                        _ => panic!("Expected createtime for message timestamp")
                    };
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(*id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(*id));
                    assert_eq!(m.topic_name(), topic_name.as_str());
                },
                e => panic!("Error receiving message: {:?}", e)
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
    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(0), None);
    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(1), None);
    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(2), None);
    let mut consumer = create_stream_consumer(&rand_test_group());
    let mut tp_list = TopicPartitionList::new();
    tp_list.add_topic_with_partitions_and_offsets(
        topic_name.as_str(), &vec![(0, OFFSET_BEGINNING), (1, 2), (2, 9)]);
    consumer.assign(&tp_list).unwrap();

    let mut partition_count = vec![0, 0, 0];

    let _consumer_future = consumer.start()
        .take(19)
        .for_each(|message| {
            match message {
                Ok(m) => partition_count[m.partition() as usize] += 1,
                e => panic!("Error receiving message: {:?}", e)
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
    let message_map = produce_messages(&topic_name, 100, &value_fn, &key_fn, Some(0), Some(1111));
    let mut consumer = create_stream_consumer(&rand_test_group());
    consumer.subscribe(&vec![topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map.get(&(m.partition(), m.offset())).unwrap();
                    assert_eq!(m.timestamp(), Timestamp::CreateTime(1111));
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(*id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(*id));
                },
                e => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(0), Some(999999));

    // Lookup the offsets
    let mut offsets = consumer.offsets_for_timestamp(999999, 100).unwrap();
    assert_eq!(offsets.topics.remove(&topic_name).unwrap().take().unwrap().get(0).unwrap().offset, 100);
}


// METADATA

#[test]
fn test_metadata() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(0), None);
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(1), None);
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(2), None);
    let consumer = create_stream_consumer(&rand_test_group());

    let metadata = consumer.fetch_metadata(None, 5000).unwrap();

    let topic_metadata = metadata.topics().iter()
        .find(|m| m.name() == topic_name).unwrap();

    let mut ids = topic_metadata.partitions().iter().map(|p| p.id()).collect::<Vec<_>>();
    ids.sort();

    assert_eq!(ids, vec![0, 1, 2]);
    // assert_eq!(topic_metadata.error(), None);
    assert_eq!(topic_metadata.partitions().len(), 3);
    assert_eq!(topic_metadata.partitions()[0].leader(), 0);
    assert_eq!(topic_metadata.partitions()[1].leader(), 0);
    assert_eq!(topic_metadata.partitions()[2].leader(), 0);
    assert_eq!(topic_metadata.partitions()[0].replicas(), &[0]);
    assert_eq!(topic_metadata.partitions()[0].isr(), &[0]);

    let metadata_one_topic = consumer.fetch_metadata(Some(&topic_name), 5000).unwrap();
    assert_eq!(metadata_one_topic.topics().len(), 1);
}

#[test]
fn test_consumer_commit() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(0), None);
    produce_messages(&topic_name, 11, &value_fn, &key_fn, Some(1), None);
    produce_messages(&topic_name, 12, &value_fn, &key_fn, Some(2), None);
    let mut consumer = create_stream_consumer(&rand_test_group());
    consumer.subscribe(&vec![topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start()
        .take(33)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                },
                e => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();

    assert_eq!(consumer.fetch_watermarks(&topic_name, 0, 5000).unwrap(), (0, 10));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 1, 5000).unwrap(), (0, 11));
    assert_eq!(consumer.fetch_watermarks(&topic_name, 2, 5000).unwrap(), (0, 12));

    let mut assignment = TopicPartitionList::new();
    assignment.add_topic_with_partitions_and_offsets(&topic_name, &vec![(0, -1001), (1, -1001), (2, -1001)]);
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed.add_topic_with_partitions_and_offsets(&topic_name, &vec![(0, -1001), (1, 11), (2, -1001)]);
    assert_eq!(committed, consumer.committed(5000).unwrap());

    let mut position = TopicPartitionList::new();
    position.add_topic_with_partitions_and_offsets(&topic_name, &vec![(0, 10), (1, 11), (2, 12)]);
    assert_eq!(position, consumer.position().unwrap());
}

#[test]
fn test_subscription() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    produce_messages(&topic_name, 10, &value_fn, &key_fn, None, None);
    let mut consumer = create_stream_consumer(&rand_test_group());
    consumer.subscribe(&vec![topic_name.as_str()]).unwrap();

    let _consumer_future = consumer.start().take(10).wait();

    let subscription = TopicPartitionList::with_topics(vec![topic_name.as_str()].as_slice());
    assert_eq!(subscription, consumer.subscription().unwrap());
}

#[test]
fn test_group_membership() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let group_name = rand_test_group();
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(0), None);
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(1), None);
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(2), None);
    let mut consumer = create_stream_consumer(&group_name);
    consumer.subscribe(&vec![topic_name.as_str()]).unwrap();

    // Make sure the consumer joins the group
    let _consumer_future = consumer.start()
        .take(1)
        .for_each(|_| Ok(()))
        .wait();

    let group_list = consumer.fetch_group_list(None, 5000).unwrap();

    // Print all the data, valgrind will check memory access
    for group in group_list.groups().iter() {
        println!("{} {} {} {}", group.name(), group.state(), group.protocol(), group.protocol_type());
        for member in group.members() {
            println!("  {} {} {}", member.id(), member.client_id(), member.client_host());
        }
    }

    let group_list2 = consumer.fetch_group_list(Some(&group_name), 5000).unwrap();
    assert_eq!(group_list2.groups().len(), 1);

    let consumer_group = group_list2.groups().iter().find(|&g| g.name() == group_name).unwrap();
    assert_eq!(consumer_group.members().len(), 1);

    let consumer_member = &consumer_group.members()[0];
    assert_eq!(consumer_member.client_id(), "rdkafka_integration_test_client");
}
