extern crate env_logger;
extern crate futures;
extern crate rand;
extern crate rdkafka;

mod test_utils;

use futures::*;

use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::topic_partition_list::TopicPartitionList;

use test_utils::*;


// All produced messages should be consumed.
#[test]
fn test_produce_consume_base() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = produce_messages(&topic_name, 100, &value_fn, &key_fn, None);
    let mut consumer = create_stream_consumer(&topic_name);

    let _consumer_future = consumer.start()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map.get(&(m.partition(), m.offset())).unwrap();
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(*id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(*id));
                },
                e => panic!("Error receiving message: {:?}", e)
            };
            Ok(())
        })
        .wait();
}

// All messages should go to the same partition.
#[test]
fn test_produce_partition() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let message_map = produce_messages(&topic_name, 100, &value_fn, &key_fn, Some(0));

    let res = message_map.iter()
        .filter(|&(&(partition, _), _)| partition == 0)
        .count();

    assert_eq!(res, 100);
}

#[test]
fn test_metadata() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(0));
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(1));
    produce_messages(&topic_name, 1, &value_fn, &key_fn, Some(2));
    let consumer = create_stream_consumer(&topic_name);

    let metadata = consumer.fetch_metadata(5000).unwrap();

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
}

#[test]
fn test_consumer_commit() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    produce_messages(&topic_name, 10, &value_fn, &key_fn, Some(0));
    produce_messages(&topic_name, 11, &value_fn, &key_fn, Some(1));
    produce_messages(&topic_name, 12, &value_fn, &key_fn, Some(2));
    let mut consumer = create_stream_consumer(&topic_name);


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
    produce_messages(&topic_name, 10, &value_fn, &key_fn, None);
    let mut consumer = create_stream_consumer(&topic_name);

    let _consumer_future = consumer.start().take(10).wait();

    let subscription = TopicPartitionList::with_topics(vec![topic_name.as_str()].as_slice());
    assert_eq!(subscription, consumer.subscription().unwrap());
}
