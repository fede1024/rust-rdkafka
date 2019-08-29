//! Test metadata fetch, group membership, consumer metadata.
extern crate env_logger;
extern crate futures;
extern crate rand;
extern crate rdkafka;

use futures::StreamExt;
use futures::executor::{block_on, block_on_stream};

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::config::ClientConfig;

use std::time::Duration;

mod utils;
use crate::utils::*;


fn create_consumer(group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("group.id", group_id)
        .set("enable.partition.eof", "true")
        .set("client.id", "rdkafka_integration_test_client")
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("session.timeout.ms", "6000")
        .set("api.version.request", "true")
        .set("debug", "all")
        .create()
        .expect("Failed to create StreamConsumer")
}

#[test]
fn test_metadata() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(0), None));
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(1), None));
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(2), None));
    let consumer = create_consumer(&rand_test_group());

    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5)).unwrap();
    let orig_broker_id = metadata.orig_broker_id();
    // The orig_broker_id may be -1 if librdkafka's bootstrap "broker" handles
    // the request.
    if orig_broker_id != -1 && orig_broker_id != 0 {
        panic!("metadata.orig_broker_id = {}, not 0 or 1 as expected", orig_broker_id)
    }
    assert!(!metadata.orig_broker_name().is_empty());

    let broker_metadata = metadata.brokers();
    assert_eq!(broker_metadata.len(), 1);
    assert_eq!(broker_metadata[0].id(), 0);
    assert!(!broker_metadata[0].host().is_empty());
    assert_eq!(broker_metadata[0].port(), 9092);

    let topic_metadata = metadata.topics().iter()
        .find(|m| m.name() == topic_name).unwrap();

    let mut ids = topic_metadata.partitions().iter()
        .map(|p| {
            assert_eq!(p.error(), None);
            p.id()
        })
        .collect::<Vec<_>>();
    ids.sort();

    assert_eq!(ids, vec![0, 1, 2]);
    assert_eq!(topic_metadata.error(), None);
    assert_eq!(topic_metadata.partitions().len(), 3);
    assert_eq!(topic_metadata.partitions()[0].leader(), 0);
    assert_eq!(topic_metadata.partitions()[1].leader(), 0);
    assert_eq!(topic_metadata.partitions()[2].leader(), 0);
    assert_eq!(topic_metadata.partitions()[0].replicas(), &[0]);
    assert_eq!(topic_metadata.partitions()[0].isr(), &[0]);

    let metadata_one_topic = consumer.fetch_metadata(Some(&topic_name), Duration::from_secs(5))
        .unwrap();
    assert_eq!(metadata_one_topic.topics().len(), 1);
}

#[test]
fn test_subscription() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    block_on(populate_topic(&topic_name, 10, &value_fn, &key_fn, None, None));
    let consumer = create_consumer(&rand_test_group());
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = block_on_stream(consumer.start()).take(10);

    let mut tpl = TopicPartitionList::new();
    tpl.add_topic_unassigned(&topic_name);
    assert_eq!(tpl, consumer.subscription().unwrap());
}

#[test]
fn test_group_membership() {
    let _r = env_logger::init();

    let topic_name = rand_test_topic();
    let group_name = rand_test_group();
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(0), None));
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(1), None));
    block_on(populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(2), None));
    let consumer = create_consumer(&group_name);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    // Make sure the consumer joins the group
    let _ = block_on_stream(consumer.start().take(1)).collect::<Vec<_>>();

    let group_list = consumer.fetch_group_list(None, Duration::from_secs(5)).unwrap();

    // Print all the data, valgrind will check memory access
    for group in group_list.groups().iter() {
        println!("{} {} {} {}", group.name(), group.state(), group.protocol(), group.protocol_type());
        for member in group.members() {
            println!("  {} {} {}", member.id(), member.client_id(), member.client_host());
        }
    }

    let group_list2 = consumer.fetch_group_list(Some(&group_name), Duration::from_secs(5))
        .unwrap();
    assert_eq!(group_list2.groups().len(), 1);

    let consumer_group = group_list2.groups().iter().find(|&g| g.name() == group_name).unwrap();
    assert_eq!(consumer_group.members().len(), 1);

    let consumer_member = &consumer_group.members()[0];
    assert_eq!(consumer_member.client_id(), "rdkafka_integration_test_client");
}
