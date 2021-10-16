//! Test metadata fetch, group membership, consumer metadata.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;

use rdkafka_sys::types::RDKafkaConfRes;

use crate::utils::*;

mod utils;

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

#[tokio::test]
async fn test_metadata() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_consumer(&rand_test_group());

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .unwrap();
    let orig_broker_id = metadata.orig_broker_id();
    // The orig_broker_id may be -1 if librdkafka's bootstrap "broker" handles
    // the request.
    if orig_broker_id != -1 && orig_broker_id != 0 {
        panic!(
            "metadata.orig_broker_id = {}, not 0 or 1 as expected",
            orig_broker_id
        )
    }
    assert!(!metadata.orig_broker_name().is_empty());

    let broker_metadata = metadata.brokers();
    assert_eq!(broker_metadata.len(), 1);
    assert_eq!(broker_metadata[0].id(), 0);
    assert!(!broker_metadata[0].host().is_empty());
    assert_eq!(broker_metadata[0].port(), 9092);

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|m| m.name() == topic_name)
        .unwrap();

    let mut ids = topic_metadata
        .partitions()
        .iter()
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

    let metadata_one_topic = consumer
        .fetch_metadata(Some(&topic_name), Duration::from_secs(5))
        .unwrap();
    assert_eq!(metadata_one_topic.topics().len(), 1);
}

#[tokio::test]
async fn test_subscription() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, None, None).await;
    let consumer = create_consumer(&rand_test_group());
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    // Make sure the consumer joins the group.
    let _consumer_future = consumer.recv().await;

    let mut tpl = TopicPartitionList::new();
    tpl.add_topic_unassigned(&topic_name);
    assert_eq!(tpl, consumer.subscription().unwrap());
}

#[tokio::test]
async fn test_group_membership() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    let group_name = rand_test_group();
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 1, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_consumer(&group_name);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    // Make sure the consumer joins the group.
    let _consumer_future = consumer.recv().await;

    let group_list = consumer
        .fetch_group_list(None, Duration::from_secs(5))
        .unwrap();

    // Print all the data, valgrind will check memory access
    for group in group_list.groups().iter() {
        println!(
            "{} {} {} {}",
            group.name(),
            group.state(),
            group.protocol(),
            group.protocol_type()
        );
        for member in group.members() {
            println!(
                "  {} {} {}",
                member.id(),
                member.client_id(),
                member.client_host()
            );
        }
    }

    let group_list2 = consumer
        .fetch_group_list(Some(&group_name), Duration::from_secs(5))
        .unwrap();
    assert_eq!(group_list2.groups().len(), 1);

    let consumer_group = group_list2
        .groups()
        .iter()
        .find(|&g| g.name() == group_name)
        .unwrap();
    assert_eq!(consumer_group.members().len(), 1);

    let consumer_member = &consumer_group.members()[0];
    assert_eq!(
        consumer_member.client_id(),
        "rdkafka_integration_test_client"
    );
}

#[tokio::test]
async fn test_client_config() {
    // If not overridden, `NativeConfig::get` should get the default value for
    // a valid parameter.
    let config = ClientConfig::new().create_native_config().unwrap();
    assert_eq!(config.get("session.timeout.ms").unwrap(), "45000");

    // But if the parameter is overridden, `NativeConfig::get` should reflect
    // the overridden value.
    let config = ClientConfig::new()
        .set("session.timeout.ms", "42")
        .create_native_config()
        .unwrap();
    assert_eq!(config.get("session.timeout.ms").unwrap(), "42");

    // Getting an invalid parameter should produce a nice error message.
    assert_eq!(
        config.get("noexist"),
        Err(KafkaError::ClientConfig(
            RDKafkaConfRes::RD_KAFKA_CONF_UNKNOWN,
            "Unknown configuration name".into(),
            "noexist".into(),
            "".into(),
        ))
    );
}
