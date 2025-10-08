//! Test administrative commands using the admin API.

use std::time::Duration;

use backon::{BlockingRetryable, ExponentialBuilder};

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, GroupResult, NewPartitions,
    NewTopic, OwnedResourceSpecifier, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::admin::{
    group_description_result_key, ConsumerGroupState,
};
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use rdkafka::admin::{group_result_key, ListConsumerGroupOffsets};
use rdkafka::admin::list_offsets_result_key;
use crate::utils::*;

mod utils;

fn create_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config
}

fn create_admin_client() -> AdminClient<DefaultClientContext> {
    configure_logging_for_tests();
    create_config()
        .create()
        .expect("admin client creation failed")
}

async fn create_topics(topic_name_list: &[&str]) -> KafkaResult<()> {
    let admin_client: AdminClient<DefaultClientContext> = create_admin_client();

    let topic_list: Vec<_> = topic_name_list
        .iter()
        .map(|name| {
            NewTopic::new(name, 1, TopicReplication::Fixed(1)).set("max.message.bytes", "1234")
        })
        .collect();

    match admin_client
        .create_topics(&topic_list, &AdminOptions::default())
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn create_consumer_group(consumer_group_name: &str) {
    let admin_client: AdminClient<DefaultClientContext> = create_admin_client();
    let topic_name = &rand_test_topic(consumer_group_name);
    let consumer: BaseConsumer = create_config()
        .set("group.id", consumer_group_name)
        .create()
        .expect("create consumer failed");

    admin_client
        .create_topics(
            &[NewTopic {
                name: topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");
    let topic_partition_list = {
        let mut lst = TopicPartitionList::new();
        lst.add_partition(topic_name, 0);
        lst
    };
    consumer
        .assign(&topic_partition_list)
        .expect("assign topic partition list failed");
    consumer
        .fetch_metadata(None, Duration::from_secs(3))
        .expect("unable to fetch metadata");
    (|| consumer.store_offset(topic_name, 0, -1))
        .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
        .call()
        .expect("store offset failed");
    consumer
        .commit_consumer_state(CommitMode::Sync)
        .expect("commit the consumer state failed");
}

fn fetch_metadata(topic: &str) -> Metadata {
    let consumer: BaseConsumer<DefaultConsumerContext> =
        create_config().create().expect("consumer creation failed");
    let timeout = Some(Duration::from_secs(1));

    (|| {
        let metadata = consumer
            .fetch_metadata(Some(topic), timeout)
            .map_err(|e| e.to_string())?;
        if metadata.topics().is_empty() {
            Err("metadata fetch returned no topics".to_string())?
        }
        let topic = &metadata.topics()[0];
        if topic.partitions().is_empty() {
            Err("metadata fetch returned a topic with no partitions".to_string())?
        }
        Ok::<_, String>(metadata)
    })
    .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
    .call()
    .unwrap()
}

fn verify_delete(topic: &str) {
    let consumer: BaseConsumer<DefaultConsumerContext> =
        create_config().create().expect("consumer creation failed");
    let timeout = Some(Duration::from_secs(1));

    (|| {
        // Asking about the topic specifically will recreate it (under the
        // default Kafka configuration, at least) so we have to ask for the list
        // of all topics and search through it.
        let metadata = consumer
            .fetch_metadata(None, timeout)
            .map_err(|e| e.to_string())?;
        if metadata.topics().iter().any(|t| t.name() == topic) {
            Err(format!("topic {} still exists", topic))?
        }
        Ok::<(), String>(())
    })
    .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
    .call()
    .unwrap()
}

#[tokio::test]
async fn test_topics() {
    let admin_client = create_admin_client();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Verify that topics are created as specified, and that they can later
    // be deleted.
    {
        let name1 = rand_test_topic("test_topics");
        let name2 = rand_test_topic("test_topics");

        // Test both the builder API and the literal construction.
        let topic1 =
            NewTopic::new(&name1, 1, TopicReplication::Fixed(1)).set("max.message.bytes", "1234");
        let topic2 = NewTopic {
            name: &name2,
            num_partitions: 3,
            replication: TopicReplication::Variable(&[&[0], &[0], &[0]]),
            config: Vec::new(),
        };

        let res = admin_client
            .create_topics(&[topic1, topic2], &opts)
            .await
            .expect("topic creation failed");
        assert_eq!(res, &[Ok(name1.clone()), Ok(name2.clone())]);

        let metadata1 = fetch_metadata(&name1);
        let metadata2 = fetch_metadata(&name2);
        assert_eq!(1, metadata1.topics().len());
        assert_eq!(1, metadata2.topics().len());
        let metadata_topic1 = &metadata1.topics()[0];
        let metadata_topic2 = &metadata2.topics()[0];
        assert_eq!(&name1, metadata_topic1.name());
        assert_eq!(&name2, metadata_topic2.name());
        assert_eq!(1, metadata_topic1.partitions().len());
        assert_eq!(3, metadata_topic2.partitions().len());

        let res = admin_client
            .describe_configs(
                &[
                    ResourceSpecifier::Topic(&name1),
                    ResourceSpecifier::Topic(&name2),
                ],
                &opts,
            )
            .await
            .expect("describe configs failed");
        let config1 = &res[0].as_ref().expect("describe configs failed on topic 1");
        let config2 = &res[1].as_ref().expect("describe configs failed on topic 2");
        let mut expected_entry1 = ConfigEntry {
            name: "max.message.bytes".into(),
            value: Some("1234".into()),
            source: ConfigSource::DynamicTopic,
            is_read_only: false,
            is_default: false,
            is_sensitive: false,
        };
        let default_max_msg_bytes = if get_broker_version() <= KafkaVersion(2, 3, 0, 0) {
            "1000012"
        } else {
            "1048588"
        };
        let expected_entry2 = ConfigEntry {
            name: "max.message.bytes".into(),
            value: Some(default_max_msg_bytes.into()),
            source: ConfigSource::Default,
            is_read_only: false,
            is_default: true,
            is_sensitive: false,
        };
        if get_broker_version() < KafkaVersion(1, 1, 0, 0) {
            expected_entry1.source = ConfigSource::Unknown;
        }
        assert_eq!(Some(&expected_entry1), config1.get("max.message.bytes"));
        assert_eq!(Some(&expected_entry2), config2.get("max.message.bytes"));
        let config_entries1 = config1.entry_map();
        let config_entries2 = config2.entry_map();
        assert_eq!(config1.entries.len(), config_entries1.len());
        assert_eq!(config2.entries.len(), config_entries2.len());
        assert_eq!(
            Some(&&expected_entry1),
            config_entries1.get("max.message.bytes")
        );
        assert_eq!(
            Some(&&expected_entry2),
            config_entries2.get("max.message.bytes")
        );

        let partitions1 = NewPartitions::new(&name1, 5);
        let res = admin_client
            .create_partitions(&[partitions1], &opts)
            .await
            .expect("partition creation failed");
        assert_eq!(res, &[Ok(name1.clone())]);

        let mut tries = 0;
        loop {
            let metadata = fetch_metadata(&name1);
            let topic = &metadata.topics()[0];
            let n = topic.partitions().len();
            if n == 5 {
                break;
            } else if tries >= 5 {
                panic!("topic has {} partitions, but expected {}", n, 5);
            } else {
                tries += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        let res = admin_client
            .delete_topics(&[&name1, &name2], &opts)
            .await
            .expect("topic deletion failed");
        assert_eq!(res, &[Ok(name1.clone()), Ok(name2.clone())]);
        verify_delete(&name1);
        verify_delete(&name2);
    }

    // Verify that incorrect replication configurations are ignored when
    // creating topics.
    {
        let topic = NewTopic::new("ignored", 1, TopicReplication::Variable(&[&[0], &[0]]));
        let res = admin_client.create_topics(&[topic], &opts).await;
        assert_eq!(
            Err(KafkaError::AdminOpCreation(
                "replication configuration for topic 'ignored' assigns 2 partition(s), \
                 which does not match the specified number of partitions (1)"
                    .into()
            )),
            res,
        )
    }

    // Verify that incorrect replication configurations are ignored when
    // creating partitions.
    {
        let name = rand_test_topic("test_topics");
        let topic = NewTopic::new(&name, 1, TopicReplication::Fixed(1));

        let res = admin_client
            .create_topics(vec![&topic], &opts)
            .await
            .expect("topic creation failed");
        assert_eq!(res, &[Ok(name.clone())]);
        let _ = fetch_metadata(&name);

        // This partition specification is obviously garbage, and so trips
        // a client-side error.
        let partitions = NewPartitions::new(&name, 2).assign(&[&[0], &[0], &[0]]);
        let res = admin_client.create_partitions(&[partitions], &opts).await;
        assert_eq!(
            res,
            Err(KafkaError::AdminOpCreation(format!(
                "partition assignment for topic '{}' assigns 3 partition(s), \
                 which is more than the requested total number of partitions (2)",
                name
            )))
        );

        // Only the server knows that this partition specification is garbage.
        let partitions = NewPartitions::new(&name, 2).assign(&[&[0], &[0]]);
        let res = admin_client
            .create_partitions(&[partitions], &opts)
            .await
            .expect("partition creation failed");
        assert_eq!(
            res,
            &[Err((name, RDKafkaErrorCode::InvalidReplicaAssignment))],
        );
    }

    // Verify that deleting a non-existent topic fails.
    {
        let name = rand_test_topic("test_topics");
        let res = admin_client
            .delete_topics(&[&name], &opts)
            .await
            .expect("delete topics failed");
        assert_eq!(
            res,
            &[Err((name, RDKafkaErrorCode::UnknownTopicOrPartition))]
        );
    }

    // Verify that mixed-success operations properly report the successful and
    // failing operators.
    {
        let name1 = rand_test_topic("test_topics");
        let name2 = rand_test_topic("test_topics");

        let topic1 = NewTopic::new(&name1, 1, TopicReplication::Fixed(1));
        let topic2 = NewTopic::new(&name2, 1, TopicReplication::Fixed(1));

        let res = admin_client
            .create_topics(vec![&topic1], &opts)
            .await
            .expect("topic creation failed");
        assert_eq!(res, &[Ok(name1.clone())]);
        let _ = fetch_metadata(&name1);

        let res = admin_client
            .create_topics(vec![&topic1, &topic2], &opts)
            .await
            .expect("topic creation failed");
        assert_eq!(
            res,
            &[
                Err((name1.clone(), RDKafkaErrorCode::TopicAlreadyExists)),
                Ok(name2.clone())
            ]
        );
        let _ = fetch_metadata(&name2);

        let res = admin_client
            .delete_topics(&[&name1], &opts)
            .await
            .expect("topic deletion failed");
        assert_eq!(res, &[Ok(name1.clone())]);
        verify_delete(&name1);

        let res = admin_client
            .delete_topics(&[&name2, &name1], &opts)
            .await
            .expect("topic deletion failed");
        assert_eq!(
            res,
            &[
                Ok(name2.clone()),
                Err((name1.clone(), RDKafkaErrorCode::UnknownTopicOrPartition))
            ]
        );
    }
}

/// Test the admin client's delete records functionality.
#[tokio::test]
async fn test_delete_records() {
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let admin_client = create_admin_client();
    let timeout = Some(Duration::from_secs(1));
    let opts = AdminOptions::new().operation_timeout(timeout);
    let topic = rand_test_topic("test_delete_records");
    let make_record = || FutureRecord::<str, str>::to(&topic).payload("data");

    // Create a topic with a single partition.
    admin_client
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .expect("topic creation failed");

    // Ensure that the topic begins with low and high water marks of 0.
    let (lo, hi) = (|| producer.client().fetch_watermarks(&topic, 0, timeout))
        .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
        .call()
        .unwrap();
    assert_eq!(lo, 0);
    assert_eq!(hi, 0);

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record(), timeout).await.unwrap();
    }

    // Ensure that the high water mark has advanced to 5.
    let (lo, hi) = producer
        .client()
        .fetch_watermarks(&topic, 0, timeout)
        .unwrap();
    assert_eq!(lo, 0);
    assert_eq!(hi, 5);

    // Delete the record at offset 0.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(1))
        .unwrap();
    let res_tpl = admin_client.delete_records(&tpl, &opts).await.unwrap();
    assert_eq!(res_tpl.count(), 1);
    assert_eq!(res_tpl.elements()[0].topic(), topic);
    assert_eq!(res_tpl.elements()[0].partition(), 0);
    assert_eq!(res_tpl.elements()[0].offset(), Offset::Offset(1));
    assert_eq!(res_tpl.elements()[0].error(), Ok(()));

    // Ensure that the low water mark has advanced to 1.
    let (lo, hi) = producer
        .client()
        .fetch_watermarks(&topic, 0, timeout)
        .unwrap();
    assert_eq!(lo, 1);
    assert_eq!(hi, 5);

    // Delete the record at offset 1 and also include an invalid partition in
    // the request. The invalid partition should not cause the request to fail,
    // but we should be able to see the per-partition error in the returned
    // topic partition list.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(2))
        .unwrap();
    tpl.add_partition_offset(&topic, 1, Offset::Offset(1))
        .unwrap();
    let res_tpl = admin_client.delete_records(&tpl, &opts).await.unwrap();
    assert_eq!(res_tpl.count(), 2);
    assert_eq!(res_tpl.elements()[0].topic(), topic);
    assert_eq!(res_tpl.elements()[0].partition(), 0);
    assert_eq!(res_tpl.elements()[0].offset(), Offset::Offset(2));
    assert_eq!(res_tpl.elements()[0].error(), Ok(()));
    assert_eq!(res_tpl.elements()[1].topic(), topic);
    assert_eq!(res_tpl.elements()[1].partition(), 1);
    assert_eq!(
        res_tpl.elements()[1].error(),
        Err(KafkaError::OffsetFetch(RDKafkaErrorCode::UnknownPartition))
    );

    // Ensure that the low water mark has advanced to 2.
    let (lo, hi) = producer
        .client()
        .fetch_watermarks(&topic, 0, timeout)
        .unwrap();
    assert_eq!(lo, 2);
    assert_eq!(hi, 5);

    // Delete all records up to offset 5.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::End).unwrap();
    let res_tpl = admin_client.delete_records(&tpl, &opts).await.unwrap();
    assert_eq!(res_tpl.count(), 1);
    assert_eq!(res_tpl.elements()[0].topic(), topic);
    assert_eq!(res_tpl.elements()[0].partition(), 0);
    assert_eq!(res_tpl.elements()[0].offset(), Offset::Offset(5));
    assert_eq!(res_tpl.elements()[0].error(), Ok(()));

    // Ensure that the low water mark has advanced to 5.
    let (lo, hi) = producer
        .client()
        .fetch_watermarks(&topic, 0, timeout)
        .unwrap();
    assert_eq!(lo, 5);
    assert_eq!(hi, 5);
}

#[tokio::test]
async fn test_configs() {
    let admin_client = create_admin_client();
    let opts = AdminOptions::new();
    let broker = ResourceSpecifier::Broker(0);

    let res = admin_client
        .describe_configs(&[broker], &opts)
        .await
        .expect("describe configs failed");
    let config = &res[0].as_ref().expect("describe configs failed");
    let orig_val = config
        .get("log.flush.interval.messages")
        .expect("original config entry missing")
        .value
        .as_ref()
        .expect("original value missing");

    let config = AlterConfig::new(broker).set("log.flush.interval.messages", "1234");
    let res = admin_client
        .alter_configs(&[config], &opts)
        .await
        .expect("alter configs failed");
    assert_eq!(res, &[Ok(OwnedResourceSpecifier::Broker(0))]);

    let mut tries = 0;
    loop {
        let res = admin_client
            .describe_configs(&[broker], &opts)
            .await
            .expect("describe configs failed");
        let config = &res[0].as_ref().expect("describe configs failed");
        let entry = config.get("log.flush.interval.messages");
        let expected_entry = if get_broker_version() < KafkaVersion(1, 1, 0, 0) {
            // Pre-1.1, the AlterConfig operation will silently fail, and the
            // config will remain unchanged, which I guess is worth testing.
            ConfigEntry {
                name: "log.flush.interval.messages".into(),
                value: Some(orig_val.clone()),
                source: ConfigSource::Default,
                is_read_only: true,
                is_default: true,
                is_sensitive: false,
            }
        } else {
            ConfigEntry {
                name: "log.flush.interval.messages".into(),
                value: Some("1234".into()),
                source: ConfigSource::DynamicBroker,
                is_read_only: false,
                is_default: false,
                is_sensitive: false,
            }
        };
        if entry == Some(&expected_entry) {
            break;
        } else if tries >= 5 {
            panic!("{:?} != {:?}", entry, Some(&expected_entry));
        } else {
            tries += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    let config = AlterConfig::new(broker).set("log.flush.interval.ms", orig_val);
    let res = admin_client
        .alter_configs(&[config], &opts)
        .await
        .expect("alter configs failed");
    assert_eq!(res, &[Ok(OwnedResourceSpecifier::Broker(0))]);
}

#[tokio::test]
async fn test_groups() {
    let admin_client = create_admin_client();

    // Verify that a valid group can be deleted.
    {
        let group_name = rand_test_group();
        create_consumer_group(&group_name).await;
        let res = admin_client
            .delete_groups(&[&group_name], &AdminOptions::default())
            .await;
        assert_eq!(res, Ok(vec![Ok(group_name.to_string())]));
    }

    // Verify that attempting to delete an unknown group returns a "group not
    // found" error.
    {
        let unknown_group_name = rand_test_group();
        let res = admin_client
            .delete_groups(&[&unknown_group_name], &AdminOptions::default())
            .await;
        let expected: GroupResult = Err((unknown_group_name, RDKafkaErrorCode::GroupIdNotFound));
        assert_eq!(res, Ok(vec![expected]));
    }

    // Verify that deleting a valid and invalid group results in a mixed result
    // set.
    {
        let group_name = rand_test_group();
        let unknown_group_name = rand_test_group();
        create_consumer_group(&group_name).await;
        let res = admin_client
            .delete_groups(
                &[&group_name, &unknown_group_name],
                &AdminOptions::default(),
            )
            .await;
        assert_eq!(
            res,
            Ok(vec![
                Ok(group_name.to_string()),
                Err((
                    unknown_group_name.to_string(),
                    RDKafkaErrorCode::GroupIdNotFound
                ))
            ])
        );
    }
}

// Tests whether each admin operation properly reports an error if the entire
// request fails. The original implementations failed to check this, resulting
// in confusing situations where a failed admin request would return Ok([]).
#[tokio::test]
async fn test_event_errors() {
    // Configure an admin client to target a Kafka server that doesn't exist,
    // then set an impossible timeout. This will ensure that every request fails
    // with an OperationTimedOut error, assuming, of course, that the request
    // passes client-side validation.
    let admin_client = ClientConfig::new()
        .set("bootstrap.servers", "noexist")
        .create::<AdminClient<DefaultClientContext>>()
        .expect("admin client creation failed");
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_nanos(1)));

    let res = admin_client.create_topics(&[], &opts).await;
    assert_eq!(
        res,
        Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );

    let res = admin_client.create_partitions(&[], &opts).await;
    assert_eq!(
        res,
        Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );

    let res = admin_client.delete_topics(&[], &opts).await;
    assert_eq!(
        res,
        Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );

    let res = admin_client.describe_configs(&[], &opts).await;
    assert_eq!(
        res.err(),
        Some(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );

    let res = admin_client.alter_configs(&[], &opts).await;
    assert_eq!(
        res,
        Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );
}

// Test the list offsets request
#[tokio::test]
async fn test_list_offsets() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let name1 = rand_test_topic("test_list_offsets");
    let name2 = rand_test_topic("test_list_offsets");

    let make_record1 = || FutureRecord::<str, str>::to(&name1).payload("data");
    let make_record2 = || FutureRecord::<str, str>::to(&name2).payload("data");

    // Create a couple of topics
    create_topics(&vec![name1.as_str(), name2.as_str()])
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
        producer.send(make_record2(), timeout).await.unwrap();
    }

    let mut tpl = TopicPartitionList::with_capacity(2);
    tpl.add_partition_offset(&name1, 0, Offset::End)
        .expect("Adding topic partition element list failed");
    tpl.add_partition_offset(&name2, 0, Offset::End)
        .expect("Adding topic partition element list failed");

    let result = admin_client
        .list_offsets(&tpl, &opts)
        .await
        .expect("list offsets failed");
    eprintln!("result={:?}", result);

    assert!(result
        .iter()
        .any(|topic| list_offsets_result_key(topic) == (name1.as_str(), 0)));
    let topic_info1 = result
        .iter()
        .find(|topic| list_offsets_result_key(topic) == (name1.as_str(), 0))
        .unwrap();
    match &topic_info1 {
        Ok(topic_partition) => assert_eq!(topic_partition.offset, Offset::Offset(5)),
        Err(_) => assert!(false, "List offsets returned error"),
    }

    assert!(result
        .iter()
        .any(|topic| list_offsets_result_key(topic) == (name2.as_str(), 0)));
    let topic_info2 = result
        .iter()
        .find(|topic| list_offsets_result_key(topic) == (name2.as_str(), 0))
        .unwrap();
    match &topic_info2 {
        Ok(topic_partition) => assert_eq!(topic_partition.offset, Offset::Offset(5)),
        Err(_) => assert!(false, "List offsets returned error"),
    }
}

// Test the list offsets request. Case where one of the requested topics does
// not exist.
#[tokio::test]
async fn test_list_offsets_one_does_not_exist() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let name1 = rand_test_topic("test_list_offsets_one_does_not_exist");
    let name2 = rand_test_topic("test_list_offsets_one_does_not_exist");

    let make_record1 = || FutureRecord::<str, str>::to(&name1).payload("data");

    // Create a couple of topics
    create_topics(&vec![name1.as_str()])
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
    }

    let mut tpl = TopicPartitionList::with_capacity(2);
    tpl.add_partition_offset(&name1, 0, Offset::End)
        .expect("Adding topic partition element list failed");
    tpl.add_partition_offset(&name2, 0, Offset::End)
        .expect("Adding topic partition element list failed");

    let result = admin_client
        .list_offsets(&tpl, &opts)
        .await
        .expect("list offsets failed");
    eprintln!("result={:?}", result);

    assert!(result
        .iter()
        .any(|topic| list_offsets_result_key(topic) == (name1.as_str(), 0)));
    let topic_info1 = result
        .iter()
        .find(|topic| list_offsets_result_key(topic) == (name1.as_str(), 0))
        .unwrap();
    match &topic_info1 {
        Ok(topic_partition) => assert_eq!(topic_partition.offset, Offset::Offset(5)),
        Err(_) => assert!(false, "List offsets returned error"),
    }

    assert!(result
        .iter()
        .any(|topic| list_offsets_result_key(topic) == (name2.as_str(), 0)));
    let topic_info2 = result
        .iter()
        .find(|topic| list_offsets_result_key(topic) == (name2.as_str(), 0))
        .unwrap();
    match &topic_info2 {
        Ok(_) => assert!(false, "List offsets was expected to return error"),
        Err(_) => {}
    }
}

// Test the list offsets request
// Check the case where the list provided is empty.
#[tokio::test]
async fn test_list_offsets_empty_list() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let name1 = rand_test_topic("test_list_offsets_empty_list");

    let make_record1 = || FutureRecord::<str, str>::to(&name1).payload("data");

    // Create a couple of topics
    create_topics(&vec![name1.as_str()])
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
    }

    let tpl = TopicPartitionList::new();

    let result = admin_client
        .list_offsets(&tpl, &opts)
        .await
        .expect("list offsets failed");
    eprintln!("result={:?}", result);

    assert_eq!(result.len(), 0);
}

// Test the the describe consumer groups request
#[tokio::test]
async fn test_describe_groups() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();

    let topic1 = rand_test_topic("test_describe_groups");
    let topic2 = rand_test_topic("test_describe_groups");
    let topic_list = vec![topic1.as_str(), topic2.as_str()];
    let group_list = vec![group_name1.as_str()];

    let make_record1 = || FutureRecord::<str, str>::to(&topic1).payload("data");
    let make_record2 = || FutureRecord::<str, str>::to(&topic2).payload("data");

    // Create the topics
    create_topics(&topic_list)
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
        producer.send(make_record2(), timeout).await.unwrap();
    }

    {
        let consumer: BaseConsumer = create_config()
            .set("group.id", group_name1.as_str())
            .set("auto.offset.reset", "earliest")
            .set("auto.commit.enable", "true")
            .create()
            .expect("create consumer failed");

        consumer
            .subscribe(&topic_list)
            .expect("subscribe topic failed");

        // Consume some messages
        for message in consumer.iter().take(3) {
            match message {
                Ok(_) => (),
                Err(e) => panic!("Error receiving message: {:?}", e),
            }
        }

        // Get the description.
        let result = admin_client
            .describe_consumer_groups(&group_list, &opts)
            .await
            .expect("describe_consumer_groups failed");

        eprintln!("result: {:?}", result);
        assert!(result
            .iter()
            .any(|description| group_description_result_key(description) == group_name1));
        let group_description = result
            .iter()
            .find(|description| group_description_result_key(description) == group_name1)
            .expect("Did not find the group we requested");
        match group_description {
            Ok(group_description) => assert_eq!(group_description.members.len(), 1),
            Err(_) => assert!(false, "Failed describe consumer group"),
        }
    }

    // Get the description again when the consumer has been recycled.

    let result = admin_client
        .describe_consumer_groups(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");

    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|description| group_description_result_key(description) == group_name1));
    let group_description = result
        .iter()
        .find(|description| group_description_result_key(description) == group_name1)
        .expect("Did not find the group we requested");

    match group_description {
        Ok(group_description) => assert_eq!(group_description.members.len(), 0),
        Err(_) => assert!(false, "Failed describe consumer group"),
    }
}

// Test the describe_groups operation.
// Request more than one group.
#[tokio::test]
async fn test_describe_groups_more_than_one_group() {
    // create the admin client.
    let admin_client = create_admin_client();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();
    let group_name2 = rand_test_group();

    create_consumer_group(&group_name1).await;
    create_consumer_group(&group_name2).await;

    let group_list = vec![group_name1.as_str(), group_name2.as_str()];

    let result = admin_client
        .describe_consumer_groups(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");
    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|description| group_description_result_key(description) == group_name1));
    assert!(result
        .iter()
        .any(|description| group_description_result_key(description) == group_name2));
}

// Test the describe_consumer_groups operation.
// Request more than one group, but one of them does not exist.
#[tokio::test]
async fn test_describe_groups_one_does_not_exist() {
    // create the admin client.
    let admin_client = create_admin_client();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();
    let group_name2 = rand_test_group();

    create_consumer_group(&group_name1).await;

    let group_list = vec![group_name1.as_str(), group_name2.as_str()];

    let result = admin_client
        .describe_consumer_groups(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");
    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|description| if let Ok(description) = description {
            description.group_id == group_name1 && description.state == ConsumerGroupState::Empty
        } else {
            false
        }));
    assert!(result
        .iter()
        .any(|description| if let Ok(description) = description {
            description.group_id == group_name2 && description.state == ConsumerGroupState::Dead
        } else {
            false
        }));
}

// Test the describe_consumer_groups operation.
// The request is empty.
#[tokio::test]
async fn test_describe_groups_with_empty_array() {
    // create the admin client.
    let admin_client = create_admin_client();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();

    create_consumer_group(&group_name1).await;

    let group_list = vec![];

    let result = admin_client
        .describe_consumer_groups(&group_list, &opts)
        .await;
    assert!(result.is_err())
}


// Test the list_consumer_group_offsets operation
#[tokio::test]
async fn test_list_consumer_group_offsets() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();

    let topic1 = rand_test_topic("test_describe_groups");
    let topic2 = rand_test_topic("test_describe_groups");
    let topic_list = vec![topic1.as_str(), topic2.as_str()];

    let make_record1 = || FutureRecord::<str, str>::to(&topic1).payload("data");
    let make_record2 = || FutureRecord::<str, str>::to(&topic2).payload("data");

    let group_list = vec![ListConsumerGroupOffsets::from_group(group_name1.as_str())];

    // Create the topics
    create_topics(&topic_list)
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
        producer.send(make_record2(), timeout).await.unwrap();
    }

    {
        let consumer: BaseConsumer = create_config()
            .set("group.id", group_name1.as_str())
            .set("auto.offset.reset", "earliest")
            .set("auto.commit.enable", "true")
            .create()
            .expect("create consumer failed");

        consumer
            .subscribe(&topic_list)
            .expect("subscribe topic failed");

        // Consume some messages
        for message in consumer.iter().take(6) {
            match message {
                Ok(_) => (),
                Err(e) => panic!("Error receiving message: {:?}", e),
            }
        }
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();

        // Get the offsets.
        let result = admin_client
            .list_consumer_group_offsets(&group_list, &opts)
            .await
            .expect("describe_consumer_groups failed");

        eprintln!("result: {:?}", result);
        assert!(result
            .iter()
            .any(|group_result| group_result_key(group_result) == group_name1));
        let group_result = result
            .iter()
            .find(|group_result| group_result_key(group_result) == group_name1)
            .expect("Did not find the group we requested");
        match group_result {
            Ok(group) => {
                assert_eq!(group.group_id, group_name1);
                assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic1));
                assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic2));
            }
            Err(_) => assert!(false, "Failed describe consumer group"),
        }

    }

    // Get the offsets.
    let result = admin_client
        .list_consumer_group_offsets(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");

    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|group_result| group_result_key(group_result) == group_name1));
    let group_result = result
        .iter()
        .find(|group_result| group_result_key(group_result) == group_name1)
        .expect("Did not find the group we requested");
    match group_result {
        Ok(group) => {
            assert_eq!(group.group_id, group_name1);
            assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic1));
            assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic2));
        }
        Err(_) => assert!(false, "Failed describe consumer group"),
    }

}

// Test the list_consumer_group_offsets operation
// The consumer does not have any topics assigned.
#[tokio::test]
async fn test_list_consumer_group_offsets_no_topics() {
    // create the admin client.
    let admin_client = create_admin_client();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();


    let group_list = vec![ListConsumerGroupOffsets::from_group(group_name1.as_str())];

    // Get the offsets.
    let result = admin_client
        .list_consumer_group_offsets(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");

    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|group_result| group_result_key(group_result) == group_name1));
    let group_result = result
        .iter()
        .find(|group_result| group_result_key(group_result) == group_name1)
        .expect("Did not find the group we requested");
    match group_result {
        Ok(group) => {
            assert_eq!(group.group_id, group_name1);
            assert_eq!(group.topic_partitions.elements().len(), 0);
        }
        Err(_) => assert!(false, "Failed describe consumer group"),
    }
}

// Test the list_consumer_group_offsets operation
// Request information for more than one group.
#[tokio::test]
async fn test_list_consumer_group_offsets_more_than_one_group() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();
    let group_name2 = rand_test_group();


    let group_list = vec![
        ListConsumerGroupOffsets::from_group(group_name1.as_str()),
        ListConsumerGroupOffsets::from_group(group_name2.as_str()),
    ];

    // Get the offsets.
    let result = admin_client
        .list_consumer_group_offsets(&group_list, &opts)
        .await;

    // Fails because the API only supports one group at a time.
    assert!(result.is_err());

}

// Test the list_consumer_group_offsets operation
// Request information on a specific topic and partition.
#[tokio::test]
async fn test_list_consumer_group_offsets_one_existing_partition() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();

    let topic1 = rand_test_topic("test_describe_groups");
    let topic2 = rand_test_topic("test_describe_groups");
    let topic_list = vec![topic1.as_str(), topic2.as_str()];

    let make_record1 = || FutureRecord::<str, str>::to(&topic1).payload("data");
    let make_record2 = || FutureRecord::<str, str>::to(&topic2).payload("data");

    let mut request_tpl = TopicPartitionList::new();
    request_tpl.add_partition(topic1.as_str(), 0);

    let group_list = vec![ListConsumerGroupOffsets::new(group_name1.as_str(), request_tpl)];

    // Create the topics
    create_topics(&topic_list)
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
        producer.send(make_record2(), timeout).await.unwrap();
    }

    {
        let consumer: BaseConsumer = create_config()
            .set("group.id", group_name1.as_str())
            .set("auto.offset.reset", "earliest")
            .set("auto.commit.enable", "true")
            .create()
            .expect("create consumer failed");

        consumer
            .subscribe(&topic_list)
            .expect("subscribe topic failed");

        // Consume some messages
        for message in consumer.iter().take(6) {
            match message {
                Ok(_) => (),
                Err(e) => panic!("Error receiving message: {:?}", e),
            }
        }
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();

        // Get the offsets.
        let result = admin_client
            .list_consumer_group_offsets(&group_list, &opts)
            .await
            .expect("describe_consumer_groups failed");

        eprintln!("result: {:?}", result);
        assert!(result
            .iter()
            .any(|group_result| group_result_key(group_result) == group_name1));
        let group_result = result
            .iter()
            .find(|group_result| group_result_key(group_result) == group_name1)
            .expect("Did not find the group we requested");
        match group_result {
            Ok(group) => {
                assert_eq!(group.group_id, group_name1);
                assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic1));
                assert_eq!(group.topic_partitions.elements().len(), 1);
            }
            Err(_) => assert!(false, "Failed describe consumer group"),
        }

    }

    // Get the offsets.
    let result = admin_client
        .list_consumer_group_offsets(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");

    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|group_result| group_result_key(group_result) == group_name1));
    let group_result = result
        .iter()
        .find(|group_result| group_result_key(group_result) == group_name1)
        .expect("Did not find the group we requested");
    match group_result {
        Ok(group) => {
            assert_eq!(group.group_id, group_name1);
            assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic1));
            assert_eq!(group.topic_partitions.elements().len(), 1);
        }
        Err(_) => assert!(false, "Failed describe consumer group"),
    }

}

// Test the list_consumer_group_offsets operation
// Request information on a specific topic and partition, but the
// topic does not have that partition.
#[tokio::test]
async fn test_list_consumer_group_offsets_one_non_existing_partition() {
    // create the admin client.
    let admin_client = create_admin_client();
    // create a producer
    let producer = create_config().create::<FutureProducer<_>>().unwrap();
    let timeout = Some(Duration::from_secs(1));

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let group_name1 = rand_test_group();

    let topic1 = rand_test_topic("test_describe_groups");
    let topic2 = rand_test_topic("test_describe_groups");
    let topic_list = vec![topic1.as_str(), topic2.as_str()];

    let make_record1 = || FutureRecord::<str, str>::to(&topic1).payload("data");
    let make_record2 = || FutureRecord::<str, str>::to(&topic2).payload("data");

    let mut request_tpl = TopicPartitionList::new();
    request_tpl.add_partition(topic1.as_str(), 10);

    let group_list = vec![ListConsumerGroupOffsets::new(group_name1.as_str(), request_tpl)];

    // Create the topics
    create_topics(&topic_list)
        .await
        .expect("Failed creating topics");

    // Produce five messages to the topic.
    for _ in 0..5 {
        producer.send(make_record1(), timeout).await.unwrap();
        producer.send(make_record2(), timeout).await.unwrap();
    }

    {
        let consumer: BaseConsumer = create_config()
            .set("group.id", group_name1.as_str())
            .set("auto.offset.reset", "earliest")
            .set("auto.commit.enable", "true")
            .create()
            .expect("create consumer failed");

        consumer
            .subscribe(&topic_list)
            .expect("subscribe topic failed");

        // Consume some messages
        for message in consumer.iter().take(6) {
            match message {
                Ok(_) => (),
                Err(e) => panic!("Error receiving message: {:?}", e),
            }
        }
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();

        // Get the offsets.
        let result = admin_client
            .list_consumer_group_offsets(&group_list, &opts)
            .await
            .expect("describe_consumer_groups failed");

        eprintln!("result: {:?}", result);
        assert!(result
            .iter()
            .any(|group_result| group_result_key(group_result) == group_name1));
        let group_result = result
            .iter()
            .find(|group_result| group_result_key(group_result) == group_name1)
            .expect("Did not find the group we requested");
        match group_result {
            Ok(group) => {
                assert_eq!(group.group_id, group_name1);
                let elements = group.topic_partitions.elements();
                //
                // For an inexistent partition, the API returns success with an invalid offset.
                //
                assert!(elements.iter().any(|tp| tp.topic() == topic1));
                assert_eq!(elements.len(), 1);
                let element = elements.iter().find(|tp| tp.topic() == topic1 && tp.partition()==10).unwrap();
                assert_eq!(element.offset(), Offset::Invalid);
            }
            Err(_) => assert!(false, "Failed describe consumer group"),
        }

    }

    // Get the offsets.
    let result = admin_client
        .list_consumer_group_offsets(&group_list, &opts)
        .await
        .expect("describe_consumer_groups failed");

    eprintln!("result: {:?}", result);
    assert!(result
        .iter()
        .any(|group_result| group_result_key(group_result) == group_name1));
    let group_result = result
        .iter()
        .find(|group_result| group_result_key(group_result) == group_name1)
        .expect("Did not find the group we requested");
    match group_result {
        Ok(group) => {
            assert_eq!(group.group_id, group_name1);
            assert!(group.topic_partitions.elements().iter().any(|tp| tp.topic() == topic1));
            assert_eq!(group.topic_partitions.elements().len(), 1);
        }
        Err(_) => assert!(false, "Failed describe consumer group"),
    }

}

