//! Test administrative commands using the admin API.

use std::collections::HashSet;
use std::time::Duration;

use backon::{BlockingRetryable, ExponentialBuilder};

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, GroupResult,
    ListConsumerGroupOffsets, NewPartitions, NewTopic, OwnedResourceSpecifier, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

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

async fn create_consumer_group(consumer_group_name: &str) {
    let admin_client = create_admin_client();
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
async fn test_list_consumer_group_offsets() {
    let admin_client = create_admin_client();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Test 1: Fetch offsets for an existing consumer group
    {
        let group = rand_test_group();
        create_consumer_group(&group).await;

        let results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group)], &opts)
            .await
            .expect("list consumer group offsets failed");

        assert_eq!(results.len(), 1);
        match &results[0] {
            Ok((name, tpl)) => {
                assert_eq!(name, &group);
                assert!(tpl.count() > 0, "expected at least one partition");
                for elem in tpl.elements() {
                    elem.error().expect("partition should not have an error");
                    // Verify we can access topic, partition, and offset
                    assert!(!elem.topic().is_empty(), "topic name should not be empty");
                    assert!(elem.partition() >= 0, "partition should be non-negative");
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }
    }

    // Test 2: Fetch offsets for a non-existent group should return an error
    {
        let unknown_group = rand_test_group();
        let results = admin_client
            .list_consumer_group_offsets(
                &[ListConsumerGroupOffsets::new(&unknown_group)],
                &opts,
            )
            .await
            .expect("list consumer group offsets failed");

        assert_eq!(results.len(), 1);
        match &results[0] {
            Ok((name, _)) => {
                panic!("expected error for unknown group {name}, but got success");
            }
            Err((name, err)) => {
                assert_eq!(name, &unknown_group);
                assert_eq!(*err, RDKafkaErrorCode::GroupIdNotFound);
            }
        }
    }

    // Test 3: Fetch offsets with partition filtering
    {
        let group = rand_test_group();
        create_consumer_group(&group).await;

        // Create a topic partition list for filtering
        let mut filter_tpl = TopicPartitionList::new();
        // Note: We need to know the topic name from create_consumer_group
        // For this test, we'll fetch all offsets first, then filter
        let all_results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group)], &opts)
            .await
            .expect("list consumer group offsets failed");

        if let Ok((_, tpl)) = &all_results[0] {
            if tpl.count() > 0 {
                let first_elem = &tpl.elements()[0];
                filter_tpl.add_partition(first_elem.topic(), first_elem.partition());

                let filtered_results = admin_client
                    .list_consumer_group_offsets(
                        &[ListConsumerGroupOffsets::new(&group).partitions(&filter_tpl)],
                        &opts,
                    )
                    .await
                    .expect("list consumer group offsets failed");

                assert_eq!(filtered_results.len(), 1);
                match &filtered_results[0] {
                    Ok((name, filtered_tpl)) => {
                        assert_eq!(name, &group);
                        assert_eq!(
                            filtered_tpl.count(),
                            1,
                            "filtered result should contain exactly one partition"
                        );
                        let elem = &filtered_tpl.elements()[0];
                        assert_eq!(elem.topic(), first_elem.topic());
                        assert_eq!(elem.partition(), first_elem.partition());
                    }
                    Err((name, err)) => {
                        panic!("failed to fetch filtered offsets for {name}: {err:?}");
                    }
                }
            }
        }
    }

    // Test 4: Fetch offsets for multiple groups at once
    {
        let group1 = rand_test_group();
        let group2 = rand_test_group();
        create_consumer_group(&group1).await;
        create_consumer_group(&group2).await;

        let results = admin_client
            .list_consumer_group_offsets(
                &[
                    ListConsumerGroupOffsets::new(&group1),
                    ListConsumerGroupOffsets::new(&group2),
                ],
                &opts,
            )
            .await
            .expect("list consumer group offsets failed");

        assert_eq!(results.len(), 2);
        for result in &results {
            match result {
                Ok((name, tpl)) => {
                    assert!(
                        name == &group1 || name == &group2,
                        "group name should match one of the requested groups"
                    );
                    assert!(tpl.count() > 0, "expected at least one partition");
                }
                Err((name, err)) => {
                    panic!("failed to fetch offsets for {name}: {err:?}");
                }
            }
        }
    }

    // Test 5: Verify offset values are valid (not Invalid)
    {
        let group = rand_test_group();
        create_consumer_group(&group).await;

        let results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group)], &opts)
            .await
            .expect("list consumer group offsets failed");

        match &results[0] {
            Ok((_, tpl)) => {
                for elem in tpl.elements() {
                    let offset = elem.offset();
                    // The offset should not be Invalid for a committed offset
                    assert_ne!(
                        offset,
                        Offset::Invalid,
                        "offset should not be Invalid for committed offsets"
                    );
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }
    }
}

#[tokio::test]
async fn test_list_consumer_group_offsets_with_commits() {
    let admin_client = create_admin_client();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Create a topic with multiple partitions
    let topic_name = rand_test_topic("test_commits_and_fetch");
    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 3,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &opts,
        )
        .await
        .expect("topic creation failed");

    // Wait for topic to be available
    let consumer_for_metadata: BaseConsumer = create_config()
        .create()
        .expect("create consumer failed");
    (|| {
        consumer_for_metadata
            .fetch_metadata(Some(&topic_name), Duration::from_secs(3))
            .map_err(|e| e.to_string())
    })
    .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
    .call()
    .unwrap();

    // Test 1: Commit offsets for a single group and verify they can be fetched
    {
        let group1 = rand_test_group();
        let consumer1: BaseConsumer = create_config()
            .set("group.id", &group1)
            .create()
            .expect("create consumer failed");

        // Assign all partitions
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(&topic_name, 0);
        assignment.add_partition(&topic_name, 1);
        assignment.add_partition(&topic_name, 2);
        consumer1
            .assign(&assignment)
            .expect("assign failed");

        // Commit specific offsets for each partition
        consumer1
            .store_offset(&topic_name, 0, 100)
            .expect("store offset failed");
        consumer1
            .store_offset(&topic_name, 1, 200)
            .expect("store offset failed");
        consumer1
            .store_offset(&topic_name, 2, 300)
            .expect("store offset failed");

        // Commit the offsets
        consumer1
            .commit_consumer_state(CommitMode::Sync)
            .expect("commit failed");

        // Fetch the offsets using the admin API
        let results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group1)], &opts)
            .await
            .expect("list consumer group offsets failed");

        assert_eq!(results.len(), 1);
        match &results[0] {
            Ok((name, tpl)) => {
                assert_eq!(name, &group1);
                assert_eq!(tpl.count(), 3, "expected 3 partitions");

                // Verify each partition has the correct offset
                let mut found_partitions = HashSet::new();
                for elem in tpl.elements() {
                    assert_eq!(elem.topic(), topic_name);
                    elem.error().expect("partition should not have an error");
                    found_partitions.insert(elem.partition());

                    match elem.offset() {
                        Offset::Offset(offset) => {
                            match elem.partition() {
                                0 => assert_eq!(offset, 100, "partition 0 should have offset 100"),
                                1 => assert_eq!(offset, 200, "partition 1 should have offset 200"),
                                2 => assert_eq!(offset, 300, "partition 2 should have offset 300"),
                                _ => panic!("unexpected partition {}", elem.partition()),
                            }
                        }
                        other => panic!(
                            "expected Offset::Offset, got {:?} for partition {}",
                            other,
                            elem.partition()
                        ),
                    }
                }
                assert_eq!(
                    found_partitions.len(),
                    3,
                    "should have found all 3 partitions"
                );
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }
    }

    // Test 2: Commit offsets for multiple groups and verify all can be fetched
    {
        let group2 = rand_test_group();
        let group3 = rand_test_group();

        // Commit offsets for group2
        let consumer2: BaseConsumer = create_config()
            .set("group.id", &group2)
            .create()
            .expect("create consumer failed");
        let mut assignment2 = TopicPartitionList::new();
        assignment2.add_partition(&topic_name, 0);
        assignment2.add_partition(&topic_name, 1);
        consumer2.assign(&assignment2).expect("assign failed");
        consumer2
            .store_offset(&topic_name, 0, 500)
            .expect("store offset failed");
        consumer2
            .store_offset(&topic_name, 1, 600)
            .expect("store offset failed");
        consumer2
            .commit_consumer_state(CommitMode::Sync)
            .expect("commit failed");

        // Commit offsets for group3
        let consumer3: BaseConsumer = create_config()
            .set("group.id", &group3)
            .create()
            .expect("create consumer failed");
        let mut assignment3 = TopicPartitionList::new();
        assignment3.add_partition(&topic_name, 2);
        consumer3.assign(&assignment3).expect("assign failed");
        consumer3
            .store_offset(&topic_name, 2, 700)
            .expect("store offset failed");
        consumer3
            .commit_consumer_state(CommitMode::Sync)
            .expect("commit failed");

        // Fetch offsets for both groups
        let results = admin_client
            .list_consumer_group_offsets(
                &[
                    ListConsumerGroupOffsets::new(&group2),
                    ListConsumerGroupOffsets::new(&group3),
                ],
                &opts,
            )
            .await
            .expect("list consumer group offsets failed");

        assert_eq!(results.len(), 2);

        // Verify group2 offsets
        let group2_result = results
            .iter()
            .find(|r| match r {
                Ok((name, _)) => name == &group2,
                Err((name, _)) => name == &group2,
            })
            .expect("should find group2 result");
        match group2_result {
            Ok((name, tpl)) => {
                assert_eq!(name, &group2);
                assert_eq!(tpl.count(), 2, "group2 should have 2 partitions");
                for elem in tpl.elements() {
                    match elem.partition() {
                        0 => {
                            if let Offset::Offset(offset) = elem.offset() {
                                assert_eq!(offset, 500, "group2 partition 0 should have offset 500");
                            } else {
                                panic!("expected Offset::Offset for group2 partition 0");
                            }
                        }
                        1 => {
                            if let Offset::Offset(offset) = elem.offset() {
                                assert_eq!(offset, 600, "group2 partition 1 should have offset 600");
                            } else {
                                panic!("expected Offset::Offset for group2 partition 1");
                            }
                        }
                        _ => panic!("unexpected partition in group2"),
                    }
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }

        // Verify group3 offsets
        let group3_result = results
            .iter()
            .find(|r| match r {
                Ok((name, _)) => name == &group3,
                Err((name, _)) => name == &group3,
            })
            .expect("should find group3 result");
        match group3_result {
            Ok((name, tpl)) => {
                assert_eq!(name, &group3);
                assert_eq!(tpl.count(), 1, "group3 should have 1 partition");
                let elem = &tpl.elements()[0];
                assert_eq!(elem.partition(), 2);
                if let Offset::Offset(offset) = elem.offset() {
                    assert_eq!(offset, 700, "group3 partition 2 should have offset 700");
                } else {
                    panic!("expected Offset::Offset for group3 partition 2");
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }
    }

    // Test 3: Update committed offsets and verify the new values are fetched
    {
        let group4 = rand_test_group();
        let consumer4: BaseConsumer = create_config()
            .set("group.id", &group4)
            .create()
            .expect("create consumer failed");

        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(&topic_name, 0);
        consumer4.assign(&assignment).expect("assign failed");

        // Commit initial offset
        consumer4
            .store_offset(&topic_name, 0, 1000)
            .expect("store offset failed");
        consumer4
            .commit_consumer_state(CommitMode::Sync)
            .expect("commit failed");

        // Verify initial offset
        let results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group4)], &opts)
            .await
            .expect("list consumer group offsets failed");
        match &results[0] {
            Ok((_, tpl)) => {
                let elem = &tpl.elements()[0];
                if let Offset::Offset(offset) = elem.offset() {
                    assert_eq!(offset, 1000, "initial offset should be 1000");
                } else {
                    panic!("expected Offset::Offset");
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }

        // Update the offset
        consumer4
            .store_offset(&topic_name, 0, 2000)
            .expect("store offset failed");
        consumer4
            .commit_consumer_state(CommitMode::Sync)
            .expect("commit failed");

        // Verify updated offset
        let results = admin_client
            .list_consumer_group_offsets(&[ListConsumerGroupOffsets::new(&group4)], &opts)
            .await
            .expect("list consumer group offsets failed");
        match &results[0] {
            Ok((_, tpl)) => {
                let elem = &tpl.elements()[0];
                if let Offset::Offset(offset) = elem.offset() {
                    assert_eq!(offset, 2000, "updated offset should be 2000");
                } else {
                    panic!("expected Offset::Offset");
                }
            }
            Err((name, err)) => {
                panic!("failed to fetch offsets for {name}: {err:?}");
            }
        }
    }
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
