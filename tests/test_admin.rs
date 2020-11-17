//! Test administrative commands using the admin API.

use std::time::Duration;

use backoff::{ExponentialBackoff, Operation};

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, NewPartitions, NewTopic,
    OwnedResourceSpecifier, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;

use crate::utils::*;

mod utils;

fn create_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config
}

fn create_admin_client() -> AdminClient<DefaultClientContext> {
    create_config()
        .create()
        .expect("admin client creation failed")
}

fn fetch_metadata(topic: &str) -> Metadata {
    let consumer: BaseConsumer<DefaultConsumerContext> =
        create_config().create().expect("consumer creation failed");
    let timeout = Some(Duration::from_secs(1));

    let mut backoff = ExponentialBackoff::default();
    backoff.max_elapsed_time = Some(Duration::from_secs(5));
    (|| {
        let metadata = consumer
            .fetch_metadata(Some(topic), timeout)
            .map_err(|e| e.to_string())?;
        if metadata.topics().len() == 0 {
            Err("metadata fetch returned no topics".to_string())?
        }
        let topic = &metadata.topics()[0];
        if topic.partitions().len() == 0 {
            Err("metadata fetch returned a topic with no partitions".to_string())?
        }
        Ok(metadata)
    })
    .retry(&mut backoff)
    .unwrap()
}

fn verify_delete(topic: &str) {
    let consumer: BaseConsumer<DefaultConsumerContext> =
        create_config().create().expect("consumer creation failed");
    let timeout = Some(Duration::from_secs(1));

    let mut backoff = ExponentialBackoff::default();
    backoff.max_elapsed_time = Some(Duration::from_secs(5));
    (|| {
        // Asking about the topic specifically will recreate it (under the
        // default Kafka configuration, at least) so we have to ask for the list
        // of all topics and search through it.
        let metadata = consumer
            .fetch_metadata(None, timeout)
            .map_err(|e| e.to_string())?;
        if let Some(_) = metadata.topics().iter().find(|t| t.name() == topic) {
            Err(format!("topic {} still exists", topic))?
        }
        Ok(())
    })
    .retry(&mut backoff)
    .unwrap()
}

#[tokio::test]
async fn test_topics() {
    let admin_client = create_admin_client();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(1)));

    // Verify that topics are created as specified, and that they can later
    // be deleted.
    {
        let name1 = rand_test_topic();
        let name2 = rand_test_topic();

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
        let expected_entry2 = ConfigEntry {
            name: "max.message.bytes".into(),
            value: Some("1000012".into()),
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
        let name = rand_test_topic();
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
        let name = rand_test_topic();
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
        let name1 = rand_test_topic();
        let name2 = rand_test_topic();

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

    let config = AlterConfig::new(broker).set("log.flush.interval.ms", &orig_val);
    let res = admin_client
        .alter_configs(&[config], &opts)
        .await
        .expect("alter configs failed");
    assert_eq!(res, &[Ok(OwnedResourceSpecifier::Broker(0))]);
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
