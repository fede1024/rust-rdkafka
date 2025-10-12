use crate::utils::admin::create_topic;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::rand_test_topic;
use crate::utils::{get_broker_version, KafkaVersion};
use rdkafka::admin::{
    AdminOptions, ConfigEntry, ConfigSource, NewPartitions, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::error::KafkaError;
use std::time::Duration;
use rdkafka_sys::RDKafkaErrorCode;

#[path = "utils/mod.rs"]
mod utils;

#[tokio::test]
pub async fn test_topic_creation() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };
    let test_topic_name = rand_test_topic("testing-topic");

    let admin_client_result =
        utils::admin::create_admin_client(&kafka_context.bootstrap_servers).await;
    let Ok(admin_client) = admin_client_result else {
        panic!(
            "could not create admin client: {}",
            admin_client_result.unwrap_err()
        );
    };

    let create_topic_result = create_topic(&admin_client, &test_topic_name).await;
    if create_topic_result.is_err() {
        panic!(
            "could not create topic: {}",
            create_topic_result.unwrap_err()
        );
    };
}

#[tokio::test]
async fn test_topics() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Create consumer client
    let consumer_client =
        utils::consumer::create_unsubscribed_base_consumer(&kafka_context.bootstrap_servers)
            .await
            .expect("could not create consumer client");

    //
    // // Verify that deleting a non-existent topic fails.
    // {
    //     let name = rand_test_topic("test_topics");
    //     let res = admin_client
    //         .delete_topics(&[&name], &opts)
    //         .await
    //         .expect("delete topics failed");
    //     assert_eq!(
    //         res,
    //         &[Err((name, RDKafkaErrorCode::UnknownTopicOrPartition))]
    //     );
    // }
    //
    // // Verify that mixed-success operations properly report the successful and
    // // failing operators.
    // {
    //     let name1 = rand_test_topic("test_topics");
    //     let name2 = rand_test_topic("test_topics");
    //
    //     let topic1 = NewTopic::new(&name1, 1, TopicReplication::Fixed(1));
    //     let topic2 = NewTopic::new(&name2, 1, TopicReplication::Fixed(1));
    //
    //     let res = admin_client
    //         .create_topics(vec![&topic1], &opts)
    //         .await
    //         .expect("topic creation failed");
    //     assert_eq!(res, &[Ok(name1.clone())]);
    //     let _ = fetch_metadata(&name1);
    //
    //     let res = admin_client
    //         .create_topics(vec![&topic1, &topic2], &opts)
    //         .await
    //         .expect("topic creation failed");
    //     assert_eq!(
    //         res,
    //         &[
    //             Err((name1.clone(), RDKafkaErrorCode::TopicAlreadyExists)),
    //             Ok(name2.clone())
    //         ]
    //     );
    //     let _ = fetch_metadata(&name2);
    //
    //     let res = admin_client
    //         .delete_topics(&[&name1], &opts)
    //         .await
    //         .expect("topic deletion failed");
    //     assert_eq!(res, &[Ok(name1.clone())]);
    //     verify_delete(&name1);
    //
    //     let res = admin_client
    //         .delete_topics(&[&name2, &name1], &opts)
    //         .await
    //         .expect("topic deletion failed");
    //     assert_eq!(
    //         res,
    //         &[
    //             Ok(name2.clone()),
    //             Err((name1.clone(), RDKafkaErrorCode::UnknownTopicOrPartition))
    //         ]
    //     );
    // }
}

#[tokio::test]
pub async fn test_topic_create_and_delete() {
    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Create consumer client
    let consumer_client =
        utils::consumer::create_unsubscribed_base_consumer(&kafka_context.bootstrap_servers)
            .await
            .expect("could not create consumer client");

    let topic_name_1 = rand_test_topic("test_topics");
    let topic_name_2 = rand_test_topic("test_topics");
    let topic1 = NewTopic::new(&topic_name_1, 1, TopicReplication::Fixed(1))
        .set("max.message.bytes", "1234");
    let topic2 = NewTopic {
        name: &topic_name_2,
        num_partitions: 3,
        replication: TopicReplication::Variable(&[
            &[utils::BROKER_ID],
            &[utils::BROKER_ID],
            &[utils::BROKER_ID],
        ]),
        config: Vec::new(),
    };

    // Topics created
    let topic_results = admin_client
        .create_topics(&[topic1, topic2], &opts)
        .await
        .expect("topic creation failed");
    assert_eq!(
        topic_results,
        &[Ok(topic_name_1.clone()), Ok(topic_name_2.clone())]
    );

    // Verify metadata
    let metadata1 = utils::consumer::fetch_consumer_metadata(&consumer_client, &topic_name_1)
        .expect(&format!("failed to fetch metadata for {}", &topic_name_1));
    let metadata2 = utils::consumer::fetch_consumer_metadata(&consumer_client, &topic_name_2)
        .expect(&format!("failed to fetch metadata for {}", topic_name_2));
    assert_eq!(1, metadata1.topics().len());
    assert_eq!(1, metadata2.topics().len());
    let metadata_topic1 = &metadata1.topics()[0];
    let metadata_topic2 = &metadata2.topics()[0];
    assert_eq!(&topic_name_1, metadata_topic1.name());
    assert_eq!(&topic_name_2, metadata_topic2.name());
    assert_eq!(1, metadata_topic1.partitions().len());
    assert_eq!(3, metadata_topic2.partitions().len());

    // Verifying topic configurations
    let config_resource_results = admin_client
        .describe_configs(
            &[
                ResourceSpecifier::Topic(&topic_name_1),
                ResourceSpecifier::Topic(&topic_name_2),
            ],
            &opts,
        )
        .await
        .expect("could not describe configs");
    let topic_config1 = &config_resource_results[0]
        .as_ref()
        .expect(&format!("failed to describe config for {}", &topic_name_1));
    let topic_config2 = &config_resource_results[1]
        .as_ref()
        .expect(&format!("failed to describe config for {}", &topic_name_2));
    let mut expected_entry1 = ConfigEntry {
        name: "max.message.bytes".into(),
        value: Some("1234".into()),
        source: ConfigSource::DynamicTopic,
        is_read_only: false,
        is_default: false,
        is_sensitive: false,
    };
    let default_max_msg_bytes = if get_broker_version(&kafka_context) <= KafkaVersion(2, 3, 0, 0) {
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
    if get_broker_version(&kafka_context) < KafkaVersion(1, 1, 0, 0) {
        expected_entry1.source = ConfigSource::Unknown;
    }
    assert_eq!(
        Some(&expected_entry1),
        topic_config1.get("max.message.bytes")
    );
    assert_eq!(
        Some(&expected_entry2),
        topic_config2.get("max.message.bytes")
    );
    let config_entries1 = topic_config1.entry_map();
    let config_entries2 = topic_config2.entry_map();
    assert_eq!(topic_config1.entries.len(), config_entries1.len());
    assert_eq!(topic_config2.entries.len(), config_entries2.len());
    assert_eq!(
        Some(&&expected_entry1),
        config_entries1.get("max.message.bytes")
    );
    assert_eq!(
        Some(&&expected_entry2),
        config_entries2.get("max.message.bytes")
    );

    let partitions1 = NewPartitions::new(&topic_name_1, 5);
    let res = admin_client
        .create_partitions(&[partitions1], &opts)
        .await
        .expect("partition creation failed");
    assert_eq!(res, &[Ok(topic_name_1.clone())]);

    let mut tries = 0;
    loop {
        let metadata = utils::consumer::fetch_consumer_metadata(&consumer_client, &topic_name_1)
            .expect(&format!("failed to fetch metadata for {}", &topic_name_1));
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
        .delete_topics(&[&topic_name_1, &topic_name_2], &opts)
        .await
        .expect("topic deletion failed");
    assert_eq!(res, &[Ok(topic_name_1.clone()), Ok(topic_name_2.clone())]);
    utils::consumer::verify_topic_deleted(&consumer_client, &topic_name_1)
        .expect(&format!("could not delete topic for {}", &topic_name_1));
    utils::consumer::verify_topic_deleted(&consumer_client, &topic_name_2)
        .expect(&format!("could not delete topic for {}", &topic_name_2));
}

/// Verify that incorrect replication configurations are ignored when
/// creating topics.
#[tokio::test]
pub async fn test_incorrect_replication_factors_are_ignored_when_creating_topics() {
    // Get Kafka container context.
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };
    let test_topic_name = rand_test_topic("testing-topic");

    let admin_client_result =
        utils::admin::create_admin_client(&kafka_context.bootstrap_servers).await;
    let Ok(admin_client) = admin_client_result else {
        panic!(
            "could not create admin client: {}",
            admin_client_result.unwrap_err()
        );
    };
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let topic = NewTopic::new(
        "ignored",
        1,
        TopicReplication::Variable(&[&[utils::BROKER_ID], &[utils::BROKER_ID]]),
    );
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

/// Verify that incorrect replication configurations are ignored when
/// creating partitions.
#[tokio::test]
pub async fn test_incorrect_replication_factors_are_ignored_when_creating_partitions() {
    // Get Kafka container context.
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };

    let admin_client_result =
        utils::admin::create_admin_client(&kafka_context.bootstrap_servers).await;
    let Ok(admin_client) = admin_client_result else {
        panic!(
            "could not create admin client: {}",
            admin_client_result.unwrap_err()
        );
    };
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // Create consumer client
    let consumer_client =
        utils::consumer::create_unsubscribed_base_consumer(&kafka_context.bootstrap_servers)
            .await
            .expect("could not create consumer client");

    let name = rand_test_topic("test_topics");
    let topic = NewTopic::new(&name, 1, TopicReplication::Fixed(1));

    let res = admin_client
        .create_topics(vec![&topic], &opts)
        .await
        .expect("topic creation failed");
    assert_eq!(res, &[Ok(name.clone())]);
    let _ = utils::consumer::fetch_consumer_metadata(&consumer_client, &name);

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
