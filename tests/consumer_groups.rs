use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::{rand_test_group, rand_test_topic};
use rdkafka::admin::{AdminOptions, GroupResult, NewTopic, TopicReplication};
use rdkafka_sys::RDKafkaErrorCode;

mod utils;

/// Verify that a valid group can be deleted.
#[tokio::test]
pub async fn test_consumer_groups_deletion() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    // Create consumer_client
    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_topic");
    let consumer_client = utils::consumer::create_unsubscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
    )
    .await
    .expect("could not create subscribed base consumer");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    utils::consumer::create_consumer_group_on_topic(&consumer_client, &topic_name)
        .await
        .expect("could not create group");
    let res = admin_client
        .delete_groups(&[&group_name], &AdminOptions::default())
        .await
        .expect("could not delete groups");
    assert_eq!(res, [Ok(group_name.to_string())]);
}

/// Verify that attempting to delete an unknown group returns a "group not
/// found" error.
#[tokio::test]
pub async fn delete_unknown_group() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let unknown_group_name = rand_test_group();
    let res = admin_client
        .delete_groups(&[&unknown_group_name], &AdminOptions::default())
        .await;
    let expected: GroupResult = Err((unknown_group_name, RDKafkaErrorCode::NotCoordinator));
    assert_eq!(res, Ok(vec![expected]));
}
