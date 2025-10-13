//! Test administrative commands using the admin API.

use std::time::Duration;

use backon::{BlockingRetryable, ExponentialBuilder};

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigSource, GroupResult, NewPartitions,
    NewTopic, OwnedResourceSpecifier, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::utils::logging::init_test_logger;
use crate::utils::rand::{rand_test_group, rand_test_topic};
use crate::utils::*;

mod utils;

fn create_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config
}

fn create_admin_client() -> AdminClient<DefaultClientContext> {
    init_test_logger();
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
