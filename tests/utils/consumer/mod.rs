pub mod stream_consumer;

use crate::utils::rand::rand_test_group;
use crate::utils::ConsumerTestContext;
use anyhow::{bail, Context};
use backon::{BlockingRetryable, ExponentialBuilder};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use rdkafka::{ClientConfig, TopicPartitionList};
use std::time::Duration;

pub async fn create_subscribed_base_consumer(
    bootstrap_servers: &str,
    consumer_group_option: Option<&str>,
    test_topic: &str,
) -> anyhow::Result<BaseConsumer> {
    let unsubscribed_base_consumer =
        create_unsubscribed_base_consumer(bootstrap_servers, consumer_group_option).await?;
    unsubscribed_base_consumer
        .subscribe(&[test_topic])
        .context("Failed to subscribe to topic")?;
    Ok(unsubscribed_base_consumer)
}

pub async fn create_unsubscribed_base_consumer(
    bootstrap_servers: &str,
    consumer_group_option: Option<&str>,
) -> anyhow::Result<BaseConsumer> {
    let consumer_group_name = match consumer_group_option {
        Some(consumer_group_name) => consumer_group_name,
        None => &rand_test_group(),
    };
    let mut consumer_client_config = ClientConfig::default();
    consumer_client_config.set("group.id", consumer_group_name);
    consumer_client_config.set("client.id", "rdkafka_integration_test_client");
    consumer_client_config.set("bootstrap.servers", bootstrap_servers);
    consumer_client_config.set("enable.partition.eof", "false");
    consumer_client_config.set("session.timeout.ms", "6000");
    consumer_client_config.set("enable.auto.commit", "false");
    consumer_client_config.set("debug", "all");
    consumer_client_config.set("auto.offset.reset", "earliest");

    BaseConsumer::from_config(&consumer_client_config).context("Failed to create consumer")
}

pub fn create_base_consumer(
    bootstrap_servers: &str,
    consumer_group: &str,
    config_overrides: Option<&[(&str, &str)]>,
) -> anyhow::Result<BaseConsumer<ConsumerTestContext>> {
    let mut consumer_client_config = ClientConfig::default();
    consumer_client_config.set("group.id", consumer_group);
    consumer_client_config.set("client.id", "rdkafka_integration_test_client");
    consumer_client_config.set("bootstrap.servers", bootstrap_servers);
    consumer_client_config.set("enable.partition.eof", "false");
    consumer_client_config.set("session.timeout.ms", "6000");
    consumer_client_config.set("enable.auto.commit", "false");
    consumer_client_config.set("debug", "all");
    consumer_client_config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            consumer_client_config.set(*key, *value);
        }
    }

    consumer_client_config
        .create_with_context::<ConsumerTestContext, BaseConsumer<ConsumerTestContext>>(
            ConsumerTestContext { _n: 64 },
        )
        .context("Failed to create consumer with context")
}

pub async fn poll_x_times_for_messages(
    consumer: &BaseConsumer,
    times_to_poll: i32,
) -> anyhow::Result<Vec<BorrowedMessage<'_>>> {
    let mut borrowed_messages: Vec<BorrowedMessage> = Vec::new();

    for _ in 0..times_to_poll {
        let Some(next_message_result) = consumer.poll(Duration::from_secs(2)) else {
            continue;
        };

        let Ok(borrowed_next_message) = next_message_result else {
            panic!(
                "could not get next message from based_consumer: {}",
                next_message_result.unwrap_err()
            );
        };
        borrowed_messages.push(borrowed_next_message);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(borrowed_messages)
}

pub fn fetch_consumer_metadata(consumer: &BaseConsumer, topic: &str) -> anyhow::Result<Metadata> {
    let timeout = Some(Duration::from_secs(1));

    (|| {
        let metadata = consumer
            .fetch_metadata(Some(topic), timeout)
            .context("Failed to fetch metadata")?;
        if metadata.topics().is_empty() {
            bail!("metadata fetch returned no topics".to_string())
        }
        let topic = &metadata.topics()[0];
        if topic.partitions().is_empty() {
            bail!("metadata fetch returned a topic with no partitions".to_string())
        }
        Ok(metadata)
    })
    .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
    .call()
}

pub fn verify_topic_deleted(consumer: &BaseConsumer, topic: &str) -> anyhow::Result<()> {
    let timeout = Some(Duration::from_secs(1));

    (|| {
        // Asking about the topic specifically will recreate it (under the
        // default Kafka configuration, at least) so we have to ask for the list
        // of all topics and search through it.
        let metadata = consumer
            .fetch_metadata(None, timeout)
            .context("Failed to fetch metadata")?;
        if metadata.topics().iter().any(|t| t.name() == topic) {
            bail!(format!("topic {} still exists", topic))
        }
        Ok(())
    })
    .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
    .call()
}

pub async fn create_consumer_group_on_topic(
    consumer_client: &BaseConsumer,
    topic_name: &str,
) -> anyhow::Result<()> {
    let topic_partition_list = {
        let mut lst = TopicPartitionList::new();
        lst.add_partition(topic_name, 0);
        lst
    };
    consumer_client
        .assign(&topic_partition_list)
        .context("assign topic partition list failed")?;
    consumer_client
        .fetch_metadata(None, Duration::from_secs(3))
        .context("unable to fetch metadata")?;
    (|| consumer_client.store_offset(topic_name, 0, -1))
        .retry(ExponentialBuilder::default().with_max_delay(Duration::from_secs(5)))
        .call()
        .context("store offset failed")?;
    consumer_client
        .commit_consumer_state(CommitMode::Sync)
        .context("commit the consumer state failed")
}
