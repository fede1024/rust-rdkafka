use crate::utils::rand::rand_test_group;
use anyhow::{bail, Context};
use backon::{BlockingRetryable, ExponentialBuilder};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;
use std::time::Duration;

pub async fn create_subscribed_base_consumer(
    bootstrap_servers: &str,
    test_topic: &str,
) -> anyhow::Result<BaseConsumer> {
    let unsubscribed_base_consumer = create_unsubscribed_base_consumer(bootstrap_servers).await?;
    unsubscribed_base_consumer
        .subscribe(&[test_topic])
        .context("Failed to subscribe to topic")?;
    Ok(unsubscribed_base_consumer)
}

pub async fn create_unsubscribed_base_consumer(
    bootstrap_servers: &str,
) -> anyhow::Result<BaseConsumer> {
    let mut consumer_client_config = ClientConfig::default();
    consumer_client_config.set("group.id", rand_test_group());
    consumer_client_config.set("client.id", "rdkafka_integration_test_client");
    consumer_client_config.set("bootstrap.servers", bootstrap_servers);
    consumer_client_config.set("enable.partition.eof", "false");
    consumer_client_config.set("session.timeout.ms", "6000");
    consumer_client_config.set("enable.auto.commit", "false");
    consumer_client_config.set("debug", "all");
    consumer_client_config.set("auto.offset.reset", "earliest");

    BaseConsumer::from_config(&consumer_client_config).context("Failed to create consumer")
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
