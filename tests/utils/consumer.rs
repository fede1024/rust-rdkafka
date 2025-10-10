use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;

pub async fn create_subscribed_base(
    client_config: ClientConfig,
    topics: &[&str],
) -> anyhow::Result<BaseConsumer> {
    let base_consumer =
        BaseConsumer::from_config(&client_config).context("Failed to create consumer")?;
    base_consumer
        .subscribe(topics)
        .context("Failed to subscribe to topic")?;

    Ok(base_consumer)
}
