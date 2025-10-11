use crate::utils::consumer;
use crate::utils::rand::rand_test_group;
use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::ClientConfig;
use std::time::Duration;

pub async fn create_consumer(
    bootstrap_servers: &str,
    test_topic: &str,
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

    let base_consumer_result =
        consumer::create_subscribed_base(consumer_client_config, &[&test_topic]).await;
    let Ok(base_consumer) = base_consumer_result else {
        panic!(
            "could not create base consumer: {}",
            base_consumer_result.unwrap_err()
        )
    };

    Ok(base_consumer)
}

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
