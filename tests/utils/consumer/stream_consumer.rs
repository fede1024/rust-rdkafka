use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;

pub async fn create_stream_consumer(
    bootstrap_server: &str,
    consumer_group_option: Option<&str>,
) -> anyhow::Result<StreamConsumer> {
    let mut client_config = ClientConfig::default();
    client_config.set("bootstrap.servers", bootstrap_server);
    client_config.set("auto.offset.reset", "earliest");
    if let Some(group) = consumer_group_option {
        client_config.set("group.id", group);
    }

    let stream_consumer =
        StreamConsumer::from_config(&client_config).context("failed to create stream consumer")?;
    Ok(stream_consumer)
}

pub async fn create_stream_consumer_with_options(
    bootstrap_server: &str,
    consumer_group: &str,
    options: &[(&str, &str)],
) -> anyhow::Result<StreamConsumer> {
    let mut client_config = ClientConfig::default();
    client_config.set("bootstrap.servers", bootstrap_server);
    client_config.set("group.id", consumer_group);
    client_config.set("auto.offset.reset", "earliest");
    client_config.set("enable.auto.commit", "false");

    for (key, value) in options {
        client_config.set(*key, *value);
    }

    let stream_consumer =
        StreamConsumer::from_config(&client_config).context("failed to create stream consumer")?;
    Ok(stream_consumer)
}
