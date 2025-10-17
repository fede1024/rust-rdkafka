use anyhow::Context;
use rdkafka::ClientConfig;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::StreamConsumer;

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

    let stream_consumer = StreamConsumer::from_config(&client_config).context("failed to create stream consumer")?;
    Ok(stream_consumer)
}
