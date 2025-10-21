use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

pub async fn create_producer(bootstrap_servers: &str) -> anyhow::Result<FutureProducer> {
    let mut producer_client_config = ClientConfig::default();
    producer_client_config.set("bootstrap.servers", bootstrap_servers);
    let future_producer = FutureProducer::from_config(&producer_client_config)
        .context("couldn't create producer client")?;
    Ok(future_producer)
}
