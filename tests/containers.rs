use anyhow::Context;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;

pub struct KafkaContext {
    kafka_node: ContainerAsync<Kafka>,
}

impl KafkaContext {
    pub async fn new() -> anyhow::Result<Self> {
        let kafka_node = Kafka::default()
            .start()
            .await
            .context("Failed to start Kafka")?;

        Ok(Self { kafka_node })
    }

    pub async fn bootstrap_servers(&self) -> String {
        unimplemented!("not yet implemented")
    }
}
