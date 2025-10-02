use anyhow::Context;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::core::ContainerPort;
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

    pub async fn bootstrap_servers(&self) -> anyhow::Result<String> {
        let kafka_host = self
            .kafka_node
            .get_host()
            .await
            .context("Failed to get Kafka host")?;
        let kafka_port = self
            .kafka_node
            .get_host_port_ipv4(ContainerPort::Tcp(9092))
            .await?;
        Ok(format!("{}:{}", kafka_host.to_string(), kafka_port))
    }
}
