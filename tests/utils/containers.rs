use anyhow::Context;
use std::fmt::Debug;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;

pub struct KafkaContext {
    kafka_node: ContainerAsync<Kafka>,
}

impl Debug for KafkaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaContext").finish()
    }
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

#[tokio::test]
pub async fn test_kafka_context_works() {
    let kafka_context_result = KafkaContext::new().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "Failed to get Kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };

    let bootstrap_servers_result = kafka_context.bootstrap_servers().await;
    let Ok(bootstrap_servers) = bootstrap_servers_result else {
        panic!(
            "Failed to get bootstrap servers: {}",
            bootstrap_servers_result.unwrap_err()
        );
    };

    assert_ne!(bootstrap_servers.len(), 0, "Bootstrap servers empty");
}
