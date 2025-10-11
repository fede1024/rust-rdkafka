use anyhow::Context;
use std::fmt::Debug;
use std::sync::Arc;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, Image};
use tokio::sync::OnceCell;

pub struct KafkaContext {
    kafka_node: ContainerAsync<Kafka>,
    pub bootstrap_servers: String,
    pub version: String,
}

impl Debug for KafkaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaContext").finish()
    }
}

impl KafkaContext {
    pub async fn shared() -> anyhow::Result<Arc<Self>> {
        static INSTANCE: OnceCell<Arc<KafkaContext>> = OnceCell::const_new();

        INSTANCE
            .get_or_try_init(init)
            .await
            .context("Failed to initialize Kafka shared instance")
            .map(Arc::clone)
    }

    pub async fn std_out(&self) -> anyhow::Result<String> {
        let std_out_byte_vec = self
            .kafka_node
            .stdout_to_vec()
            .await
            .context("Failed to get stdout")?;
        Ok(String::from_utf8(std_out_byte_vec)?)
    }

    pub async fn std_err(&self) -> anyhow::Result<String> {
        let std_err_byte_vec = self
            .kafka_node
            .stderr_to_vec()
            .await
            .context("Failed to get stderr")?;
        Ok(String::from_utf8(std_err_byte_vec)?)
    }
}

async fn init() -> anyhow::Result<Arc<KafkaContext>> {
    let kafka_container = Kafka::default();
    let kafka_version = kafka_container.tag().to_string();

    let kafka_node = kafka_container
        .start()
        .await
        .context("Failed to start Kafka")?;
    let kafka_host = kafka_node
        .get_host()
        .await
        .context("Failed to get Kafka host")?;
    let kafka_port = kafka_node
        .get_host_port_ipv4(ContainerPort::Tcp(9092))
        .await?;

    Ok::<Arc<KafkaContext>, anyhow::Error>(Arc::new(KafkaContext {
        kafka_node,
        bootstrap_servers: format!("{}:{}", kafka_host, kafka_port),
        version: kafka_version,
    }))
}

#[tokio::test]
pub async fn test_kafka_context_works() {
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "Failed to get Kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };

    assert_ne!(
        kafka_context.bootstrap_servers.len(),
        0,
        "Bootstrap servers empty"
    );
}
