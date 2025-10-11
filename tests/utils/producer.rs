use anyhow::{bail, Context};
use rdkafka::config::FromClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::time::Duration;

pub async fn create_producer(bootstrap_servers: &str) -> anyhow::Result<BaseProducer> {
    let mut producer_client_config = ClientConfig::default();
    producer_client_config.set("bootstrap.servers", bootstrap_servers);
    let base_producer_result = create_base_producer(&producer_client_config);
    let Ok(base_producer) = base_producer_result else {
        panic!(
            "could not create based_producer: {}",
            base_producer_result.unwrap_err()
        );
    };
    Ok(base_producer)
}

pub fn create_base_producer(config: &ClientConfig) -> anyhow::Result<BaseProducer> {
    let base_producer_result = BaseProducer::from_config(config);
    let Ok(base_producer) = base_producer_result else {
        anyhow::bail!(
            "error creating base producer: {}",
            base_producer_result.unwrap_err()
        )
    };
    Ok(base_producer)
}

pub async fn send_record(
    producer: &BaseProducer,
    record: BaseRecord<'_, [u8; 4], str>,
) -> anyhow::Result<()> {
    let send_result = producer.send(record);
    if send_result.is_err() {
        bail!("could not produce record: {:?}", send_result.unwrap_err());
    }
    if poll_and_flush(&producer).is_err() {
        bail!("could not poll and flush base producer")
    };

    Ok(())
}

pub fn poll_and_flush(base_producer: &BaseProducer) -> anyhow::Result<()> {
    for _ in 0..5 {
        base_producer.poll(Duration::from_millis(100));
    }
    base_producer
        .flush(Timeout::After(Duration::from_secs(10)))
        .context("flush failed")
}
