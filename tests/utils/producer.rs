use rdkafka::config::FromClientConfig;
use rdkafka::producer::BaseProducer;
use rdkafka::ClientConfig;

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
