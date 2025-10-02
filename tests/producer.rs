use rdkafka::ClientConfig;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer::create_base_producer;
use crate::utils::rand::rand_test_topic;
use rdkafka::producer::BaseRecord;

mod utils;

#[tokio::test]
pub async fn test_basic_produce() {
    init_test_logger();

    let kafka_context_result = KafkaContext::new().await;
    let Ok(_kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };

    let bootstrap_servers_result = _kafka_context.bootstrap_servers().await;
    let Ok(bootstrap_servers) = bootstrap_servers_result else {
        panic!("could not create bootstrap servers: {}", bootstrap_servers_result.unwrap_err());
    };
    let mut client_config = ClientConfig::default();
    client_config.set("bootstrap.servers", bootstrap_servers);

    let base_producer_result = create_base_producer(&client_config);
    let Ok(base_producer) = base_producer_result else {
        panic!(
            "could not create based_producer: {}",
            base_producer_result.unwrap_err()
        );
    };

    let test_topic = rand_test_topic("testing-topic");
    let record = BaseRecord::to(&test_topic) // destination topic
        .key(&[1, 2, 3, 4]) // message key
        .payload("content") // message payload
        .partition(5);

    let send_result = base_producer.send(record);
    if send_result.is_err() {
        panic!("could not produce record: {:?}", send_result.unwrap_err());
    }
}
