use crate::utils::consumer;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer::create_base_producer;
use crate::utils::rand::{rand_test_group, rand_test_topic};
use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::time::Duration;

mod utils;

#[tokio::test]
pub async fn test_basic_produce() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };
    let test_topic_name = rand_test_topic("testing-topic");

    let consumer_result = create_consumer(&kafka_context.bootstrap_servers, &test_topic_name).await;
    let Ok(consumer) = consumer_result else {
        panic!(
            "could not create consumer: {}",
            consumer_result.unwrap_err()
        );
    };
    let create_producer_result = create_producer(&kafka_context.bootstrap_servers).await;
    let Ok(base_producer) = create_producer_result else {
        panic!(
            "could not create base producer: {}",
            create_producer_result.unwrap_err()
        );
    };

    let record = BaseRecord::to(&test_topic_name) // destination topic
        .key(&[1, 2, 3, 4]) // message key
        .payload("content"); // message payload

    let send_result = base_producer.send(record);
    if send_result.is_err() {
        panic!("could not produce record: {:?}", send_result.unwrap_err());
    }
    let flush_result = base_producer.flush(Timeout::After(Duration::from_secs(10)));
    if let Err(flush_error) = flush_result {
        panic!("timed out waiting for producer flush: {}", flush_error);
    }

    let Some(next_message_result) = consumer.poll(Duration::from_secs(2)) else {
        panic!("there is no next message on the topic: {}", test_topic_name);
    };
    let Ok(borrowed_next_message) = next_message_result else {
        panic!(
            "could not get next message from based_consumer: {}",
            next_message_result.unwrap_err()
        );
    };
    let owned_next_message = borrowed_next_message.detach();
    let Some(message_payload) = owned_next_message.payload() else {
        panic!("message payload is empty");
    };
    let message_string_result = String::from_utf8(message_payload.to_vec());
    let Ok(message_string) = message_string_result else {
        panic!("message payload is not valid UTF-8");
    };

    assert!(message_string.contains("content"));
}

async fn create_consumer(
    bootstrap_servers: &str,
    test_topic: &str,
) -> anyhow::Result<BaseConsumer> {
    let mut consumer_client_config = ClientConfig::default();
    consumer_client_config.set("group.id", rand_test_group());
    consumer_client_config.set("client.id", "rdkafka_integration_test_client");
    consumer_client_config.set("bootstrap.servers", bootstrap_servers);
    consumer_client_config.set("enable.partition.eof", "false");
    consumer_client_config.set("session.timeout.ms", "6000");
    consumer_client_config.set("enable.auto.commit", "false");
    consumer_client_config.set("debug", "all");
    consumer_client_config.set("auto.offset.reset", "earliest");

    let base_consumer_result =
        consumer::create_subscribed_base(consumer_client_config, &[&test_topic]).await;
    let Ok(base_consumer) = base_consumer_result else {
        panic!(
            "could not create base consumer: {}",
            base_consumer_result.unwrap_err()
        )
    };

    Ok(base_consumer)
}

async fn create_producer(bootstrap_servers: &str) -> anyhow::Result<BaseProducer> {
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
