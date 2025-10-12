use crate::utils::admin::{create_admin_client, create_topic};
use crate::utils::consumer::{create_subscribed_base_consumer, poll_x_times_for_messages};
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer::base_producer::create_producer;
use crate::utils::rand::rand_test_topic;
use rdkafka::producer::BaseRecord;
use rdkafka::Message;

#[path = "utils/mod.rs"]
mod utils;

#[tokio::test]
pub async fn test_basic_produce() {
    init_test_logger();

    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "could not create kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };
    let test_topic_name = rand_test_topic("testing-topic");

    let admin_client_result = create_admin_client(&kafka_context.bootstrap_servers).await;
    let Ok(admin_client) = admin_client_result else {
        panic!(
            "could not create admin client: {}",
            admin_client_result.unwrap_err()
        );
    };
    let create_topic_result = create_topic(&admin_client, &test_topic_name).await;
    if create_topic_result.is_err() {
        panic!(
            "could not create topic: {}",
            create_topic_result.unwrap_err()
        );
    }

    let consumer_result =
        create_subscribed_base_consumer(&kafka_context.bootstrap_servers, &test_topic_name).await;
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
    let send_record_result =
        crate::utils::producer::base_producer::send_record(&base_producer, record).await;
    if send_record_result.is_err() {
        panic!("could not send record: {}", send_record_result.unwrap_err());
    }

    let messages_result = poll_x_times_for_messages(&consumer, 10).await;
    let Ok(messages) = messages_result else {
        panic!("could not get messages from consumer");
    };
    if messages.len() != 1 {
        panic!("expected exactly one message");
    }
    let borrowed_next_message = messages.get(0).unwrap();

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
