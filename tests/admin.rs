use crate::utils::admin::create_topic;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::rand_test_topic;

#[path = "utils/mod.rs"]
mod utils;

#[tokio::test]
pub async fn test_topic_creation() {
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

    let admin_client_result =
        utils::admin::create_admin_client(&kafka_context.bootstrap_servers).await;
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
    };
}
