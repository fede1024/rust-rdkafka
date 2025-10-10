use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
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

    let admin_client_result = utils::admin::create_admin(&kafka_context.bootstrap_servers).await;
    let Ok(admin_client) = admin_client_result else {
        panic!(
            "could not create admin client: {}",
            admin_client_result.unwrap_err()
        );
    };

    let new_topic = NewTopic::new(&test_topic_name, 1, TopicReplication::Fixed(1));
    let admin_opts = AdminOptions::new();
    let create_topics_result = admin_client
        .create_topics(vec![&new_topic], &admin_opts)
        .await;
    let Ok(create_topics) = create_topics_result else {
        panic!(
            "could not create new topics: {}",
            create_topics_result.unwrap_err()
        );
    };

    for topic_result in create_topics {
        if topic_result.is_err() {
            panic!("failed to create topic: {:?}", topic_result.unwrap_err());
        };
    }
}
