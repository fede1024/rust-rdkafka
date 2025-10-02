use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;

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
}
