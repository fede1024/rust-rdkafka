use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::time::Duration;

pub type PartitionOffset = (i32, i64);
pub type MessageId = usize;

pub async fn populate_topic_using_future_producer(
    producer: &FutureProducer,
    topic_name: &str,
    num_messages: usize,
    partition: Option<i32>,
) -> anyhow::Result<HashMap<PartitionOffset, MessageId>> {
    let message_send_futures = (0..num_messages)
        .map(|id| {
            let future = async move {
                producer
                    .send(
                        FutureRecord {
                            topic: topic_name,
                            payload: Some(&id.to_string()),
                            key: Some(&id.to_string()),
                            partition,
                            timestamp: None,
                            headers: None,
                        },
                        Duration::from_secs(1),
                    )
                    .await
            };
            (id, future)
        })
        .collect::<Vec<_>>();

    let mut message_map = HashMap::<PartitionOffset, MessageId>::new();
    for (id, future) in message_send_futures {
        match future.await {
            Ok(delivered) => message_map.insert((delivered.partition, delivered.offset), id),
            Err((kafka_error, _message)) => panic!("Delivery failed: {}", kafka_error),
        };
    }

    Ok(message_map)
}
