use std::convert::TryInto;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rdkafka::mocking::MockCluster;
use rdkafka::{TopicPartitionList};
use rdkafka::producer::{FutureProducer, FutureRecord};



async fn consume_and_print(consumer: &StreamConsumer, max: i32) {
    let mut count = 0;
    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                count+=1;
                consumer.commit_message(&m, CommitMode::Async).unwrap();
                if count >= max {
                    return;
                }
            }
        };
    }
}

#[tokio::main]
async fn main() {
    const TOPIC: &str = "seek_topic";
    let mock_cluster = MockCluster::new(3).unwrap();
    mock_cluster
        .create_topic(TOPIC, 3, 1)
        .expect("Failed to create topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", mock_cluster.bootstrap_servers())
        .create()
        .expect("Producer creation error");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", mock_cluster.bootstrap_servers())
        .set("group.id", "rust-rdkafka-mock-example")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[TOPIC]).unwrap();

    tokio::spawn(async move {
        let mut i = 0_usize;
        loop {
            producer
                .send_result(
                    FutureRecord::to(TOPIC)
                        .key(&i.to_string())
                        .payload(&format!("Message {}",i))
                        .timestamp(now()),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            i += 1;
            // if i > 5 {
            //     break;
            // }
        }
    });
    consume_and_print(&consumer, 5).await;
    println!("Seek Partitions");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TOPIC, 0, rdkafka::Offset::Offset(3)).unwrap();
    tpl.add_partition_offset(TOPIC, 2, rdkafka::Offset::Offset(3)).unwrap();
    let timeout = Duration::from_secs(5);
    let seek = consumer.seek_partitions(&tpl, timeout);
    match seek {
        Err(_) => println!("Error on seek"),
        Ok(_) => println!("Success!"),
    }
    consume_and_print(&consumer, 5).await;

}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}