//! Test transactions using low level consumer and producer.

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::utils::*;

mod utils;

const TIMEOUT: Duration = Duration::from_secs(10);
const POPULATE_COUNT: i32 = 30;
const COMMIT_OFFSET: i64 = 10;
const SEND_OFFSET: i64 = 20;
const PRODUCE_COUNT: usize = 20;

fn create_base_consumer(group_id: &str) -> BaseConsumer<DefaultConsumerContext> {
    consumer_config(
        group_id,
        Some(map!(
            "isolation.level" => "read_committed",
            "enable.partition.eof" => "true"
        )),
    )
    .create()
    .expect("Consumer creation failed")
}

fn create_base_producer(transactional_id: &str) -> BaseProducer<DefaultProducerContext> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &get_bootstrap_server())
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", transactional_id)
        .set("debug", "eos");
    config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
    config.create().expect("Producer creation failed")
}

async fn prepare_transaction(
    consumer: &BaseConsumer,
    producer: &BaseProducer,
    consume_topic: &str,
    produce_topic: &str,
) {
    // populate consume_topic
    populate_topic(
        &consume_topic,
        POPULATE_COUNT,
        &value_fn,
        &key_fn,
        Some(0),
        None,
    )
    .await;
    consumer.subscribe(&[&consume_topic]).unwrap();
    consumer.poll(TIMEOUT).unwrap().unwrap();

    // commit initial consumer offset
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(COMMIT_OFFSET));
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // start transaction
    producer.init_transactions(TIMEOUT).unwrap();
    producer.begin_transaction().unwrap();

    // send offsets to transaction
    let cgm = consumer.group_metadata();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(consume_topic, 0, Offset::Offset(SEND_OFFSET));
    producer
        .send_offsets_to_transaction(&txn_tpl, &cgm, TIMEOUT)
        .unwrap();

    // produce records in transaction
    for _ in 0..PRODUCE_COUNT {
        producer
            .send(
                BaseRecord::to(produce_topic)
                    .payload("A")
                    .key("B")
                    .partition(0),
            )
            .unwrap();
    }
}

fn assert_transaction(
    consumer: &BaseConsumer,
    consume_topic: &str,
    produce_topic: &str,
    consumer_offset: i64,
    produced_count: usize,
) {
    // check consumer committed offset
    let committed = consumer.committed(TIMEOUT).unwrap();
    assert_eq!(
        committed.find_partition(consume_topic, 0).unwrap().offset(),
        Offset::Offset(consumer_offset)
    );

    // check how many records have been produced after the transaction
    let txn_consumer = create_base_consumer(&rand_test_group());
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(produce_topic, 0);
    txn_consumer.assign(&tpl).unwrap();

    // count all messages in produce_topic
    let consumed = txn_consumer
        .iter()
        .take_while(|r| !matches!(r, Err(KafkaError::PartitionEOF(_))))
        .count();

    assert_eq!(consumed, produced_count);
}

#[tokio::test]
async fn test_transaction_abort() {
    let consumer = create_base_consumer(&rand_test_group());
    let producer = create_base_producer(&rand_test_transactional_id());

    let consume_topic = rand_test_topic();
    let produce_topic = rand_test_topic();

    prepare_transaction(&consumer, &producer, &consume_topic, &produce_topic).await;

    producer.flush(TIMEOUT);
    producer.abort_transaction(TIMEOUT).unwrap();

    assert_transaction(&consumer, &consume_topic, &produce_topic, COMMIT_OFFSET, 0);
}

#[tokio::test]
async fn test_transaction_commit() {
    let consumer = create_base_consumer(&rand_test_group());
    let producer = create_base_producer(&rand_test_transactional_id());

    let consume_topic = rand_test_topic();
    let produce_topic = rand_test_topic();

    prepare_transaction(&consumer, &producer, &consume_topic, &produce_topic).await;

    producer.commit_transaction(TIMEOUT).unwrap();

    assert_transaction(
        &consumer,
        &consume_topic,
        &produce_topic,
        SEND_OFFSET,
        PRODUCE_COUNT,
    );
}
