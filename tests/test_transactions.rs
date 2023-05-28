//! Test transactions using the base consumer and producer.

use std::collections::HashMap;
use std::error::Error;

use maplit::hashmap;

use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use utils::*;

mod utils;

fn create_consumer(
    config_overrides: Option<HashMap<&str, &str>>,
) -> Result<BaseConsumer, KafkaError> {
    consumer_config(&rand_test_group(), config_overrides).create()
}

fn create_producer() -> Result<BaseProducer, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &get_bootstrap_server())
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", &rand_test_transactional_id())
        .set("debug", "eos");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create()
}

enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

fn count_records(topic: &str, iso: IsolationLevel) -> Result<usize, KafkaError> {
    let consumer = create_consumer(Some(hashmap! {
        "isolation.level" => match iso {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        },
        "enable.partition.eof" => "true"
    }))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    consumer.assign(&tpl)?;
    let mut count = 0;
    for message in consumer.iter() {
        match message {
            Ok(_) => count += 1,
            Err(KafkaError::PartitionEOF(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(count)
}

#[tokio::test]
async fn test_transaction_abort() -> Result<(), Box<dyn Error>> {
    let consume_topic = rand_test_topic();
    let produce_topic = rand_test_topic();

    populate_topic(&consume_topic, 30, &value_fn, &key_fn, Some(0), None).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(None)?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer()?;
    producer.init_transactions(Timeout::Never)?;
    producer.begin_transaction()?;

    // Tie the commit of offset 20 to the transaction.
    let cgm = consumer.group_metadata().unwrap();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

    // Produce 10 records in the transaction.
    for _ in 0..10 {
        producer
            .send(
                BaseRecord::to(&produce_topic)
                    .payload("A")
                    .key("B")
                    .partition(0),
            )
            .unwrap();
    }

    // Abort the transaction, but only after producing all messages.
    producer.flush(Timeout::Never)?;
    producer.abort_transaction(Timeout::Never)?;

    // Check that no records were produced in read committed mode, but that
    // the records are visible in read uncommitted mode.
    assert_eq!(
        count_records(&produce_topic, IsolationLevel::ReadCommitted)?,
        0,
    );
    assert_eq!(
        count_records(&produce_topic, IsolationLevel::ReadUncommitted)?,
        10,
    );

    // Check that the consumer's committed offset is still 10.
    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(10)
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_commit() -> Result<(), Box<dyn Error>> {
    let consume_topic = rand_test_topic();
    let produce_topic = rand_test_topic();

    populate_topic(&consume_topic, 30, &value_fn, &key_fn, Some(0), None).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(None)?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer()?;
    producer.init_transactions(Timeout::Never)?;
    producer.begin_transaction()?;

    // Tie the commit of offset 20 to the transaction.
    let cgm = consumer.group_metadata().unwrap();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

    // Produce 10 records in the transaction.
    for _ in 0..10 {
        producer
            .send(
                BaseRecord::to(&produce_topic)
                    .payload("A")
                    .key("B")
                    .partition(0),
            )
            .unwrap();
    }

    // Commit the transaction.
    producer.commit_transaction(Timeout::Never)?;

    // Check that 10 records were produced.
    assert_eq!(
        count_records(&produce_topic, IsolationLevel::ReadUncommitted)?,
        10,
    );
    assert_eq!(
        count_records(&produce_topic, IsolationLevel::ReadCommitted)?,
        10,
    );

    // Check that the consumer's committed offset is now 20.
    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(20)
    );

    Ok(())
}
