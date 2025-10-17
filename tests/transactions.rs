//! Test transactions using the base consumer and producer.

use std::error::Error;
use std::time::Duration;

use log::info;

use rdkafka::admin::AdminOptions;
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer;
use crate::utils::rand::*;
use crate::utils::*;

mod utils;

async fn create_consumer(
    kafka_context: &KafkaContext,
    config_overrides: Option<&[(&str, &str)]>,
) -> Result<BaseConsumer<ConsumerTestContext>, KafkaError> {
    init_test_logger();
    let group_id = rand_test_group();
    let mut config = ClientConfig::new();
    config
        .set("group.id", &group_id)
        .set("enable.partition.eof", "false")
        .set("client.id", "rdkafka_integration_test_client")
        .set("bootstrap.servers", &kafka_context.bootstrap_servers)
        .set("session.timeout.ms", "6000")
        .set("debug", "all")
        .set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(*key, *value);
        }
    }

    config.create_with_context(ConsumerTestContext { _n: 64 })
}

fn create_producer(kafka_context: &KafkaContext) -> Result<BaseProducer, KafkaError> {
    init_test_logger();
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &kafka_context.bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", rand_test_transactional_id())
        .set("debug", "eos");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create()
}

enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

async fn count_records(
    kafka_context: &KafkaContext,
    topic: &str,
    iso: IsolationLevel,
) -> Result<usize, KafkaError> {
    let isolation = match iso {
        IsolationLevel::ReadUncommitted => "read_uncommitted",
        IsolationLevel::ReadCommitted => "read_committed",
    };

    let consumer = create_consumer(
        kafka_context,
        Some(&[("isolation.level", isolation), ("enable.partition.eof", "true")]),
    )
    .await?;

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
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let consume_topic = rand_test_topic("test_transaction_abort");
    let produce_topic = rand_test_topic("test_transaction_abort");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&consume_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create consume topic");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&produce_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create produce topic");

    let future_producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&future_producer, &consume_topic, 30, 0).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(&kafka_context, None).await?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer(&kafka_context)?;
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
    info!("BEFORE FLUSH");
    producer.flush(Duration::from_secs(20))?;
    info!("AFTER FLUSH");
    producer.abort_transaction(Duration::from_secs(20))?;
    info!("AFTER ABORT");

    // Check that no records were produced in read committed mode, but that
    // the records are visible in read uncommitted mode.
    assert_eq!(
        count_records(&kafka_context, &produce_topic, IsolationLevel::ReadCommitted).await?,
        0,
    );
    assert_eq!(
        count_records(&kafka_context, &produce_topic, IsolationLevel::ReadUncommitted).await?,
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
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let consume_topic = rand_test_topic("test_transaction_commit");
    let produce_topic = rand_test_topic("test_transaction_commit");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&consume_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create consume topic");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&produce_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create produce topic");

    let future_producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&future_producer, &consume_topic, 30, 0).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(&kafka_context, None).await?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer(&kafka_context)?;
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
        count_records(&kafka_context, &produce_topic, IsolationLevel::ReadUncommitted).await?,
        10,
    );
    assert_eq!(
        count_records(&kafka_context, &produce_topic, IsolationLevel::ReadCommitted).await?,
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
