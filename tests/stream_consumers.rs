//! Test data consumption using high level consumers.

use std::error::Error;
use std::sync::Arc;

use anyhow::Context;
use futures::future;
use futures::stream::StreamExt;
use maplit::hashmap;
use rdkafka_sys::RDKafkaErrorCode;
use tokio::time::{self, Duration};

use rdkafka::admin::AdminOptions;
use rdkafka::consumer::{CommitMode, Consumer, RebalanceProtocol, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::current_time_millis;
use rdkafka::{Message, Timestamp};
use rdkafka_sys::types::RDKafkaConfRes;

use crate::utils::admin::new_topic_vec;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::*;
use crate::utils::*;

mod utils;

#[tokio::test]
async fn test_invalid_max_poll_interval() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let res: Result<StreamConsumer, _> = consumer_config(
        &kafka_context.bootstrap_servers,
        &crate::utils::rand::rand_test_group(),
        Some(hashmap! { "max.poll.interval.ms" => "-1" }),
    )
    .create();
    match res {
        Err(KafkaError::ClientConfig(RDKafkaConfRes::RD_KAFKA_CONF_INVALID, desc, key, value)) => {
            assert_eq!(
                desc,
                "Configuration property \"max.poll.interval.ms\" value -1 is outside allowed range 1..86400000\n"
            );
            assert_eq!(key, "max.poll.interval.ms");
            assert_eq!(value, "-1");
        }
        Ok(_) => panic!("invalid max poll interval configuration accepted"),
        Err(e) => panic!(
            "incorrect error returned for invalid max poll interval: {:?}",
            e
        ),
    }
}

// All produced messages should be consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_base() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let num_of_messages_to_send = 100usize;
    let start_time = current_time_millis();
    let topic_name = rand_test_topic("test_produce_consume_base");
    let message_map = topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        None,
    )
    .await
    .expect("Could not populate topic using Future producer");
    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    consumer
        .subscribe(&[topic_name.as_str()])
        .expect("could not subscribe to kafka topic");

    consumer
        .stream()
        .take(num_of_messages_to_send)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map[&(m.partition(), m.offset())];
                    match m.timestamp() {
                        Timestamp::CreateTime(timestamp) => assert!(timestamp >= start_time),
                        _ => panic!("Expected create time for message timestamp"),
                    };
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), id.to_string());
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), id.to_string());
                    assert_eq!(m.topic(), topic_name.as_str());
                }
                Err(e) => panic!("Error receiving message: {:?}", e),
            };
            future::ready(())
        })
        .await;
}

/// Test that multiple message streams from the same consumer all receive
/// messages. In a previous version of rust-rdkafka, the `StreamConsumerContext`
/// could only manage one waker, so each `MessageStream` would compete for the
/// waker slot.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_base_concurrent() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let num_of_messages_to_send = 100usize;
    let topic_name = rand_test_topic("test_produce_consume_base_concurrent");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        None,
    )
    .await
    .expect("Could not populate topic using Future producer");
    let consumer = Arc::new(
        consumer::stream_consumer::create_stream_consumer(
            &kafka_context.bootstrap_servers,
            Some(&rand_test_group()),
        )
        .await
        .expect("could not create stream consumer"),
    );
    consumer
        .subscribe(&[topic_name.as_str()])
        .expect("could not subscribe to kafka topic");

    let mk_task = || {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            consumer
                .stream()
                .take(20)
                .for_each(|message| match message {
                    Ok(_) => future::ready(()),
                    Err(e) => panic!("Error receiving message: {:?}", e),
                })
                .await;
        })
    };

    for res in future::join_all((0..5).map(|_| mk_task())).await {
        res.unwrap();
    }
}

// All produced messages should be consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_base_assign() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let topic_name = rand_test_topic("test_produce_consume_base_assign");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let num_of_messages_to_send = 10usize;
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(0),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(1),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(2),
    )
    .await
    .expect("Could not populate topic using Future producer");

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 1, Offset::Offset(2))
        .unwrap();
    tpl.add_partition_offset(&topic_name, 2, Offset::Offset(9))
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let mut partition_count = vec![0, 0, 0];

    let _consumer_future = consumer
        .stream()
        .take(19)
        .for_each(|message| {
            match message {
                Ok(m) => partition_count[m.partition() as usize] += 1,
                Err(e) => panic!("Error receiving message: {:?}", e),
            };
            future::ready(())
        })
        .await;

    assert_eq!(partition_count, vec![10, 8, 1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_base_unassign() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let topic_name = rand_test_topic("test_produce_consume_base_assign");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");

    let num_of_messages_to_send = 10usize;
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(0),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(1),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(2),
    )
    .await
    .expect("Could not populate topic using Future producer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 1, Offset::Offset(2))
        .unwrap();
    tpl.add_partition_offset(&topic_name, 2, Offset::Offset(9))
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let mut assignments = consumer.assignment().unwrap();
    assert_eq!(assignments.count(), 3);

    consumer.unassign().unwrap();
    assignments = consumer.assignment().unwrap();
    assert_eq!(assignments.count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_base_incremental_assign_and_unassign() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let topic_name = rand_test_topic("test_produce_consume_base_incremental_assign_and_unassign");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");

    let num_of_messages_to_send = 10usize;
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(0),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(1),
    )
    .await
    .expect("Could not populate topic using Future producer");
    topics::populate_topic_using_future_producer(
        &producer,
        &topic_name,
        num_of_messages_to_send,
        Some(2),
    )
    .await
    .expect("Could not populate topic using Future producer");

    // Adding a simple partition
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    consumer.incremental_assign(&tpl).unwrap();
    let mut assignments = consumer.assignment().unwrap();
    assert_eq!(assignments.count(), 1);

    // Adding another partition
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
        .unwrap();
    consumer.incremental_assign(&tpl).unwrap();
    assignments = consumer.assignment().unwrap();
    assert_eq!(assignments.count(), 2);

    // Removing one partition
    consumer.incremental_unassign(&tpl).unwrap();
    assignments = consumer.assignment().unwrap();
    assert_eq!(assignments.count(), 1);

    // unassigning an non assigned partition should fail
    let err = consumer.incremental_unassign(&tpl);

    assert_eq!(
        err,
        Err(KafkaError::Subscription("_INVALID_ARG".to_string()))
    )
}

// All produced messages should be consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_with_timestamp() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_produce_consume_with_timestamp");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let message_map = produce_messages_with_timestamp(&producer, &topic_name, 100, 0, 1111).await;

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .stream()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map[&(m.partition(), m.offset())];
                    assert_eq!(m.timestamp(), Timestamp::CreateTime(1111));
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(id));
                }
                Err(e) => panic!("Error receiving message: {:?}", e),
            };
            future::ready(())
        })
        .await;

    let _ = produce_messages_with_timestamp(&producer, &topic_name, 10, 0, 999_999).await;

    // Lookup the offsets
    let tpl = consumer
        .offsets_for_timestamp(999_999, Duration::from_secs(10))
        .unwrap();
    let tp = tpl.find_partition(&topic_name, 0).unwrap();
    assert_eq!(tp.topic(), topic_name);
    assert_eq!(tp.offset(), Offset::Offset(100));
    assert_eq!(tp.partition(), 0);
    assert_eq!(tp.error(), Ok(()));
}

// `get_watermark_offsets` reads the local librdkafka cache rather than querying
// the broker. Consuming a message populates the cached high watermark. The low
// watermark may remain invalid unless statistics are enabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_get_watermark_offsets() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_get_watermark_offsets");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create future producer");
    produce_messages_to_partition(&producer, &topic_name, 10, 0).await;

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");

    let (_, queried_high) = consumer
        .fetch_watermarks(&topic_name, 0, Duration::from_secs(10))
        .expect("could not fetch watermarks");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();
    consumer
        .stream()
        .take(10)
        .for_each(|message| {
            if let Err(error) = message {
                panic!("could not consume message: {error}");
            }
            future::ready(())
        })
        .await;

    let (cached_low, cached_high) = consumer
        .get_watermark_offsets(&topic_name, 0)
        .expect("could not get cached watermarks");
    assert_eq!(cached_high, queried_high);
    assert!(cached_low < cached_high);
}

// TODO: add check that commit cb gets called correctly
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_commit_message() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_commit_message");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let _ = produce_messages_to_partition(&producer, &topic_name, 10, 0).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 11, 1).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 12, 2).await;

    let group_name = rand_test_group();
    let consumer = utils::consumer::stream_consumer::create_stream_consumer_with_options(
        &kafka_context.bootstrap_servers,
        &group_name,
        &[],
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .stream()
        .take(33)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                }
                Err(e) => panic!("error receiving message: {:?}", e),
            };
            future::ready(())
        })
        .await;

    let timeout = Duration::from_secs(5);
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 0, timeout).unwrap(),
        (0, 10)
    );
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 1, timeout).unwrap(),
        (0, 11)
    );
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 2, timeout).unwrap(),
        (0, 12)
    );

    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&topic_name, 0, Offset::Stored)
        .unwrap();
    assignment
        .add_partition_offset(&topic_name, 1, Offset::Stored)
        .unwrap();
    assignment
        .add_partition_offset(&topic_name, 2, Offset::Stored)
        .unwrap();
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed
        .add_partition_offset(&topic_name, 0, Offset::Invalid)
        .unwrap();
    committed
        .add_partition_offset(&topic_name, 1, Offset::Offset(11))
        .unwrap();
    committed
        .add_partition_offset(&topic_name, 2, Offset::Invalid)
        .unwrap();
    assert_eq!(committed, consumer.committed(timeout).unwrap());

    let mut position = TopicPartitionList::new();
    position
        .add_partition_offset(&topic_name, 0, Offset::Offset(10))
        .unwrap();
    position
        .add_partition_offset(&topic_name, 1, Offset::Offset(11))
        .unwrap();
    position
        .add_partition_offset(&topic_name, 2, Offset::Offset(12))
        .unwrap();
    assert_eq!(position, consumer.position().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_store_offset_commit() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_store_offset_commit");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let _ = produce_messages_to_partition(&producer, &topic_name, 10, 0).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 11, 1).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 12, 2).await;

    let group_name = rand_test_group();
    let consumer = utils::consumer::stream_consumer::create_stream_consumer_with_options(
        &kafka_context.bootstrap_servers,
        &group_name,
        &[
            ("enable.auto.offset.store", "false"),
            ("enable.partition.eof", "true"),
        ],
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .stream()
        .take(36)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.store_offset_from_message(&m).unwrap();
                    }
                }
                Err(KafkaError::PartitionEOF(_)) => {}
                Err(e) => panic!("Error receiving message: {:?}", e),
            };
            future::ready(())
        })
        .await;

    // Commit the whole current state
    consumer.commit_consumer_state(CommitMode::Sync).unwrap();

    let timeout = Duration::from_secs(5);
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 0, timeout).unwrap(),
        (0, 10)
    );
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 1, timeout).unwrap(),
        (0, 11)
    );
    assert_eq!(
        consumer.fetch_watermarks(&topic_name, 2, timeout).unwrap(),
        (0, 12)
    );

    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&topic_name, 0, Offset::Stored)
        .unwrap();
    assignment
        .add_partition_offset(&topic_name, 1, Offset::Stored)
        .unwrap();
    assignment
        .add_partition_offset(&topic_name, 2, Offset::Stored)
        .unwrap();
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed
        .add_partition_offset(&topic_name, 0, Offset::Invalid)
        .unwrap();
    committed
        .add_partition_offset(&topic_name, 1, Offset::Offset(11))
        .unwrap();
    committed
        .add_partition_offset(&topic_name, 2, Offset::Invalid)
        .unwrap();
    assert_eq!(committed, consumer.committed(timeout).unwrap());

    let mut position = TopicPartitionList::new();
    position
        .add_partition_offset(&topic_name, 0, Offset::Offset(10))
        .unwrap();
    position
        .add_partition_offset(&topic_name, 1, Offset::Offset(11))
        .unwrap();
    position
        .add_partition_offset(&topic_name, 2, Offset::Offset(12))
        .unwrap();
    assert_eq!(position, consumer.position().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_commit_metadata() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_commit_metadata");
    let group_name = rand_test_group();
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");
    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 0).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 1).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 2).await;

    let create_consumer = || async {
        let consumer = utils::consumer::stream_consumer::create_stream_consumer_with_options(
            &kafka_context.bootstrap_servers,
            &group_name,
            &[],
        )
        .await
        .context("failed to create stream consumer")?;

        consumer
            .subscribe(&[topic_name.as_str()])
            .context("failed to subscribe to topic")?;
        let _ = consumer.stream().next().await;

        Ok::<_, Box<dyn Error>>(consumer)
    };

    // Create a topic partition list where each element has some associated
    // metadata.
    let tpl = {
        let mut tpl = TopicPartitionList::new();
        let mut tpl1 = tpl.add_partition(&topic_name, 0);
        tpl1.set_offset(Offset::Offset(1))?;
        tpl1.set_metadata("one");
        let mut tpl2 = tpl.add_partition(&topic_name, 1);
        tpl2.set_offset(Offset::Offset(1))?;
        tpl2.set_metadata("two");
        let mut tpl3 = tpl.add_partition(&topic_name, 2);
        tpl3.set_offset(Offset::Offset(1))?;
        tpl3.set_metadata("three");
        tpl
    };

    // Ensure that the commit state immediately includes the metadata.
    {
        let consumer = create_consumer().await?;
        consumer.commit(&tpl, CommitMode::Sync)?;
        assert_eq!(consumer.committed(None)?, tpl);
    }

    // Ensure that the commit state on a new consumer in the same group
    // can see the same metadata.
    {
        let consumer = create_consumer().await?;
        assert_eq!(consumer.committed(None)?, tpl);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consume_partition_order() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consume_partition_order");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topics");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 0).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 1).await;
    let _ = produce_messages_to_partition(&producer, &topic_name, 4, 2).await;

    // Using partition queues should allow us to consume the partitions
    // in a round-robin fashion.
    {
        let consumer = Arc::new(
            utils::consumer::stream_consumer::create_stream_consumer_with_options(
                &kafka_context.bootstrap_servers,
                &rand_test_group(),
                &[],
            )
            .await
            .expect("could not create stream consumer"),
        );
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 2, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();

        let mut partition_streams: Vec<_> = (0..3)
            .map(|i| consumer.split_partition_queue(&topic_name, i).unwrap())
            .collect();

        for _ in 0..4 {
            let main_message =
                time::timeout(Duration::from_millis(100), consumer.stream().next()).await;
            assert!(main_message.is_err());

            for (i, stream) in partition_streams.iter_mut().enumerate() {
                let queue_message = stream.recv().await.unwrap();
                assert_eq!(queue_message.partition(), i as i32);
            }
        }
    }

    // When not all partitions have been split into separate queues, the
    // unsplit partitions should still be accessible via the main queue.
    {
        let consumer = Arc::new(
            utils::consumer::stream_consumer::create_stream_consumer_with_options(
                &kafka_context.bootstrap_servers,
                &rand_test_group(),
                &[],
            )
            .await
            .expect("could not create stream consumer"),
        );
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 2, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();

        let partition1 = consumer.split_partition_queue(&topic_name, 1).unwrap();

        let mut i = 0;
        while i < 5 {
            if let Ok(m) = time::timeout(Duration::from_millis(1000), consumer.recv()).await {
                // retry on transient errors until we get a message
                let m = match m {
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::BrokerTransportFailure,
                    ))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut)) => {
                        continue;
                    }
                    Err(err) => {
                        panic!("Unexpected error receiving message: {:?}", err);
                    }
                    Ok(m) => m,
                };
                let partition: i32 = m.partition();
                assert!(partition == 0 || partition == 2);
                i += 1;
            } else {
                panic!("Timeout receiving message");
            }

            if let Ok(m) = time::timeout(Duration::from_millis(1000), partition1.recv()).await {
                // retry on transient errors until we get a message
                let m = match m {
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::BrokerTransportFailure,
                    ))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut)) => {
                        continue;
                    }
                    Err(err) => {
                        panic!("Unexpected error receiving message: {:?}", err);
                    }
                    Ok(m) => m,
                };
                assert_eq!(m.partition(), 1);
                i += 1;
            } else {
                panic!("Timeout receiving message");
            }
        }
    }

    // Sending the queue to another task that is likely to outlive the
    // original thread should work. This is not idiomatic, as the consumer
    // should be continuously polled to serve callbacks, but it should not panic
    // or result in memory unsafety, etc.
    {
        let consumer = Arc::new(
            utils::consumer::stream_consumer::create_stream_consumer_with_options(
                &kafka_context.bootstrap_servers,
                &rand_test_group(),
                &[],
            )
            .await
            .expect("could not create stream consumer"),
        );
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();
        let stream = consumer.split_partition_queue(&topic_name, 0).unwrap();

        let worker = tokio::spawn({
            async move {
                for _ in 0..4 {
                    let stream_message = stream.recv().await.unwrap();
                    assert_eq!(stream_message.partition(), 0);
                }
            }
        });

        let main_message =
            time::timeout(Duration::from_millis(100), consumer.stream().next()).await;
        assert!(main_message.is_err());

        drop(consumer);
        worker.await.unwrap();
    }
}

// `test_produce_consume_base_incremental_assign_and_unassign` exercises the
// `incremental_assign`/`incremental_unassign` API on a manually-assigned
// consumer (no group join, so `rebalance_protocol` stays `None`). This test
// joins a group with `partition.assignment.strategy=cooperative-sticky`, drives
// the consumer until the initial assignment lands, and asserts that
// `rebalance_protocol()` reports `Cooperative`. A binding regression in the
// `rebalance_protocol` accessor (or that ignored the cooperative-sticky
// configuration) would fail this check.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_cooperative_sticky_rebalance_protocol() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_cooperative_sticky");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(2)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create future producer");
    for partition in 0..2 {
        producer
            .send(
                FutureRecord::to(&topic_name)
                    .partition(partition)
                    .key("k")
                    .payload("p"),
                Duration::from_secs(10),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("delivery failed: {}", e));
    }

    let consumer = utils::consumer::stream_consumer::create_stream_consumer_with_options(
        &kafka_context.bootstrap_servers,
        &rand_test_group(),
        &[("partition.assignment.strategy", "cooperative-sticky")],
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    assert!(
        matches!(consumer.rebalance_protocol(), RebalanceProtocol::None),
        "rebalance_protocol should be None before the first group join",
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let _ = time::timeout(Duration::from_secs(2), consumer.stream().next()).await;
        if consumer.assignment().unwrap().count() == 2 {
            break;
        }
    }
    let assignment = consumer.assignment().unwrap();
    assert_eq!(
        assignment.count(),
        2,
        "consumer should have been assigned both partitions, got {:?}",
        assignment
    );
    assert!(
        matches!(
            consumer.rebalance_protocol(),
            RebalanceProtocol::Cooperative
        ),
        "rebalance_protocol should report Cooperative after a cooperative-sticky join",
    );
}

// librdkafka treats subscription strings beginning with `^` as a regex
// pattern and resolves them against the broker's topic metadata on every
// metadata refresh. This test creates two topics that match a unique
// `^<rand>.*` pattern and one topic that does not, subscribes the consumer
// to the regex, drives the consumer until its assignment stabilizes, and
// asserts only the matching topics are present. A binding regression that
// passed the subscription string through unmodified (so `^` was lost) or
// that crossed up topic ownership would fail the topic-set comparison.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_regex_subscription_matches_only_prefixed() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let prefix = rand_test_topic("regex_match");
    let match1 = format!("{}_a", prefix);
    let match2 = format!("{}_b", prefix);
    let nonmatch = format!("other_{}", prefix);

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    for topic in [&match1, &match2, &nonmatch] {
        admin_client
            .create_topics(
                &admin::new_topic_vec(topic, Some(1)),
                &AdminOptions::default(),
            )
            .await
            .expect("could not create topic");
    }

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create future producer");
    for topic in [&match1, &match2, &nonmatch] {
        producer
            .send(
                FutureRecord::to(topic.as_str())
                    .partition(0)
                    .key("k")
                    .payload("p"),
                Duration::from_secs(10),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("delivery failed: {}", e));
    }

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    let pattern = format!("^{}.*", prefix);
    consumer.subscribe(&[pattern.as_str()]).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let expected: std::collections::HashSet<String> =
        [match1.clone(), match2.clone()].into_iter().collect();
    let mut observed: std::collections::HashSet<String> = std::collections::HashSet::new();
    while std::time::Instant::now() < deadline {
        match time::timeout(Duration::from_secs(2), consumer.stream().next()).await {
            Ok(Some(Ok(m))) => {
                observed.insert(m.topic().to_string());
            }
            Ok(Some(Err(e))) => panic!("stream error: {:?}", e),
            Ok(None) => panic!("stream ended"),
            Err(_) => {}
        }
        let assignment = consumer.assignment().unwrap();
        let assigned: std::collections::HashSet<String> = assignment
            .elements()
            .iter()
            .map(|e| e.topic().to_string())
            .collect();
        if assigned == expected {
            break;
        }
    }
    let assignment = consumer.assignment().unwrap();
    let assigned: std::collections::HashSet<String> = assignment
        .elements()
        .iter()
        .map(|e| e.topic().to_string())
        .collect();
    assert_eq!(
        assigned, expected,
        "regex subscription should converge to the two matching topics",
    );
    assert!(
        !observed.contains(&nonmatch),
        "non-matching topic {} should not be delivered",
        nonmatch
    );
}

// `test_produce_consume_with_timestamp` already calls `offsets_for_timestamp`,
// but only with two distinct timestamp values. This test produces a strictly
// monotonic sequence of distinct timestamps and queries at a midpoint, then
// verifies the returned offset is the first message at-or-after the query
// timestamp. A binding regression in `offsets_for_times` (or its `tpl`
// serialization) that returned the nearest neighbour, or the first ever
// offset, would fail this assertion.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_offsets_for_times_first_at_or_after() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_offsets_for_times_first_at_or_after");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create future producer");

    const N: i64 = 20;
    const BASE_TS: i64 = 1_700_000_000_000;
    const STEP: i64 = 100;
    for i in 0..N {
        let ts = BASE_TS + i * STEP;
        let payload = format!("ts-{}", ts);
        producer
            .send(
                FutureRecord::to(&topic_name)
                    .partition(0)
                    .key("k")
                    .payload(&payload)
                    .timestamp(ts),
                Duration::from_secs(10),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("delivery failed: {}", e));
    }

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let exact_query_ts = BASE_TS + 7 * STEP;
    let exact_tpl = consumer
        .offsets_for_timestamp(exact_query_ts, Duration::from_secs(10))
        .expect("offsets_for_timestamp failed for exact timestamp");
    let exact_offset = exact_tpl
        .find_partition(&topic_name, 0)
        .expect("missing partition entry")
        .offset();
    assert_eq!(
        exact_offset,
        Offset::Offset(7),
        "exact-timestamp query should return the matching offset",
    );

    let midpoint_query_ts = BASE_TS + 7 * STEP + STEP / 2;
    let midpoint_tpl = consumer
        .offsets_for_timestamp(midpoint_query_ts, Duration::from_secs(10))
        .expect("offsets_for_timestamp failed for midpoint timestamp");
    let midpoint_offset = midpoint_tpl
        .find_partition(&topic_name, 0)
        .expect("missing partition entry")
        .offset();
    assert_eq!(
        midpoint_offset,
        Offset::Offset(8),
        "midpoint-timestamp query should return the first offset at-or-after the query",
    );

    let after_query_ts = BASE_TS + (N + 10) * STEP;
    let after_tpl = consumer
        .offsets_for_timestamp(after_query_ts, Duration::from_secs(10))
        .expect("offsets_for_timestamp failed for after-end timestamp");
    let after_offset = after_tpl
        .find_partition(&topic_name, 0)
        .expect("missing partition entry")
        .offset();
    assert_eq!(
        after_offset,
        Offset::End,
        "timestamp past the high watermark should return Offset::End",
    );
}

// `test_base_producer_headers` already covers the produce side via the
// delivery callback. This test covers the consume side: produce a message
// with a mixed set of headers (str-valued, byte-valued, empty, and explicitly
// null), consume it through a StreamConsumer, and verify
// `BorrowedMessage::headers` exposes each header in order with the correct
// key, value type, and value bytes. A binding regression in the headers
// FFI conversion path would surface here.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_reads_message_headers() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_reads_message_headers");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create future producer");

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "h-bytes",
            value: Some(&[0u8, 1, 2, 3][..]),
        })
        .insert(Header {
            key: "h-str",
            value: Some("v-str"),
        })
        .insert(Header {
            key: "h-empty",
            value: Some(&[][..]),
        })
        .insert::<Vec<u8>>(Header {
            key: "h-null",
            value: None,
        });

    producer
        .send(
            FutureRecord::to(&topic_name)
                .partition(0)
                .key("k")
                .payload("p")
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|(e, _)| panic!("delivery failed: {}", e));

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let message = time::timeout(Duration::from_secs(15), consumer.stream().next())
        .await
        .expect("timed out waiting for consumed message")
        .expect("stream ended unexpectedly")
        .expect("error receiving message");

    let received = message
        .headers()
        .expect("consumed message should expose headers");
    assert_eq!(received.count(), 4);
    assert_eq!(
        received.get(0),
        Header {
            key: "h-bytes",
            value: Some(&[0u8, 1, 2, 3][..]),
        }
    );
    assert_eq!(
        received.get_as::<str>(1),
        Ok(Header {
            key: "h-str",
            value: Some("v-str"),
        })
    );
    assert_eq!(
        received.get_as::<[u8]>(2),
        Ok(Header {
            key: "h-empty",
            value: Some(&[][..]),
        })
    );
    assert_eq!(
        received.get_as::<[u8]>(3),
        Ok(Header {
            key: "h-null",
            value: None,
        })
    );
    let collected: Vec<_> = received.iter().collect();
    assert_eq!(
        collected,
        vec![
            Header {
                key: "h-bytes",
                value: Some(&[0u8, 1, 2, 3][..]),
            },
            Header {
                key: "h-str",
                value: Some(b"v-str" as &[u8]),
            },
            Header {
                key: "h-empty",
                value: Some(&[][..]),
            },
            Header {
                key: "h-null",
                value: None,
            },
        ],
    );
}
