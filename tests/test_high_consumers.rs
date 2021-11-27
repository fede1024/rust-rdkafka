//! Test data consumption using high level consumers.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use futures::future::{self, FutureExt};
use futures::stream::StreamExt;
use maplit::hashmap;
use tokio::time::{self, Duration};

use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::current_time_millis;
use rdkafka::{Message, Timestamp};
use rdkafka_sys::types::RDKafkaConfRes;

use crate::utils::*;

mod utils;

// Create stream consumer for tests
fn create_stream_consumer(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> StreamConsumer<ConsumerTestContext> {
    let cons_context = ConsumerTestContext { _n: 64 };
    create_stream_consumer_with_context(group_id, config_overrides, cons_context)
}

fn create_stream_consumer_with_context<C>(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
    context: C,
) -> StreamConsumer<C>
where
    C: ConsumerContext + 'static,
{
    consumer_config(group_id, config_overrides)
        .create_with_context(context)
        .expect("Consumer creation failed")
}

#[tokio::test]
async fn test_invalid_max_poll_interval() {
    let res: Result<StreamConsumer, _> = consumer_config(
        &rand_test_group(),
        Some(hashmap! { "max.poll.interval.ms" => "-1" }),
    )
    .create();
    match res {
        Err(KafkaError::ClientConfig(RDKafkaConfRes::RD_KAFKA_CONF_INVALID, desc, key, value)) => {
            assert_eq!(desc, "Configuration property \"max.poll.interval.ms\" value -1 is outside allowed range 1..86400000\n");
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
    let _r = env_logger::try_init();

    let start_time = current_time_millis();
    let topic_name = rand_test_topic();
    let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, None, None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .stream()
        .take(100)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    let id = message_map[&(m.partition(), m.offset())];
                    match m.timestamp() {
                        Timestamp::CreateTime(timestamp) => assert!(timestamp >= start_time),
                        _ => panic!("Expected createtime for message timestamp"),
                    };
                    assert_eq!(m.payload_view::<str>().unwrap().unwrap(), value_fn(id));
                    assert_eq!(m.key_view::<str>().unwrap().unwrap(), key_fn(id));
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
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 100, &value_fn, &key_fn, None, None).await;

    let consumer = Arc::new(create_stream_consumer(&rand_test_group(), None));
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

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
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
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

// All produced messages should be consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_consume_with_timestamp() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    let message_map =
        populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), Some(1111)).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
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

    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), Some(999_999)).await;

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

// TODO: add check that commit cb gets called correctly
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_commit_message() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 11, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 12, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
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
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 11, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 12, &value_fn, &key_fn, Some(2), None).await;
    let mut config = HashMap::new();
    config.insert("enable.auto.offset.store", "false");
    config.insert("enable.partition.eof", "true");
    let consumer = create_stream_consumer(&rand_test_group(), Some(config));
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
    let _ = env_logger::try_init();

    let topic_name = rand_test_topic();
    let group_name = rand_test_group();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, None, None).await;

    let create_consumer = || async {
        // Disable auto-commit so we can manually drive the commits.
        let mut config = HashMap::new();
        config.insert("enable.auto.commit", "false");
        let consumer = create_stream_consumer(&group_name, Some(config));

        // Subscribe to the topic and wait for at least one message, which
        // ensures that the consumer group has been joined and such.
        consumer.subscribe(&[topic_name.as_str()])?;
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

#[tokio::test]
async fn test_consume_partition_order() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    // Using partition queues should allow us to consume the partitions
    // in a round-robin fashion.
    {
        let consumer = Arc::new(create_stream_consumer(&rand_test_group(), None));
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
        let consumer = Arc::new(create_stream_consumer(&rand_test_group(), None));
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
        while i < 12 {
            if let Some(m) = consumer.recv().now_or_never() {
                let partition = m.unwrap().partition();
                assert!(partition == 0 || partition == 2);
                i += 1;
            }

            if let Some(m) = partition1.recv().now_or_never() {
                assert_eq!(m.unwrap().partition(), 1);
                i += 1;
            }
        }
    }

    // Sending the queue to another task that is likely to outlive the
    // original thread should work. This is not idiomatic, as the consumer
    // should be continuously polled to serve callbacks, but it should not panic
    // or result in memory unsafety, etc.
    {
        let consumer = Arc::new(create_stream_consumer(&rand_test_group(), None));
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
