//! Test data consumption using low level and high level consumers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use futures::{future, StreamExt};

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::current_time_millis;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics, Timestamp};

use crate::utils::*;

mod utils;

struct TestContext {
    _n: i64, // Add data for memory access validation
    wakeups: Arc<AtomicUsize>,
}

impl ClientContext for TestContext {
    // Access stats
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ConsumerContext for TestContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }

    fn message_queue_nonempty_callback(&self) {
        self.wakeups.fetch_add(1, Ordering::SeqCst);
    }
}

fn consumer_config(group_id: &str, config_overrides: Option<HashMap<&str, &str>>) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config.set("enable.partition.eof", "false");
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("statistics.interval.ms", "500");
    config.set("api.version.request", "true");
    config.set("debug", "all");
    config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}

// Create stream consumer for tests
fn create_stream_consumer(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> StreamConsumer<TestContext> {
    let cons_context = TestContext {
        _n: 64,
        wakeups: Arc::new(AtomicUsize::new(0)),
    };
    create_stream_consumer_with_context(group_id, config_overrides, cons_context)
}

fn create_stream_consumer_with_context<C: ConsumerContext>(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
    context: C,
) -> StreamConsumer<C> {
    consumer_config(group_id, config_overrides)
        .create_with_context(context)
        .expect("Consumer creation failed")
}

fn create_base_consumer(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> BaseConsumer<TestContext> {
    consumer_config(group_id, config_overrides)
        .create_with_context(TestContext {
            _n: 64,
            wakeups: Arc::new(AtomicUsize::new(0)),
        })
        .expect("Consumer creation failed")
}

// All produced messages should be consumed.
#[tokio::test]
async fn test_produce_consume_iter() {
    let _r = env_logger::try_init();

    let start_time = current_time_millis();
    let topic_name = rand_test_topic();
    let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, None, None).await;
    let consumer = create_base_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    for message in consumer.iter().take(100) {
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
        }
    }
}

// All produced messages should be consumed.
#[tokio::test]
async fn test_produce_consume_base() {
    let _r = env_logger::try_init();

    let start_time = current_time_millis();
    let topic_name = rand_test_topic();
    let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, None, None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .start()
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

// Seeking should allow replaying messages and skipping messages.
#[tokio::test]
async fn test_produce_consume_seek() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 5, &value_fn, &key_fn, Some(0), None).await;
    let consumer = create_base_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    for (i, message) in consumer.iter().take(3).enumerate() {
        match message {
            Ok(message) => assert_eq!(dbg!(message.offset()), i as i64),
            Err(e) => panic!("Error receiving message: {:?}", e),
        }
    }

    consumer
        .seek(&topic_name, 0, Offset::Offset(1), None)
        .unwrap();

    for (i, message) in consumer.iter().take(3).enumerate() {
        match message {
            Ok(message) => assert_eq!(message.offset(), i as i64 + 1),
            Err(e) => panic!("Error receiving message: {:?}", e),
        }
    }

    consumer.seek(&topic_name, 0, Offset::End, None).unwrap();

    ensure_empty(&consumer, "There should be no messages left");
}

// All produced messages should be consumed.
#[tokio::test]
async fn test_produce_consume_base_assign() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning);
    tpl.add_partition_offset(&topic_name, 1, Offset::Offset(2));
    tpl.add_partition_offset(&topic_name, 2, Offset::Offset(9));
    consumer.assign(&tpl).unwrap();

    let mut partition_count = vec![0, 0, 0];

    let _consumer_future = consumer
        .start()
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
#[tokio::test]
async fn test_produce_consume_with_timestamp() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    let message_map =
        populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), Some(1111)).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .start()
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

#[tokio::test]
async fn test_consume_with_no_message_error() {
    let _r = env_logger::try_init();

    let consumer = create_stream_consumer(&rand_test_group(), None);

    let mut message_stream = consumer.start_with(Duration::from_millis(200), true);

    let mut first_poll_time = None;
    let mut timeouts_count = 0;
    while let Some(message) = message_stream.next().await {
        match message {
            Err(KafkaError::NoMessageReceived) => {
                // TODO: use entry interface for Options once available
                if first_poll_time.is_none() {
                    first_poll_time = Some(Instant::now());
                }
                timeouts_count += 1;
                if timeouts_count == 26 {
                    break;
                }
            }
            Ok(m) => panic!("A message was actually received: {:?}", m),
            Err(e) => panic!("Unexpected error while receiving message: {:?}", e),
        };
    }

    assert_eq!(timeouts_count, 26);
    // It should take 5000ms
    println!("Duration: {:?}", first_poll_time.unwrap().elapsed());
    assert!(first_poll_time.unwrap().elapsed() < Duration::from_millis(7000));
    assert!(first_poll_time.unwrap().elapsed() > Duration::from_millis(4500));
}

// TODO: add check that commit cb gets called correctly
#[tokio::test]
async fn test_consumer_commit_message() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 10, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 11, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 12, &value_fn, &key_fn, Some(2), None).await;
    let consumer = create_stream_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let _consumer_future = consumer
        .start()
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
    assignment.add_partition_offset(&topic_name, 0, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 1, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed.add_partition_offset(&topic_name, 0, Offset::Invalid);
    committed.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    committed.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(committed, consumer.committed(timeout).unwrap());

    let mut position = TopicPartitionList::new();
    position.add_partition_offset(&topic_name, 0, Offset::Offset(10));
    position.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    position.add_partition_offset(&topic_name, 2, Offset::Offset(12));
    assert_eq!(position, consumer.position().unwrap());
}

#[tokio::test]
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
        .start()
        .take(36)
        .for_each(|message| {
            match message {
                Ok(m) => {
                    if m.partition() == 1 {
                        consumer.store_offset(&m).unwrap();
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
    assignment.add_partition_offset(&topic_name, 0, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 1, Offset::Invalid);
    assignment.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(assignment, consumer.assignment().unwrap());

    let mut committed = TopicPartitionList::new();
    committed.add_partition_offset(&topic_name, 0, Offset::Invalid);
    committed.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    committed.add_partition_offset(&topic_name, 2, Offset::Invalid);
    assert_eq!(committed, consumer.committed(timeout).unwrap());

    let mut position = TopicPartitionList::new();
    position.add_partition_offset(&topic_name, 0, Offset::Offset(10));
    position.add_partition_offset(&topic_name, 1, Offset::Offset(11));
    position.add_partition_offset(&topic_name, 2, Offset::Offset(12));
    assert_eq!(position, consumer.position().unwrap());
}

fn ensure_empty<C: ConsumerContext>(consumer: &BaseConsumer<C>, err_msg: &str) {
    const MAX_TRY_TIME: Duration = Duration::from_secs(2);
    let start = Instant::now();
    while start.elapsed() < MAX_TRY_TIME {
        assert!(consumer.poll(MAX_TRY_TIME).is_none(), "{}", err_msg);
    }
}

#[tokio::test]
async fn test_pause_resume_consumer_iter() {
    const PAUSE_COUNT: i32 = 3;
    const MESSAGE_COUNT: i32 = 300;
    const MESSAGES_PER_PAUSE: i32 = MESSAGE_COUNT / PAUSE_COUNT;

    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(
        &topic_name,
        MESSAGE_COUNT,
        &value_fn,
        &key_fn,
        Some(0),
        None,
    )
    .await;
    let group_id = rand_test_group();
    let consumer = create_base_consumer(&group_id, None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    for _ in 0..PAUSE_COUNT {
        let mut num_taken = 0;
        for message in consumer.iter().take(MESSAGES_PER_PAUSE as usize) {
            message.unwrap();
            num_taken += 1;
        }
        assert_eq!(num_taken, MESSAGES_PER_PAUSE);

        let partitions = consumer.assignment().unwrap();
        assert!(partitions.count() > 0);
        consumer.pause(&partitions).unwrap();

        ensure_empty(
            &consumer,
            "Partition is paused - we should not receive anything",
        );

        consumer.resume(&partitions).unwrap();
    }

    ensure_empty(&consumer, "There should be no messages left");
}

// All produced messages should be consumed.
#[tokio::test]
async fn test_produce_consume_message_queue_nonempty_callback() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();

    let consumer: BaseConsumer<_> = consumer_config(&rand_test_group(), None)
        .create_with_context(TestContext {
            _n: 64,
            wakeups: Arc::new(AtomicUsize::new(0)),
        })
        .expect("Consumer creation failed");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let wakeups = consumer.context().wakeups.clone();
    let wait_for_wakeups = |target| {
        let start = Instant::now();
        let timeout = Duration::from_secs(15);
        loop {
            let w = wakeups.load(Ordering::SeqCst);
            if w == target {
                break;
            } else if w > target {
                panic!("wakeups {} exceeds target {}", w, target);
            }
            thread::sleep(Duration::from_millis(100));
            if start.elapsed() > timeout {
                panic!("timeout exceeded while waiting for wakeup");
            }
        }
    };

    // Initiate connection.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());

    // Expect one initial rebalance callback.
    wait_for_wakeups(1);

    // Expect no additional wakeups for 1s.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 1);

    // Verify there are no messages waiting.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());

    // Populate the topic, and expect a wakeup notifying us of the new messages.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(2);

    // Read one of the messages.
    assert!(consumer.poll(Duration::from_secs(0)).is_some());

    // Add more messages to the topic. Expect no additional wakeups, as the
    // queue is not fully drained, for 1s.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);

    // Drain the consumer.
    assert_eq!(consumer.iter().take(3).count(), 3);

    // Expect no additional wakeups for 1s.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);

    // Add another message, and expect a wakeup.
    populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(3);
}
