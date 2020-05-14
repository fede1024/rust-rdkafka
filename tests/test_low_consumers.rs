//! Test data consumption using low level consumers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::current_time_millis;
use rdkafka::{Message, Timestamp};

use crate::utils::*;

mod utils;

fn create_base_consumer(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> BaseConsumer<ConsumerTestContext> {
    consumer_config(group_id, config_overrides)
        .create_with_context(ConsumerTestContext {
            _n: 64,
            wakeups: Arc::new(AtomicUsize::new(0)),
        })
        .expect("Consumer creation failed")
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
        .create_with_context(ConsumerTestContext {
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
