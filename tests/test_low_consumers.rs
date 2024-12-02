//! Test data consumption using low level consumers.

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::consumer::base_consumer::{ForwardMode, PartitionQueue};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::{current_time_millis, Timeout};
use rdkafka::{ClientConfig, Message, Timestamp};

use crate::utils::*;

mod utils;

fn create_base_consumer(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> BaseConsumer<ConsumerTestContext> {
    consumer_config(group_id, config_overrides)
        .create_with_context(ConsumerTestContext { _n: 64 })
        .expect("Consumer creation failed")
}

// Seeking should allow replaying messages and skipping messages.
#[tokio::test]
async fn test_produce_consume_seek() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic("test_produce_consume_seek");
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

    consumer
        .seek(&topic_name, 0, Offset::OffsetTail(3), None)
        .unwrap();

    for (i, message) in consumer.iter().take(2).enumerate() {
        match message {
            Ok(message) => assert_eq!(message.offset(), i as i64 + 2),
            Err(e) => panic!("Error receiving message: {:?}", e),
        }
    }

    consumer.seek(&topic_name, 0, Offset::End, None).unwrap();

    ensure_empty(&consumer, "There should be no messages left");

    // Validate that unrepresentable offsets are rejected.
    match consumer.seek(&topic_name, 0, Offset::Offset(-1), None) {
        Err(KafkaError::Seek(s)) => assert_eq!(s, "Local: Unrepresentable offset"),
        bad => panic!("unexpected return from invalid seek: {:?}", bad),
    }
    let mut tpl = TopicPartitionList::new();
    match tpl.add_partition_offset(&topic_name, 0, Offset::OffsetTail(-1)) {
        Err(KafkaError::SetPartitionOffset(RDKafkaErrorCode::InvalidArgument)) => (),
        bad => panic!(
            "unexpected return from invalid add_partition_offset: {:?}",
            bad
        ),
    }
    match tpl.set_all_offsets(Offset::OffsetTail(-1)) {
        Err(KafkaError::SetPartitionOffset(RDKafkaErrorCode::InvalidArgument)) => (),
        bad => panic!(
            "unexpected return from invalid add_partition_offset: {:?}",
            bad
        ),
    }
}

// Seeking should allow replaying messages and skipping messages.
#[tokio::test]
async fn test_produce_consume_seek_partitions() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic("test_produce_consume_seek_partitions");
    populate_topic(&topic_name, 30, &value_fn, &key_fn, None, None).await;

    let consumer = create_base_consumer(&rand_test_group(), None);
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let mut partition_offset_map = HashMap::new();
    for message in consumer.iter().take(30) {
        match message {
            Ok(m) => {
                let offset = partition_offset_map.entry(m.partition()).or_insert(0);
                assert_eq!(m.offset(), *offset);
                *offset += 1;
            }
            Err(e) => panic!("Error receiving message: {:?}", e),
        }
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 1, Offset::End)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 2, Offset::Offset(2))
        .unwrap();

    let r_tpl = consumer.seek_partitions(tpl, None).unwrap();
    assert_eq!(r_tpl.elements().len(), 3);
    for tpe in r_tpl.elements().iter() {
        assert!(tpe.error().is_ok());
    }

    let msg_cnt_p0 = partition_offset_map.get(&0).unwrap();
    let msg_cnt_p2 = partition_offset_map.get(&2).unwrap();
    let total_msgs_to_read = msg_cnt_p0 + (msg_cnt_p2 - 2);
    let mut poffset_map = HashMap::new();
    for message in consumer.iter().take(total_msgs_to_read.try_into().unwrap()) {
        match message {
            Ok(m) => {
                let offset = poffset_map.entry(m.partition()).or_insert(0);
                if m.partition() == 0 {
                    assert_eq!(m.offset(), *offset);
                } else if m.partition() == 2 {
                    assert_eq!(m.offset(), *offset + 2);
                } else if m.partition() == 1 {
                    panic!("Unexpected message from partition 1")
                }
                *offset += 1;
            }
            Err(e) => panic!("Error receiving message: {:?}", e),
        }
    }
    assert_eq!(msg_cnt_p0, poffset_map.get(&0).unwrap());
    assert_eq!(msg_cnt_p2 - 2, *poffset_map.get(&2).unwrap());
}

// All produced messages should be consumed.
#[tokio::test]
async fn test_produce_consume_iter() {
    let _r = env_logger::try_init();

    let start_time = current_time_millis();
    let topic_name = rand_test_topic("test_produce_consume_iter");
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

    let topic_name = rand_test_topic("test_pause_resume_consumer_iter");
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

#[tokio::test]
async fn test_consume_partition_order() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic("test_consume_partition_order");
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    // Using partition queues should allow us to consume the partitions
    // in a round-robin fashion.
    {
        let consumer = Arc::new(create_base_consumer(&rand_test_group(), None));
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
            .unwrap();
        tpl.add_partition_offset(&topic_name, 2, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();

        let partition_queues: Vec<_> = (0..3)
            .map(|i| consumer.split_partition_queue(&topic_name, i).unwrap())
            .collect();

        for _ in 0..4 {
            let main_message = consumer.poll(Timeout::After(Duration::from_secs(0)));
            assert!(main_message.is_none());

            for (i, queue) in partition_queues.iter().enumerate() {
                let queue_message = queue.poll(Timeout::Never).unwrap().unwrap();
                assert_eq!(queue_message.partition(), i as i32);
            }
        }
    }

    // When not all partitions have been split into separate queues, the
    // unsplit partitions should still be accessible via the main queue.
    {
        let consumer = Arc::new(create_base_consumer(&rand_test_group(), None));
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
            if let Some(m) = consumer.poll(Timeout::After(Duration::from_secs(0))) {
                // retry on transient errors until we get a message
                let m = match m {
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::BrokerTransportFailure,
                    ))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut)) => {
                        continue
                    }
                    Err(err) => {
                        panic!("Unexpected error receiving message: {:?}", err);
                    }
                    Ok(m) => m,
                };
                let partition = m.partition();
                assert!(partition == 0 || partition == 2);
                i += 1;
            }

            if let Some(m) = partition1.poll(Timeout::After(Duration::from_secs(0))) {
                // retry on transient errors until we get a message
                let m = match m {
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::BrokerTransportFailure,
                    ))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown))
                    | Err(KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut)) => {
                        continue
                    }
                    Err(err) => {
                        panic!("Unexpected error receiving message: {:?}", err);
                    }
                    Ok(m) => m,
                };
                assert_eq!(m.partition(), 1);
                i += 1;
            }
        }
    }

    // Sending the queue to another thread that is likely to outlive the
    // original thread should work. This is not idiomatic, as the consumer
    // should be continuously polled to serve callbacks, but it should not panic
    // or result in memory unsafety, etc.
    {
        let consumer = Arc::new(create_base_consumer(&rand_test_group(), None));
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();
        let queue = consumer.split_partition_queue(&topic_name, 0).unwrap();

        let worker = thread::spawn(move || {
            for _ in 0..4 {
                let queue_message = queue.poll(Timeout::Never).unwrap().unwrap();
                assert_eq!(queue_message.partition(), 0);
            }
        });

        consumer.poll(Duration::from_secs(0));
        drop(consumer);
        worker.join().unwrap();
    }
}

trait TestQueue: Sized {
    fn poll_one(&self) -> BorrowedMessage<'_>;

    fn assert_consume(&mut self, expected: &[usize]) {
        let mut seen = vec![0; expected.len()];
        for _ in 0..expected.iter().sum() {
            let partition = self.poll_one().partition();
            seen[partition as usize] += 1;
        }

        // Check that we've received all messages from expected partitions.
        assert_eq!(&seen, expected);
    }
}

impl TestQueue for Arc<BaseConsumer<ConsumerTestContext>> {
    fn poll_one(&self) -> BorrowedMessage<'_> {
        self.poll(Timeout::Never).unwrap().unwrap()
    }
}

impl TestQueue for PartitionQueue<ConsumerTestContext> {
    fn poll_one(&self) -> BorrowedMessage<'_> {
        self.poll(Timeout::Never).unwrap().unwrap()
    }
}

// Forward queue back to consumer's queue.
#[inline]
fn reset_forwarding(queue: &mut PartitionQueue<ConsumerTestContext>) {
    queue.forward(ForwardMode::Consumer).unwrap();
}

// Remove all forwarding from given queue.
#[inline]
fn clear_forwarding(queue: &mut PartitionQueue<ConsumerTestContext>) {
    queue.forward(ForwardMode::Clear).unwrap();
}

#[tokio::test]
async fn test_partition_queue_forwarding() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    create_topic(&topic_name, 3).await;

    let mut consumer = Arc::new(create_base_consumer(&rand_test_group(), None));
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 2, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let mut q0 = consumer.split_partition_queue(&topic_name, 0).unwrap();
    let mut q1 = consumer.split_partition_queue(&topic_name, 1).unwrap();
    let mut q2 = consumer.split_partition_queue(&topic_name, 2).unwrap();

    // Forward q0 into q1, but keep q0 around.
    q0.forward(&mut q1).expect("Partition queue forward failed");

    // Worker consuming from (q0 + q1) should poll 8 messages from partitions 1 and 2 combined
    let worker_1 = thread::spawn(move || {
        q1.assert_consume(&[4, 4, 0]);
        q1
    });

    // Worker consuming from q2 should see just 4 messages from partition 2
    let worker_2 = thread::spawn(move || {
        q2.assert_consume(&[0, 0, 4]);
        q2
    });

    consumer.poll(Duration::from_secs(0));

    // Join to retrieve q1 and q2
    let mut q1 = worker_1.join().unwrap();
    let mut q2 = worker_2.join().unwrap();

    // Reset forwarding of q1 and q2 back to the main consumer queue
    reset_forwarding(&mut q1);
    reset_forwarding(&mut q2);

    // Generate new messages
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    // Worker consuming from q0 should now observe 4 messages in partition 0
    let worker_0 = thread::spawn(move || {
        q0.assert_consume(&[4, 0, 0]);
        reset_forwarding(&mut q0);
    });

    // Expect 4 messages from partitions 1 and 2 back in the main consumer queue
    consumer.assert_consume(&[0, 4, 4]);

    worker_0.join().unwrap();
}

#[tokio::test]
async fn test_partition_queue_clear_forwarding() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic();
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    create_topic(&topic_name, 3).await;

    let mut consumer = Arc::new(create_base_consumer(&rand_test_group(), None));
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 1, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(&topic_name, 2, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let mut q0 = consumer.split_partition_queue(&topic_name, 0).unwrap();
    let mut q1 = consumer.split_partition_queue(&topic_name, 1).unwrap();
    let mut q2 = consumer.split_partition_queue(&topic_name, 2).unwrap();

    // Forward q0 -> q1 -> q2
    q1.forward(&mut q2).expect("Partition queue forward failed");
    q0.forward(&mut q1).expect("Partition queue forward failed");

    // Check that a clear and re-forward still produces a q0 -> q1 -> q2 chain
    clear_forwarding(&mut q1);
    q1.forward(&mut q2).expect("Partition queue forward failed");

    // Worker consuming from q0 should see nothing due to q0 -> q1
    let worker_0 = thread::spawn(move || {
        q0.assert_consume(&[0, 0, 0]);
        q0
    });

    // Worker consuming from q1 should see nothing due to q1 -> q2
    let worker_1 = thread::spawn(move || {
        q1.assert_consume(&[0, 0, 0]);
        q1
    });

    // Worker consuming from q2 should see all 12 messages from q0 -> q1 -> q2
    let worker_2 = thread::spawn(move || {
        q2.assert_consume(&[4, 4, 4]);
        q2
    });

    // There should be no messages in the main consumer queue
    consumer.poll(Duration::from_secs(0));
    consumer.assert_consume(&[0, 0, 0]);

    // Join to retrieve all the queues back
    let mut q0 = worker_0.join().unwrap();
    let mut q1 = worker_1.join().unwrap();
    let mut q2 = worker_2.join().unwrap();

    // Clear forwarding on q0 and q1, making them independent again
    clear_forwarding(&mut q0);
    clear_forwarding(&mut q1);

    // Reset q2 back to the main consumer queue and dispose of the queue wrapper
    reset_forwarding(&mut q2);
    drop(q2);

    // Generate new messages
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(&topic_name, 4, &value_fn, &key_fn, Some(2), None).await;

    // Worker consuming from q0 should poll 4 messages from partitions 0
    let worker_0 = thread::spawn(move || {
        q0.assert_consume(&[4, 0, 0]);
    });

    // Worker consuming from q1 should poll 4 messages from partitions 1
    let worker_1 = thread::spawn(move || {
        q1.assert_consume(&[0, 4, 0]);
    });

    // Consumer should see the remaining 4 messages from partition/queue 2
    consumer.assert_consume(&[0, 0, 4]);

    worker_0.join().unwrap();
    worker_1.join().unwrap();
}

#[tokio::test]
async fn test_partition_queue_forwarding_client_mismatch() {
    let topic_name = rand_test_topic();
    create_topic(&topic_name, 1).await;

    let consumer_a: Arc<BaseConsumer> = Arc::new(ClientConfig::new().create().unwrap());
    let consumer_b: Arc<BaseConsumer> = Arc::new(ClientConfig::new().create().unwrap());

    let mut queue_a = consumer_a.split_partition_queue(&topic_name, 0).unwrap();
    let mut queue_b = consumer_b.split_partition_queue(&topic_name, 1).unwrap();

    // Both queues come from different underlying clients, so forwarding must fail.
    assert_eq!(
        queue_a.forward(&mut queue_b),
        Err(KafkaError::ClientMismatch)
    );
}

#[tokio::test]
async fn test_produce_consume_message_queue_nonempty_callback() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic("test_produce_consume_message_queue_nonempty_callback");

    create_topic(&topic_name, 1).await;

    let consumer: BaseConsumer<_> = consumer_config(&rand_test_group(), None)
        .create_with_context(ConsumerTestContext { _n: 64 })
        .expect("Consumer creation failed");
    let consumer = Arc::new(consumer);

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let wakeups = Arc::new(AtomicUsize::new(0));
    let mut queue = consumer.split_partition_queue(&topic_name, 0).unwrap();
    queue.set_nonempty_callback({
        let wakeups = wakeups.clone();
        move || {
            wakeups.fetch_add(1, Ordering::SeqCst);
        }
    });

    let wait_for_wakeups = |target| {
        let start = Instant::now();
        let timeout = Duration::from_secs(15);
        loop {
            let w = wakeups.load(Ordering::SeqCst);
            match w.cmp(&target) {
                std::cmp::Ordering::Equal => break,
                std::cmp::Ordering::Greater => panic!("wakeups {} exceeds target {}", w, target),
                std::cmp::Ordering::Less => (),
            };
            thread::sleep(Duration::from_millis(100));
            if start.elapsed() > timeout {
                panic!("timeout exceeded while waiting for wakeup");
            }
        }
    };

    // Initiate connection.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());

    // Expect no wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 0);

    // Verify there are no messages waiting.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());
    assert!(queue.poll(Duration::from_secs(0)).is_none());

    // Populate the topic, and expect a wakeup notifying us of the new messages.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(1);

    // Read one of the messages.
    assert!(queue.poll(Duration::from_secs(0)).is_some());

    // Add more messages to the topic. Expect no additional wakeups, as the
    // queue is not fully drained, for 1s.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 1);

    // Drain the queue.
    assert!(queue.poll(None).is_some());
    assert!(queue.poll(None).is_some());
    assert!(queue.poll(None).is_some());

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 1);

    // Add another message, and expect a wakeup.
    populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(2);

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);

    // Disable the queue and add another message.
    queue.set_nonempty_callback(|| ());
    populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_produce_consume_consumer_nonempty_callback() {
    let _r = env_logger::try_init();

    let topic_name = rand_test_topic("test_produce_consume_consumer_nonempty_callback");

    create_topic(&topic_name, 1).await;

    // Turn off statistics to prevent interference with the wakeups counter.
    let mut config_overrides = HashMap::new();
    config_overrides.insert("statistics.interval.ms", "0");

    let mut consumer: BaseConsumer<_> = consumer_config(&rand_test_group(), Some(config_overrides))
        .create_with_context(ConsumerTestContext { _n: 64 })
        .expect("Consumer creation failed");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let wakeups = Arc::new(AtomicUsize::new(0));
    consumer.set_nonempty_callback({
        let wakeups = wakeups.clone();
        move || {
            wakeups.fetch_add(1, Ordering::SeqCst);
        }
    });

    let wait_for_wakeups = |target| {
        let start = Instant::now();
        let timeout = Duration::from_secs(15);
        loop {
            let w = wakeups.load(Ordering::SeqCst);
            match w.cmp(&target) {
                std::cmp::Ordering::Equal => break,
                std::cmp::Ordering::Greater => panic!("wakeups {} exceeds target {}", w, target),
                std::cmp::Ordering::Less => (),
            };
            thread::sleep(Duration::from_millis(100));
            if start.elapsed() > timeout {
                panic!("timeout exceeded while waiting for wakeup");
            }
        }
    };

    // Initiate connection.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());

    // Expect no wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 0);

    // Verify there are no messages waiting.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());

    // Populate the topic, and expect a wakeup notifying us of the new messages.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(1);

    // Read one of the messages.
    assert!(consumer.poll(Duration::from_secs(0)).is_some());

    // Add more messages to the topic. Expect no additional wakeups, as the
    // queue is not fully drained, for 1s.
    populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 1);

    // Drain the queue.
    assert!(consumer.poll(None).is_some());
    assert!(consumer.poll(None).is_some());
    assert!(consumer.poll(None).is_some());

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 1);

    // Add another message, and expect a wakeup.
    populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;
    wait_for_wakeups(2);

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);

    // Disable the queue and add another message.
    consumer.set_nonempty_callback(|| ());
    populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_invalid_consumer_position() {
    // Regression test for #360, in which calling `position` on a consumer which
    // does not have a `group.id` configured segfaulted.
    let consumer: BaseConsumer = ClientConfig::new().create().unwrap();
    assert_eq!(
        consumer.position(),
        Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup))
    );
}
