//! Test data consumption using low level consumers.

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::admin::AdminOptions;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::{current_time_millis, Timeout};
use rdkafka::{ClientConfig, Message, Timestamp};

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer;
use crate::utils::rand::*;
use crate::utils::*;

mod utils;

// Seeking should allow replaying messages and skipping messages.
#[tokio::test]
async fn test_produce_consume_seek() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_produce_consume_seek");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    produce_messages_to_partition(&producer, &topic_name, 5, 0).await;

    let group_id = rand_test_group();
    let consumer =
        utils::consumer::create_base_consumer(&kafka_context.bootstrap_servers, &group_id, None)
            .expect("could not create base consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    for (i, message) in consumer.iter().take(3).enumerate() {
        match message {
            Ok(message) => assert_eq!(message.offset(), i as i64),
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
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_produce_consume_seek_partitions");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");
    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    produce_messages(&producer, &topic_name, 30, None, None).await;

    let group_id = rand_test_group();
    let consumer =
        utils::consumer::create_base_consumer(&kafka_context.bootstrap_servers, &group_id, None)
            .expect("could not create base consumer");
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
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let start_time = current_time_millis();
    let topic_name = rand_test_topic("test_produce_consume_iter");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    let message_map = produce_messages(&producer, &topic_name, 100, None, None).await;

    let group_id = rand_test_group();
    let consumer =
        utils::consumer::create_base_consumer(&kafka_context.bootstrap_servers, &group_id, None)
            .expect("could not create base consumer");
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

    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_pause_resume_consumer_iter");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");
    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    produce_messages_to_partition(&producer, &topic_name, MESSAGE_COUNT as usize, 0).await;
    let group_id = rand_test_group();
    let consumer =
        utils::consumer::create_base_consumer(&kafka_context.bootstrap_servers, &group_id, None)
            .expect("could not create base consumer");
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
            &admin::new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");
    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    produce_messages_to_partition(&producer, &topic_name, 4, 0).await;
    produce_messages_to_partition(&producer, &topic_name, 4, 1).await;
    produce_messages_to_partition(&producer, &topic_name, 4, 2).await;

    // Using partition queues should allow us to consume the partitions
    // in a round-robin fashion.
    {
        let group_id = rand_test_group();
        let consumer = Arc::new(
            utils::consumer::create_base_consumer(
                &kafka_context.bootstrap_servers,
                &group_id,
                None,
            )
            .expect("could not create base consumer"),
        );
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
        let group_id = rand_test_group();
        let consumer = Arc::new(
            utils::consumer::create_base_consumer(
                &kafka_context.bootstrap_servers,
                &group_id,
                None,
            )
            .expect("could not create base consumer"),
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
        while i < 12 {
            if let Some(m) = consumer.poll(Timeout::After(Duration::from_secs(0))) {
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
                        continue;
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
        let group_id = rand_test_group();
        let consumer = Arc::new(
            utils::consumer::create_base_consumer(
                &kafka_context.bootstrap_servers,
                &group_id,
                None,
            )
            .expect("could not create base consumer"),
        );
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

#[tokio::test]
async fn test_produce_consume_message_queue_nonempty_callback() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_produce_consume_message_queue_nonempty_callback");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let group_id = rand_test_group();
    let consumer =
        utils::consumer::create_base_consumer(&kafka_context.bootstrap_servers, &group_id, None)
            .expect("could not create base consumer");
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

    // Let any startup events drain through. apache/kafka 3.7.x posts an
    // event to the split partition queue during initial position setup
    // (the partition is assigned at Offset::Beginning, so librdkafka has
    // to query the log start offset), which invokes the nonempty
    // callback once before any messages exist. 3.8+ doesn't show this.
    // Capture the post-setup wakeup count as our baseline and assert
    // deltas from here on.
    thread::sleep(Duration::from_secs(1));
    let baseline = wakeups.load(Ordering::SeqCst);

    // Verify there are no messages waiting.
    assert!(consumer.poll(Duration::from_secs(0)).is_none());
    assert!(queue.poll(Duration::from_secs(0)).is_none());

    // Populate the topic, and expect a wakeup notifying us of the new messages.
    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");
    produce_messages(&producer, &topic_name, 2, None, None).await;
    wait_for_wakeups(baseline + 1);

    // Read one of the messages.
    assert!(queue.poll(Duration::from_secs(0)).is_some());

    // Add more messages to the topic. Expect no additional wakeups, as the
    // queue is not fully drained, for 1s.
    produce_messages(&producer, &topic_name, 2, None, None).await;
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), baseline + 1);

    // Drain the queue.
    assert!(queue.poll(None).is_some());
    assert!(queue.poll(None).is_some());
    assert!(queue.poll(None).is_some());

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), baseline + 1);

    // Add another message, and expect a wakeup.
    produce_messages(&producer, &topic_name, 1, None, None).await;
    wait_for_wakeups(baseline + 2);

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), baseline + 2);

    // Disable the queue and add another message.
    queue.set_nonempty_callback(|| ());
    produce_messages(&producer, &topic_name, 1, None, None).await;

    // Expect no additional wakeups for 1s.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(wakeups.load(Ordering::SeqCst), baseline + 2);
}

//TODO: adjust the test to work, today set_nonempty_callback param is never called.
// The test is disabled for now, as it is not working.
// #[tokio::test]
// async fn test_produce_consume_consumer_nonempty_callback() {
//     let _r = env_logger::try_init();

//     let topic_name = rand_test_topic("test_produce_consume_consumer_nonempty_callback");

//     create_topic(&topic_name, 1).await;

//     // Turn off statistics to prevent interference with the wakeups counter.
//     let mut config_overrides = HashMap::new();
//     config_overrides.insert("statistics.interval.ms", "0");

//     let mut consumer: BaseConsumer<_> = consumer_config(&rand_test_group(), Some(config_overrides))
//         .create_with_context(ConsumerTestContext { _n: 64 })
//         .expect("Consumer creation failed");

//     let mut tpl = TopicPartitionList::new();
//     tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
//         .unwrap();
//     consumer.assign(&tpl).unwrap();

//     let wakeups = Arc::new(AtomicUsize::new(0));
//     consumer.set_nonempty_callback({
//         let wakeups = wakeups.clone();
//         move || {
//             print!("Setting nonempty callback");
//             wakeups.fetch_add(1, Ordering::SeqCst);
//         }
//     });

//     let wait_for_wakeups = |target| {
//         let start = Instant::now();
//         let timeout = Duration::from_secs(15);
//         loop {
//             let w = wakeups.load(Ordering::SeqCst);
//             match w.cmp(&target) {
//                 std::cmp::Ordering::Equal => break,
//                 std::cmp::Ordering::Greater => panic!("wakeups {} exceeds target {}", w, target),
//                 std::cmp::Ordering::Less => (),
//             };
//             thread::sleep(Duration::from_millis(100));
//             if start.elapsed() > timeout {
//                 panic!("timeout exceeded while waiting for wakeup");
//             }
//         }
//     };

//     // Initiate connection.
//     assert!(consumer.poll(Duration::from_secs(0)).is_none());

//     // Expect no wakeups for 1s.
//     thread::sleep(Duration::from_secs(1));
//     assert_eq!(wakeups.load(Ordering::SeqCst), 0);

//     // Verify there are no messages waiting.
//     assert!(consumer.poll(Duration::from_secs(0)).is_none());

//     // Populate the topic, and expect a wakeup notifying us of the new messages.
//     populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
//     wait_for_wakeups(1);

//     // Read one of the messages.
//     assert!(consumer.poll(Duration::from_secs(0)).is_some());

//     // Add more messages to the topic. Expect no additional wakeups, as the
//     // queue is not fully drained, for 1s.
//     populate_topic(&topic_name, 2, &value_fn, &key_fn, None, None).await;
//     thread::sleep(Duration::from_secs(1));
//     assert_eq!(wakeups.load(Ordering::SeqCst), 1);

//     // Drain the queue.
//     assert!(consumer.poll(None).is_some());
//     assert!(consumer.poll(None).is_some());
//     assert!(consumer.poll(None).is_some());

//     // Expect no additional wakeups for 1s.
//     thread::sleep(Duration::from_secs(1));
//     assert_eq!(wakeups.load(Ordering::SeqCst), 1);

//     // Add another message, and expect a wakeup.
//     populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;
//     wait_for_wakeups(2);

//     // Expect no additional wakeups for 1s.
//     thread::sleep(Duration::from_secs(1));
//     assert_eq!(wakeups.load(Ordering::SeqCst), 2);

//     // Disable the queue and add another message.
//     consumer.set_nonempty_callback(|| ());
//     populate_topic(&topic_name, 1, &value_fn, &key_fn, None, None).await;

//     // Expect no additional wakeups for 1s.
//     thread::sleep(Duration::from_secs(1));
//     assert_eq!(wakeups.load(Ordering::SeqCst), 2);
// }

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

#[derive(Clone, Debug, PartialEq, Eq)]
enum RebalanceEventKind {
    Assign,
    Revoke,
    Error,
}

#[derive(Clone, Debug)]
struct RebalanceEvent {
    kind: RebalanceEventKind,
    partitions: Vec<(String, i32)>,
}

#[derive(Clone)]
struct RecordingRebalanceContext {
    pre: Arc<Mutex<Vec<RebalanceEvent>>>,
    post: Arc<Mutex<Vec<RebalanceEvent>>>,
}

impl RecordingRebalanceContext {
    fn new() -> Self {
        Self {
            pre: Arc::new(Mutex::new(Vec::new())),
            post: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn drain(&self) -> (Vec<RebalanceEvent>, Vec<RebalanceEvent>) {
        let pre = self.pre.lock().unwrap().clone();
        let post = self.post.lock().unwrap().clone();
        (pre, post)
    }
}

fn record_rebalance(rebalance: &Rebalance) -> RebalanceEvent {
    match rebalance {
        Rebalance::Assign(tpl) => RebalanceEvent {
            kind: RebalanceEventKind::Assign,
            partitions: tpl
                .elements()
                .iter()
                .map(|e| (e.topic().to_string(), e.partition()))
                .collect(),
        },
        Rebalance::Revoke(tpl) => RebalanceEvent {
            kind: RebalanceEventKind::Revoke,
            partitions: tpl
                .elements()
                .iter()
                .map(|e| (e.topic().to_string(), e.partition()))
                .collect(),
        },
        Rebalance::Error(_) => RebalanceEvent {
            kind: RebalanceEventKind::Error,
            partitions: Vec::new(),
        },
    }
}

impl ClientContext for RecordingRebalanceContext {}

impl ConsumerContext for RecordingRebalanceContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.pre.lock().unwrap().push(record_rebalance(rebalance));
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.post.lock().unwrap().push(record_rebalance(rebalance));
    }
}

fn build_recording_consumer(
    bootstrap_servers: &str,
    group_id: &str,
) -> BaseConsumer<RecordingRebalanceContext> {
    let mut config = ClientConfig::new();
    config
        .set("group.id", group_id)
        .set("bootstrap.servers", bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    config
        .create_with_context::<RecordingRebalanceContext, BaseConsumer<RecordingRebalanceContext>>(
            RecordingRebalanceContext::new(),
        )
        .expect("could not create recording base consumer")
}

#[tokio::test]
async fn test_consumer_rebalance_callbacks() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_consumer_rebalance_callbacks");
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

    let group = rand_test_group();

    let consumer1 = build_recording_consumer(&kafka_context.bootstrap_servers, &group);
    let context1 = consumer1.context().clone();
    consumer1.subscribe(&[topic_name.as_str()]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        consumer1.poll(Duration::from_millis(200));
        let assignment = consumer1.assignment().unwrap();
        if assignment.count() == 2 {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "consumer1 never got both partitions; assignment count {}",
                assignment.count()
            );
        }
    }

    let consumer2 = build_recording_consumer(&kafka_context.bootstrap_servers, &group);
    let context2 = consumer2.context().clone();
    consumer2.subscribe(&[topic_name.as_str()]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        consumer1.poll(Duration::from_millis(200));
        consumer2.poll(Duration::from_millis(200));
        let a1 = consumer1.assignment().unwrap().count();
        let a2 = consumer2.assignment().unwrap().count();
        if a1 == 1 && a2 == 1 {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "rebalance did not converge to one partition per consumer: c1={}, c2={}",
                a1, a2
            );
        }
    }

    let (pre1, post1) = context1.drain();
    let (pre2, post2) = context2.drain();

    assert!(
        pre1.iter().any(|e| e.kind == RebalanceEventKind::Assign),
        "consumer1 never observed a pre-rebalance Assign event: {:?}",
        pre1
    );
    assert!(
        post1.iter().any(|e| e.kind == RebalanceEventKind::Assign),
        "consumer1 never observed a post-rebalance Assign event: {:?}",
        post1
    );
    let first_assign1 = post1
        .iter()
        .find(|e| e.kind == RebalanceEventKind::Assign)
        .expect("missing initial assign on consumer1");
    assert_eq!(
        first_assign1.partitions.len(),
        2,
        "consumer1's first post-rebalance assign should hold both partitions, got {:?}",
        first_assign1.partitions
    );
    for (topic, _) in &first_assign1.partitions {
        assert_eq!(topic, &topic_name);
    }
    assert!(
        post1.iter().any(|e| e.kind == RebalanceEventKind::Revoke),
        "consumer1 never observed a post-rebalance Revoke event after consumer2 joined: {:?}",
        post1
    );

    assert!(
        pre2.iter().any(|e| e.kind == RebalanceEventKind::Assign),
        "consumer2 never observed a pre-rebalance Assign event: {:?}",
        pre2
    );
    let assign2 = post2
        .iter()
        .find(|e| e.kind == RebalanceEventKind::Assign)
        .expect("consumer2 never observed a post-rebalance Assign event");
    assert_eq!(
        assign2.partitions.len(),
        1,
        "consumer2 should have been assigned exactly one partition, got {:?}",
        assign2.partitions
    );
    assert_eq!(assign2.partitions[0].0, topic_name);
}

fn poll_skipping_transient<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    timeout: Duration,
) -> KafkaResult<BorrowedMessage<'_>> {
    loop {
        match consumer.poll(Timeout::from(timeout)) {
            Some(Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::BrokerTransportFailure | RDKafkaErrorCode::AllBrokersDown,
            ))) => continue,
            Some(result) => return result,
            None => panic!("No message or error received within timeout"),
        }
    }
}

#[tokio::test]
async fn test_partition_eof_error_details() {
    init_test_logger();
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_partition_eof_error_details");
    let message_count = 5usize;

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
        .expect("could not create producer");
    produce_messages_to_partition(&producer, &topic_name, message_count, 0).await;

    let consumer = utils::consumer::create_base_consumer(
        &kafka_context.bootstrap_servers,
        &rand_test_group(),
        Some(&[("enable.partition.eof", "true")]),
    )
    .expect("could not create base consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let timeout = Duration::from_secs(5);

    for received in 0..message_count {
        let message = poll_skipping_transient(&consumer, timeout)
            .unwrap_or_else(|e| panic!("Error receiving message: {:?}", e));
        assert_eq!(message.offset(), received as i64);
        assert_eq!(message.partition(), 0);
        assert_eq!(message.topic(), topic_name);
    }

    match poll_skipping_transient(&consumer, timeout) {
        Err(KafkaError::PartitionEOF(tpo)) => {
            assert_eq!(tpo.topic, topic_name);
            assert_eq!(tpo.partition, 0);
            assert_eq!(tpo.offset, message_count as i64);
        }
        Ok(_) => panic!("Expected PartitionEOF error, got message"),
        Err(e) => panic!("Expected PartitionEOF error, got: {:?}", e),
    }
}
