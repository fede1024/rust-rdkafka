//! Test data production using low level producers.

use std::collections::HashSet;
use std::error::Error;
use std::ffi::CString;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use rdkafka::admin::AdminOptions;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{
    BaseProducer, BaseRecord, DeliveryResult, NoCustomPartitioner, Partitioner, Producer,
    ProducerContext, ThreadedProducer,
};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::current_time_millis;
use rdkafka::{ClientContext, Statistics};

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer::base_producer as base_producer_utils;
use crate::utils::rand::*;

mod utils;

struct PrintingContext {
    _n: i64, // Add data for memory access validation
}

impl ClientContext for PrintingContext {
    // Access and use all stats.
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ProducerContext for PrintingContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        println!("Delivery: {:?} {:?}", delivery_result, delivery_opaque);
    }
}

type TestProducerDeliveryResult = (OwnedMessage, Option<KafkaError>, usize);

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
    stats: Arc<Mutex<Vec<Statistics>>>,
    results: Arc<Mutex<Vec<TestProducerDeliveryResult>>>,
    partitioner: Option<Part>,
}

impl CollectingContext {
    fn new() -> CollectingContext {
        CollectingContext {
            stats: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(Vec::new())),
            partitioner: None,
        }
    }
}

impl<Part: Partitioner> CollectingContext<Part> {
    fn new_with_custom_partitioner(partitioner: Part) -> CollectingContext<Part> {
        CollectingContext {
            stats: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(Vec::new())),
            partitioner: Some(partitioner),
        }
    }
}

impl<Part: Partitioner + Send + Sync> ClientContext for CollectingContext<Part> {
    // Access and use all stats.
    fn stats(&self, stats: Statistics) {
        let mut stats_vec = self.stats.lock().unwrap();
        (*stats_vec).push(stats);
    }
}

impl<Part: Partitioner + Send + Sync> ProducerContext<Part> for CollectingContext<Part> {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        let mut results = self.results.lock().unwrap();
        match *delivery_result {
            Ok(ref message) => (*results).push((message.detach(), None, delivery_opaque)),
            Err((ref err, ref message)) => {
                (*results).push((message.detach(), Some(err.clone()), delivery_opaque))
            }
        }
    }

    fn get_custom_partitioner(&self) -> Option<&Part> {
        match &self.partitioner {
            None => None,
            Some(p) => Some(p),
        }
    }
}

// Partitioner sending all messages to single, defined partition.
#[derive(Clone)]
pub struct FixedPartitioner {
    partition: i32,
}

impl FixedPartitioner {
    fn new(partition: i32) -> Self {
        Self { partition }
    }
}

impl Partitioner for FixedPartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        _key: Option<&[u8]>,
        _partition_cnt: i32,
        _is_paritition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        self.partition
    }
}

#[derive(Clone)]
pub struct PanicPartitioner {}

impl Partitioner for PanicPartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        _key: Option<&[u8]>,
        _partition_cnt: i32,
        _is_paritition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        panic!("partition() panic");
    }
}

// TESTS

#[tokio::test(flavor = "multi_thread")]
async fn test_base_producer_queue_full() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_base_producer_queue_full");
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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        PrintingContext { _n: 123 },
        &[("queue.buffering.max.messages", "10")],
    )
    .expect("failed to create base producer");

    let results = (0..30)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("payload")
                    .key("key")
                    .timestamp(current_time_millis()),
            )
        })
        .collect::<Vec<_>>();
    while producer.in_flight_count() > 0 {
        producer.poll(Duration::from_millis(100));
    }

    let errors = results
        .iter()
        .filter(|&e| {
            matches!(
                e,
                &Err((
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                    _
                ))
            )
        })
        .count();

    let success = results.iter().filter(|&r| r.is_ok()).count();

    assert_eq!(results.len(), 30);
    assert_eq!(success, 10);
    assert_eq!(errors, 20);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_producer_timeout() {
    init_test_logger();

    let context = CollectingContext::new();
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_base_producer_timeout");

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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        context.clone(),
        &[("message.timeout.ms", "100")],
    )
    .expect("failed to create base producer");

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("A")
                    .key("B"),
            )
        })
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(results_count, 10);

    thread::sleep(Duration::from_secs(5)); // Make sure messages expire
    producer.flush(Duration::from_secs(10)).unwrap();

    let delivery_results = context.results.lock().unwrap();
    let mut ids = HashSet::new();
    for &(ref message, ref error, id) in &(*delivery_results) {
        assert_eq!(message.payload_view::<str>(), Some(Ok("A")));
        assert_eq!(message.key_view::<str>(), Some(Ok("B")));
        assert_eq!(
            error,
            &Some(KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageTimedOut
            ))
        );
        ids.insert(id);
    }
    assert_eq!(ids.len(), 10);
}

struct HeaderCheckContext {
    ids: Arc<Mutex<HashSet<usize>>>,
}

impl ClientContext for HeaderCheckContext {}

impl ProducerContext for HeaderCheckContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, delivery_result: &DeliveryResult, message_id: usize) {
        let message = delivery_result.as_ref().unwrap();
        if message_id % 2 == 0 {
            let headers = message.headers().unwrap();
            assert_eq!(headers.count(), 4);
            assert_eq!(
                headers.get(0),
                Header {
                    key: "header1",
                    value: Some(&[1, 2, 3, 4][..])
                }
            );
            assert_eq!(
                headers.get_as::<str>(1),
                Ok(Header {
                    key: "header2",
                    value: Some("value2")
                })
            );
            assert_eq!(
                headers.get_as::<[u8]>(2),
                Ok(Header {
                    key: "header3",
                    value: Some(&[][..])
                })
            );
            assert_eq!(
                headers.get_as::<[u8]>(3),
                Ok(Header {
                    key: "header4",
                    value: None
                })
            );
            let headers: Vec<_> = headers.iter().collect();
            assert_eq!(
                headers,
                &[
                    Header {
                        key: "header1",
                        value: Some(&[1, 2, 3, 4][..]),
                    },
                    Header {
                        key: "header2",
                        value: Some(b"value2"),
                    },
                    Header {
                        key: "header3",
                        value: Some(&[][..]),
                    },
                    Header {
                        key: "header4",
                        value: None,
                    },
                ],
            )
        } else {
            assert!(message.headers().is_none());
        }
        (*self.ids.lock().unwrap()).insert(message_id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_producer_headers() {
    init_test_logger();

    let ids_set = Arc::new(Mutex::new(HashSet::new()));
    let context = HeaderCheckContext {
        ids: ids_set.clone(),
    };
    let topic_name = rand_test_topic("test_base_producer_headers");
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        context,
        &[],
    )
    .expect("failed to create base producer");

    let results_count = (0..10)
        .map(|id| {
            let mut record = BaseRecord::with_opaque_to(&topic_name, id).payload("A");
            if id % 2 == 0 {
                record = record.headers(
                    OwnedHeaders::new()
                        .insert(Header {
                            key: "header1",
                            value: Some(&[1, 2, 3, 4]),
                        })
                        .insert(Header {
                            key: "header2",
                            value: Some("value2"),
                        })
                        .insert(Header {
                            key: "header3",
                            value: Some(&[]),
                        })
                        .insert::<Vec<u8>>(Header {
                            key: "header4",
                            value: None,
                        }),
                );
            }
            producer.send::<str, str>(record)
        })
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10)).unwrap();

    assert_eq!(results_count, 10);
    assert_eq!((*ids_set.lock().unwrap()).len(), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_threaded_producer_send() {
    init_test_logger();

    let context = CollectingContext::new();
    let topic_name = rand_test_topic("test_threaded_producer_send");
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
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

    let producer = base_producer_utils::create_threaded_producer_with_context(
        &kafka_context.bootstrap_servers,
        context.clone(),
        &[],
    )
    .expect("failed to create threaded producer");

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("A")
                    .key("B"),
            )
        })
        .filter(|r| r.is_ok())
        .count();

    assert_eq!(results_count, 10);
    producer.flush(Duration::from_secs(10)).unwrap();

    let delivery_results = context.results.lock().unwrap();
    let mut ids = HashSet::new();
    for &(ref message, ref error, id) in &(*delivery_results) {
        assert_eq!(message.payload_view::<str>(), Some(Ok("A")));
        assert_eq!(message.key_view::<str>(), Some(Ok("B")));
        assert_eq!(error, &None);
        ids.insert(id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_producer_opaque_arc() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    struct OpaqueArcContext {}
    impl ClientContext for OpaqueArcContext {}
    impl ProducerContext for OpaqueArcContext {
        type DeliveryOpaque = Arc<Mutex<usize>>;

        fn delivery(&self, _: &DeliveryResult, opaque: Self::DeliveryOpaque) {
            let mut shared_count = opaque.lock().unwrap();
            *shared_count += 1;
        }
    }

    let shared_count = Arc::new(Mutex::new(0));
    let context = OpaqueArcContext {};
    let topic_name = rand_test_topic("test_base_producer_opaque_arc");
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        context,
        &[],
    )
    .expect("failed to create base producer");

    let results_count = (0..10)
        .map(|_| {
            let record = BaseRecord::with_opaque_to(&topic_name, shared_count.clone()).payload("A");
            producer.send::<str, str>(record)
        })
        .filter(|r| r.is_ok())
        .count();

    producer.flush(Duration::from_secs(10)).unwrap();

    let shared_count = Arc::try_unwrap(shared_count).unwrap().into_inner()?;
    assert_eq!(results_count, shared_count);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fatal_errors() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        PrintingContext { _n: 123 },
        &[],
    )
    .expect("failed to create base producer");

    assert_eq!(producer.client().fatal_error(), None);

    let msg = CString::new("fake error").unwrap();
    unsafe {
        rdkafka_sys::rd_kafka_test_fatal_error(
            producer.client().native_ptr(),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
            msg.as_ptr(),
        );
    }

    assert_eq!(
        producer.client().fatal_error(),
        Some((
            RDKafkaErrorCode::OutOfOrderSequenceNumber,
            "test_fatal_error: fake error".into()
        ))
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_register_custom_partitioner_linger_non_zero_key_null() {
    // Custom partitioner is not used when sticky.partitioning.linger.ms > 0 and key is null.
    // https://github.com/confluentinc/librdkafka/blob/081fd972fa97f88a1e6d9a69fc893865ffbb561a/src/rdkafka_msg.c#L1192-L1196
    init_test_logger();

    let context = CollectingContext::new_with_custom_partitioner(PanicPartitioner {});
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_register_custom_partitioner_linger_non_zero_key_null");

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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        context.clone(),
        &[("sticky.partitioning.linger.ms", "10")],
    )
    .expect("failed to create base producer");

    producer
        .send(BaseRecord::<(), str, usize>::with_opaque_to(&topic_name, 0).payload(""))
        .unwrap();
    producer.flush(Duration::from_secs(10)).unwrap();

    let delivery_results = context.results.lock().unwrap();

    assert_eq!(delivery_results.len(), 1);

    for (_, error, _) in &(*delivery_results) {
        assert_eq!(*error, None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_partitioner_base_producer() {
    init_test_logger();

    let context = CollectingContext::new_with_custom_partitioner(FixedPartitioner::new(2));
    let topic_name = rand_test_topic("test_custom_partitioner_base_producer");
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
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

    let producer = base_producer_utils::create_base_producer_with_context(
        &kafka_context.bootstrap_servers,
        context.clone(),
        &[],
    )
    .expect("failed to create base producer");

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("")
                    .key(""),
            )
        })
        .filter(|r| r.is_ok())
        .count();

    assert_eq!(results_count, 10);
    producer.flush(Duration::from_secs(10)).unwrap();

    let delivery_results = context.results.lock().unwrap();

    for (message, error, _) in &(*delivery_results) {
        assert_eq!(error, &None);
        assert_eq!(message.partition(), 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_partitioner_threaded_producer() {
    init_test_logger();

    let context = CollectingContext::new_with_custom_partitioner(FixedPartitioner::new(2));
    let topic_name = rand_test_topic("test_custom_partitioner_threaded_producer");
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
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

    let producer = base_producer_utils::create_threaded_producer_with_context(
        &kafka_context.bootstrap_servers,
        context.clone(),
        &[],
    )
    .expect("failed to create threaded producer");

    let results_count = (0..10)
        .map(|id| {
            producer.send(
                BaseRecord::with_opaque_to(&topic_name, id)
                    .payload("")
                    .key(""),
            )
        })
        .filter(|r| r.is_ok())
        .count();

    assert_eq!(results_count, 10);
    producer.flush(Duration::from_secs(10)).unwrap();

    let delivery_results = context.results.lock().unwrap();

    for (message, error, _) in &(*delivery_results) {
        assert_eq!(error, &None);
        assert_eq!(message.partition(), 2);
    }
}
