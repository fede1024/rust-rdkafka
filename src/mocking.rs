//! Mocking functionality
//!
//! Provides a mock Kafka cluster with a configurable number of brokers that support a reasonable
//! subset of Kafka protocol operations, error injection, etc.
//!
//! There are two ways to use the mock clusters, the most simple approach is to configure
//! `test.mock.num.brokers` (to e.g. 3) in an existing application, which will replace the
//! configured `bootstrap.servers` with the mock cluster brokers.
//!
//! This approach is convenient to easily test existing applications.
//!
//! The second approach is to explicitly create a mock cluster by using `MockCluster::new`

use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::os::raw::c_int;
use std::time::Duration;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::producer::DefaultProducerContext;
use crate::ClientContext;

/// Used internally by `MockCluster` to distinguish whether the mock cluster is owned or referenced.
///
/// The mock cluster can be created in two ways:
///
/// - With `rd_kafka_mock_cluster_new()`. In this case the caller of the c-tor is responsible
///   for destroying the returned mock cluster instance.
///
/// - By setting `test.mock.num.brokers` in a configuration of a producer/consumer client.
///   In this case, the client creates the mock cluster internally and destroys it in its d-tor,
///   and we only hold a reference to the mock cluster obtained with `rd_kafka_handle_mock_cluster()` (cf. `Client::mock_cluser()`).
///
///   In this case, we **must neither** destroy the mock clsuter in `MockCluster`'s `drop()`,
///   **nor** outlive the `Client` from which the reference is obtained, hence the lifetime.
enum MockClusterClient<'c, C: ClientContext> {
    Owned(Client<C>),
    Ref(&'c Client<C>),
}

/// Mock Kafka cluster with a configurable number of brokers that support a reasonable subset of
/// Kafka protocol operations, error injection, etc.
///
/// Mock clusters provide localhost listeners that can be used as the bootstrap
/// servers by multiple Kafka client instances.
///
/// Currently supported functionality:
/// - Producer
/// - Idempotent Producer
/// - Transactional Producer
/// - Low-level consumer
/// - High-level balanced consumer groups with offset commits
/// - Topic Metadata and auto creation
///
/// The mock cluster can be either created with [`MockCluster::new()`]
/// or by configuring the `test.mock.num.brokers` property when creating a producer/consumer.
/// This will override that producer/consumer's bootstrap servers setting and internally
/// create a mock cluster. You can then obtain this mock cluster using [`Client::mock_cluster()`].
///
/// Warning THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
///
/// [`MockCluster::new()`]: MockCluster::new()
/// [`Client::mock_cluster()`]: crate::client::Client::mock_cluster()
pub struct MockCluster<'c, C: ClientContext> {
    mock_cluster: *mut RDKafkaMockCluster,
    client: MockClusterClient<'c, C>,
}

/// Utility macro to simplify returns for operations done on the mock API
macro_rules! return_mock_op {
    ($op:expr) => {
        match $op {
            err if err.is_error() => Err(KafkaError::MockCluster(err.into())),
            _ => Ok(()),
        }
    };
}

/// Used to denote an explictly configured coordinator
pub enum MockCoordinator {
    /// Mock out coordination by a given transaction id
    Transaction(String),
    /// Mock out coordination by a given group id
    Group(String),
}

impl MockCluster<'static, DefaultProducerContext> {
    /// Create new mock cluster with a given number of brokers
    pub fn new(broker_count: i32) -> KafkaResult<Self> {
        let config = ClientConfig::new();
        let native_config = config.create_native_config()?;
        let context = DefaultProducerContext {};

        let client = Client::new(
            &config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            context,
        )?;

        let kafka_ptr = client.native_ptr();
        let mock_cluster = unsafe { rdsys::rd_kafka_mock_cluster_new(kafka_ptr, broker_count) };
        if mock_cluster.is_null() {
            return Err(KafkaError::MockCluster(rdsys::RDKafkaErrorCode::Fail));
        }

        Ok(MockCluster {
            mock_cluster,
            client: MockClusterClient::Owned(client),
        })
    }
}

impl<'c, C> MockCluster<'c, C>
where
    C: ClientContext,
{
    pub(crate) fn new_ref(mock_cluster: *mut RDKafkaMockCluster, client: &'c Client<C>) -> Self {
        Self {
            mock_cluster,
            client: MockClusterClient::Ref(client),
        }
    }

    /// Obtain the bootstrap address for the mock cluster
    pub fn bootstrap_servers(&self) -> String {
        let raw =
            unsafe { CStr::from_ptr(rdsys::rd_kafka_mock_cluster_bootstraps(self.mock_cluster)) };
        raw.to_str()
            .expect("Unexpected non-Unicode characters in bootstrap servers")
            .to_string()
    }

    /// Create a topic
    ///
    /// This is an alternative to automatic topic creation as performed by the client itself.
    ///
    /// NOTE: The Topic Admin API (CreateTopics) is not supported by the mock broker
    pub fn create_topic(
        &self,
        topic: &str,
        partition_count: i32,
        replication_factor: i32,
    ) -> KafkaResult<()> {
        let raw_topic = CString::new(topic).unwrap();
        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_topic_create(
                    self.mock_cluster,
                    raw_topic.as_ptr(),
                    partition_count,
                    replication_factor,
                )
            }
        }
    }

    /// Sets the parititon leader
    ///
    /// The topic will be created if it does not exist.
    ///
    /// `broker_id` needs to be an existing broker, or None to make the partition leader-less.
    pub fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
        broker_id: Option<i32>,
    ) -> KafkaResult<()> {
        let raw_topic = CString::new(topic).unwrap();
        let broker_id = broker_id.unwrap_or(-1);

        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_partition_set_leader(
                    self.mock_cluster,
                    raw_topic.as_ptr(),
                    partition,
                    broker_id,
                )
            }
        }
    }

    /// Sets the partitions preferred replica / follower.
    ///
    /// The topic will be created if it does not exist.
    ///
    /// `broker_id` does not need to point to an existing broker.
    pub fn partition_follower(
        &self,
        topic: &str,
        partition: i32,
        broker_id: i32,
    ) -> KafkaResult<()> {
        let raw_topic = CString::new(topic).unwrap();

        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_partition_set_follower(
                    self.mock_cluster, raw_topic.as_ptr(), partition, broker_id)
            }
        }
    }

    /// Set the partitions preferred replicate / follower low and high watermarks.
    ///
    /// The topic will be created if it does not exist.
    ///
    /// Setting an offset to `Non` will revert back to the leaders corresponding watermark.
    pub fn follower_watermarks(
        &self,
        topic: &str,
        partition: i32,
        low_watermark: Option<i64>,
        high_watermark: Option<i64>,
    ) -> KafkaResult<()> {
        let raw_topic = CString::new(topic).unwrap();
        let low_watermark = low_watermark.unwrap_or(-1);
        let high_watermark = high_watermark.unwrap_or(-1);

        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_partition_set_follower_wmarks(
                    self.mock_cluster,
                    raw_topic.as_ptr(),
                    partition,
                    low_watermark,
                    high_watermark
                )
            }
        }
    }

    /// Disconnects the broker and disallows any new connections.
    ///
    /// NOTE: This does NOT trigger leader change.
    pub fn broker_down(&self, broker_id: i32) -> KafkaResult<()> {
        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_broker_set_down(self.mock_cluster, broker_id)
            }
        }
    }

    /// brief Makes the broker accept connections again.
    ///
    /// NOTE: This does NOT trigger leader change.
    pub fn broker_up(&self, broker_id: i32) -> KafkaResult<()> {
        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_broker_set_up(self.mock_cluster, broker_id)
            }
        }
    }

    /// Set broker round-trip-time delay in milliseconds.
    pub fn broker_round_trip_time(&self, broker_id: i32, delay: Duration) -> KafkaResult<()> {
        let rtt_ms = delay.as_millis().try_into().unwrap_or(c_int::MAX);

        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_broker_set_rtt(
                    self.mock_cluster,
                    broker_id,
                    rtt_ms
                )
            }
        }
    }

    /// Sets the broker's rack as reported in Metadata to the client.
    pub fn broker_rack(&self, broker_id: i32, rack: &str) -> KafkaResult<()> {
        let raw_rack = CString::new(rack).unwrap();
        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_broker_set_rack(
                    self.mock_cluster,
                    broker_id,
                    raw_rack.as_ptr()
                )
            }
        }
    }

    /// Explicitly sets the coordinator.
    ///
    /// If this API is not a standard hashing scheme will be used.
    ///
    /// `broker_id` does not need to point to an existing broker.`
    pub fn coordinator(&self, coordinator: MockCoordinator, broker_id: i32) -> KafkaResult<()> {
        let (kind, key) = match coordinator {
            MockCoordinator::Transaction(key) => ("transaction", key),
            MockCoordinator::Group(key) => ("group", key),
        };

        let raw_kind = CString::new(kind).unwrap();
        let raw_key = CString::new(key).unwrap();

        return_mock_op! {
            unsafe {
                rdsys::rd_kafka_mock_coordinator_set(
                    self.mock_cluster,
                    raw_kind.as_ptr(),
                    raw_key.as_ptr(),
                    broker_id
                )
            }
        }
    }
}

impl<'c, C> Drop for MockCluster<'c, C>
where
    C: ClientContext,
{
    fn drop(&mut self) {
        if let MockClusterClient::Owned(..) = self.client {
            unsafe {
                rdsys::rd_kafka_mock_cluster_destroy(self.mock_cluster);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::consumer::{Consumer, StreamConsumer};
    use crate::message::ToBytes;
    use crate::producer::{FutureProducer, FutureRecord};
    use crate::Message;
    use tokio;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockcluster() {
        const TOPIC: &str = "test_topic";
        let mock_cluster = MockCluster::new(2).unwrap();

        let bootstrap_servers = mock_cluster.bootstrap_servers();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Producer creation error");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "rust-rdkafka-mockcluster-test")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Client creation error");

        let rec = FutureRecord::to(TOPIC).key(b"msg1").payload(b"test");
        producer.send_result(rec).unwrap().await.unwrap().unwrap();

        consumer.subscribe(&[TOPIC]).unwrap();

        let msg = consumer.recv().await.unwrap();
        assert_eq!(msg.key(), Some("msg1".to_bytes()));
        assert_eq!(msg.payload(), Some("test".to_bytes()));
    }
}
