//! Kafka producers.
//!
//! ## The C librdkafka producer
//!
//! Rust-rdkafka relies on the C librdkafka producer to communicate with Kafka,
//! so in order to understand how the Rust producers work it is important to
//! understand the basics of the C one as well.
//!
//! ### Async
//!
//! The librdkafka producer is completely asynchronous: it maintains a memory
//! buffer where messages waiting to be sent or currently in flight are stored.
//! Once a message is delivered or an error occurred and the maximum number of
//! retries has been reached, the producer will enqueue a delivery event with
//! the appropriate delivery result into an internal event queue.
//!
//! The librdkafka user is responsible for calling the `poll` function at
//! regular intervals to process those events; the thread calling `poll` will be
//! the one executing the user-specified delivery callback for every delivery
//! event. If `poll` is not called, or not frequently enough, the producer will
//! return a [`RDKafkaErrorCode::QueueFull`] error and it won't be able to send
//! any other message until more delivery events are processed via `poll`. The
//! `QueueFull` error can also be returned if Kafka is not able to receive the
//! messages quickly enough.
//!
//! ### Error reporting
//!
//! The C library will try deal with all the transient errors such as broker
//! disconnection, timeouts etc. These errors, called global errors, are
//! automatically logged in rust-rdkafka, but they normally don't require any
//! handling as they are automatically handled internally. To see the logs, make
//! sure you initialize the logger.
//!
//! As mentioned earlier, errors specific to message production will be reported
//! in the delivery callback.
//!
//! ### Buffering
//!
//! Buffering is done automatically by librdkafka. When `send` is called, the
//! message is enqueued internally and once enough messages have been enqueued,
//! or when enough time has passed, they will be sent to Kafka as a single
//! batch. You can control the behavior of the buffer by configuring the the
//! `queue.buffering.max.*` parameters listed below.
//!
//! ## `rust-rdkafka` producers
//!
//! `rust-rdkafka` (rdkafka for brevity) provides two sets of producers: low
//! level and high level.
//!
//! ### Low-level producers
//!
//! The lowest level producer provided by rdkafka is called [`BaseProducer`].
//! The goal of the `BaseProducer` is to be as close as possible to the C one
//! while maintaining a safe Rust interface. In particular, the `BaseProducer`
//! needs to be polled at regular intervals to execute any delivery callback
//! that might be waiting and to make sure the queue doesn't fill up.
//!
//! Another low lever producer is the [`ThreadedProducer`], which is a
//! `BaseProducer` with a dedicated thread for polling.
//!
//! The delivery callback can be defined using a `ProducerContext`. See the
//! [`base_producer`] module for more information.
//!
//! ### High-level producer
//!
//! At the moment the only high level producer implemented is the
//! [`FutureProducer`]. The `FutureProducer` doesn't rely on user-defined
//! callbacks to notify the delivery or failure of a message; instead, this
//! information will be returned in a Future. The `FutureProducer` also uses an
//! internal thread that is used for polling, which makes calling poll
//! explicitly not necessary. The returned future will contain information about
//! the delivered message in case of success, or a copy of the original message
//! in case of failure. Additional computation can be chained to the returned
//! future, and it will executed by the future executor once the value is
//! available (for more information, check the documentation of the futures
//! crate).
//!
//! ## Transactions
//!
//! All rust-rdkafka producers support transactions. Transactional producers
//! work together with transaction-aware consumers configured with the default
//! `isolation.level` of `read_committed`.
//!
//! To configure a producer for transactions set `transactional.id` to an
//! identifier unique to the application when creating the producer. After
//! creating the producer, you must initialize it with
//! [`Producer::init_transactions`].
//!
//! To start a new transaction use [`Producer::begin_transaction`]. There can be
//! **only one ongoing transaction** at a time per producer. All records sent
//! after starting a transaction and before committing or aborting it will
//! automatically be associated with that transaction.
//!
//! Once you have initialized transactions on a producer, you are not permitted
//! to produce messages outside of a transaction.
//!
//! Consumer offsets can be sent as part of the ongoing transaction using
//! `send_offsets_to_transaction` and will be committed atomically with the
//! other records sent in the transaction.
//!
//! The current transaction can be committed with
//! [`Producer::commit_transaction`] or aborted using
//! [`Producer::abort_transaction`]. Afterwards, a new transaction can begin.
//!
//! ### Errors
//!
//! Errors returned by transaction methods may:
//!
//! * be retriable ([`RDKafkaError::is_retriable`]), in which case the operation
//!   that encountered the error may be retried.
//! * require abort ([`RDKafkaError::txn_requires_abort`], in which case the
//!   current transaction must be aborted and a new transaction begun.
//! * be fatal ([`RDKafkaError::is_fatal`]), in which case the producer must be
//!   stopped and the application terminated.
//!
//! For more details about transactions, see the [Transactional Producer]
//! section of the librdkafka introduction.
//!
//! ## Configuration
//!
//! ### Producer configuration
//!
//! For the configuration parameters common to both producers and consumers,
//! refer to the documentation in the `config` module. Here are listed the most
//! commonly used producer configuration. Click
//! [here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
//! for the full list.
//!
//! - `queue.buffering.max.messages`: Maximum number of messages allowed on the
//!   producer queue. Default: 100000.
//! - `queue.buffering.max.kbytes`: Maximum total message size sum allowed on
//!   the producer queue. This property has higher priority than
//!   queue.buffering.max.messages. Default: 4000000.
//! - `queue.buffering.max.ms`: Delay in milliseconds to wait for messages in
//!   the producer queue to accumulate before sending a request to the brokers.
//!   A higher value allows larger and more effective (less overhead, improved
//!   compression) batches of messages to accumulate at the expense of increased
//!   message delivery latency. Default: 0.
//! - `message.send.max.retries`: How many times to retry sending a failing
//!   batch. Note: retrying may cause reordering. Default: 2.
//! - `compression.codec`: Compression codec to use for compressing message
//!   sets. Default: none.
//! - `request.required.acks`: This field indicates how many acknowledgements
//!   the leader broker must receive from ISR brokers before responding to the
//!   request: 0=Broker does not send any response/ack to client, 1=Only the
//!   leader broker will need to ack the message, -1 or all=broker will block
//!   until message is committed by all in sync replicas (ISRs) or broker's
//!   in.sync.replicas setting before sending response. Default: 1.
//! - `request.timeout.ms`: The ack timeout of the producer request in
//!   milliseconds. This value is only enforced by the broker and relies on
//!   request.required.acks being != 0. Default: 5000.
//! - `message.timeout.ms`: Local message timeout. This value is only enforced
//!   locally and limits the time a produced message waits for successful
//!   delivery. A time of 0 is infinite. Default: 300000.
//!
//! [`RDKafkaErrorCode::QueueFull`]: crate::error::RDKafkaErrorCode::QueueFull
//! [`RDKafkaError::is_retriable`]: crate::error::RDKafkaError::is_retriable
//! [`RDKafkaError::txn_requires_abort`]: crate::error::RDKafkaError::txn_requires_abort
//! [`RDKafkaError::is_fatal`]: crate::error::RDKafkaError::is_fatal
//! [Transactional Producer]: https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#transactional-producer

use std::sync::Arc;

use crate::client::{Client, ClientContext};
use crate::consumer::ConsumerGroupMetadata;
use crate::error::KafkaResult;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::{IntoOpaque, Timeout};

pub mod base_producer;
pub mod future_producer;

#[doc(inline)]
pub use self::base_producer::{BaseProducer, BaseRecord, DeliveryResult, ThreadedProducer};
#[doc(inline)]
pub use self::future_producer::{DeliveryFuture, FutureProducer, FutureRecord};

//
// ********** PRODUCER CONTEXT **********
//

/// Producer-specific context.
///
/// This user-defined object can be used to provide custom callbacks for
/// producer events. Refer to the list of methods to check which callbacks can
/// be specified. It can also specify custom partitioner to register and to be
/// used for deciding to which partition write message into.
///
/// In particular, it can be used to specify the `delivery` callback that will
/// be called when the acknowledgement for a delivered message is received.
///
/// See also the [`ClientContext`] trait.
pub trait ProducerContext<Part: Partitioner = NoCustomPartitioner>: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque;

    /// This method will be called once the message has been delivered (or
    /// failed to). The `DeliveryOpaque` will be the one provided by the user
    /// when calling send.
    fn delivery(&self, delivery_result: &DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque);

    /// This method is called when creating producer in order to optionally register custom partitioner.
    /// If custom partitioner is not used then `partitioner` configuration property is used (or its default).
    ///
    /// sticky.partitioning.linger.ms must be 0 to run custom partitioner for messages with null key.
    /// See https://github.com/confluentinc/librdkafka/blob/081fd972fa97f88a1e6d9a69fc893865ffbb561a/src/rdkafka_msg.c#L1192-L1196
    fn get_custom_partitioner(&self) -> Option<&Part> {
        None
    }
}

/// Unassigned partition.
/// See RD_KAFKA_PARTITION_UA from librdkafka.
pub const PARTITION_UA: i32 = -1;

/// Trait allowing to customize the partitioning of messages.
pub trait Partitioner {
    /// Return partition to use for `topic_name`.
    /// `topic_name` is the name of a topic to which a message is being produced.
    /// `partition_cnt` is the number of partitions for this topic.
    /// `key` is an optional key of the message.
    /// `is_partition_available` is a function that can be called to check if a partition has an active leader broker.
    ///
    /// It may be called in any thread at any time,
    /// It may be called multiple times for the same message/key.
    /// MUST NOT block or execute for prolonged periods of time.
    /// MUST return a value between 0 and partition_cnt-1, or the
    /// special RD_KAFKA_PARTITION_UA value if partitioning could not be performed.
    /// See documentation for rd_kafka_topic_conf_set_partitioner_cb from librdkafka for more info.
    fn partition(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        is_partition_available: impl Fn(i32) -> bool,
    ) -> i32;
}

/// Placeholder used when no custom partitioner is needed.
#[derive(Clone)]
pub struct NoCustomPartitioner {}

impl Partitioner for NoCustomPartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        _key: Option<&[u8]>,
        _partition_cnt: i32,
        _is_paritition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        panic!("NoCustomPartitioner should not be called");
    }
}

/// An inert producer context that can be used when customizations are not
/// required.
#[derive(Clone)]
pub struct DefaultProducerContext;

impl ClientContext for DefaultProducerContext {}
impl ProducerContext<NoCustomPartitioner> for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

/// Common trait for all producers.
pub trait Producer<C = DefaultProducerContext, Part = NoCustomPartitioner>
where
    Part: Partitioner,
    C: ProducerContext<Part>,
{
    /// Returns the [`Client`] underlying this producer.
    fn client(&self) -> &Client<C>;

    /// Returns a reference to the [`ProducerContext`] used to create this
    /// producer.
    fn context(&self) -> &Arc<C> {
        self.client().context()
    }

    /// Returns the number of messages that are either waiting to be sent or are
    /// sent but are waiting to be acknowledged.
    fn in_flight_count(&self) -> i32;

    /// Flushes any pending messages.
    ///
    /// This method should be called before termination to ensure delivery of
    /// all enqueued messages. It will call `poll()` internally.
    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Purge messages currently handled by the producer instance.
    ///
    /// See the [`PurgeConfig`] documentation for the list of flags that may be provided.
    ///
    /// If providing an empty set of flags, nothing will be purged.
    ///
    /// The application will need to call `::poll()` or `::flush()`
    /// afterwards to serve the delivery report callbacks of the purged messages.
    ///
    /// Messages purged from internal queues fail with the delivery report
    /// error code set to
    /// [`KafkaError::MessageProduction(RDKafkaErrorCode::PurgeQueue)`](crate::error::RDKafkaErrorCode::PurgeQueue),
    /// while purged messages that are in-flight to or from the broker will fail
    /// with the error code set to
    /// [`KafkaError::MessageProduction(RDKafkaErrorCode::PurgeInflight)`](crate::error::RDKafkaErrorCode::PurgeInflight).
    ///
    /// This call may block for a short time while background thread queues are purged.
    fn purge(&self, flags: PurgeConfig);

    /// Enable sending transactions with this producer.
    ///
    /// # Prerequisites
    ///
    /// * The configuration used to create the producer must include a
    ///   `transactional.id` setting.
    /// * You must not have sent any messages or called any of the other
    ///   transaction-related functions.
    ///
    /// # Details
    ///
    /// This function ensures any transactions initiated by previous producers
    /// with the same `transactional.id` are completed. Any transactions left
    /// open by any such previous producers will be aborted.
    ///
    /// Once previous transactions have been fenced, this function acquires an
    /// internal producer ID and epoch that will be used by all transactional
    /// messages sent by this producer.
    ///
    /// If this function returns successfully, messages may only be sent to this
    /// producer when a transaction is active. See
    /// [`Producer::begin_transaction`].
    ///
    /// This function may block for the specified `timeout`.
    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Begins a new transaction.
    ///
    /// # Prerequisites
    ///
    /// You must have successfully called [`Producer::init_transactions`].
    ///
    /// # Details
    ///
    /// This function begins a new transaction, and implicitly associates that
    /// open transaction with this producer.
    ///
    /// After a successful call to this function, any messages sent via this
    /// producer or any calls to [`Producer::send_offsets_to_transaction`] will
    /// be implicitly associated with this transaction, until the transaction is
    /// finished.
    ///
    /// Finish the transaction by calling [`Producer::commit_transaction`] or
    /// [`Producer::abort_transaction`].
    ///
    /// While a transaction is open, you must perform at least one transaction
    /// operation every `transaction.timeout.ms` to avoid timing out the
    /// transaction on the broker.
    fn begin_transaction(&self) -> KafkaResult<()>;

    /// Associates an offset commit operation with this transaction.
    ///
    /// # Prerequisites
    ///
    /// The producer must have an open transaction via a call to
    /// [`Producer::begin_transaction`].
    ///
    /// # Details
    ///
    /// Sends a list of topic partition offsets to the consumer group
    /// coordinator for `cgm`, and marks the offsets as part of the current
    /// transaction. These offsets will be considered committed only if the
    /// transaction is committed successfully.
    ///
    /// The offsets should be the next message your application will consume,
    /// i.e., one greater than the the last processed message's offset for each
    /// partition.
    ///
    /// Use this method at the end of a consume-transform-produce loop, prior to
    /// comitting the transaction with [`Producer::commit_transaction`].
    ///
    /// This function may block for the specified `timeout`.
    ///
    /// # Hints
    ///
    /// To obtain the correct consumer group metadata, call
    /// [`Consumer::group_metadata`] on the consumer for which offsets are being
    /// committed.
    ///
    /// The consumer must not have automatic commits enabled.
    ///
    /// [`Consumer::group_metadata`]: crate::consumer::Consumer::group_metadata
    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()>;

    /// Commits the current transaction.
    ///
    /// # Prerequisites
    ///
    /// The producer must have an open transaction via a call to
    /// [`Producer::begin_transaction`].
    ///
    /// # Details
    ///
    /// Any outstanding messages will be flushed (i.e., delivered) before
    /// actually committing the transaction.
    ///
    /// If any of the outstanding messages fail permanently, the current
    /// transaction will enter an abortable error state and this function will
    /// return an abortable error. You must then call
    /// [`Producer::abort_transaction`] before attemping to create another
    /// transaction.
    ///
    /// This function may block for the specified `timeout`.
    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Aborts the current transaction.
    ///
    /// # Prerequisites
    ///
    /// The producer must have an open transaction via a call to
    /// [`Producer::begin_transaction`].
    ///
    /// # Details
    ///
    /// Any oustanding messages will be purged and failed with
    /// [`RDKafkaErrorCode::PurgeInflight`] or [`RDKafkaErrorCode::PurgeQueue`].
    ///
    /// This function should also be used to recover from non-fatal abortable
    /// transaction errors.
    ///
    /// This function may block for the specified `timeout`.
    ///
    /// [`RDKafkaErrorCode::PurgeInflight`]: crate::error::RDKafkaErrorCode::PurgeInflight
    /// [`RDKafkaErrorCode::PurgeQueue`]: crate::error::RDKafkaErrorCode::PurgeQueue
    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;
}

/// Settings to provide to [`Producer::purge`] to parametrize the purge behavior
///
/// `PurgeConfig::default()` corresponds to a setting where nothing is purged.
///
/// # Example
/// To purge both queued messages and in-flight messages:
/// ```
/// # use rdkafka::producer::PurgeConfig;
/// let settings = PurgeConfig::default().queue().inflight();
/// ```
#[derive(Default, Clone, Copy)]
pub struct PurgeConfig {
    flag_bits: i32,
}
impl PurgeConfig {
    /// Purge messages in internal queues. This does not purge inflight messages.
    #[inline]
    pub fn queue(self) -> Self {
        Self {
            flag_bits: self.flag_bits | rdkafka_sys::RD_KAFKA_PURGE_F_QUEUE,
        }
    }
    /// Purge messages in-flight to or from the broker.
    /// Purging these messages will void any future acknowledgements from the
    /// broker, making it impossible for the application to know if these
    /// messages were successfully delivered or not.
    /// Retrying these messages may lead to duplicates.
    ///
    /// This does not purge messages in internal queues.
    #[inline]
    pub fn inflight(self) -> Self {
        Self {
            flag_bits: self.flag_bits | rdkafka_sys::RD_KAFKA_PURGE_F_INFLIGHT,
        }
    }
    /// Don't wait for background thread queue purging to finish.
    #[inline]
    pub fn non_blocking(self) -> Self {
        Self {
            flag_bits: self.flag_bits | rdkafka_sys::RD_KAFKA_PURGE_F_NON_BLOCKING,
        }
    }
}

macro_rules! negative_and_debug_impls {
    ($($f: ident -> !$set_fn: ident,)*) => {
        impl PurgeConfig {
            $(
                #[inline]
                #[doc = concat!("Unsets the flag set by [`", stringify!($set_fn), "`](PurgeConfig::", stringify!($set_fn),")")]
                pub fn $f(self) -> Self {
                    Self {
                        flag_bits: self.flag_bits & !PurgeConfig::default().$set_fn().flag_bits,
                    }
                }
            )*
        }
        impl std::fmt::Debug for PurgeConfig {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // Simulate a struct that holds a set of booleans
                let mut d = f.debug_struct("PurgeConfig");
                $(
                    d.field(
                        stringify!($set_fn),
                        &((self.flag_bits & Self::default().$set_fn().flag_bits) != 0),
                    );
                )*
                d.finish()
            }
        }
    };
}
negative_and_debug_impls! {
    no_queue -> !queue,
    no_inflight -> !inflight,
    blocking -> !non_blocking,
}
