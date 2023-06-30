//! Low-level Kafka producers.
//!
//! For more information about the producers provided in rdkafka, refer to the
//! [`producer`](super) module documentation.
//!
//! ## `BaseProducer`
//!
//! The [`BaseProducer`] is a low level Kafka producer designed to be as similar
//! as possible to the underlying C librdkafka producer, while maintaining a
//! safe Rust interface.
//!
//! Production of messages is fully asynchronous. The librdkafka producer will
//! take care of buffering requests together according to configuration, and to
//! send them efficiently. Once a message has been produced, or the retry count
//! reached, a callback function called delivery callback will be called.
//!
//! The `BaseProducer` requires a [`ProducerContext`] which will be used to
//! specify the delivery callback and the
//! [`DeliveryOpaque`](ProducerContext::DeliveryOpaque). The `DeliveryOpaque` is
//! a user-defined type that the user can pass to the `send` method of the
//! producer, and that the producer will then forward to the delivery callback
//! of the corresponding message. The `DeliveryOpaque` is useful in case the
//! delivery callback requires additional information about the message (such as
//! message id for example).
//!
//! ### Calling poll
//!
//! To execute delivery callbacks the `poll` method of the producer should be
//! called regularly. If `poll` is not called, or not often enough, a
//! [`RDKafkaErrorCode::QueueFull`] error will be returned.
//!
//! ## `ThreadedProducer`
//!
//! The `ThreadedProducer` is a wrapper around the `BaseProducer` which spawns a
//! thread dedicated to calling `poll` on the producer at regular intervals, so
//! that the user doesn't have to. The thread is started when the producer is
//! created, and it will be terminated once the producer goes out of scope.
//!
//! A [`RDKafkaErrorCode::QueueFull`] error can still be returned in case the
//! polling thread is not fast enough or Kafka is not able to receive data and
//! acknowledge messages quickly enough. If this error is returned, the caller
//! should wait and try again.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::slice;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rdkafka_sys as rdsys;
use rdkafka_sys::rd_kafka_vtype_t::*;
use rdkafka_sys::types::*;

use crate::client::Client;
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::ConsumerGroupMetadata;
use crate::error::{IsError, KafkaError, KafkaResult, RDKafkaError};
use crate::log::{trace, warn};
use crate::message::{BorrowedMessage, OwnedHeaders, ToBytes};
use crate::producer::{
    DefaultProducerContext, Partitioner, Producer, ProducerContext, PurgeConfig,
};
use crate::topic_partition_list::TopicPartitionList;
use crate::util::{IntoOpaque, Timeout};

pub use crate::message::DeliveryResult;

use super::NoCustomPartitioner;

/// Callback that gets called from librdkafka every time a message succeeds or fails to be
/// delivered.
unsafe extern "C" fn delivery_cb<Part: Partitioner, C: ProducerContext<Part>>(
    _client: *mut RDKafka,
    msg: *const RDKafkaMessage,
    opaque: *mut c_void,
) {
    let producer_context = &mut *(opaque as *mut C);
    let delivery_opaque = C::DeliveryOpaque::from_ptr((*msg)._private);
    let owner = 42u8;
    // Wrap the message pointer into a BorrowedMessage that will only live for the body of this
    // function.
    let delivery_result = BorrowedMessage::from_dr_callback(msg as *mut RDKafkaMessage, &owner);
    trace!("Delivery event received: {:?}", delivery_result);
    producer_context.delivery(&delivery_result, delivery_opaque);
    match delivery_result {
        // Do not free the message, librdkafka will do it for us
        Ok(message) | Err((_, message)) => mem::forget(message),
    }
}

//
// ********** BASE PRODUCER **********
//

/// A record for the [`BaseProducer`] and [`ThreadedProducer`].
///
/// The `BaseRecord` is a structure that can be used to provide a new record to
/// [`BaseProducer::send`] or [`ThreadedProducer::send`]. Since most fields are
/// optional, a `BaseRecord` can be constructed using the builder pattern.
///
/// # Examples
///
/// This example will create a `BaseRecord` with no
/// [`DeliveryOpaque`](ProducerContext::DeliveryOpaque):
///
/// ```rust,no_run
/// # use rdkafka::producer::BaseRecord;
/// # use rdkafka::message::ToBytes;
/// let record = BaseRecord::to("topic_name")  // destination topic
///     .key(&[1, 2, 3, 4])                    // message key
///     .payload("content")                    // message payload
///     .partition(5);                         // target partition
/// ```
///
/// The following example will build a similar record, but it will use a number
/// as the `DeliveryOpaque` for the message:
///
/// ```rust,no_run
/// # use rdkafka::producer::BaseRecord;
/// # use rdkafka::message::ToBytes;
/// let record = BaseRecord::with_opaque_to("topic_name", 123) // destination topic and message id
///     .key(&[1, 2, 3, 4])                                    // message key
///     .payload("content")                                    // message payload
///     .partition(5);                                         // target partition
/// ```
#[derive(Debug)]
pub struct BaseRecord<'a, K: ToBytes + ?Sized = (), P: ToBytes + ?Sized = (), D: IntoOpaque = ()> {
    /// Required destination topic.
    pub topic: &'a str,
    /// Optional destination partition.
    pub partition: Option<i32>,
    /// Optional payload.
    pub payload: Option<&'a P>,
    /// Optional key.
    pub key: Option<&'a K>,
    /// Optional timestamp.
    ///
    /// Note that Kafka represents timestamps as the number of milliseconds
    /// since the Unix epoch.
    pub timestamp: Option<i64>,
    /// Optional message headers.
    pub headers: Option<OwnedHeaders>,
    /// Required delivery opaque (defaults to `()` if not required).
    pub delivery_opaque: D,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized, D: IntoOpaque> BaseRecord<'a, K, P, D> {
    /// Creates a new record with the specified topic name and delivery opaque.
    pub fn with_opaque_to(topic: &'a str, delivery_opaque: D) -> BaseRecord<'a, K, P, D> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque,
        }
    }

    /// Sets the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> BaseRecord<'a, K, P, D> {
        self.partition = Some(partition);
        self
    }

    /// Sets the payload of the record.
    pub fn payload(mut self, payload: &'a P) -> BaseRecord<'a, K, P, D> {
        self.payload = Some(payload);
        self
    }

    /// Sets the key of the record.
    pub fn key(mut self, key: &'a K) -> BaseRecord<'a, K, P, D> {
        self.key = Some(key);
        self
    }

    /// Sets the timestamp of the record.
    ///
    /// Note that Kafka represents timestamps as the number of milliseconds
    /// since the Unix epoch.
    pub fn timestamp(mut self, timestamp: i64) -> BaseRecord<'a, K, P, D> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> BaseRecord<'a, K, P, D> {
        self.headers = Some(headers);
        self
    }
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> BaseRecord<'a, K, P, ()> {
    /// Creates a new record with the specified topic name.
    pub fn to(topic: &'a str) -> BaseRecord<'a, K, P, ()> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque: (),
        }
    }
}

unsafe extern "C" fn partitioner_cb<Part: Partitioner, C: ProducerContext<Part>>(
    topic: *const RDKafkaTopic,
    keydata: *const c_void,
    keylen: usize,
    partition_cnt: i32,
    rkt_opaque: *mut c_void,
    _msg_opaque: *mut c_void,
) -> i32 {
    let topic_name = CStr::from_ptr(rdsys::rd_kafka_topic_name(topic));
    let topic_name = str::from_utf8_unchecked(topic_name.to_bytes());

    let is_partition_available = |p: i32| rdsys::rd_kafka_topic_partition_available(topic, p) == 1;

    let key = if keydata.is_null() {
        None
    } else {
        Some(slice::from_raw_parts(keydata as *const u8, keylen))
    };

    let producer_context = &mut *(rkt_opaque as *mut C);

    producer_context
        .get_custom_partitioner()
        .expect("custom partitioner is not set")
        .partition(topic_name, key, partition_cnt, is_partition_available)
}

impl FromClientConfig for BaseProducer<DefaultProducerContext> {
    /// Creates a new `BaseProducer` starting from a configuration.
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseProducer<DefaultProducerContext>> {
        BaseProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C, Part> FromClientConfigAndContext<C> for BaseProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part>,
{
    /// Creates a new `BaseProducer` starting from a configuration and a
    /// context.
    ///
    /// SAFETY: Raw pointer to custom partitioner is used as opaque.
    /// It's comes from reference to field in producer context so it's valid as the context is valid.
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<BaseProducer<C, Part>> {
        let native_config = config.create_native_config()?;
        let context = Arc::new(context);

        if context.get_custom_partitioner().is_some() {
            let default_topic_config =
                unsafe { rdsys::rd_kafka_conf_get_default_topic_conf(native_config.ptr()) };
            unsafe {
                rdsys::rd_kafka_topic_conf_set_opaque(
                    default_topic_config,
                    Arc::as_ptr(&context) as *mut c_void,
                )
            };
            unsafe {
                rdsys::rd_kafka_topic_conf_set_partitioner_cb(
                    default_topic_config,
                    Some(partitioner_cb::<Part, C>),
                )
            }
        }

        unsafe {
            rdsys::rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(delivery_cb::<Part, C>))
        };
        let client = Client::new_context_arc(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            context,
        )?;
        Ok(BaseProducer::from_client(client))
    }
}

/// Lowest level Kafka producer.
///
/// The `BaseProducer` needs to be polled at regular intervals in order to serve
/// queued delivery report callbacks (for more information, refer to the
/// module-level documentation).
///
/// # Example usage
///
/// This code will send a message to Kafka. No custom [`ProducerContext`] is
/// specified, so the [`DefaultProducerContext`] will be used. To see how to use
/// a producer context, refer to the examples in the [`examples`] folder.
///
/// ```rust
/// use rdkafka::config::ClientConfig;
/// use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
/// use std::time::Duration;
///
/// let producer: BaseProducer = ClientConfig::new()
///     .set("bootstrap.servers", "kafka:9092")
///     .create()
///     .expect("Producer creation error");
///
/// producer.send(
///     BaseRecord::to("destination_topic")
///         .payload("this is the payload")
///         .key("and this is a key"),
/// ).expect("Failed to enqueue");
///
/// // Poll at regular intervals to process all the asynchronous delivery events.
/// for _ in 0..10 {
///     producer.poll(Duration::from_millis(100));
/// }
///
/// // And/or flush the producer before dropping it.
/// producer.flush(Duration::from_secs(1));
/// ```
///
/// [`examples`]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/
///
pub struct BaseProducer<C = DefaultProducerContext, Part = NoCustomPartitioner>
where
    Part: Partitioner,
    C: ProducerContext<Part>,
{
    client: Client<C>,
    _partitioner: PhantomData<Part>,
}

impl<C, Part> BaseProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part>,
{
    /// Creates a base producer starting from a Client.
    fn from_client(client: Client<C>) -> BaseProducer<C, Part> {
        BaseProducer {
            client,
            _partitioner: PhantomData,
        }
    }

    /// Polls the producer, returning the number of events served.
    ///
    /// Regular calls to `poll` are required to process the events and execute
    /// the message delivery callbacks.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) -> i32 {
        unsafe { rdsys::rd_kafka_poll(self.native_ptr(), timeout.into().as_millis()) }
    }

    /// Returns a pointer to the native Kafka client.
    fn native_ptr(&self) -> *mut RDKafka {
        self.client.native_ptr()
    }

    /// Sends a message to Kafka.
    ///
    /// Message fields such as key, payload, partition, timestamp etc. are
    /// provided to this method via a [`BaseRecord`]. If the message is
    /// correctly enqueued in the producer's memory buffer, the method will take
    /// ownership of the record and return immediately; in case of failure to
    /// enqueue, the original record is returned, alongside an error code. If
    /// the message fails to be produced after being enqueued in the buffer, the
    /// [`ProducerContext::delivery`] method will be called asynchronously, with
    /// the provided [`ProducerContext::DeliveryOpaque`].
    ///
    /// When no partition is specified the underlying Kafka library picks a
    /// partition based on a hash of the key. If no key is specified, a random
    /// partition will be used. To correctly handle errors, the delivery
    /// callback should be implemented.
    ///
    /// Note that this method will never block.
    // Simplifying the return type requires generic associated types, which are
    // unstable.
    pub fn send<'a, K, P>(
        &self,
        mut record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        fn as_bytes(opt: Option<&(impl ?Sized + ToBytes)>) -> (*mut c_void, usize) {
            match opt.map(ToBytes::to_bytes) {
                None => (ptr::null_mut(), 0),
                Some(p) => (p.as_ptr() as *mut c_void, p.len()),
            }
        }
        let (payload_ptr, payload_len) = as_bytes(record.payload);
        let (key_ptr, key_len) = as_bytes(record.key);
        let topic_cstring = CString::new(record.topic.to_owned()).unwrap();
        let opaque_ptr = record.delivery_opaque.into_ptr();
        let produce_error = unsafe {
            rdsys::rd_kafka_producev(
                self.native_ptr(),
                RD_KAFKA_VTYPE_TOPIC,
                topic_cstring.as_ptr(),
                RD_KAFKA_VTYPE_PARTITION,
                record.partition.unwrap_or(-1),
                RD_KAFKA_VTYPE_MSGFLAGS,
                rdsys::RD_KAFKA_MSG_F_COPY,
                RD_KAFKA_VTYPE_VALUE,
                payload_ptr,
                payload_len,
                RD_KAFKA_VTYPE_KEY,
                key_ptr,
                key_len,
                RD_KAFKA_VTYPE_OPAQUE,
                opaque_ptr,
                RD_KAFKA_VTYPE_TIMESTAMP,
                record.timestamp.unwrap_or(0),
                RD_KAFKA_VTYPE_HEADERS,
                record
                    .headers
                    .as_ref()
                    .map_or(ptr::null_mut(), OwnedHeaders::ptr),
                RD_KAFKA_VTYPE_END,
            )
        };
        if produce_error.is_error() {
            record.delivery_opaque = unsafe { C::DeliveryOpaque::from_ptr(opaque_ptr) };
            Err((KafkaError::MessageProduction(produce_error.into()), record))
        } else {
            // The kafka producer now owns the headers
            mem::forget(record.headers);
            Ok(())
        }
    }
}

impl<C, Part> Producer<C, Part> for BaseProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part>,
{
    fn client(&self) -> &Client<C> {
        &self.client
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        let ret = unsafe { rdsys::rd_kafka_flush(self.native_ptr(), timeout.into().as_millis()) };
        if ret.is_error() {
            Err(KafkaError::Flush(ret.into()))
        } else {
            Ok(())
        }
    }

    fn purge(&self, flags: PurgeConfig) {
        let ret = unsafe { rdsys::rd_kafka_purge(self.native_ptr(), flags.flag_bits) };
        if ret.is_error() {
            panic!(
                "According to librdkafka's doc, calling this with valid arguments on a producer \
                    can only result in a success, but it still failed: {}",
                RDKafkaErrorCode::from(ret)
            )
        }
    }

    fn in_flight_count(&self) -> i32 {
        unsafe { rdsys::rd_kafka_outq_len(self.native_ptr()) }
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_init_transactions(
                self.native_ptr(),
                timeout.into().as_millis(),
            ))
        };
        if ret.is_error() {
            Err(KafkaError::Transaction(ret))
        } else {
            Ok(())
        }
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        let ret =
            unsafe { RDKafkaError::from_ptr(rdsys::rd_kafka_begin_transaction(self.native_ptr())) };
        if ret.is_error() {
            Err(KafkaError::Transaction(ret))
        } else {
            Ok(())
        }
    }

    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_send_offsets_to_transaction(
                self.native_ptr(),
                offsets.ptr(),
                cgm.ptr(),
                timeout.into().as_millis(),
            ))
        };
        if ret.is_error() {
            Err(KafkaError::Transaction(ret))
        } else {
            Ok(())
        }
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_commit_transaction(
                self.native_ptr(),
                timeout.into().as_millis(),
            ))
        };
        if ret.is_error() {
            Err(KafkaError::Transaction(ret))
        } else {
            Ok(())
        }
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_abort_transaction(
                self.native_ptr(),
                timeout.into().as_millis(),
            ))
        };
        if ret.is_error() {
            Err(KafkaError::Transaction(ret))
        } else {
            Ok(())
        }
    }
}

impl<C, Part: Partitioner> Drop for BaseProducer<C, Part>
where
    C: ProducerContext<Part>,
{
    fn drop(&mut self) {
        self.purge(PurgeConfig::default().queue().inflight());
        // Still have to poll after purging to get the results that have been made ready by the purge
        self.poll(Timeout::After(Duration::ZERO));
    }
}

//
// ********** THREADED PRODUCER **********
//

/// A low-level Kafka producer with a separate thread for event handling.
///
/// The `ThreadedProducer` is a [`BaseProducer`] with a separate thread
/// dedicated to calling `poll` at regular intervals in order to execute any
/// queued events, such as delivery notifications. The thread will be
/// automatically stopped when the producer is dropped.
#[must_use = "The threaded producer will stop immediately if unused"]
pub struct ThreadedProducer<C, Part: Partitioner = NoCustomPartitioner>
where
    C: ProducerContext<Part> + 'static,
{
    producer: Arc<BaseProducer<C, Part>>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FromClientConfig for ThreadedProducer<DefaultProducerContext, NoCustomPartitioner> {
    fn from_config(config: &ClientConfig) -> KafkaResult<ThreadedProducer<DefaultProducerContext>> {
        ThreadedProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C, Part> FromClientConfigAndContext<C> for ThreadedProducer<C, Part>
where
    Part: Partitioner + Send + Sync + 'static,
    C: ProducerContext<Part> + 'static,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<ThreadedProducer<C, Part>> {
        let producer = Arc::new(BaseProducer::from_config_and_context(config, context)?);
        let should_stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let producer = Arc::clone(&producer);
            let should_stop = should_stop.clone();
            thread::Builder::new()
                .name("producer polling thread".to_string())
                .spawn(move || {
                    trace!("Polling thread loop started");
                    loop {
                        let n = producer.poll(Duration::from_millis(100));
                        if n == 0 {
                            if should_stop.load(Ordering::Relaxed) {
                                // We received nothing and the thread should
                                // stop, so break the loop.
                                break;
                            }
                        } else {
                            trace!("Received {} events", n);
                        }
                    }
                    trace!("Polling thread loop terminated");
                })
                .expect("Failed to start polling thread")
        };
        Ok(ThreadedProducer {
            producer,
            should_stop,
            handle: Some(thread),
        })
    }
}

impl<C, Part> ThreadedProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    /// Sends a message to Kafka.
    ///
    /// See the documentation for [`BaseProducer::send`] for details.
    // Simplifying the return type requires generic associated types, which are
    // unstable.
    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.producer.send(record)
    }

    /// Polls the internal producer.
    ///
    /// This is not normally required since the `ThreadedProducer` has a thread
    /// dedicated to calling `poll` regularly.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.poll(timeout);
    }
}

impl<C, Part> Producer<C, Part> for ThreadedProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    fn client(&self) -> &Client<C> {
        self.producer.client()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.flush(timeout)
    }

    fn purge(&self, flags: PurgeConfig) {
        self.producer.purge(flags)
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.producer.begin_transaction()
    }

    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()> {
        self.producer
            .send_offsets_to_transaction(offsets, cgm, timeout)
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.abort_transaction(timeout)
    }
}

impl<C, Part> Drop for ThreadedProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    fn drop(&mut self) {
        trace!("Destroy ThreadedProducer");
        if let Some(handle) = self.handle.take() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match handle.join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
        trace!("ThreadedProducer destroyed");
    }
}
