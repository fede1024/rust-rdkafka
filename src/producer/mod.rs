//! Low level and high level rdkafka producers.
//!
//! ## The C librdkafka producer
//! Rust-rdkafka relies on the C librdkafka producer to communicate with Kafka, so in order to understand how
//! the Rust producers work it is important to understand the basics of the C one as well.
//!
//! ### Async
//! The librdkafka producer is completely asynchronous: it maintains a memory buffer where messages
//! waiting to be sent or currently in flight are stored. Once a message is delivered or an error
//! occurred and the maximum number of retries has been reached, the producer will enqueue a delivery
//! event with the appropriate delivery result into an internal event queue.
//!
//! The librdkafka user is responsible for calling the `poll` function at regular intervals to
//! process those events; the thread calling `poll` will be the one executing the user-specified
//! delivery callback for every delivery event. If `poll` is not called, or not frequently
//! enough, the producer will return a `RDKafkaError::QueueFull` error and it won't be able to send any other
//! message until more delivery event are processed via `poll`. The `QueueFull` error can also be
//! returned if Kafka is not able to receive the messages quickly enough.
//!
//! ### Error reporting
//! The C library will try deal with all the transient errors such as broker disconnection,
//! timeouts etc. These errors, called global errors, are automatically logged in rust-rdkafka, but
//! they normally don't require any handling as they are automatically handled internally.
//! To see the logs, make sure you initialize the logger.
//!
//! As mentioned earlier, errors specific to message production will be reported in the delivery callback.
//!
//! ### Buffering
//! Buffering is done automatically by librdkafka. When `send` is called, the message is enqueued
//! internally and once enough messages have been enqueued, or when enough time has passed, they will be
//! sent to Kafka as a single batch. You can control the behavior of the buffer by configuring the
//! the `queue.buffering.max.*` parameters listed below.
//!
//! ## Rust-rdkafka producers
//! Rust-rdkafka (rdkafka for brevity) provides two sets of producers: low level and high level.
//!
//! ### Low level producers
//! The lowest level producer provided by rdkafka is called `BaseProducer`. The goal of the
//! `BaseProducer` is to be as close as possible to the C one while maintaining a safe Rust interface.
//! In particular, the `BaseProducer` needs to be polled at regular intervals to execute
//! any delivery callback that might be waiting and to make sure the queue doesn't fill up.
//!
//! Another low lever producer is the `ThreadedProducer`, which is a `BaseProducer` with
//! a dedicated thread for polling.
//!
//! The delivery callback can be defined using a `ProducerContext`. More information in the
//! `base_producer` module.
//!
//! ### High level producer
//! At the moment the only high level producer implemented is the `FutureProducer`. The
//! `FutureProducer` doesn't rely on user-defined callbacks to notify the delivery or failure of
//! a message; instead, this information will be returned in a Future. The `FutureProducer` also
//! uses an internal thread that is used for polling, which makes calling poll explicitly not necessary.
//! The returned future will contain information about the delivered message in case of success,
//! or a copy of the original message in case of failure. Additional computation can be chained
//! to the returned future, and it will executed by the future executor once the value is available
//! (for more information, check the documentation of the futures crate).
//!
//! ## Configuration
//!
//! ### Producer configuration
//!
//! For the configuration parameters common to both producers and consumers, refer to the
//! documentation in the `config` module. Here are listed the most commonly used producer
//! configuration. Click [here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for the full list.
//!
//! - `queue.buffering.max.messages` (100000): Maximum number of messages allowed on the producer queue.
//! - `queue.buffering.max.kbytes` (4000000): Maximum total message size sum allowed on the producer queue. This property has higher priority than queue.buffering.max.messages.
//! - `queue.buffering.max.ms` (0): Delay in milliseconds to wait for messages in the producer queue to accumulate before sending a request to the brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
//! - `message.send.max.retries` (2): How many times to retry sending a failing batch. Note: retrying may cause reordering.
//! - `compression.codec` (none): Compression codec to use for compressing message sets.
//! - `request.required.acks` (1): This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, 1=Only the leader broker will need to ack the message, -1 or all=broker will block until message is committed by all in sync replicas (ISRs) or broker's in.sync.replicas setting before sending response.
//! - `request.timeout.ms` (5000): The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on request.required.acks being != 0.
//! - `message.timeout.ms` (300000): Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite.
//! - `produce.offset.report` (false): Report offset of produced message back to application.
//!

pub mod base_producer;
pub mod future_producer;

pub use self::base_producer::{
    BaseProducer,
    DeliveryResult,
    DefaultProducerContext,
    ProducerContext,
    ProducerRecord,
    ThreadedProducer
};
pub use self::future_producer::{
    FutureProducer,
    DeliveryFuture
};
