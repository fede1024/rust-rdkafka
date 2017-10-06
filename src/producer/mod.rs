//! This module contains all the rust-rdkafka producers.
//!
//! ## The librdkafka producer
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
//! process those events; in particular the thread calling `poll` will be the one executing
//! the delivery callback for every delivery event. If `poll` is not called, or not frequently
//! enough, the producer will return a "queue full" error and it won't be able to send any other
//! message until more delivery event are processed via `poll`.
//!
//! ### Error reporting
//! The C library will try deal with all the transient errors such as broker disconnection,
//! timeouts etc. These errors, called global errors, are automatically logged in rust-rdkafka, but
//! they normally don't require any handling as they are automatically handled internally.
//! To see the logs, make sure you initialize the logger.
//!
//! As mentioned earlier, errors specific to message production will be reported in the delivery callback.
//!
//! ## Rust-rdkafka producers
//! Rust-rdkafka (rdkafka for brevity) provides two sets of producers: low level and high level.
//!
//! ### Low level producer
//! The low level producer provided by rdkafka is called `BaseProducer`. The goal of the
//! `BaseProducer` is to be as close as possible to the C one while maintaining a safe Rust interface.
//! In particular, the `BaseProducer` needs to be polled at regular intervals to execute the
//! any delivery callback that might be waiting and to make sure the queue doesn't fill up.
//!
//! The delivery callback can be defined using a `ProducerContext`. More information in the
//! `base_producer` module.
//!
//! ### High level producer
//!
//! TODO (see `FutureProducer`).
//!

pub mod base_producer;
pub mod future_producer;

pub use self::base_producer::{
    BaseProducer,
    DeliveryResult,
    EmptyProducerContext,
    ProducerContext,
};
pub use self::future_producer::FutureProducer;
