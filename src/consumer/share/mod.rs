//! KIP-932 share consumers (Queues for Kafka).
//!
//! This module hosts the public Rust surface for the share consumer API
//! introduced by [KIP-932][kip]. Share consumers cooperatively consume
//! records from a share group, with broker-tracked per-record locks and
//! explicit acknowledgement semantics.
//!
//! # Stability
//!
//! The KIP marks the Java API as `@InterfaceStability.Evolving`. The Rust
//! surface mirrors that posture: the entire `share` module is gated behind
//! the `kip-932` cargo feature and may change in any minor release while
//! the underlying protocol stabilises.
//!
//! # Runtime support
//!
//! librdkafka does not yet expose a public C API for share consumers.
//! Construction of a [`BaseShareConsumer`] succeeds so downstream code
//! can exercise the type surface, but every runtime method returns
//! [`KafkaError::Unsupported`][unsup]. Track librdkafka progress at
//! <https://github.com/confluentinc/librdkafka/issues/5441>.
//!
//! [kip]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
//! [unsup]: crate::error::KafkaError::Unsupported

mod acknowledge;
mod config;
mod consumer;
mod context;
mod records;

pub use self::acknowledge::{
    AcknowledgeType, AcknowledgementCommitResult, ParseAcknowledgeTypeError,
};
pub use self::config::{
    AcknowledgementMode, AutoOffsetReset, IsolationLevel, ParseConfigEnumError, ShareConsumerConfig,
};
pub use self::consumer::{BaseShareConsumer, ShareConsumer};
pub use self::context::{DefaultShareConsumerContext, ShareConsumerContext};
pub use self::records::{ShareConsumerRecords, ShareRecord};
