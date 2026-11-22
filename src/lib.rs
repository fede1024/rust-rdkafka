#![doc = include_str!("../README.md")]
#![forbid(missing_docs)]
#![deny(rust_2018_idioms)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod log;

pub use rdkafka_sys::{bindings, helpers, types};

pub mod admin;
pub mod client;
pub mod config;
pub mod config_map;
pub mod consumer;
pub mod error;
pub mod groups;
pub mod message;
pub mod metadata;
pub mod mocking;
pub mod producer;
pub mod statistics;
pub mod topic_partition_list;
pub mod util;

// Re-exports.
pub use crate::client::ClientContext;
pub use crate::config::ClientConfig;
pub use crate::message::{Message, Timestamp};
pub use crate::statistics::Statistics;
pub use crate::topic_partition_list::{Offset, TopicPartitionList};
pub use crate::util::IntoOpaque;
