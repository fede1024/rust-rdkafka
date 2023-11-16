//! A fully asynchronous, [futures]-enabled [Apache Kafka] client
//! library for Rust based on [librdkafka].
//!
//! ## The library
//!
//! `rust-rdkafka` provides a safe Rust interface to librdkafka. This version
//! is compatible with librdkafka v1.9.2+.
//!
//! ### Documentation
//!
//! - [Current master branch](https://fede1024.github.io/rust-rdkafka/)
//! - [Latest release](https://docs.rs/rdkafka/)
//! - [Changelog](https://github.com/fede1024/rust-rdkafka/blob/master/changelog.md)
//!
//! ### Features
//!
//! The main features provided at the moment are:
//!
//! - Support for all Kafka versions since 0.8.x. For more information about
//!   broker compatibility options, check the [librdkafka
//!   documentation][broker-compat].
//! - Consume from single or multiple topics.
//! - Automatic consumer rebalancing.
//! - Customizable rebalance, with pre and post rebalance callbacks.
//! - Synchronous or asynchronous message production.
//! - Customizable offset commit.
//! - Create and delete topics and add and edit partitions.
//! - Alter broker and topic configurations.
//! - Access to cluster metadata (list of topic-partitions, replicas, active
//!   brokers etc).
//! - Access to group metadata (list groups, list members of groups, hostnames,
//!   etc.).
//! - Access to producer and consumer metrics, errors and callbacks.
//! - Exactly-once semantics (EOS) via idempotent and transactional producers
//!   and read-committed consumers.
//!
//! ### One million messages per second
//!
//! `rust-rdkafka` is designed to be easy and safe to use thanks to the
//! abstraction layer written in Rust, while at the same time being extremely
//! fast thanks to the librdkafka C library.
//!
//! Here are some benchmark results using the [`BaseProducer`],
//! sending data to a single Kafka 0.11 process running in localhost (default
//! configuration, 3 partitions). Hardware: Dell laptop, with Intel Core
//! i7-4712HQ @ 2.30GHz.
//!
//! - Scenario: produce 5 million messages, 10 bytes each, wait for all of them to be acked
//!   - 1045413 messages/s, 9.970 MB/s  (average over 5 runs)
//!
//! - Scenario: produce 100000 messages, 10 KB each, wait for all of them to be acked
//!   - 24623 messages/s, 234.826 MB/s  (average over 5 runs)
//!
//! For more numbers, check out the [kafka-benchmark] project.
//!
//! ### Client types
//!
//! `rust-rdkafka` provides low level and high level consumers and producers.
//!
//! Low level:
//!
//! * [`BaseConsumer`]: a simple wrapper around the librdkafka consumer. It
//!   must be periodically `poll()`ed in order to execute callbacks, rebalances
//!   and to receive messages.
//! * [`BaseProducer`]: a simple wrapper around the librdkafka producer. As in
//!   the consumer case, the user must call `poll()` periodically to execute
//!   delivery callbacks.
//! * [`ThreadedProducer`]: a `BaseProducer` with a separate thread dedicated to
//!   polling the producer.
//!
//! High level:
//!
//!  * [`StreamConsumer`]: a [`Stream`] of messages that takes care of
//!    polling the consumer automatically.
//!  * [`FutureProducer`]: a [`Future`] that will be completed once
//!    the message is delivered to Kafka (or failed).
//!
//! For more information about consumers and producers, refer to their
//! module-level documentation.
//!
//! *Warning*: the library is under active development and the APIs are likely
//! to change.
//!
//! ### Asynchronous data processing with Tokio
//!
//! [Tokio] is a platform for fast processing of asynchronous events in Rust.
//! The interfaces exposed by the [`StreamConsumer`] and the [`FutureProducer`]
//! allow rust-rdkafka users to easily integrate Kafka consumers and producers
//! within the Tokio platform, and write asynchronous message processing code.
//! Note that rust-rdkafka can be used without Tokio.
//!
//! To see rust-rdkafka in action with Tokio, check out the
//! [asynchronous processing example] in the examples folder.
//!
//! ### At-least-once delivery
//!
//! At-least-once delivery semantics are common in many streaming applications:
//! every message is guaranteed to be processed at least once; in case of
//! temporary failure, the message can be re-processed and/or re-delivered,
//! but no message will be lost.
//!
//! In order to implement at-least-once delivery the stream processing
//! application has to carefully commit the offset only once the message has
//! been processed. Committing the offset too early, instead, might cause
//! message loss, since upon recovery the consumer will start from the next
//! message, skipping the one where the failure occurred.
//!
//! To see how to implement at-least-once delivery with `rdkafka`, check out the
//! [at-least-once delivery example] in the examples folder. To know more about
//! delivery semantics, check the [message delivery semantics] chapter in the
//! Kafka documentation.
//!
//! ### Exactly-once semantics
//!
//! Exactly-once semantics (EOS) can be achieved using transactional producers,
//! which allow produced records and consumer offsets to be committed or aborted
//! atomically. Consumers that set their `isolation.level` to `read_committed`
//! will only observe committed messages.
//!
//! EOS is useful in read-process-write scenarios that require messages to be
//! processed exactly once.
//!
//! To learn more about using transactions in rust-rdkafka, see the
//! [Transactions](producer-transactions) section of the producer documentation.
//!
//! ### Users
//!
//! Here are some of the projects using rust-rdkafka:
//!
//! - [timely-dataflow]: a distributed data-parallel compute engine. See also
//!   the [blog post][timely-blog] announcing its Kafka integration.
//! - [kafka-view]: a web interface for Kafka clusters.
//! - [kafka-benchmark]: a high performance benchmarking tool for Kafka.
//! - [callysto]: Stream processing framework in Rust.
//! - [bytewax]: Python stream processing framework using Timely Dataflow.
//!
//! *If you are using rust-rdkafka, please let us know!*
//!
//! ## Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! rdkafka = { version = "0.25", features = ["cmake-build"] }
//! ```
//!
//! This crate will compile librdkafka from sources and link it statically to
//! your executable. To compile librdkafka you'll need:
//!
//! * the GNU toolchain
//! * GNU `make`
//! * `pthreads`
//! * `zlib`: optional, but included by default (feature: `libz`)
//! * `cmake`: optional, *not* included by default (feature: `cmake-build`)
//! * `libssl-dev`: optional, *not* included by default (feature: `ssl`)
//! * `libsasl2-dev`: optional, *not* included by default (feature: `gssapi`)
//! * `libzstd-dev`: optional, *not* included by default (feature: `zstd-pkg-config`)
//!
//! Note that using the CMake build system, via the `cmake-build` feature, is
//! encouraged if you can take the dependency on CMake.
//!
//! By default a submodule with the librdkafka sources pinned to a specific
//! commit will be used to compile and statically link the library. The
//! `dynamic-linking` feature can be used to instead dynamically link rdkafka to
//! the system's version of librdkafka. Example:
//!
//! ```toml
//! [dependencies]
//! rdkafka = { version = "0.25", features = ["dynamic-linking"] }
//! ```
//!
//! For a full listing of features, consult the [rdkafka-sys crate's
//! documentation][rdkafka-sys-features]. All of rdkafka-sys features are
//! re-exported as rdkafka features.
//!
//! ### Minimum supported Rust version (MSRV)
//!
//! The current minimum supported Rust version (MSRV) is 1.61.0. Note that
//! bumping the MSRV is not considered a breaking change. Any release of
//! rust-rdkafka may bump the MSRV.
//!
//! ### Asynchronous runtimes
//!
//! Some features of the [`StreamConsumer`] and [`FutureProducer`] depend on
//! Tokio, which can be a heavyweight dependency for users who only intend to
//! use the low-level consumers and producers. The Tokio integration is
//! enabled by default, but can be disabled by turning off default features:
//!
//! ```toml
//! [dependencies]
//! rdkafka = { version = "0.25", default-features = false }
//! ```
//!
//! If you would like to use an asynchronous runtime besides Tokio, you can
//! integrate it with rust-rdkafka by providing a shim that implements the
//! [`AsyncRuntime`] trait. See the following examples for details:
//!
//!   * [smol][runtime-smol]
//!   * [async-std][runtime-async-std]
//!
//! ## Examples
//!
//! You can find examples in the [`examples`] folder. To run them:
//!
//! ```bash
//! cargo run --example <example_name> -- <example_args>
//! ```
//!
//! ## Debugging
//!
//! rust-rdkafka uses the [`log`] crate to handle logging.
//! Optionally, enable the `tracing` feature to emit [`tracing`]
//! events as opposed to [`log`] records.
//!
//! In test and examples, rust-rdkafka uses the  [`env_logger`] crate
//! to format logs. In those contexts, logging can be enabled
//! using the `RUST_LOG` environment variable, for example:
//!
//! ```bash
//! RUST_LOG="librdkafka=trace,rdkafka::client=debug" cargo test
//! ```
//!
//! This will configure the logging level of librdkafka to trace, and the level
//! of the client module of the Rust client to debug. To actually receive logs
//! from librdkafka, you also have to set the `debug` option in the producer or
//! consumer configuration (see librdkafka
//! [configuration][librdkafka-config]).
//!
//! To enable debugging in your project, make sure you initialize the logger
//! with `env_logger::init()`, or the equivalent for any `log`-compatible
//! logging framework.
//!
//! [`AsyncRuntime`]: https://docs.rs/rdkafka/*/rdkafka/util/trait.AsyncRuntime.html
//! [`BaseConsumer`]: https://docs.rs/rdkafka/*/rdkafka/consumer/base_consumer/struct.BaseConsumer.html
//! [`BaseProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/base_producer/struct.BaseProducer.html
//! [`Future`]: https://doc.rust-lang.org/stable/std/future/trait.Future.html
//! [`FutureProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/future_producer/struct.FutureProducer.html
//! [`Stream`]: https://docs.rs/futures/*/futures/stream/trait.Stream.html
//! [`StreamConsumer`]: https://docs.rs/rdkafka/*/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html
//! [`ThreadedProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/base_producer/struct.ThreadedProducer.html
//! [`log`]: https://docs.rs/log
//! [`tracing`]: https://docs.rs/tracing
//! [`env_logger`]: https://docs.rs/env_logger
//! [Apache Kafka]: https://kafka.apache.org
//! [asynchronous processing example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/asynchronous_processing.rs
//! [at-least-once delivery example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/at_least_once.rs
//! [runtime-smol]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/runtime_smol.rs
//! [runtime-async-std]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/runtime_async_std.rs
//! [broker-compat]: https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility
//! [bytewax]: https://github.com/bytewax/bytewax
//! [callysto]: https://github.com/vertexclique/callysto
//! [`examples`]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/
//! [futures]: https://github.com/rust-lang/futures-rs
//! [kafka-benchmark]: https://github.com/fede1024/kafka-benchmark
//! [kafka-view]: https://github.com/fede1024/kafka-view
//! [librdkafka]: https://github.com/edenhill/librdkafka
//! [librdkafka-config]: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
//! [message delivery semantics]: https://kafka.apache.org/0101/documentation.html#semantics
//! [producer-transactions]: https://docs.rs/rdkafka/*/rdkafka/producer/#transactions
//! [rdkafka-sys-features]: https://github.com/fede1024/rust-rdkafka/tree/master/rdkafka-sys/README.md#features
//! [rdkafka-sys-known-issues]: https://github.com/fede1024/rust-rdkafka/tree/master/rdkafka-sys/README.md#known-issues
//! [smol]: https://docs.rs/smol
//! [timely-blog]: https://github.com/frankmcsherry/blog/blob/master/posts/2017-11-08.md
//! [timely-dataflow]: https://github.com/frankmcsherry/timely-dataflow
//! [Tokio]: https://tokio.rs/

#![forbid(missing_docs)]
#![deny(rust_2018_idioms)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod log;

pub use rdkafka_sys::{bindings, helpers, types};

pub mod admin;
pub mod client;
pub mod config;
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
