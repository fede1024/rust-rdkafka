//! Rust wrapper for `librdkafka`.
//! 
//! This crate is a wrapper for the `librdkafka` library. It provides a safe Rust API
//! to asynchronously produce and consume Kafka messages.
//! 
//! *Warning*: this library is still at an early development stage, the API is very likely
//! to change and it shouldn't be considered production ready.
//!
//! ## Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! rdkafka = "^0.1.0"
//! ```
//!
//! This crate will compile `librdkafka` from sources and link it statically in your
//! executable. To compile `librdkafka` you'll need:
//!
//! * the GNU toolchain
//! * GNU `make`
//! * `pthreads`
//! * `zlib`: optional, included by default (feature: `zlib`).
//! * `libssl-dev`: optional, *not* included by default (feature: `ssl`).
//! * `libsasl2-dev`: optional, *not* included by default (feature: `sasl`).
//!
//! All libraries excluding `librdkafka` will be linked dynamically. To enable ssl and sasl, use the
//! `features` field in `Cargo.toml`. Example:
//!
//! ```
//! [dependencies.rdkafka]
//! version = "0.1.0"
//! features = ["ssl", "sasl"]
//! ```
//!
//! ## Compiling from sources
//!
//! To compile from sources, you'll have to update the submodule containing `librdkafka`:
//!
//! ```
//! git submodule update --init
//! ```
//!
//! and then compile using `cargo`, selecting the features that you want. Example:
//!
//! ```bash
//! cargo build --features "ssl sasl"
//! ```
//!
//! ## Examples
//!
//! You can find examples in the `examples` folder. To run them:
//!
//! ```bash
//! RUST_LOG="rdkafka=trace" LOG_THREAD=1 cargo run --example simple_consumer
//! ```
//!
//! The `RUST_LOG` environemnt variable will configure tracing level logging for `rdkafka`,
//! and `LOG_THREAD` will add the name of the thread to log messages.

#[macro_use]
extern crate log;

pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod message;
pub mod producer;
pub mod util;
