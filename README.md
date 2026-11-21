# rust-rdkafka

[![crates.io](https://img.shields.io/crates/v/rdkafka.svg)](https://crates.io/crates/rdkafka)
[![docs.rs](https://docs.rs/rdkafka/badge.svg)](https://docs.rs/rdkafka/)
[![CI](https://github.com/fede1024/rust-rdkafka/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/fede1024/rust-rdkafka/actions/workflows/ci.yml)

A fully asynchronous, [futures]-enabled [Apache Kafka] client
library for Rust based on [librdkafka].

## The library

`rust-rdkafka` provides a safe Rust interface to librdkafka. By default it
bundles librdkafka v2.16.0 via `rdkafka-sys`.

### Documentation

- [Latest release](https://docs.rs/rdkafka/)
- [Changelog](https://github.com/fede1024/rust-rdkafka/blob/master/changelog.md)

### Features

The main features provided at the moment are:

- Support for all Kafka versions since 0.8.x. For more information about
  broker compatibility options, check the [librdkafka
  documentation][broker-compat].
- Consume from single or multiple topics.
- Automatic consumer rebalancing.
- Customizable rebalance, with pre and post rebalance callbacks.
- Synchronous or asynchronous message production.
- Customizable offset commit.
- Create and delete topics and add and edit partitions.
- Alter broker and topic configurations.
- Access to cluster metadata (list of topic-partitions, replicas, active
  brokers etc).
- Access to group metadata (list groups, list members of groups, hostnames,
  etc.).
- Access to producer and consumer metrics, errors and callbacks.
- Exactly-once semantics (EOS) via idempotent and transactional producers
  and read-committed consumers.

### Client types

`rust-rdkafka` provides low level and high level consumers and producers.

Low level:

* [`BaseConsumer`]: a simple wrapper around the librdkafka consumer. It
  must be periodically `poll()`ed in order to execute callbacks, rebalances
  and to receive messages.
* [`BaseProducer`]: a simple wrapper around the librdkafka producer. As in
  the consumer case, the user must call `poll()` periodically to execute
  delivery callbacks.
* [`ThreadedProducer`]: a `BaseProducer` with a separate thread dedicated to
  polling the producer.

High level:

 * [`StreamConsumer`]: a [`Stream`] of messages that takes care of
   polling the consumer automatically.
 * [`FutureProducer`]: a [`Future`] that will be completed once
   the message is delivered to Kafka (or failed).

For more information about consumers and producers, refer to their
module-level documentation.

### Asynchronous data processing with Tokio

[Tokio] is a platform for fast processing of asynchronous events in Rust.
The interfaces exposed by the [`StreamConsumer`] and the [`FutureProducer`]
allow rust-rdkafka users to easily integrate Kafka consumers and producers
within the Tokio platform, and write asynchronous message processing code.
Note that rust-rdkafka can be used without Tokio.

To see rust-rdkafka in action with Tokio, check out the
[asynchronous processing example] in the examples folder.

### At-least-once delivery

At-least-once delivery semantics are common in many streaming applications:
every message is guaranteed to be processed at least once; in case of
temporary failure, the message can be re-processed and/or re-delivered,
but no message will be lost.

In order to implement at-least-once delivery the stream processing
application has to carefully commit the offset only once the message has
been processed. Committing the offset too early, instead, might cause
message loss, since upon recovery the consumer will start from the next
message, skipping the one where the failure occurred.

To see how to implement at-least-once delivery with `rdkafka`, check out the
[at-least-once delivery example] in the examples folder. To know more about
delivery semantics, check the [message delivery semantics] chapter in the
Kafka documentation.

### Exactly-once semantics

Exactly-once semantics (EOS) can be achieved using transactional producers,
which allow produced records and consumer offsets to be committed or aborted
atomically. Consumers that set their `isolation.level` to `read_committed`
will only observe committed messages.

EOS is useful in read-process-write scenarios that require messages to be
processed exactly once.

To learn more about using transactions in rust-rdkafka, see the
[Transactions][producer-transactions] section of the producer documentation.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rdkafka = { version = "0.40", features = ["cmake-build"] }
```

This crate will compile librdkafka from sources and link it statically to
your executable. To compile librdkafka you'll need:

* the GNU toolchain
* GNU `make`
* `pthreads`
* `zlib`: optional, but included by default (feature: `libz`)
* `cmake`: optional, *not* included by default (feature: `cmake-build`)
* `libssl-dev`: optional, *not* included by default (feature: `ssl`)
* `libsasl2-dev`: optional, *not* included by default (feature: `gssapi`)
* `libcurl-dev` (e.g. `libcurl4-openssl-dev` on Ubuntu): optional, *not* included by default (feature: `curl`)
* `libzstd-dev`: optional, *not* included by default (feature: `zstd-pkg-config`)

Note that using the CMake build system, via the `cmake-build` feature, is
encouraged if you can take the dependency on CMake.

By default a submodule with the librdkafka sources pinned to a specific
commit will be used to compile and statically link the library. The
`dynamic-linking` feature can be used to instead dynamically link rdkafka to
the system's version of librdkafka. Example:

```toml
[dependencies]
rdkafka = { version = "0.40", features = ["dynamic-linking"] }
```

If you'd like to compile librdkafka statically yourself, you can use
`static-linking` while supplying `DEP_LIBRDKAFKA_STATIC_ROOT` with the path
to where librdkafka was built.

For a full listing of features, consult the [rdkafka-sys crate's
documentation][rdkafka-sys-features]. All of rdkafka-sys features are
re-exported as rdkafka features.

### Minimum supported Rust version (MSRV)

The current minimum supported Rust version (MSRV) is 1.74. Note that
bumping the MSRV is not considered a breaking change. Any release of
rust-rdkafka may bump the MSRV.

### Asynchronous runtimes

Some features of the [`StreamConsumer`] and [`FutureProducer`] depend on
Tokio, which can be a heavyweight dependency for users who only intend to
use the low-level consumers and producers. The Tokio integration is
enabled by default, but can be disabled by turning off default features:

```toml
[dependencies]
rdkafka = { version = "0.40", default-features = false }
```

If you would like to use an asynchronous runtime besides Tokio, you can
integrate it with rust-rdkafka by providing a shim that implements the
[`AsyncRuntime`] trait. See the following examples for details:

  * [smol][runtime-smol]
  * [async-std][runtime-async-std]

## Examples

You can find examples in the [`examples`] folder. To run them:

```bash
cargo run --example <example_name> -- <example_args>
```

## Debugging

rust-rdkafka uses the [`log`] crate to handle logging.
Optionally, enable the `tracing` feature to emit [`tracing`]
events as opposed to [`log`] records.

In test and examples, rust-rdkafka uses the  [`env_logger`] crate
to format logs. In those contexts, logging can be enabled
using the `RUST_LOG` environment variable, for example:

```bash
RUST_LOG="librdkafka=trace,rdkafka::client=debug" cargo test
```

This will configure the logging level of librdkafka to trace, and the level
of the client module of the Rust client to debug. To actually receive logs
from librdkafka, you also have to set the `debug` option in the producer or
consumer configuration (see librdkafka
[configuration][librdkafka-config]).

To enable debugging in your project, make sure you initialize the logger
with `env_logger::init()`, or the equivalent for any `log`-compatible
logging framework.

[`AsyncRuntime`]: https://docs.rs/rdkafka/*/rdkafka/util/trait.AsyncRuntime.html
[`BaseConsumer`]: https://docs.rs/rdkafka/*/rdkafka/consumer/base_consumer/struct.BaseConsumer.html
[`BaseProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/base_producer/struct.BaseProducer.html
[`Future`]: https://doc.rust-lang.org/stable/std/future/trait.Future.html
[`FutureProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/future_producer/struct.FutureProducer.html
[`Stream`]: https://docs.rs/futures/*/futures/stream/trait.Stream.html
[`StreamConsumer`]: https://docs.rs/rdkafka/*/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html
[`ThreadedProducer`]: https://docs.rs/rdkafka/*/rdkafka/producer/base_producer/struct.ThreadedProducer.html
[`log`]: https://docs.rs/log
[`tracing`]: https://docs.rs/tracing
[`env_logger`]: https://docs.rs/env_logger
[Apache Kafka]: https://kafka.apache.org
[asynchronous processing example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/asynchronous_processing.rs
[at-least-once delivery example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/at_least_once.rs
[runtime-smol]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/runtime_smol.rs
[runtime-async-std]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/runtime_async_std.rs
[broker-compat]: https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility
[`examples`]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/
[futures]: https://github.com/rust-lang/futures-rs
[librdkafka]: https://github.com/confluentinc/librdkafka
[librdkafka-config]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
[message delivery semantics]: https://kafka.apache.org/documentation/#semantics
[producer-transactions]: https://docs.rs/rdkafka/*/rdkafka/producer/#transactions
[rdkafka-sys-features]: https://github.com/fede1024/rust-rdkafka/tree/master/rdkafka-sys/README.md#features
[Tokio]: https://tokio.rs/

## rdkafka-sys

See [rdkafka-sys](https://github.com/fede1024/rust-rdkafka/tree/master/rdkafka-sys).

## Alternatives

* [kafka-rust]: a pure Rust implementation of the Kafka client.

[kafka-rust]: https://github.com/kafka-rust/kafka-rust
