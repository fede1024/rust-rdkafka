# rust-rdkafka

[![crates.io](https://img.shields.io/crates/v/rdkafka.svg)](https://crates.io/crates/rdkafka)
[![docs.rs](https://docs.rs/rdkafka/badge.svg)](https://docs.rs/rdkafka/)
[![Build Status](https://travis-ci.org/fede1024/rust-rdkafka.svg?branch=master)](https://travis-ci.org/fede1024/rust-rdkafka)
[![Join the chat at https://gitter.im/rust-rdkafka/Lobby](https://badges.gitter.im/rust-rdkafka/Lobby.svg)](https://gitter.im/rust-rdkafka/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A fully asynchronous, [futures]-based Kafka client library for Rust based on [librdkafka].

## The library
`rust-rdkafka` provides a safe Rust interface to librdkafka. It is currently based on librdkafka 0.9.5.

### Documentation

- [Current master branch](https://fede1024.github.io/rust-rdkafka/)
- [Latest release](https://docs.rs/rdkafka/)
- [Changelog](https://github.com/fede1024/rust-rdkafka/blob/master/changelog.md)

### Features

The main features provided at the moment are:

- Support for Kafka 0.8.x, 0.9.x and 0.10.x. For more information about broker compatibility options, check the [librdkafka documentation].
- Consume from single or multiple topics.
- Automatic consumer rebalancing.
- Customizable rebalance, with pre and post rebalance callbacks.
- Synchronous or asynchronous message production.
- Customizable offset commit.
- Access to cluster metadata (list of topic-partitions, replicas, active brokers etc).
- Access to group metadata (list groups, list members of groups, hostnames etc).
- Access to producer and consumer metrics, errors and callbacks.

[librdkafka documentation]: https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility

### Client types

`rust-rdkafka` provides low level and high level consumers and producers. Low level:

* [`BaseConsumer`]: simple wrapper around the librdkafka consumer. It requires to be periodically `poll()`ed in order to execute callbacks, rebalances and to receive messages.
* [`BaseProducer`]: simple wrapper around the librdkafka producer. As in the consumer case, the user must call `poll()` periodically to execute delivery callbacks.

High level:

 * [`StreamConsumer`]: it returns a [`stream`] of messages and takes care of polling the consumer internally.
 * [`FutureProducer`]: it returns a [`future`] that will be completed once the message is delivered to Kafka (or failed).

[`BaseConsumer`]: https://docs.rs/rdkafka/0.10.0/rdkafka/consumer/base_consumer/struct.BaseConsumer.html
[`BaseProducer`]: https://docs.rs/rdkafka/0.10.0/rdkafka/producer/struct.BaseProducer.html
[`StreamConsumer`]: https://docs.rs/rdkafka/0.10.0/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html
[`FutureProducer`]: https://docs.rs/rdkafka/0.10.0/rdkafka/producer/struct.FutureProducer.html
[librdkafka]: https://github.com/edenhill/librdkafka
[futures]: https://github.com/alexcrichton/futures-rs
[`future`]: https://docs.rs/futures/0.1.3/futures/trait.Future.html
[`stream`]: https://docs.rs/futures/0.1.3/futures/stream/trait.Stream.html

*Warning*: the library is under active development and the APIs are likely to change.

### Asynchronous data processing with tokio-rs
[tokio-rs] is a platform for fast processing of asynchronous events in Rust. The interfaces exposed by the `StreamConsumer` and the `FutureProducer` allow rust-rdkafka users to easily integrate Kafka consumers and producers within the tokio-rs platform, and write asynchronous message processing code. Note that rust-rdkafka can be used without tokio-rs.

To see rust-rdkafka in action with tokio-rs, check out the [asynchronous processing example] in the examples folder.

[tokio-rs]: https://tokio.rs/
[asynchronous processing example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/asynchronous_processing.rs

### At-least-once delivery

At-least-once delivery semantic is common in many streaming applications: every message is guaranteed to be processed at least once; in case of temporary failure, the message can be re-processed and/or re-delivered, but no message will be lost.

In order to implement at-least-once delivery the stream processing application has to carefully commit the offset only once the message has been processed. Committing the offset too early, instead, might cause message loss, since upon recovery the consumer will start from the next message, skipping the one where the failure occurred.

To see how to implement at-least-once delivery with `rdkafka`, check out the [at-least-once delivery example] in the examples folder. To know more about delivery semantics, check the [message delivery semantics] chapter in the Kafka documentation.

[at-least-once delivery example]: https://github.com/fede1024/rust-rdkafka/blob/master/examples/at_least_once.rs
[message delivery semantics]: https://kafka.apache.org/0101/documentation.html#semantics

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rdkafka = "^0.10.0"
```

This crate will compile librdkafka from sources and link it statically to your executable. To compile librdkafka you'll need:

* the GNU toolchain
* GNU `make`
* `pthreads`
* `zlib`
* `libssl-dev`: optional, *not* included by default (feature: `ssl`).
* `libsasl2-dev`: optional, *not* included by default (feature: `sasl`).

To enable ssl and sasl, use the `features` field in `Cargo.toml`. Example:

```toml
[dependencies.rdkafka]
version = "^0.10.0"
features = ["ssl", "sasl"]
```

## Compiling from sources

To compile from sources, you'll have to update the submodule containing librdkafka:

```bash
git submodule update --init
```

and then compile using `cargo`, selecting the features that you want. Example:

```bash
cargo build --features "ssl sasl"
```

## Examples

You can find examples in the `examples` folder. To run them:

```bash
cargo run --example <example_name> -- <example_args>
```

## Tests

### Unit tests

The unit tests can run without a Kafka broker present:

```bash
cargo test --lib
```

### Automatic testing

rust-rdkafka contains a suite of tests which is automatically executed by travis in
docker-compose. Given the interaction with C code that rust-rdkafka has to do, tests
are executed in valgrind to check eventual memory errors and leaks.

To run the full suite using docker-compose:

```bash
./test_suite.sh
```

To run locally, instead:

```bash
KAFKA_HOST="kafka_server:9092" cargo test
```

In this case there is a broker expected to be running on `KAFKA_HOST`.
The broker must be configured with default partition number 3 and topic autocreation in order
for the tests to succeed.

## rdkafka-sys

Sys wrapper around [librdkafka](https://github.com/edenhill/librdkafka).

To regenerate the bindings:

```
bindgen --builtins --convert-macros librdkafka/src/rdkafka.h > src/bindings/{platform}.rs
```

## Contributors

Thanks to:
* Thijs Cadier - [thijsc](https://github.com/thijsc)

## Alternatives

* [kafka-rust]: a pure Rust implementation of the Kafka client.

[kafka-rust]: https://github.com/spicavigo/kafka-rust
