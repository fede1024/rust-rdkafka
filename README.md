# rust-rdkafka

Kafka client library for Rust based on `librdkafka`.

[![docs.rs](https://docs.rs/rdkafka/badge.svg)](https://docs.rs/rdkafka/)
[![Join the chat at https://gitter.im/rust-rdkafka/Lobby](https://badges.gitter.im/rust-rdkafka/Lobby.svg)](https://gitter.im/rust-rdkafka/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![crates.io](https://img.shields.io/crates/v/rdkafka.svg)](https://crates.io/crates/rdkafka)

## The library
This library aims to provide a safe interface to [`librdkafka`].
It currently exports some of the funcionalities provided by the producer and consumer
of `librdkafka` 0.9.1.

Producers and consumers can be accessed and polled directly, or alternatively
a [`futures`]-based interface can be used:

* A consumer will return a [`stream`] of messages, as they are received from Kafka.
* A producer will return a [`future`] that will eventually contain the delivery
status of the message.

[`librdkafka`]: https://github.com/edenhill/librdkafka
[`futures`]: https://github.com/alexcrichton/futures-rs
[`future`]: https://docs.rs/futures/0.1.3/futures/trait.Future.html
[`stream`]: https://docs.rs/futures/0.1.3/futures/stream/trait.Stream.html

*Warning*: this library is still at an early development stage, the API is very likely
to change and it shouldn't be considered production ready.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rdkafka = "^0.1.0"
```

This crate will compile `librdkafka` from sources and link it statically in your
executable. To compile `librdkafka` you'll need:

* the GNU toolchain
* GNU `make`
* `pthreads`
* `zlib`: optional, included by default (feature: `zlib`).
* `libssl-dev`: optional, *not* included by default (feature: `ssl`).
* `libsasl2-dev`: optional, *not* included by default (feature: `sasl`).

To enable ssl and sasl, use the `features` field in `Cargo.toml`. Example:

```
[dependencies.rdkafka]
version = "0.1.0"
features = ["ssl", "sasl"]
```

## Compiling from sources

To compile from sources, you'll have to update the submodule containing `librdkafka`:

```
git submodule update --init
```

and then compile using `cargo`, selecting the features that you want. Example:

```bash
cargo build --features "ssl sasl"
```

## Examples

You can find examples in the `examples` folder. To run them:

```bash
RUST_LOG="rdkafka=trace" LOG_THREAD=1 cargo run --example simple_consumer
```

The `RUST_LOG` environemnt variable will configure tracing level logging for `rdkafka`,
and `LOG_THREAD` will add the name of the thread to log messages.

## Documentation

Documentation is available on [docs.rs](https://docs.rs/rdkafka/).
