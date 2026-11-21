# Maintainer and contributor instructions

## Compiling from source

To compile from source, you'll have to initialize the submodule containing
librdkafka:

```bash
git submodule update --init
```

and then compile using `cargo`, selecting the features that you want.
Example:

```bash
cargo build --features "ssl gssapi"
```

## Tests

### Unit tests

The unit tests can run without a Kafka broker present:

```bash
cargo test --lib
```

### Integration tests

The integration tests start their own Kafka broker via
[testcontainers-rs], so all you need locally is a running Docker daemon
and the usual Rust toolchain:

```bash
cargo test
```

To pick a specific Kafka version (default `4.0`), set `KAFKA_VERSION`:

```bash
KAFKA_VERSION=3.9 cargo test
```

For the full walkthrough, including how the shared broker is wired up,
how to add a new test, the helper cheatsheet, and known quirks, see
[`tests/README.md`](tests/README.md).

[testcontainers-rs]: https://github.com/testcontainers/testcontainers-rs

## Releasing

* Checkout into master and pull the latest changes.
* Ensure `rdkafka-sys` has no unreleased changes. If it does, release `rdkafka-sys` first.
* Ensure the changelog is up to date (i.e not Unreleased changes).
* Bump the version in Cargo.toml and commit locally.
* Run `cargo publish`.
* Run `git tag -am $VERSION $VERSION`.
* Run `git push`.
* Run `git push origin $VERSION`.
