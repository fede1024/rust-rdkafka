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
The broker must be configured with default partition number 3 and topic
autocreation in order for the tests to succeed.

## Releasing

* Ensure `rdkafka-sys` has no unreleased changes.
* Ensure the changelog is up to date.
* Ensure Cargo.toml is up to date.
* Run `./generate_readme.py > README.md`.
* Run `git tag -am $VERSION $VERSION`.
* Run `git push`.
* Run `git push origin $VERSION`.
* Run `cargo publish`.
