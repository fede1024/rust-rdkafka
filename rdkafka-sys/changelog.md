# Changelog

## v4.1.0+1.7.0 (2021-06-30)

* Upgrade to librdkafka v1.7.0.

## v4.0.0+1.6.1 (2021-03-16)

* **Breaking change.** Mark the `RDKafkaErrorCode` enum as [non-exhaustive], so
  that future additions to the enum will not be considered breaking changes.

* Upgrade to librdkafka v1.6.1.

## v3.0.0+1.6.0 (2021-01-30)

* **Breaking change.** Rename `RDKafkaError` to `RDKafkaErrorCode`. This makes
  space for the new `RDKafkaError` type, which mirrors the `rd_kafka_error_t`
  type added to librdkafka in v1.4.0.

  This change was made to reduce long-term confusion by ensuring the types in
  rust-rdkafka map to types in librdkafka as directly as possible. The
  maintainers apologize for the difficulty in upgrading through this change.

* Upgrade to librdkafka v1.6.0.

* Enforce a minimum zstd-sys version of 1.4.19. This bumps the vendored version
  of libzstd to at least v1.4.8, which avoids a bug in libzstd v1.4.5 that could
  cause decompression failures ([edenhill/librdkafka#2672]).

## v2.1.1+1.5.3 (2021-01-05)

* Yanked due to an inadvertent breaking change.

## v2.1.0+1.5.0 (2020-08-02)

* Upgrade to librdkafka v1.5.0.

## v2.0.0+1.4.2 (2020-07-08)

* Start separate changelog for rdkafka-sys.

* Upgrade to librdkafka v1.4.2.

* Correct several references to `usize` in the generated bindings to `size_t`.

[edenhill/librdkafka#2672]: https://github.com/edenhill/librdkafka/issues/2672
[edenhill/librdkafka#3249]: https://github.com/edenhill/librdkafka/issues/3249
[non-exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute
