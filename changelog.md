# Changelog

See also the [rdkafka-sys changelog](rdkafka-sys/changelog.md).

## Unreleased

## 0.35.0 (2023-11-07)

* Update bundled librdkafka to 2.3.0.
* Add cargo enforcement of MSRV of 1.61.
* Derives serde::Serialize on Statistics

## 0.34.0 (2023-08-25)

* Update bundled librdkafka to 2.2.0.

## 0.33.2 (2023-07-06)

* **Breaking change.** Change signature for `seek_partitions`. Following
  librdkafka, individual partition errors should be reported in the per-partition
  `error` field of `TopicPartitionList` elements.

## 0.33.0 (2023-06-30)

* Add interface to specify custom partitioners by extending `ProducerContext`
  trait with capability to return optional custom partitioner.
* Add `seek_partitions` to consumer.

## 0.32.1 (2023-06-09)

* Add support for the cluster mock API.
* Expose assignment_lost method on the consumer.

## 0.31.0 (2023-05-17)

* **Breaking change.** Pass `KafkaError` to rebalance hooks instead of human-readable string
  representation.

## 0.30.0 (2023-05-12)

* Support for unassigning static partitions by passing `null` to `rdsys::rd_kafka_assign` and expose the
feature as `unassign` in `base_consumer`

* Expose `rdsys::rd_kafka_incremental_assign` and `rdsys::rd_kafka_incremental_unassign` in `base_consumer` for 
incremental changes to static assignments

* **Breaking change.** `util::get_rdkafka_version` now returns `(i32, String)`.
  Previously, it returned `(u16, String)` which would silently truncate the hex
  representation of the version:
  > Interpreted as hex MM.mm.rr.xx:
  > 
  > MM = Major
  > mm = minor
  > rr = revision
  > xx = pre-release id (0xff is the final release)
  > E.g.: 0x010902ff = 1.9.2

* Add the `AdminClient::delete_groups` method, which deletes consumer groups
  from a Kafka cluster ([#510]).

  Thanks, [@andrewinci].

[@andrewinci]: https://github.com/andrewinci
[#510]: https://github.com/fede1024/rust-rdkafka/issues/510

* Add support for the `purge` API, that allows retreiving messages that were
  queued for production when shutting down. It is automatically called on `Drop`.
  Fixes leaking associated data (futures...).


## 0.29.0 (2022-10-29)

* **Breaking change.** Pass through errors from librdkafka in
  `BaseProducer::flush`, `StreamProducer::flush`, and `FutureProducer::flush`.

  Thanks, [@cjubb39].

* **Breaking change.** Only provide `NaiveRuntime` if the `naive-runtime`
  feature is enabled. This elides a dependency on `futures-executor` when the
  `naive-runtime` feature is disabled.

* **Breaking change.** Remove the deprecated `StreamConsumer::start` method.
  Use the more clearly named `StreamConsumer::stream` method instead.

* **Breaking change.** Rework the `Headers` trait to distinguish between
  headers with null values and headers with empty values. The `Headers::get`
  and `Headers::get_as` methods now return a `Header` struct with the following
  definition:

  ```rust
  pub struct Header<'a, V> {
      pub key: &'a str,
      pub value: Option<V>,
  }
  ```

  Previously, these methods operated in terms of key–value pair `(&str, &[u8])`.

  These methods now panic if presented with an out-of-bounds index. This
  simplifies their usage in the common case where the index is known to be
  valid. Use the new `Headers::try_get` and `Headers::try_get_as` methods if you
  need the old behavior of returning `None` if the index is invalid.

* Rename the `OwnedHeader::add` method to `OwnedHeader::insert`, for parity with
  the equivalent method for the map types in `std::collection` and to avoid
  confusion with the `add` method of the `std::ops::Add` trait. The method now
  takes the `Header` type mentioned above as an argument, rather than the key
  and value as separate arguments.

* Add the `Headers::iter` method to iterate over all message headers in order.

* Add the `PartitionQueue::set_nonempty_callback` method to register a callback
  for a specific partition queue that will run when that queue becomes
  nonempty. This is a more flexible replacement for the
  `ConsumerContext::message_queue_nonempty_callback` method that was removed
  in the last release.

* In `BaseConsumer::rebalance_protocol` and `StreamConsumer::rebalance_protocol`,
  handle null return values from the underlying librdkakfa API ([#417]). This
  avoids an occasional segfault in the rebalance callback.

  Thanks, [@bruceg].

* Add a `tracing` feature which, when enabled, emits log messages using the
  `tracing` crate rather than the `log` crate.

* Add support for the `OAUTHBEARER` SASL authentication mechanism via the new
  `ClientContext::ENABLE_REFRESH_OAUTH_TOKEN` constant and the new
  `ClientContext::generate_oauth_token` method.

  Thanks, [@jsurany-bloomberg].

[#417]: https://github.com/fede1024/rust-rdkafka/issues/417
[@bruceg]: https://github.com/bruceg
[@cjubb39]: https://github.com/cjubb39
[@jsurany-bloomberg]: https://github.com/jsurany-bloomberg

## 0.28.0 (2021-11-27)

* Add the `StreamConsumer::split_partition_queue` method to mirror
  `BaseConsumer::split_partition_queue` ([#411]).

  Thanks to [@davidblewett], [@duarten], and [@nemosupremo] for contributing to
  the implementation.

* **Breaking change.** Remove the `StreamConsumerContext` type and the
  `ConsumerContext::message_queue_nonempty_callback` method. These were
  essentially implementation details of `StreamConsumer` that had leaked into
  the public API. The vast majority of users should be unaffected.

* **Breaking change.** Remove the type parameters from the `MessageStream` type.

* **Breaking change.** Add the received `TopicPartitionList` to the
  `Rebalance::Revoke` variant, which is useful when using incremental
  cooperative rebalancing ([#398]).

* Avoid crashing if librdkafka invokes the commit callback with a null
  topic partition list ([#406]).

  Thanks, [@thijsc].

* Add the new statistics fields in librdkafka v1.7.0 to the various statistics
  types. The new fields are:

    * `Partition::consumer_lag_stored`
    * `Broker::txidle`
    * `Broker::rxidle`
    * `Statistics::age`

* **Breaking change.** Change the type of the following statistics fields from
  `i64` to `u64` to reflect the signedness of the upstream types:

    * `Statistics::msg_cnt`
    * `Statistics::msg_size`
    * `Statistics::msg_max`
    * `Statistics::msg_size_max`
    * `Broker::tx`
    * `Broker::txbytes`
    * `Broker::txretries`
    * `Broker::req_timeouts`
    * `Broker::rx`
    * `Broker::rxbytes`
    * `Broker::rxerrs`
    * `Broker::rxcorriderrs`
    * `Broker::rxpartial`
    * `Broker::zbuf_grow`
    * `Broker::buf_grow`
    * `Broker::wakeups`
    * `Broker::msgq_bytes`
    * `Broker::xmit_msgq_bytes`
    * `Broker::fetchq_size`
    * `Partition::txmsgs`
    * `Partition::txbytes`
    * `Partition::rxmsgs`
    * `Partition::rxbytes`
    * `Partition::msgs`
    * `Partition::rx_ver_drops`
    * `Partition::acked_msgid`

* Add the `ClientContext::stats_raw` method to consume the JSON-encoded
  statistics from librdkafka. The default implementation calls
  `ClientContext::stats` with the decoded statistics.

* Add the `Default` trait to the statistics types: `Statistics`, `Broker`,
  `Window`, `TopicPartition`, `Topic`, `Partition`, `ConsumerGroup`, and
  `ExactlyOnceSemantics` ([#410]).

  Thanks, [@scanterog].

* Add the `Debug` trait to `DefaultClientContext` and `DefaultConsumerContext`
  ([#401]).

  Thanks, [@DXist].

[#398]: https://github.com/fede1024/rust-rdkafka/issues/398
[#401]: https://github.com/fede1024/rust-rdkafka/issues/401
[#406]: https://github.com/fede1024/rust-rdkafka/issues/406
[#410]: https://github.com/fede1024/rust-rdkafka/issues/410
[#411]: https://github.com/fede1024/rust-rdkafka/issues/411
[@davidblewett]: https://github.com/davidblewett
[@duarten]: https://github.com/duarten
[@DXist]: https://github.com/DXist
[@nemosupremo]: https://github.com/nemosupremo
[@scanterog]: https://github.com/scanterog
[@thijsc]: https://github.com/thijsc

<a name="0.27.0"></a>
## 0.27.0 (2021-10-17)

* Allow offset 0 in `Offset::to_raw`.

  Thanks, [@roignpar].

* Fix a segfault when calling `Consumer::position` on a consumer that was
  improperly configured ([#360]).

* Provide a mutable accessor (`Message::payload_mut`) for a message's
  payload ([#95]).

* Implement `std::iter::Extend<(String, String)>` and
  `std::iter::FromIterator<(String, String)` for `ClientConfig` ([#367]).

  Thanks, [@djKooks].

* **Breaking change.** Change `Consumer::store_offset` to accept the topic,
  partition, and offset directly ([#89], [#368]). The old API, which took a
  `BorrowedMessage`, is still accessible as
  `Consumer::store_offset_from_message`.

* Support incremental cooperative rebalancing ([#364]). There are two changes
  of note:

    * The addition of `Consumer::rebalance_protocol` to determine the rebalance
      protocol in use.

    * The modification of the default rebalance callback
      (`ConsumerContext::rebalance`) to perform incremental assignments and
      unassignments when the rebalance protocol in use is
      [`RebalanceProtocol::Cooperative`].

  Thanks, [@SreeniIO].

* Support reading and writing commit metadata via
  `TopicPartitionListElem::metadata` and `TopicPartitionListElem::set_metadata`,
  respectively ([#391]).

  Thanks, [@phaazon].

[#89]: https://github.com/fede1024/rust-rdkafka/issues/89
[#95]: https://github.com/fede1024/rust-rdkafka/issues/95
[#360]: https://github.com/fede1024/rust-rdkafka/issues/360
[#364]: https://github.com/fede1024/rust-rdkafka/issues/364
[#367]: https://github.com/fede1024/rust-rdkafka/issues/367
[#368]: https://github.com/fede1024/rust-rdkafka/issues/368
[#391]: https://github.com/fede1024/rust-rdkafka/issues/391
[@djKooks]: https://github.com/djKooks
[@phaazon]: https://github.com/phaazon
[@SreeniIO]: https://github.com/SreeniIO

<a name="0.26.0"></a>
## 0.26.0 (2021-03-16)

* Fix compilation for the aarch64 target.

* Add an `inner` method to `StreamConsumerContext` to enable access to the
  underlying context.

  Thanks, [@marcelo140].

* Mark the `KafkaError` enum as [non-exhaustive] so that future additions to
  the enum will not be considered breaking changes.

[@marcelo140]: https://github.com/marcelo140

<a name="0.25.0"></a>
## 0.25.0 (2021-01-30)

* Add support for transactional producers. The new methods are
  `Producer::init_transactions`, `Producer::begin_transaction`,
  `Producer::commit_transaction`, `Producer::abort_transaction`, and
  `Producer::send_offsets_to_transaction`.

  Thanks to [@roignpar] for the implementation.

* **Breaking change.** Rename `RDKafkaError` to `RDKafkaErrorCode`. This makes
  space for the new `RDKafkaError` type, which mirrors the `rd_kafka_error_t`
  type added to librdkafka in v1.4.0.

  This change was made to reduce long-term confusion by ensuring the types in
  rust-rdkafka map to types in librdkafka as directly as possible. The
  maintainers apologize for the difficulty in upgrading through this change.

* **Breaking change.** Rework the consumer APIs to fix several bugs and design
  warts:

  * Rename `StreamConsumer::start` to `StreamConsumer::stream`, though the
    former name will be retained as a deprecated alias for one release to ease
    the transition. The new name better reflects that the method is a cheap
    operation that can be called repeatedly and in multiple threads
    simultaneously.

  * Remove the `StreamConsumer::start_with` and
    `StreamConsumer::start_with_runtime` methods.

    There is no replacement in rust-rdkafka itself for the `no_message_error`
    parameter. If you need this message, use a downstream combinator like
    `tokio_stream::StreamExt::timeout`.

    There is no longer a need for the `poll_interval` parameter to these
    methods. Message delivery is now entirely event driven, so no time-based
    polling occurs.

    To specify an `AsyncRuntime` besides the default, specify the desired
    runtime type as the new `R` parameter of `StreamConsumer` when you create
    it.

  * Remove the `Consumer::get_base_consumer` method, as
    accessing the `BaseConsumer` that underlied a `StreamConsumer` was
    dangerous.

  * Return an `&Arc<C>` from `Client::context` rather than an
    `&C`. This is expected to cause very little breakage in practice.

  * Move the `BaseConsumer::context` method to the `Consumer`
    trait, so that it is available when using the `StreamConsumer` as well.

* **Breaking change.** Rework the producer APIs to fix several design warts:

  * Remove the `FutureProducer::send_with_runtime` method. Use the `send`
    method instead. The `AsyncRuntime` to use is determined by the new `R`
    type parameter to `FutureProducer`, which you can specify when you create
    the producer.

    This change makes the `FutureProducer` mirror the redesigned
    `StreamConsumer`.

    This change should have no impact on users who use the default runtime.

  * Move the `producer::base_producer::{ProducerContext, DefaultProducerContext}`
    types out of the `base_producer` module and into the `producer` module
    directly, to match the `consumer` module layout.

  * Move the `client`, `in_flight_count`, and `flush` methods inherent to all
    producers to a new `Producer` trait. This trait is analogous to the
    `Consumer` trait.

* **Breaking change.** Calls to `BaseConsumer::assign` deactivate any
  partition queues previously created with
  `BaseConsumer::split_partition_queue`. You will need to re-split all
  partition queues after every call to `assign`.

  This is due to an upstream change in librdkafka. See
  [edenhill/librdkafka#3231](https://github.com/edenhill/librdkafka/issues/3231)
  for details.

* **Breaking change.** Several `TopicPartitionList`-related methods now return
  `Result<T, KafkaError>` rather than `T`:

  * `TopicPartitionListElem::set_offset`
  * `TopicPartitionList::from_topic_map`
  * `TopicPartitionList::add_partition_offset`
  * `TopicPartitionList::set_all_offsets`

  This was necessary to properly throw errors when an `Offset` passed to one
  of these methods is representable in Rust but not in C.

* Support end-relative offsets via `Offset::OffsetTail`.

* Fix stalls when using multiple `MessageStream`s simultaneously.

  Thanks to [@Marwes] for discovering the issue and contributing the initial
  fix.

* Add a convenience method, `StreamConsumer::recv`, to yield the next message
  from a stream.

  Thanks again to [@Marwes].

* Add a new implementation of `AsyncRuntime` called `NaiveRuntime` that does not
  depend on Tokio.

  This runtime has poor performance, but is necessary to make the crate compile
  when the `tokio` feature is disabled.

* Add the `ClientConfig::get` and `ClientConfig::remove` methods to retrieve
  and remove configuration parameters that were set with `ClientConfig::set`.

* **Breaking change.** Change the `key` and `value` parameters of the
  `ClientConfig::set` method to accept any type that implements `Into<String>`,
  rather than only `&str`.

  This is technically a breaking change as values of type `&&str` are no longer
  accepted, but this is expected to be a rare case.

  Thanks, [@koushiro].

* Add the `NativeClientConfig::get` method, which reflects librdkafka's
  view of a parameter value. Unlike `ClientConfig::get`, this method is capable
  of surfacing librdkafka's default value for a parameter.

* Add the missing `req` field, which counts the number of requests of each type
  that librdkafka has sent, to the `Statistics` struct. Thanks, [@pablosichert]!

[@koushiro]: https://github.com/koushiro
[@Marwes]: https://github.com/Marwes
[@pablosichert]: https://github.com/pablosichert
[@roignpar]: https://github.com/roignpar

<a name="0.24.0"></a>
## 0.24.0 (2020-07-08)

* **Breaking change.** Introduce a dependency on Tokio for the `StreamConsumer`
  in its default configuration. The new implementation is more efficient and
  does not require a background thread and an extra futures executor.

* Introduce the `StreamConsumer::start_with_runtime` and
  `FutureProducer::send_with_runtime` methods. These methods are identical to
  their respective non-`_with_runtime` counterparts, except that they take
  an additional `AsyncRuntime` generic parameter that permits using an
  asynchronous runtime besides Tokio.

  For an example of using rdkafka with the [smol] runtime, see the
  new [smol runtime] example.

* **Breaking change.** Remove the `StreamConsumer::stop` method. To stop a
  `StreamConsumer` after calling `start`, simply drop the resulting
  `MessageStream`.

* **Breaking change.** Overhaul the `FutureProducer::send` method. The old
  implementation incorrectly blocked asynchronous tasks with
  `std::thread::sleep` and the `block_ms` parameter did not behave as
  documented.

  The new implementation:

    * changes the `block_ms: i64` parameter to
      `queue_timeout: impl Into<Timeout>`, to better match how timeouts are
      handled elsewhere in the rust-rdkafka API,

    * depends on Tokio, in order to retry enqueuing after a time interval
      without using `std::thread::sleep`,

    * returns an opaque future that borrows its input, rather than a
      `DeliveryFuture` with no internal references,

    * simplifies the output type of the returned future from
      `Result<OwnedDeliveryResult, oneshot::Canceled>` to `OwnedDeliveryResult`.

  Thanks to [@FSMaxB-dooshop] for discovering the issue and contributing the
  initial fix.

* **Breaking change.** Remove the `util::duration_to_millis` function. This
  functionality has been available in the standard library as
  [`std::time::Duration::as_millis`] for over a year.

* Introduce the `BaseConsumer::split_partition_queue` method to allow reading
  messages from partitions independently of one another.

* Implement `Clone`, `Copy`, and `Debug` for `CommitMode`.

* Decouple versioning of rdkafka-sys from rdkafka. rdkafka-sys now has its
  own [changelog](rdkafka-sys/changelog.md) and will follow SemVer conventions.
  ([#211])

[#211]: https://github.com/fede1024/rust-rdkafka/issues/211
[`std::time::Duration::as_millis`]: https://doc.rust-lang.org/stable/std/time/struct.Duration.html#method.as_millis
[smol runtime]: https://github.com/fede1024/rust-rdkafka/tree/master/examples/smol_runtime.rs
[smol]: docs.rs/smol

[@FSMaxB-dooshop]: https://github.com/FSMaxB-dooshop

<a name="0.23.1"></a>
## 0.23.1 (2020-01-13)

* Fix build on docs.rs.

<a name="0.23.0"></a>
## 0.23.0 (2019-12-31)

* Upgrade to the async/await ecosystem, including `std::future::Future`, v0.3
  of the futures crate, and v0.2 of Tokio. The minimum supported Rust version
  is now Rust 1.39. Special thanks to [@sd2k] and [@dbcfd]. ([#187])

  The main difference is that functions that previously returned
  ```rust
  futures01::Future<Item = T, Error = E>
  ```
  now return:
  ```rust
  std::future::Future<Output = Result<T, E>>
  ```
  In the special case when the error was `()`, the new signature is further
  simplified to:
  ```rust
  std::future::Future<Output = T>
  ```
  Functions that return `future::Stream`s have had the analogous transformation
  applied.

* Implement `Send` and `Sync` on `BorrowedMessage`, so that holding a reference
  to a `BorrowedMessage` across an await point is possible. ([#190])

* Implement `Sync` on `OwnedHeaders`, which applies transitively to
  `OwnedMessage`, so that holding a reference to an `OwnedMessage` across an
  await point is possible. ([#203])

* Bump librdkafka to v1.3.0. ([#202])

* Change the signature of `ConsumerContext::commit_callback` so that the
  offsets are passed via a safe `TopicPartitionList` struct, and not a
  raw `*mut rdkafka_sys::RDKafkaPartitionList` pointer. Thanks, [@scrogson]!
  ([#198]).

* Fix CMake build on Windows when debug information is enabled ([#194]).

[#187]: https://github.com/fede1024/rust-rdkafka/pull/187
[#190]: https://github.com/fede1024/rust-rdkafka/pull/190
[#194]: https://github.com/fede1024/rust-rdkafka/pull/194
[#198]: https://github.com/fede1024/rust-rdkafka/pull/198
[#202]: https://github.com/fede1024/rust-rdkafka/pull/202
[#203]: https://github.com/fede1024/rust-rdkafka/pull/203

[@sd2k]: https://github.com/sd2k
[@dbcfd]: https://github.com/dbcfd
[@scrogson]: https://github.com/scrogson

<a name="0.22.0"></a>
## 0.22.0 (2019-12-01)

* Add a client for Kafka's Admin API, which allows actions like creating and
  deleting Kafka topics and changing configuration parameters. ([#122])
* Fix compliation on ARM, and ensure it stays fixed by adding an ARM builder
  to CI. ([#134], [#162])
* Stop automatically generating librdkafka bindings. Platform-independent
  bindings are now checked in to the repository. ([#163])
* Move zstd compression support behind the `zstd` feature flag. ([#163])
* Remove build-time dependency on bindgen, clang, and libclang. ([#163])
* Support `Consumer::pause` and `Consumer::resume`. ([#167])
* Expose the `message_queue_nonempty` callback, which allows clients to put
  their poll thread to sleep and be woken up when new data arrives. ([#164])
* Implement `IntoOpaque` for `Arc<T>`. ([#171])
* Add `Consumer::seek` method. ([#172])
* Support building with Microsoft Visual C++ (MSVC) on Windows. ([#176])
* Bump librdkafka to v1.2.2. ([#177])
* Run tests against multiple Kafka versions in CI. ([#182])
* Standardize feature names. All feature names now use hyphens instead of
  underscores, as is conventional, though the old names remain for
  backwards compatibility. ([#183])
* Optionalize libz via a new `libz` feature. The new feature is a default
  feature for backwards compatibility. ([#183])
* Better attempt to make build systems agree on what version of a dependency
  to compile and link against, and document this hazard. ([#183])

[#122]: https://github.com/fede1024/rust-rdkafka/pull/122
[#134]: https://github.com/fede1024/rust-rdkafka/pull/134
[#162]: https://github.com/fede1024/rust-rdkafka/pull/162
[#163]: https://github.com/fede1024/rust-rdkafka/pull/163
[#164]: https://github.com/fede1024/rust-rdkafka/pull/164
[#167]: https://github.com/fede1024/rust-rdkafka/pull/167
[#171]: https://github.com/fede1024/rust-rdkafka/pull/171
[#172]: https://github.com/fede1024/rust-rdkafka/pull/172
[#176]: https://github.com/fede1024/rust-rdkafka/pull/176
[#177]: https://github.com/fede1024/rust-rdkafka/pull/177
[#182]: https://github.com/fede1024/rust-rdkafka/pull/182
[#183]: https://github.com/fede1024/rust-rdkafka/pull/183


<a name="0.21.0"></a>
## 0.21.0 (2019-04-24)

* Add librdkafka 1.0 support
* Automatically generate librdkafka bindings
* Use updated tokio version in asynchronous\_processing example


<a name="0.20.0"></a>
## 0.20.0 (2019-02-25)

* Add FreeBSD support
* Add `offsets_for_times` method
* Add `committed_offsets` method

<a name="0.19.0"></a>
## 0.19.0 (2019-02-06)

* Fix ordering of generics in FutureProducer::send

<a name="0.18.1"></a>
## 0.18.1 (2019-02-06)

* Add method for storing multiple offsets

<a name="0.18.0"></a>
## 0.18.0 (2019-01-18)

* Upgrade librdkafka to 0.11.6

<a name="0.17.0"></a>
## 0.17.0 (2018-06-30)

* Add missing documentation warning.
* Add new experimental producer API. Instead of taking key, value and timestamp directly,
  producers now get them in a `ProducerRecord` which allows to specify optional arguments using
  the builder pattern.
* Add message headers support.
* Upgrade tokio-core to tokio in async example, remove futures-cpupool.
* `MessageStream` is now Send and Sync

<a name="0.16.0"></a>
## 0.16.0 (2018-05-20)

* Upgrade librdkafka to 0.11.4

<a name="0.15.0"></a>
## 0.15.0 (2018-03-15)

* Added iterator interface to the `BaseConsumer`.
* Change timeout to more rust-idiomatic `Option<Duration>`.
* Add `external_lz4` feature to use external lz4 library instead of
  the one one built in librdkafka. Disable by default.
* Mark all `from_ptr` methods as unsafe.
* Remove `Timestamp::from_system_time` and implement `From` trait instead.
* Rename `Context` to `ClientContext`.
* Rename `Empty(...)Context` to `Default(...)Context`.
* Use default type parameters for the context of `Client`, producers and consumers
  with `Default(...)Context` set as the default one.
* Increase default buffer size in `StreamConsumer` from 0 to 10 to reduce context switching.

<a name="0.14.1"></a>
## 0.14.1 (2017-12-30)

* Upgrade to librdkafka 0.11.3
* Add `send_copy_result` method to `FutureProducer`

<a name="0.14.0"></a>
## 0.14.0 (2017-11-26)

#### Features
* Make `PollingProducer` methods public
* Rename `PollingProducer` to `ThreadedProducer`

#### Refactoring
* Remove `TopicConfig` since librdkafka supports default topic configuration
  directly in the top level configuration
* Rename `DeliveryContext` into `DeliveryOpaque`
* Add `IntoOpaque` trait to support different opaque types.

#### Bugs
* Fix regression in producer error reporting (#65)

<a name="0.13.0"></a>
## 0.13.0 (2017-10-22)

#### Refactoring
* Split producer.rs into multiple files
* Both producers now return the original message after failure
* BaseConsumer returns an Option\<Result\> instead of Result\<Option\>

#### Features
* Upgrade to librdkafka 0.11.1
* Enable dynamic linking via feature
* Refactor BaseConsumer, which now implements the Consumer trait directly
* A negative timestamp will now automatically be reported as NonAvailable timestamp
* Point rdkafka-sys to latest librdkafka master branch
* Add producer.flush and producer.in\_flight\_count
* Add max block time for FutureProducer

#### Bugs
* Fix memory leak during consumer error reporting
* Fix memory leak during producer error reporting

<a name="0.12.0"></a>
## 0.12.0 (2017-07-25)

#### Features
* Upgrade librdkafka to 0.11.0.
* `FutureProducer::send_copy` will now return a `DeliveryFuture` direcly.
* TPL entries now also export errors.
* `KafkaError` is now Clone and Eq.

#### Bugs
* Fix flaky tests.

<a name="0.11.1"></a>
## 0.11.1 (2017-06-25)

#### Features

* Support direct creation of OwnedMessages.

<a name="0.11.0"></a>
## 0.11.0 (2017-06-20)

#### Features

* The topic partition list object from librdkafka is now completely accessible
  from Rust.
* The test suite will now run both unit tests and integration tests in
  valgrind, and it will also check for memory leaks.
* rdkafka-sys will use the system librdkafka if it's already installed.
* rdkafka-sys will verify that the crate version corresponds to the librdkafka
  version during the build.
* Timestamp is now Copy.
* Message has been renamed to BorrowedMessage. Borrowed messages can be transformed
  into owned messages. Both implement the new Message trait.
* Improved error enumerations.

#### Bugs

* Fix memory access bug in statistics callback.
* Fix memory leak in topic partition list.
* Messages lifetime is now explicit (issue [#48](https://github.com/fede1024/rust-rdkafka/issues/48))

<a name="0.10.0"></a>
## 0.10.0 (2017-05-15)

#### Features

* Consumer commit callback
* Add configurable poll timeout
* Add special error code for message not received within poll time
* Add topic field for messages
* Make the topic partition list optional for consumer commit
* Add `store_offset` to consumer
* Add at-least-once delivery example

<a name="0.9.1"></a>
## 0.9.1 (2017-04-19)

#### Features

* OpenSSL dependency optional
* librdkafka 0.9.5

<a name="0.9.0"></a>
## 0.9.0 (2017-04-16)

#### Bugs

* Fix termination sequence

#### Features

* Integration tests running in docker and valgrind
* Producer topics are not needed anymore

<a name="0.8.1"></a>
## 0.8.1 (2017-03-21)

##### Bugs

* Implement Clone for `BaseProducerTopic`

#### Features

<a name="0.8.0"></a>
## 0.8.0 (2017-03-20)

#### Features

* Add timestamp support
* librdkafka 0.9.4
* Add client statistics callback and parsing

<a name="0.7.0"></a>
## 0.7.0 (2017-02-18)

#### Features

* Asynchronous message processing example based on tokio
* More metadata for consumers
* Watermark API
* First iteration of integration test suite

[non-exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute
