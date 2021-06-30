# Changelog

See also the [rdkafka-sys changelog](rdkafka-sys/changelog.md).

<a name="0.26.1"></a>
## 0.26.1 (Unreleased)

* Allow offset 0 in `Offset::to_raw`.

  Thanks, [@roignpar].

* Fix a segfault when calling `Consumer::position` on a consumer that was
  improperly configured ([#360]).

* Provide a mutable accessor (`Message::payload_mut`) for a message's
  payload ([#95]).

[#95]: https://github.com/fede1024/rust-rdkafka/issues/95
[#360]: https://github.com/fede1024/rust-rdkafka/issues/360

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
