# Implementation plan: KIP-932 (Queues for Kafka) scaffolding

Target branch: `worktree-kip-932-queues` (fork: `j7nw4r/rust-rdkafka`).
Target PR: draft against `j7nw4r/rust-rdkafka:master`. Upstream
(`fede1024/rust-rdkafka`) is a follow-up once librdkafka exposes the
public KIP-932 C API.

Reference: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka>
Upstream tracking: librdkafka issue
<https://github.com/confluentinc/librdkafka/issues/5441>.

## 0. Working mode

This plan is a living document. It ships in a draft PR on the fork
and is iterated on in-tree while we wait for librdkafka to land
public KIP-932 C API. Expect the document to evolve as upstream
shapes solidify; treat the latest commit on this branch (not the
initial version) as the source of truth.

Practical consequences:

- The draft PR opens as soon as phase 1 lands and stays draft until
  librdkafka's public API is available, the scaffolding is wired to
  it, and the validation gate (section 7) passes.
- Phases 2 through 4 (types, error variant, trait + stub) can be
  executed against the current librdkafka pin since they introduce
  no FFI calls.
- Phase 5 onward (tests, CI lane, docs) may be paused or revised
  pending the upstream API shape, to avoid churn against a moving
  target.
- API shapes in section 5 are provisional. When librdkafka's public
  header lands, this document is updated first, then the code.
- The "Risks and follow-ups" section is the working backlog;
  add entries as we learn more rather than starting a separate
  tracking doc.

## 1. Context and constraints

- rust-rdkafka is a Rust FFI wrapper over librdkafka via `rdkafka-sys`.
- rdkafka-sys is currently pinned to librdkafka `2.10.0`
  (`rdkafka-sys` crate version `4.9.0+2.10.0`).
- As of today (2026-05-22), librdkafka master `src/rdkafka.h` exposes
  **zero** public symbols for KIP-932: no `rd_kafka_share_consumer_*`,
  no `RD_KAFKA_SHARE_*`, no share group / share session entrypoints.
  Verified by direct fetch of `src/rdkafka.h` and listing of `src/`.
- librdkafka has multiple in-progress `[KIP-932]` PRs (#5436, #5437,
  #5442, #5443, #5444, #5449, #5451, #5452, #5453, #5455). Tracking
  issue #5441 is open.
- Consequence: a functional ShareConsumer is **not implementable today**
  in rust-rdkafka. This PR delivers the **public Rust API surface** and
  scaffolding so that wiring real FFI is a mechanical follow-up once
  librdkafka ships a public API.

## 2. Goal and non-goals

### Goal

Land a draft PR that:

1. Adds the public Rust API for KIP-932 share consumers, behind a
   cargo feature flag `kip-932` (off by default).
2. Compiles cleanly with `--features kip-932` and without.
3. Has unit tests that exercise the public surface (enum
   round-trips, config builder, trait method dispatch through a
   stubbed implementation).
4. Drives CI green: `cargo build`, `cargo build --features kip-932`,
   `cargo test`, `cargo test --features kip-932`, `cargo fmt -- --check`,
   `cargo clippy --features kip-932 -- -D warnings`.

### Non-goals

- No real broker RPC traffic. No `ShareFetch`, `ShareAcknowledge`,
  `ShareGroupHeartbeat` wire calls.
- No changes to `rdkafka-sys/librdkafka` submodule pin. No new C bindings.
- No integration tests against a Kafka broker for share groups. The
  existing docker-compose harness is not extended.
- No CLI / admin parity for `kafka-share-groups.sh` operations on
  `AdminClient`. That belongs in a follow-up.
- No changes to the existing `BaseConsumer` / `StreamConsumer` /
  `Consumer` trait. KIP-932 is a parallel surface, not a subtype.

## 3. Module layout

```
src/
  consumer/
    mod.rs                 (unchanged trait surface; re-export gated)
    base_consumer.rs       (unchanged)
    stream_consumer.rs     (unchanged)
    share/                 (NEW, gated on feature = "kip-932")
      mod.rs               (module declarations + re-exports)
      acknowledge.rs       (AcknowledgeType, AcknowledgementCommitCallback)
      config.rs            (ShareConsumerConfig builder + parse helpers)
      context.rs           (ShareConsumerContext trait, default impl)
      consumer.rs          (ShareConsumer trait + BaseShareConsumer stub)
      records.rs           (ShareConsumerRecords, ShareRecord wrappers)
      error.rs             (KipNotSupported helpers, ShareError shapes)
tests/
  test_share_consumer.rs   (NEW, feature-gated)
```

Rationale for a `share/` submodule rather than flat files: KIP-932
introduces five tightly-coupled types and a parallel consumer surface;
a sub-module keeps the diff isolated and lets the feature flag gate at
the `pub mod share;` line.

Re-exports from `consumer/mod.rs`:

```rust
#[cfg(feature = "kip-932")]
pub mod share;

#[cfg(feature = "kip-932")]
#[doc(inline)]
pub use self::share::{
    AcknowledgeType, AcknowledgementCommitCallback, BaseShareConsumer,
    ShareConsumer, ShareConsumerConfig, ShareConsumerContext,
    ShareConsumerRecords, ShareRecord,
};
```

## 4. Cargo feature flag

Add to `Cargo.toml`:

```toml
[features]
# ... existing ...
kip-932 = []
```

- Off by default. Not in the `default` feature list.
- Documented in `Cargo.toml` comment block and in `lib.rs` rustdoc.
- `[package.metadata.docs.rs]` updated to add `kip-932` so docs.rs
  renders the share API.

No changes to `rdkafka-sys` features. No bindings change.

## 5. Public API surface

All types live in `crate::consumer::share::*` and are re-exported per
section 3. Types track the KIP's Java surface but use idiomatic Rust.

### 5.1 `AcknowledgeType`

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AcknowledgeType {
    Accept,
    Release,
    Reject,
}
```

`Display` implementation maps to the KIP string forms `accept`,
`release`, `reject`. `FromStr` accepts the same case-insensitively.

### 5.2 `ShareConsumerConfig`

A typed builder backed by a `ClientConfig` for librdkafka-style passthrough
properties. Validated fields per the KIP:

| Property | Type | Default |
|---|---|---|
| `group_id` | `String` | required |
| `share_acknowledgement_mode` | `AcknowledgementMode::{Implicit, Explicit}` | `Implicit` |
| `share_auto_offset_reset` | `AutoOffsetReset::{Earliest, Latest}` | `Latest` |
| `share_isolation_level` | `IsolationLevel::{ReadUncommitted, ReadCommitted}` | `ReadUncommitted` |
| `group_share_delivery_attempt_limit` | `u32` | `5` |
| `group_share_record_lock_duration_ms` | `u32` | `30_000` |
| `group_share_heartbeat_interval_ms` | `Option<u32>` | `None` (broker default) |
| `group_share_session_timeout_ms` | `Option<u32>` | `None` (broker default) |

Method `into_client_config(self) -> ClientConfig` writes these as
canonical librdkafka property keys (`group.id`,
`share.acknowledgement.mode`, etc.), matching the KIP names verbatim.

### 5.3 `ShareConsumerContext`

Mirrors `ConsumerContext`:

```rust
pub trait ShareConsumerContext: ClientContext + Sized {
    fn acknowledgement_commit_callback(
        &self,
        results: &[(crate::message::OwnedHeaders /* topic */, /* partition */ i32, /* offset */ i64, Result<AcknowledgeType, KafkaError>)],
    ) {}
}

#[derive(Clone, Debug, Default)]
pub struct DefaultShareConsumerContext;
impl ClientContext for DefaultShareConsumerContext {}
impl ShareConsumerContext for DefaultShareConsumerContext {}
```

(Exact callback signature finalised during implementation; intent is
to surface the KIP's `AcknowledgementCommitCallback` results.)

### 5.4 `ShareConsumer` trait + `BaseShareConsumer`

```rust
pub trait ShareConsumer<C = DefaultShareConsumerContext>
where C: ShareConsumerContext,
{
    fn client(&self) -> &Client<C>;
    fn context(&self) -> &Arc<C> { self.client().context() }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;
    fn unsubscribe(&self) -> KafkaResult<()>;
    fn subscription(&self) -> KafkaResult<Vec<String>>;

    fn poll<T: Into<Timeout>>(&self, timeout: T)
        -> KafkaResult<ShareConsumerRecords<'_>>;

    fn acknowledge(&self, record: &ShareRecord<'_>, ack: AcknowledgeType)
        -> KafkaResult<()>;
    fn acknowledge_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack: AcknowledgeType,
    ) -> KafkaResult<()>;

    fn commit_sync<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;
    fn commit_async(&self) -> KafkaResult<()>;

    fn client_instance_id<T: Into<Timeout>>(&self, timeout: T)
        -> KafkaResult<uuid::Uuid>;

    fn wakeup(&self);
    fn close(&self) -> KafkaResult<()>;
}
```

`BaseShareConsumer<C>` is a concrete struct that implements
`ShareConsumer`. In this PR every method body is:

```rust
Err(KafkaError::Unsupported(
    "KIP-932 share consumer support is not yet available in librdkafka; \
     see https://github.com/confluentinc/librdkafka/issues/5441",
))
```

`wakeup()` and `close()` are no-ops that log a warning under the
`log` crate at `trace` level (no allocation in hot path; matches
the `tracing` opt-in pattern from `Cargo.toml`).

`FromClientConfig` and `FromClientConfigAndContext` impls construct a
`BaseShareConsumer` from a `ClientConfig` produced by
`ShareConsumerConfig::into_client_config`. Construction returns `Ok`
so that downstream tests can exercise dispatch; only the runtime
methods return `Unsupported`.

### 5.5 `ShareConsumerRecords` and `ShareRecord`

Borrowed wrappers around `Vec<ShareRecord<'_>>`. `ShareRecord` exposes:

- `topic() -> &str`
- `partition() -> i32`
- `offset() -> i64`
- `delivery_count() -> u32`
- `key() -> Option<&[u8]>`
- `payload() -> Option<&[u8]>`
- `headers() -> Option<&BorrowedHeaders<'_>>`
- `timestamp() -> Timestamp`
- `acknowledge(&self, ack: AcknowledgeType) -> KafkaResult<()>`

Backed by the same `BorrowedMessage`-style lifetime story as the
existing consumer. In stub form `ShareConsumerRecords::empty()` is
the only constructor reachable; `BaseShareConsumer::poll` returns
`Unsupported` rather than empty so callers don't silently hang.

### 5.6 Error variants

`src/error.rs`:

```rust
pub enum KafkaError {
    // ... existing variants ...
    /// Returned when an API surface exists in rust-rdkafka but is not
    /// yet backed by librdkafka. Currently used by the KIP-932 share
    /// consumer scaffolding.
    Unsupported(&'static str),
}
```

`Display`, `Error`, and `IsError` impls extended accordingly.
`Unsupported` reports as a non-retriable error.

## 6. Implementation phases (commit breakdown)

Each phase is one commit. All Conventional Commits. PR opens after
phase 1 as a draft.

### Phase 1: `feat(consumer): add kip-932 feature flag and module skeleton`

- Add `kip-932 = []` to `[features]` in `Cargo.toml`.
- Create `src/consumer/share/mod.rs` with empty submodules.
- Wire `pub mod share;` into `src/consumer/mod.rs` behind
  `#[cfg(feature = "kip-932")]`.
- Verify `cargo build` and `cargo build --features kip-932`.

### Phase 2: `feat(consumer/share): add AcknowledgeType and config types`

- `AcknowledgeType` enum + `Display` + `FromStr` + tests.
- `AcknowledgementMode`, `AutoOffsetReset`, `IsolationLevel` enums.
- `ShareConsumerConfig` builder and `into_client_config`.
- Unit tests: round-trip parse, default values, key formatting.

### Phase 3: `feat(error): add KafkaError::Unsupported variant`

- Extend `KafkaError`, `Display`, and `IsError`.
- Single-purpose commit so reviewers see the new error surface
  clearly. Used by phase 4.

### Phase 4: `feat(consumer/share): add ShareConsumer trait and stub`

- `ShareConsumerContext` trait + `DefaultShareConsumerContext`.
- `ShareConsumer` trait.
- `BaseShareConsumer<C>` struct with `FromClientConfig` /
  `FromClientConfigAndContext` impls and all methods returning
  `KafkaError::Unsupported`.
- `ShareConsumerRecords` and `ShareRecord` skeleton types.
- Re-exports from `consumer/mod.rs`.

### Phase 5: `test(consumer/share): public surface smoke tests`

- `tests/test_share_consumer.rs` feature-gated on `kip-932`.
- Cases:
  - Build a `ShareConsumerConfig` and convert to `ClientConfig`,
    assert canonical key names.
  - Create a `BaseShareConsumer` from config; assert `client()`
    returns a live client.
  - Assert `poll`, `acknowledge`, `commit_sync` return
    `KafkaError::Unsupported`.
  - `AcknowledgeType` Display / FromStr round-trip.
- Doc test on `ShareConsumer` trait that shows the intended usage
  pattern and is annotated with `ignore` (`should_panic` is wrong
  here; doc tests don't run under feature flags by default).

### Phase 6: `ci: build and test rust-rdkafka with kip-932 feature`

- Inspect `.github/workflows/*.yml` (or equivalent CI config).
- Add a matrix entry or extra step that runs:
  - `cargo build --features kip-932`
  - `cargo test --features kip-932`
  - `cargo clippy --features kip-932 -- -D warnings`
- Add a CI lane for the no-default-features build to confirm the
  default path is untouched.

### Phase 7: `docs: document KIP-932 scaffolding and limitations`

- `README.md`: short subsection under "Features" noting KIP-932
  scaffolding behind `kip-932` and the librdkafka tracking issue.
- `changelog.md`: entry under the next unreleased version.
- `src/lib.rs` module-level rustdoc on the share module: explain
  the gating, link the KIP and librdkafka issue, set expectations.
- No new top-level docs file unless `README.md`'s structure forces it.

## 7. Validation gate

Run before declaring CI-green and ready-for-review:

```bash
cargo fmt --all -- --check
cargo build
cargo build --features kip-932
cargo build --no-default-features
cargo test
cargo test --features kip-932
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features kip-932 -- -D warnings
cargo doc --no-deps --features kip-932
```

If any step regresses on `master` it is fixed in-place, not
papered over.

## 8. Tests we intentionally do not write

- Broker integration tests against a real Kafka 4.x cluster with
  share groups enabled. librdkafka cannot speak the protocol yet,
  so these would be vapor tests.
- Mocked `ShareFetch` / `ShareAcknowledge` responses. The mock
  surface in rdkafka-sys does not yet model share sessions.

These tests land in the follow-up PR that wires real FFI.

## 9. Upstream coordination

- Open a tracking issue on `j7nw4r/rust-rdkafka` titled
  "KIP-932 share consumer support (scaffolding + tracking)". Body
  links the KIP, librdkafka #5441, and this PR.
- Do not file anything on `fede1024/rust-rdkafka` until the API
  shape has settled and CI is green on the fork.
- Subscribe to librdkafka #5441 so we know when the public C API
  ships.

## 10. Risks and follow-ups

| Risk | Mitigation |
|---|---|
| librdkafka's eventual public API may differ from the KIP shapes we mirror, requiring breaking changes in rust-rdkafka. | Gate the entire surface behind `kip-932` and document it as `unstable: API will change to match librdkafka when KIP-932 ships`. Tag types with `#[doc(hidden)]` notes if needed. |
| KafkaError gets a new public variant; consumers of the enum that match exhaustively will need to update. | `KafkaError` is already marked non-exhaustive in spirit (large enum). Confirm `#[non_exhaustive]` is present; add if not. |
| Feature combinations explode CI time. | Add only one new `--features kip-932` lane, not a full matrix. |
| Stub methods returning `Unsupported` get accidentally used in production by a user enabling the feature flag. | Module-level rustdoc warns loudly; `ShareConsumerConfig::into_client_config` writes a log warning at construction; the error message itself names the issue. |

### Follow-up PRs (out of scope here)

1. `rdkafka-sys`: regenerate bindings against librdkafka with KIP-932
   public C API, bump pinned version.
2. `consumer/share`: replace `Unsupported` returns with real FFI calls.
3. `admin`: `alter_share_group_offsets`, `delete_share_group_offsets`,
   `list_share_groups`, `describe_share_groups`.
4. Integration tests against the docker-compose Kafka cluster
   (requires a Kafka image with share groups enabled).
5. Stream-style async wrapper analogous to `StreamConsumer` for
   share consumers (likely `ShareStreamConsumer`).

## 11. PR shape

- Title: `feat(consumer): scaffold KIP-932 share consumer surface`
- Draft from the start. Do not mark ready-for-review until the
  validation gate passes.
- Body: 2-3 paragraphs summarising scope, the librdkafka blocker,
  and the follow-up roadmap. Include the "Test plan" checklist
  matching section 7.
- Target: `j7nw4r/rust-rdkafka:master`.
