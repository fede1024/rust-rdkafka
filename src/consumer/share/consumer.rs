//! Share consumer trait and stub implementation.
//!
//! The runtime methods on [`BaseShareConsumer`] return
//! [`KafkaError::Unsupported`] until librdkafka exposes the share
//! consumer C API; see
//! <https://github.com/confluentinc/librdkafka/issues/5441>.

use std::fmt;
use std::sync::Arc;

use log::trace;

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::share::acknowledge::AcknowledgeType;
use crate::consumer::share::context::{DefaultShareConsumerContext, ShareConsumerContext};
use crate::consumer::share::records::{ShareConsumerRecords, ShareRecord};
use crate::error::{KafkaError, KafkaResult};
use crate::util::Timeout;

const UNSUPPORTED_REASON: &str =
    "KIP-932 share consumer support is not yet available in librdkafka; \
     see https://github.com/confluentinc/librdkafka/issues/5441";

fn unsupported<T>() -> KafkaResult<T> {
    Err(KafkaError::Unsupported(UNSUPPORTED_REASON))
}

/// Common trait for share consumers.
///
/// Mirrors the [`KafkaShareConsumer`][java] surface from KIP-932,
/// adapted to idiomatic Rust. All methods are non-blocking apart from
/// [`Self::poll`] and [`Self::commit_sync`], which respect the supplied
/// timeout.
///
/// # Stability
///
/// `ShareConsumer` is gated behind the `kip-932` cargo feature and
/// tracks the KIP's `@InterfaceStability.Evolving` posture. Method
/// shapes will change to match librdkafka once it exposes the public C
/// API.
///
/// [java]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
pub trait ShareConsumer<C = DefaultShareConsumerContext>
where
    C: ShareConsumerContext,
{
    /// Returns the consumer context.
    fn context(&self) -> &Arc<C>;

    /// Subscribes the consumer to a list of topics. The broker assigns
    /// partitions automatically; explicit assignment is not supported
    /// for share consumers.
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;

    /// Unsubscribes the consumer from all topics.
    fn unsubscribe(&self) -> KafkaResult<()>;

    /// Returns the current subscription list.
    fn subscription(&self) -> KafkaResult<Vec<String>>;

    /// Fetches the next batch of acquired records.
    fn poll<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<ShareConsumerRecords<'_>>;

    /// Acknowledges a record with the given disposition.
    fn acknowledge(&self, record: &ShareRecord<'_>, ack: AcknowledgeType) -> KafkaResult<()>;

    /// Acknowledges a record by (topic, partition, offset). Use when
    /// the original [`ShareRecord`] is no longer in scope (for example
    /// after a deserialization error).
    fn acknowledge_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack: AcknowledgeType,
    ) -> KafkaResult<()>;

    /// Synchronously commits all pending acknowledgements. Blocks until
    /// the broker responds or the timeout elapses.
    fn commit_sync<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Asynchronously commits all pending acknowledgements. Results are
    /// reported through
    /// [`ShareConsumerContext::acknowledgement_commit`][cb].
    ///
    /// [cb]: crate::consumer::share::ShareConsumerContext::acknowledgement_commit
    fn commit_async(&self) -> KafkaResult<()>;

    /// Interrupts an in-progress [`Self::poll`] from another thread.
    ///
    /// Safe to call from any thread; the rest of the consumer is not
    /// thread-safe.
    fn wakeup(&self);

    /// Releases acquired records, commits pending acknowledgements, and
    /// leaves the share group.
    fn close(&self) -> KafkaResult<()>;
}

/// Low-level share consumer.
///
/// Today this is a scaffolding stub: construction parses the
/// configuration and stores the context, but every runtime method
/// returns [`KafkaError::Unsupported`]. See the [module docs][m] for
/// the upstream tracking issue.
///
/// [m]: crate::consumer::share
pub struct BaseShareConsumer<C = DefaultShareConsumerContext>
where
    C: ShareConsumerContext,
{
    context: Arc<C>,
    group_id: String,
}

impl<C> fmt::Debug for BaseShareConsumer<C>
where
    C: ShareConsumerContext,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BaseShareConsumer")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

impl<C> BaseShareConsumer<C>
where
    C: ShareConsumerContext,
{
    fn new(config: &ClientConfig, context: C) -> KafkaResult<Self> {
        let group_id = config
            .get("group.id")
            .ok_or_else(|| {
                KafkaError::ClientCreation("share consumer requires group.id to be set".to_owned())
            })?
            .to_owned();
        Ok(Self {
            context: Arc::new(context),
            group_id,
        })
    }

    /// Returns the configured share group identifier.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

impl FromClientConfig for BaseShareConsumer<DefaultShareConsumerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<Self> {
        BaseShareConsumer::new(config, DefaultShareConsumerContext)
    }
}

impl<C> FromClientConfigAndContext<C> for BaseShareConsumer<C>
where
    C: ShareConsumerContext,
{
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<Self> {
        BaseShareConsumer::new(config, context)
    }
}

impl<C> ShareConsumer<C> for BaseShareConsumer<C>
where
    C: ShareConsumerContext,
{
    fn context(&self) -> &Arc<C> {
        &self.context
    }

    fn subscribe(&self, _topics: &[&str]) -> KafkaResult<()> {
        unsupported()
    }

    fn unsubscribe(&self) -> KafkaResult<()> {
        unsupported()
    }

    fn subscription(&self) -> KafkaResult<Vec<String>> {
        unsupported()
    }

    fn poll<T: Into<Timeout>>(&self, _timeout: T) -> KafkaResult<ShareConsumerRecords<'_>> {
        unsupported()
    }

    fn acknowledge(&self, _record: &ShareRecord<'_>, _ack: AcknowledgeType) -> KafkaResult<()> {
        unsupported()
    }

    fn acknowledge_offset(
        &self,
        _topic: &str,
        _partition: i32,
        _offset: i64,
        _ack: AcknowledgeType,
    ) -> KafkaResult<()> {
        unsupported()
    }

    fn commit_sync<T: Into<Timeout>>(&self, _timeout: T) -> KafkaResult<()> {
        unsupported()
    }

    fn commit_async(&self) -> KafkaResult<()> {
        unsupported()
    }

    fn wakeup(&self) {
        trace!("BaseShareConsumer::wakeup is a no-op until librdkafka exposes KIP-932");
    }

    fn close(&self) -> KafkaResult<()> {
        trace!("BaseShareConsumer::close is a no-op until librdkafka exposes KIP-932");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::share::config::ShareConsumerConfig;

    fn make_config() -> ClientConfig {
        let mut config = ShareConsumerConfig::new();
        config.group_id("share-test");
        config.into_client_config().expect("group.id set")
    }

    #[test]
    fn construction_requires_group_id() {
        let config = ClientConfig::new();
        let err = BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&config)
            .expect_err("missing group.id should fail");
        match err {
            KafkaError::ClientCreation(msg) => assert!(msg.contains("group.id")),
            other => panic!("expected ClientCreation, got {other:?}"),
        }
    }

    #[test]
    fn construction_succeeds_with_group_id() {
        let config = make_config();
        let consumer = BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&config)
            .expect("config has group.id");
        assert_eq!(consumer.group_id(), "share-test");
    }

    fn assert_unsupported<T: std::fmt::Debug>(result: KafkaResult<T>) {
        match result {
            Err(KafkaError::Unsupported(_)) => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn runtime_methods_return_unsupported() {
        let consumer =
            BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&make_config())
                .expect("construction");

        assert_unsupported(consumer.subscribe(&["topic"]));
        assert_unsupported(consumer.unsubscribe());
        assert_unsupported(consumer.subscription());
        assert_unsupported(consumer.poll(std::time::Duration::from_millis(0)));
        assert_unsupported(consumer.acknowledge_offset("topic", 0, 42, AcknowledgeType::Accept));
        assert_unsupported(consumer.commit_sync(std::time::Duration::from_millis(0)));
        assert_unsupported(consumer.commit_async());

        // wakeup/close are not unsupported, but they are explicitly safe
        // no-ops in the stub.
        consumer.wakeup();
        consumer.close().expect("close stub is Ok");
    }
}
