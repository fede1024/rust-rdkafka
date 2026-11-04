//! Share consumer context trait.
//!
//! Mirrors [`ConsumerContext`][cc] for the KIP-932 surface: lets the
//! application observe asynchronous events without owning the consumer
//! poll loop. The single callback today,
//! [`ShareConsumerContext::acknowledgement_commit`], surfaces the result
//! of asynchronous acknowledgement commits.
//!
//! [cc]: crate::consumer::ConsumerContext

use crate::client::ClientContext;
use crate::consumer::share::acknowledge::AcknowledgementCommitResult;

/// Per-consumer callback surface for KIP-932 share consumers.
///
/// The default implementations are intentionally empty so downstream
/// crates can override only what they need.
pub trait ShareConsumerContext: ClientContext + Sized {
    /// Called when an asynchronous acknowledgement commit returns.
    ///
    /// The slice contains one entry per acknowledged record from the
    /// associated [`commit_async`][ca] call. The callback runs on a
    /// librdkafka-owned thread; do not call back into the consumer here
    /// (other than `wakeup`).
    ///
    /// [ca]: crate::consumer::share::ShareConsumer::commit_async
    #[allow(unused_variables)]
    fn acknowledgement_commit(&self, results: &[AcknowledgementCommitResult]) {}
}

/// Inert [`ShareConsumerContext`] for callers that need no callbacks.
#[derive(Clone, Debug, Default)]
pub struct DefaultShareConsumerContext;

impl ClientContext for DefaultShareConsumerContext {}
impl ShareConsumerContext for DefaultShareConsumerContext {}
