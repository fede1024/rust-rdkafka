//! Share consumer configuration builder.
//!
//! Wraps a [`ClientConfig`] and exposes typed setters for the new
//! KIP-932 properties. Call [`ShareConsumerConfig::into_client_config`]
//! to obtain a [`ClientConfig`] suitable for
//! [`BaseShareConsumer::from_config`][bsc].
//!
//! Property keys are written verbatim from KIP-932 so that, once
//! librdkafka exposes a public share consumer API, the existing
//! configuration plumbing flows through unchanged.
//!
//! [bsc]: crate::consumer::share::BaseShareConsumer

use std::fmt;
use std::str::FromStr;

use crate::config::ClientConfig;

/// Whether the share consumer auto-acknowledges records on the next
/// `poll` (`Implicit`) or requires the application to acknowledge each
/// record explicitly (`Explicit`).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum AcknowledgementMode {
    /// The next call to `poll` or `commit_sync`/`commit_async`
    /// acknowledges every record returned by the previous `poll`.
    #[default]
    Implicit,
    /// The application must call `acknowledge` on every record before
    /// the next `poll`, otherwise an error is raised.
    Explicit,
}

impl AcknowledgementMode {
    /// Returns the canonical librdkafka string for this mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            AcknowledgementMode::Implicit => "implicit",
            AcknowledgementMode::Explicit => "explicit",
        }
    }
}

impl fmt::Display for AcknowledgementMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AcknowledgementMode {
    type Err = ParseConfigEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "implicit" => Ok(AcknowledgementMode::Implicit),
            "explicit" => Ok(AcknowledgementMode::Explicit),
            _ => Err(ParseConfigEnumError {
                value: s.to_owned(),
                expected: "implicit or explicit",
            }),
        }
    }
}

/// Initial position for the share-partition start offset (SPSO) when a
/// share group reads a topic for the first time.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest (next-produced) offset.
    #[default]
    Latest,
}

impl AutoOffsetReset {
    /// Returns the canonical librdkafka string for this reset policy.
    pub fn as_str(&self) -> &'static str {
        match self {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
        }
    }
}

impl fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoOffsetReset {
    type Err = ParseConfigEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "earliest" => Ok(AutoOffsetReset::Earliest),
            "latest" => Ok(AutoOffsetReset::Latest),
            _ => Err(ParseConfigEnumError {
                value: s.to_owned(),
                expected: "earliest or latest",
            }),
        }
    }
}

/// Per-share-group transactional isolation. Applies to the entire share
/// group, not per consumer.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum IsolationLevel {
    /// All produced records are visible (including aborted transactions).
    #[default]
    ReadUncommitted,
    /// Only committed records are visible; aborted records are filtered
    /// out by the broker.
    ReadCommitted,
}

impl IsolationLevel {
    /// Returns the canonical librdkafka string for this isolation level.
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IsolationLevel {
    type Err = ParseConfigEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "read_uncommitted" => Ok(IsolationLevel::ReadUncommitted),
            "read_committed" => Ok(IsolationLevel::ReadCommitted),
            _ => Err(ParseConfigEnumError {
                value: s.to_owned(),
                expected: "read_uncommitted or read_committed",
            }),
        }
    }
}

/// Error returned by the share-config enum [`FromStr`] impls when the
/// input does not match any known variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseConfigEnumError {
    value: String,
    expected: &'static str,
}

impl fmt::Display for ParseConfigEnumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown share config value {:?}; expected {}",
            self.value, self.expected
        )
    }
}

impl std::error::Error for ParseConfigEnumError {}

/// Typed builder for the configuration values introduced by KIP-932.
///
/// The builder is layered on top of [`ClientConfig`]: KIP-932 specific
/// fields are typed; arbitrary librdkafka properties (for example
/// `bootstrap.servers`) flow through the embedded [`ClientConfig`]. Call
/// [`ShareConsumerConfig::into_client_config`] to materialise the final
/// [`ClientConfig`] understood by
/// [`BaseShareConsumer::from_config`][bsc].
///
/// All field names match the KIP verbatim; canonical librdkafka key
/// formatting is applied by [`Self::into_client_config`].
///
/// [bsc]: crate::consumer::share::BaseShareConsumer
#[derive(Clone, Debug)]
pub struct ShareConsumerConfig {
    base: ClientConfig,
    group_id: Option<String>,
    acknowledgement_mode: AcknowledgementMode,
    auto_offset_reset: AutoOffsetReset,
    isolation_level: IsolationLevel,
    delivery_attempt_limit: u32,
    record_lock_duration_ms: u32,
    heartbeat_interval_ms: Option<u32>,
    session_timeout_ms: Option<u32>,
}

impl ShareConsumerConfig {
    /// KIP-932 default for `group.share.delivery.attempt.limit`.
    pub const DEFAULT_DELIVERY_ATTEMPT_LIMIT: u32 = 5;
    /// KIP-932 default for `group.share.record.lock.duration.ms`.
    pub const DEFAULT_RECORD_LOCK_DURATION_MS: u32 = 30_000;

    /// Creates an empty configuration with KIP-932 defaults.
    pub fn new() -> Self {
        Self {
            base: ClientConfig::new(),
            group_id: None,
            acknowledgement_mode: AcknowledgementMode::default(),
            auto_offset_reset: AutoOffsetReset::default(),
            isolation_level: IsolationLevel::default(),
            delivery_attempt_limit: Self::DEFAULT_DELIVERY_ATTEMPT_LIMIT,
            record_lock_duration_ms: Self::DEFAULT_RECORD_LOCK_DURATION_MS,
            heartbeat_interval_ms: None,
            session_timeout_ms: None,
        }
    }

    /// Sets the share group identifier (`group.id`). Required.
    pub fn group_id(&mut self, group_id: impl Into<String>) -> &mut Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Sets `share.acknowledgement.mode`.
    pub fn acknowledgement_mode(&mut self, mode: AcknowledgementMode) -> &mut Self {
        self.acknowledgement_mode = mode;
        self
    }

    /// Sets `share.auto.offset.reset`.
    pub fn auto_offset_reset(&mut self, reset: AutoOffsetReset) -> &mut Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Sets `share.isolation.level`.
    pub fn isolation_level(&mut self, level: IsolationLevel) -> &mut Self {
        self.isolation_level = level;
        self
    }

    /// Sets `group.share.delivery.attempt.limit` (default `5`).
    pub fn delivery_attempt_limit(&mut self, limit: u32) -> &mut Self {
        self.delivery_attempt_limit = limit;
        self
    }

    /// Sets `group.share.record.lock.duration.ms` (default `30000`).
    pub fn record_lock_duration_ms(&mut self, ms: u32) -> &mut Self {
        self.record_lock_duration_ms = ms;
        self
    }

    /// Sets `group.share.heartbeat.interval.ms`. Leave unset to use the
    /// broker default.
    pub fn heartbeat_interval_ms(&mut self, ms: u32) -> &mut Self {
        self.heartbeat_interval_ms = Some(ms);
        self
    }

    /// Sets `group.share.session.timeout.ms`. Leave unset to use the
    /// broker default.
    pub fn session_timeout_ms(&mut self, ms: u32) -> &mut Self {
        self.session_timeout_ms = Some(ms);
        self
    }

    /// Sets an arbitrary librdkafka property on the embedded
    /// [`ClientConfig`] (for example `bootstrap.servers`).
    pub fn set<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.base.set(key, value);
        self
    }

    /// Materialises the final [`ClientConfig`].
    ///
    /// Returns `None` if no `group_id` was set, since KIP-932 requires a
    /// share group identifier and downstream construction would
    /// otherwise reject the configuration with an unhelpful error.
    pub fn into_client_config(self) -> Option<ClientConfig> {
        let group_id = self.group_id?;
        let mut config = self.base;
        config.set("group.id", group_id);
        config.set(
            "share.acknowledgement.mode",
            self.acknowledgement_mode.as_str(),
        );
        config.set("share.auto.offset.reset", self.auto_offset_reset.as_str());
        config.set("share.isolation.level", self.isolation_level.as_str());
        config.set(
            "group.share.delivery.attempt.limit",
            self.delivery_attempt_limit.to_string(),
        );
        config.set(
            "group.share.record.lock.duration.ms",
            self.record_lock_duration_ms.to_string(),
        );
        if let Some(ms) = self.heartbeat_interval_ms {
            config.set("group.share.heartbeat.interval.ms", ms.to_string());
        }
        if let Some(ms) = self.session_timeout_ms {
            config.set("group.share.session.timeout.ms", ms.to_string());
        }
        Some(config)
    }
}

impl Default for ShareConsumerConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_kip() {
        let config = ShareConsumerConfig::new();
        assert_eq!(config.acknowledgement_mode, AcknowledgementMode::Implicit);
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert_eq!(config.isolation_level, IsolationLevel::ReadUncommitted);
        assert_eq!(config.delivery_attempt_limit, 5);
        assert_eq!(config.record_lock_duration_ms, 30_000);
        assert!(config.heartbeat_interval_ms.is_none());
        assert!(config.session_timeout_ms.is_none());
    }

    #[test]
    fn into_client_config_requires_group_id() {
        assert!(ShareConsumerConfig::new().into_client_config().is_none());
    }

    #[test]
    fn into_client_config_writes_canonical_keys() {
        let mut config = ShareConsumerConfig::new();
        config
            .group_id("shares-1")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .isolation_level(IsolationLevel::ReadCommitted)
            .delivery_attempt_limit(7)
            .record_lock_duration_ms(45_000)
            .heartbeat_interval_ms(5_000)
            .session_timeout_ms(60_000)
            .set("bootstrap.servers", "localhost:9092");

        let client_config = config.into_client_config().expect("group.id set");
        let map = client_config.config_map();
        assert_eq!(map.get("group.id").copied(), Some("shares-1"));
        assert_eq!(
            map.get("share.acknowledgement.mode").copied(),
            Some("explicit")
        );
        assert_eq!(
            map.get("share.auto.offset.reset").copied(),
            Some("earliest")
        );
        assert_eq!(
            map.get("share.isolation.level").copied(),
            Some("read_committed")
        );
        assert_eq!(
            map.get("group.share.delivery.attempt.limit").copied(),
            Some("7")
        );
        assert_eq!(
            map.get("group.share.record.lock.duration.ms").copied(),
            Some("45000")
        );
        assert_eq!(
            map.get("group.share.heartbeat.interval.ms").copied(),
            Some("5000")
        );
        assert_eq!(
            map.get("group.share.session.timeout.ms").copied(),
            Some("60000")
        );
        assert_eq!(
            map.get("bootstrap.servers").copied(),
            Some("localhost:9092")
        );
    }

    #[test]
    fn enum_parse_round_trip() {
        for mode in [AcknowledgementMode::Implicit, AcknowledgementMode::Explicit] {
            assert_eq!(
                mode.to_string().parse::<AcknowledgementMode>().unwrap(),
                mode
            );
        }
        for reset in [AutoOffsetReset::Earliest, AutoOffsetReset::Latest] {
            assert_eq!(reset.to_string().parse::<AutoOffsetReset>().unwrap(), reset);
        }
        for level in [
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
        ] {
            assert_eq!(level.to_string().parse::<IsolationLevel>().unwrap(), level);
        }
    }

    #[test]
    fn enum_parse_is_case_insensitive() {
        assert_eq!(
            "READ_COMMITTED".parse::<IsolationLevel>().unwrap(),
            IsolationLevel::ReadCommitted
        );
        assert_eq!(
            "Earliest".parse::<AutoOffsetReset>().unwrap(),
            AutoOffsetReset::Earliest
        );
    }
}
