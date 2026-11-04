//! Acknowledgement primitives for the share consumer surface.
//!
//! See [`KIP-932`][kip] for the semantics of each acknowledgement type
//! and the record state machine they drive.
//!
//! [kip]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka

use std::fmt;
use std::str::FromStr;

use crate::error::KafkaError;

/// The disposition of a record delivered to a share consumer.
///
/// Drives the broker-side record state machine described by KIP-932.
/// `Accept` moves the record to the Acknowledged state, `Release` returns
/// it to Available for redelivery (subject to the delivery attempt limit),
/// and `Reject` archives it (poison message).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AcknowledgeType {
    /// The record was processed successfully.
    Accept,
    /// The record could not be processed but may be retried. Returns the
    /// record to the Available state for redelivery.
    Release,
    /// The record cannot be processed (poison message). Moves the record
    /// to the Archived state.
    Reject,
}

impl AcknowledgeType {
    /// Returns the canonical lowercase wire name (`"accept"`, `"release"`,
    /// `"reject"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            AcknowledgeType::Accept => "accept",
            AcknowledgeType::Release => "release",
            AcknowledgeType::Reject => "reject",
        }
    }
}

impl fmt::Display for AcknowledgeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when [`AcknowledgeType::from_str`] receives an
/// unrecognised string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseAcknowledgeTypeError(String);

impl fmt::Display for ParseAcknowledgeTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown acknowledge type {:?}; expected accept, release, or reject",
            self.0
        )
    }
}

impl std::error::Error for ParseAcknowledgeTypeError {}

impl FromStr for AcknowledgeType {
    type Err = ParseAcknowledgeTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "accept" => Ok(AcknowledgeType::Accept),
            "release" => Ok(AcknowledgeType::Release),
            "reject" => Ok(AcknowledgeType::Reject),
            _ => Err(ParseAcknowledgeTypeError(s.to_owned())),
        }
    }
}

/// Result of an asynchronous acknowledgement commit, reported to
/// [`ShareConsumerContext::acknowledgement_commit`][cb].
///
/// [cb]: crate::consumer::share::ShareConsumerContext::acknowledgement_commit
#[derive(Clone, Debug)]
pub struct AcknowledgementCommitResult {
    /// Topic of the acknowledged record.
    pub topic: String,
    /// Partition of the acknowledged record.
    pub partition: i32,
    /// Offset of the acknowledged record.
    pub offset: i64,
    /// The acknowledgement disposition, or the error reported by the
    /// broker.
    pub result: Result<AcknowledgeType, KafkaError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_round_trip() {
        for ack in [
            AcknowledgeType::Accept,
            AcknowledgeType::Release,
            AcknowledgeType::Reject,
        ] {
            let s = ack.to_string();
            assert_eq!(s.parse::<AcknowledgeType>().unwrap(), ack);
        }
    }

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(
            "ACCEPT".parse::<AcknowledgeType>().unwrap(),
            AcknowledgeType::Accept
        );
        assert_eq!(
            "Release".parse::<AcknowledgeType>().unwrap(),
            AcknowledgeType::Release
        );
    }

    #[test]
    fn parse_rejects_unknown() {
        let err = "discard".parse::<AcknowledgeType>().unwrap_err();
        assert!(err.to_string().contains("discard"));
    }

    #[test]
    fn as_str_matches_display() {
        assert_eq!(AcknowledgeType::Accept.as_str(), "accept");
        assert_eq!(AcknowledgeType::Release.to_string(), "release");
        assert_eq!(AcknowledgeType::Reject.as_str(), "reject");
    }
}
