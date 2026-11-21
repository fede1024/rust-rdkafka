//! Borrowed record wrappers returned from share consumer polls.
//!
//! The types model the KIP-932 fetch result: a batch of records carrying
//! per-record metadata (offset, partition, delivery count) plus borrowed
//! key/payload bytes. Today the producer-side stub never yields records,
//! but the lifetime story is established so the real implementation can
//! slot in without breaking the API.

use std::marker::PhantomData;
use std::slice;

use crate::consumer::share::acknowledge::AcknowledgeType;
use crate::error::KafkaError;

/// A batch of records returned from
/// [`ShareConsumer::poll`][poll].
///
/// Today the stub never produces a non-empty batch; once librdkafka
/// exposes a share fetch API the borrowed payload and key slices will
/// reference librdkafka-owned memory tied to the consumer's lifetime.
///
/// [poll]: crate::consumer::share::ShareConsumer::poll
#[derive(Debug)]
pub struct ShareConsumerRecords<'a> {
    records: Vec<ShareRecord<'a>>,
}

impl<'a> ShareConsumerRecords<'a> {
    /// Returns an empty batch. Used by the stub implementation and by
    /// tests that need a placeholder.
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Returns `true` if the batch contains no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns the number of records in the batch.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Borrows the records in iteration order.
    pub fn iter(&self) -> slice::Iter<'_, ShareRecord<'a>> {
        self.records.iter()
    }
}

impl<'a, 'b> IntoIterator for &'b ShareConsumerRecords<'a> {
    type Item = &'b ShareRecord<'a>;
    type IntoIter = slice::Iter<'b, ShareRecord<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A single record returned from
/// [`ShareConsumer::poll`][poll].
///
/// Carries the KIP-932 per-record metadata (topic, partition, offset,
/// delivery count) plus borrowed key/payload bytes.
///
/// [poll]: crate::consumer::share::ShareConsumer::poll
#[derive(Debug)]
pub struct ShareRecord<'a> {
    topic: String,
    partition: i32,
    offset: i64,
    delivery_count: u32,
    key: Option<&'a [u8]>,
    payload: Option<&'a [u8]>,
    _phantom: PhantomData<&'a ()>,
}

impl ShareRecord<'_> {
    /// Topic of the record.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Partition of the record.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Offset of the record within its partition.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Number of times the broker has delivered this record to a share
    /// consumer in the group; `1` on the first delivery.
    pub fn delivery_count(&self) -> u32 {
        self.delivery_count
    }

    /// Borrowed key bytes, if the record carries a key.
    pub fn key(&self) -> Option<&[u8]> {
        self.key
    }

    /// Borrowed payload bytes, if the record carries a payload.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload
    }

    /// Acknowledges this record with the given disposition.
    ///
    /// Returns [`KafkaError::Unsupported`] until librdkafka exposes the
    /// share consumer C API.
    #[allow(unused_variables)]
    pub fn acknowledge(&self, ack: AcknowledgeType) -> Result<(), KafkaError> {
        Err(KafkaError::Unsupported(
            "KIP-932 share consumer support is not yet available in librdkafka; \
             see https://github.com/confluentinc/librdkafka/issues/5441",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_is_empty() {
        let batch = ShareConsumerRecords::empty();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert_eq!(batch.iter().count(), 0);
    }
}
