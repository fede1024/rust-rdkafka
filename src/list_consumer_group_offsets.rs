//! Types required for the list_consumer_group_offsets operations.

use std::ffi::CString;
use crate::util::{cstr_to_owned, KafkaDrop, NativePtr};
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::TopicPartitionList;


type NativeListConsumerGroupOffsets = NativePtr<RDKafkaListConsumerGroupOffsets>;

unsafe impl KafkaDrop for RDKafkaListConsumerGroupOffsets {
    const TYPE: &'static str = "list consumer group offsets";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_ListConsumerGroupOffsets_destroy;

}

///
/// Specification of consumer group offsets to list using
/// `AdminClient.list_consumer_group_offsets`.
///
/// If the partitions is set to `None`, the operation will provide information about all
/// topics assigned to the consumer group.
///
pub struct ListConsumerGroupOffsets<'a> {
    /// The group_id of the consumer group requested.
    pub group_id: &'a str,
    /// The topic and partitions, assigned to the group, that we request information for.
    pub partitions: Option<TopicPartitionList>
}


impl<'a> ListConsumerGroupOffsets<'a> {
    /// Creates a new `ListConsumerGroupOffsets`
    pub fn new(group_id: &'a str, partitions: TopicPartitionList) -> ListConsumerGroupOffsets<'a> {
        ListConsumerGroupOffsets {
            group_id,
            partitions: Some(partitions)
        }
    }

    /// Creates a bew `ListConsumerGroupOffsets` providing only the group_id.
    /// This will retrieve all the topics associated to the group.
    pub fn from_group(group_id: &'a str) -> ListConsumerGroupOffsets<'a> {
        ListConsumerGroupOffsets {
            group_id,
            partitions: None
        }
    }

    pub(crate) fn to_native(&self) -> KafkaResult<NativeListConsumerGroupOffsets> {
        let group_id = CString::new(self.group_id)?;

        let list_consumer_group_offset = unsafe {
            NativeListConsumerGroupOffsets::from_ptr(rdsys::rd_kafka_ListConsumerGroupOffsets_new(
                group_id.as_ptr(),
                if let Some(partition) = &self.partitions { partition.ptr() } else { std::ptr::null() },
            ))
        };
        list_consumer_group_offset.ok_or(KafkaError::AdminOpCreation("ListConsumer Group Offset creation failed".to_string()))
    }
}

/// Each of the items returned by the `AdminClient.list_consumer_group_offsets`
/// The error branch contains the group_id as first element of the tuple.
pub type ConsumerGroupResult = Result<ConsumerGroup, (String, KafkaError)>;

/// Obtain the identification (the group_id) extracted from either the `Ok` or the `Err` branches.
pub fn group_result_key(result: &ConsumerGroupResult) -> &str {
    match result {
        Ok(description) => description.group_id.as_str(),
        Err((group_id, _error)) => group_id.as_str(),
    }
}

///
/// Information retrieved for the requested group on success.
///
#[derive(Debug, PartialEq)]
pub struct ConsumerGroup {
    /// Identifies the group
    pub group_id: String,
    /// the partitions assigned to the group that the operation provided information for.
    pub topic_partitions: TopicPartitionList,
}


impl ConsumerGroup {
    pub(crate) fn vec_result_from_ptr(groups: *mut *const RDKafkaGroupResult,
                                      count: usize) -> Vec<ConsumerGroupResult> {
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let group = unsafe { *groups.add(i) };
            let r = Self::from_ptr(group);
            out.push(r);
        }
        out
    }

    fn from_ptr(group: *const RDKafkaGroupResult) -> Result<ConsumerGroup, (String, KafkaError)> {
        let name = unsafe { cstr_to_owned(rdsys::rd_kafka_group_result_name(group)) };
        let err = unsafe {
            let err = rdsys::rd_kafka_group_result_error(group);
            rdsys::rd_kafka_error_code(err)
        };
        let r = if err.is_error() {
            Err((name.clone(), KafkaError::AdminOp(err.into())))
        } else {
            let partitions_native = unsafe { rdsys::rd_kafka_group_result_partitions(group) };
            let cloned_partition_native = unsafe { rdsys::rd_kafka_topic_partition_list_copy(partitions_native) };
            let partition = unsafe { TopicPartitionList::from_ptr(cloned_partition_native) };
            let group_tp = ConsumerGroup {
                group_id: name,
                topic_partitions: partition,
            };
            Ok(group_tp)
        };
        r
    }
}
