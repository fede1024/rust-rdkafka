//!
//! Data structures supporting the [AdminClient::list_offsets] operation
//!
use crate::error::{IsError, KafkaError};
use crate::Offset;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;
use std::ffi::CStr;
use std::slice;

/// Each of the items returned by [AdminClient::list_offsets] operation.
/// The error branch contains the topic name and partition to identify the source
/// of the error.
pub type ListOffsetsResult = Result<ListOffsetsResultInfo, (String, i32, KafkaError)>;

/// Obtain the identification (the topic_id and partition) extracted from either
/// the `Ok` or the `Err` branches.
pub fn list_offsets_result_key(result: &ListOffsetsResult) -> (&str, i32) {
    match result {
        Ok(tp) => (tp.topic.as_str(), tp.partition),
        Err((topic, partition, _error)) => (topic.as_str(), *partition),
    }
}


///
/// The information returned on success for each topic and instance requested through the
/// [AdminClient::list_offsets] operation.
///
#[derive(Debug, PartialEq)]
pub struct ListOffsetsResultInfo {
    /// The name of the topic
    pub topic: String,
    /// The partition of the topic.
    pub partition: i32,
    /// The requested offset from the topic.
    pub offset: Offset,
    /// Additional information in raw format.
    pub metadata: Option<Vec<u8>>,
    /// The timestamp in milliseconds corresponding to the offset. Not available (-1)
    /// when querying for the earliest or the latest offsets.
    pub timestamp: i64,
}

impl ListOffsetsResultInfo {
    pub(crate) unsafe fn vec_from_ptr(ptr: *mut RDKafkaListOffsetsResult) -> Vec<ListOffsetsResult> {
        let mut result_count: usize = 0;
        let info_list_ptr = rdsys::rd_kafka_ListOffsets_result_infos(ptr, &mut result_count);

        let mut info_list = Vec::new();

        // Copy the offsets from the C structure
        for i in 0..result_count as isize {
            let info_ptr = *info_list_ptr.offset(i);
            let tp = rdsys::rd_kafka_ListOffsetsResultInfo_topic_partition(info_ptr);

            let topic = CStr::from_ptr((*tp).topic)
                .to_str()
                .expect("Topic name is not UTF-8").to_string();

            let partition = (*tp).partition;

            let info = if (*tp).err.is_error() {
                Err((topic.clone(), partition, KafkaError::AdminOp((*tp).err.into())))
            } else {
                let offset = Offset::from_raw((*tp).offset);

                let metadata = if (*tp).metadata.is_null() {
                    None
                } else {
                    Some(slice::from_raw_parts::<u8>(
                        (*tp).metadata as *const u8,
                        (*tp).metadata_size).to_vec())
                };

                let timestamp = rdsys::rd_kafka_ListOffsetsResultInfo_timestamp(info_ptr);

                Ok(ListOffsetsResultInfo {
                    topic,
                    partition,
                    offset,
                    metadata,
                    timestamp,
                })
            };
            info_list.push(info);
        }

        info_list
    }
}