//! A data structure representing topic, partitions and offsets, compatible with the
//! `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.
use rdsys;
use rdsys::types::*;

use util::cstr_to_owned;
use error::{IsError, KafkaError, KafkaResult};

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ops::Deref;
use std::slice;

pub const OFFSET_BEGINNING: i64 = rdsys::RD_KAFKA_OFFSET_BEGINNING as i64;
pub const OFFSET_END: i64 = rdsys::RD_KAFKA_OFFSET_END as i64;
pub const OFFSET_STORED: i64 = rdsys::RD_KAFKA_OFFSET_STORED as i64;
pub const OFFSET_INVALID: i64 = rdsys::RD_KAFKA_OFFSET_INVALID as i64;

/// A map of topic names to partitions.
pub type Topics = HashMap<String, Option<Vec<Partition>>>;

pub struct TopicPartitionListElem<'a> {
    ptr: *mut RDKafkaTopicPartition,
    _owner_list: &'a TopicPartitionList2,
}

impl<'a> TopicPartitionListElem<'a> {
    fn from_ptr(list: &'a TopicPartitionList2, ptr: *mut RDKafkaTopicPartition) -> TopicPartitionListElem {
        TopicPartitionListElem {
            ptr: ptr,
            _owner_list: list,
        }
    }

    pub fn topic(&self) -> &str {
        unsafe {
            let c_str = (*self.ptr).topic;
            CStr::from_ptr(c_str).to_str().expect("Topic name is not UTF-8")
        }
    }

    pub fn partition(&self) -> i32 {
        unsafe { (*self.ptr).partition }
    }

    pub fn offset(&self) -> i64 {
        unsafe { (*self.ptr).offset }
    }

    pub fn set_offset(&self, offset: i64) {
        unsafe { (*self.ptr).offset = offset };
    }
}

// The RDKafkaTopicPartitionList takes care of deallocating the elements.
//impl<'a> Drop for TopicPartitionListElem<'a> {
//    fn drop(&mut self) {
//        unsafe { rdsys::rd_kafka_topic_partition_destroy(self.ptr) }
//    }
//}


pub struct TopicPartitionList2 {
    ptr: *mut RDKafkaTopicPartitionList,
}

impl TopicPartitionList2 {
    pub fn new() -> TopicPartitionList2 {
        TopicPartitionList2::new_with_capacity(5)
    }

    pub fn new_with_capacity(capacity: usize) -> TopicPartitionList2 {
        let ptr = unsafe { rdsys::rd_kafka_topic_partition_list_new(capacity as i32) };
        TopicPartitionList2::from_ptr(ptr)
    }

    pub fn from_ptr(ptr: *mut RDKafkaTopicPartitionList) -> TopicPartitionList2 {
        TopicPartitionList2 { ptr: ptr }
    }

    pub fn add_partition<'a>(&'a self, topic: &str, partition: i32) -> TopicPartitionListElem<'a> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let tp_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_add(self.ptr, topic_c.as_ptr(), partition)
        };
        TopicPartitionListElem::from_ptr(self, tp_ptr)
    }

    pub fn add_partition_range(&self, topic: &str, start_partition: i32, stop_partition: i32) {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        unsafe {
            rdsys::rd_kafka_topic_partition_list_add_range(self.ptr, topic_c.as_ptr(),
                start_partition, stop_partition);
        }
    }

    pub fn set_partition_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let kafka_err = unsafe {
            rdsys::rd_kafka_topic_partition_list_set_offset(self.ptr, topic_c.as_ptr(), partition,
                offset)
        };

        if kafka_err.is_error() {
            Err(KafkaError::SetPartitionOffset(kafka_err))
        } else {
            Ok(())
        }
    }

    pub fn find_partition(&self, topic: &str, partition: i32) -> Option<TopicPartitionListElem> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let elem_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_find(self.ptr, topic_c.as_ptr(), partition)
        };
        if elem_ptr.is_null() {
            None
        } else {
            Some(TopicPartitionListElem::from_ptr(self, elem_ptr))
        }
    }
}

impl Drop for TopicPartitionList2 {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(self.ptr) }
    }
}


/// Configuration of a partition
#[derive(Clone, Debug, PartialEq)]
pub struct Partition {
    pub id: i32,
    pub offset: i64
}

/// Map of topics with optionally partition configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct TopicPartitionList {
    pub topics: Topics
}

// TODO: we should support accessing RDKafkaTopicPartitionList directly, without having to
// build the Rust data structures.
impl TopicPartitionList {
    /// Create list based on a list from the rdkafka side.
    pub fn from_rdkafka(tp_list: *const RDKafkaTopicPartitionList) -> TopicPartitionList {
        let mut topics: Topics = HashMap::new();

        let elements = unsafe { slice::from_raw_parts((*tp_list).elems, (*tp_list).cnt as usize) };
        for tp in elements {
            // TODO: check if the topic_name is a copy or a view in the C data. The C data is not
            // guaranteed to be immutable.
            let topic_name = unsafe { cstr_to_owned(tp.topic) };
            if tp.partition >= 0 || tp.offset >= 0 {
                let topic = topics.entry(topic_name).or_insert(Some(vec![]));
                match *topic {
                    Some(ref mut p) => {
                        p.push(Partition {
                            id: tp.partition,
                            offset: tp.offset
                        });
                    },
                    None => ()
                }
            } else {
                // No configuration
                topics.insert(topic_name, None);
            };
        }

        TopicPartitionList {
            topics: topics
        }
    }

    /// Create an empty list
    pub fn new() -> TopicPartitionList {
        TopicPartitionList {
            topics: HashMap::new()
        }
    }

    /// Create list with specified topics with default configuration
    pub fn with_topics(topic_names: &[&str]) -> TopicPartitionList {
        let mut topics: Topics = HashMap::with_capacity(topic_names.len());

        for topic_name in topic_names {
            topics.insert(topic_name.to_string(), None);
        }

        TopicPartitionList {
            topics: topics
        }
    }

    /// Add topic with partitions configured
    pub fn add_topic_with_partitions(&mut self, topic: &str, partitions: &Vec<i32>) {
        let partitions_configs: Vec<Partition> = partitions.iter()
            .map(|p| Partition { id: *p, offset: -1001 } )
            .collect();
        self.topics.insert(topic.to_string(), Some(partitions_configs));
    }

    /// Add topic with partitions and offsets configured
    pub fn add_topic_with_partitions_and_offsets(&mut self, topic: &str, partitions: &Vec<(i32, i64)>) {
        let partitions_configs: Vec<Partition> = partitions.iter()
            .map(|p| Partition { id: p.0, offset: p.1 } )
            .collect();
        self.topics.insert(topic.to_string(), Some(partitions_configs));
    }

    pub fn create_native_topic_partition_list(&self) -> *mut RDKafkaTopicPartitionList {
        let tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(self.topics.len() as i32) };

        for (topic, partitions) in self.topics.iter() {
            let topic_cstring = CString::new(topic.as_str()).expect("could not create name CString");
            match partitions {
                &Some(ref ps) => {
                    // Partitions specified
                    for p in ps {
                        unsafe { rdsys::rd_kafka_topic_partition_list_add(tp_list, topic_cstring.as_ptr(), p.id) };
                        if p.offset >= 0 {
                            unsafe { rdsys::rd_kafka_topic_partition_list_set_offset(tp_list, topic_cstring.as_ptr(), p.id, p.offset) };
                        }
                    }
                },
                &None => {
                    // No partitions specified
                    unsafe { rdsys::rd_kafka_topic_partition_list_add(tp_list, topic_cstring.as_ptr(), -1); }
                }
            }
        }

        tp_list
    }
}

impl Deref for TopicPartitionList {
    type Target = Topics;

    fn deref(&self) -> &Topics {
        &self.topics
    }
}

#[cfg(test)]
mod tests {
    extern crate rdkafka_sys as rdkafka;
    use super::*;

    #[test]
    fn test_topic_partition_list_no_configuration() {
        let list = TopicPartitionList::with_topics(&vec!["topic_1", "topic_2"]);
        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());

        assert_eq!(list, through_rdkafka);
    }

    #[test]
    fn test_topic_partition_list_with_partitions() {
        let mut list = TopicPartitionList::new();
        list.add_topic_with_partitions("topic_1", &vec![1, 2, 3]);
        list.add_topic_with_partitions("topic_2", &vec![1, 3, 5]);

        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());

        assert_eq!(list, through_rdkafka);
    }

    #[test]
    fn test_topic_partition_list_with_partitions_and_offsets() {
        let mut list = TopicPartitionList::new();
        list.add_topic_with_partitions_and_offsets("topic_1", &vec![(1, 1), (2, 1), (3, 1)]);
        list.add_topic_with_partitions_and_offsets("topic_2", &vec![(1, 1), (3, 1), (5, 1)]);

        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());

        assert_eq!(list, through_rdkafka);
    }
}
