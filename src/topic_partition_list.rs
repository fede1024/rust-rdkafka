//! A data structure representing topic, partitions and offsets, compatible with the
//! `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.
use std::collections::HashMap;
use std::ffi::CString;
use std::ops::Deref;
use std::slice;

use util::cstr_to_owned;

use rdsys;
use rdsys::types::*;

pub const OFFSET_BEGINNING: i64 = rdsys::RD_KAFKA_OFFSET_BEGINNING as i64;
pub const OFFSET_END: i64 = rdsys::RD_KAFKA_OFFSET_END as i64;
pub const OFFSET_STORED: i64 = rdsys::RD_KAFKA_OFFSET_STORED as i64;
pub const OFFSET_INVALID: i64 = rdsys::RD_KAFKA_OFFSET_INVALID as i64;

/// A map of topic names to partitions.
pub type Topics = HashMap<String, HashMap<i32, i64>>;

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
            let topic = topics.entry(topic_name).or_insert(HashMap::new());
            if tp.partition >= 0 || tp.offset >= 0 {
                topic.insert(tp.partition, tp.offset);
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
            topics.insert(topic_name.to_string(), HashMap::new());
        }

        TopicPartitionList {
            topics: topics
        }
    }

    /// Add topic with partitions configured
    pub fn add_topic_with_partitions(&mut self, topic: &str, partitions: &Vec<i32>) {
        let partition_map: HashMap<i32, i64> = partitions.iter()
            .map(|p| (*p, OFFSET_INVALID))
            .collect();
        self.topics.insert(topic.to_string(), partition_map);
    }

    /// Add topic with partitions and offsets configured
    pub fn add_topic_with_partitions_and_offsets(&mut self, topic: &str, partitions: &Vec<(i32, i64)>) {
        let partition_map: HashMap<i32, i64> = partitions.iter()
            .collect();
        self.topics.insert(topic.to_string(), partition_map);
    }

    pub fn topic(&self, topic: &String) -> Option<&HashMap<i32, i64>> {
        self.topics.get(topic)
    }

    pub fn partition(&self, topic: &String, partition: i32) -> Option<i64> {
        (self.topics.get(topic).and_then(|t| t.get(&partition).cloned()))
    }

    pub fn create_native_topic_partition_list(&self) -> *mut RDKafkaTopicPartitionList {
        let tp_list = unsafe { rdsys::rd_kafka_topic_partition_list_new(self.topics.len() as i32) };

        for (topic, partitions) in self.topics.iter() {
            let topic_cstring = CString::new(topic.as_str()).expect("could not create name CString");
            if partitions.is_empty() {
                // No partitions specified
                unsafe { rdsys::rd_kafka_topic_partition_list_add(tp_list, topic_cstring.as_ptr(), -1); }
            } else {
                // Partitions specified
                for (partition, offset) in partitions {
                    unsafe { rdsys::rd_kafka_topic_partition_list_add(tp_list, topic_cstring.as_ptr(), *partition) };
                    if *offset >= 0 {
                        unsafe { rdsys::rd_kafka_topic_partition_list_set_offset(tp_list, topic_cstring.as_ptr(), *partition, *offset) };
                    }
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
