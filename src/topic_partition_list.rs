//! A data structure representing topic, partitions and offsets, compatible with the
//! `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.
use rdsys;
use rdsys::types::*;

use util::cstr_to_owned;
use error::{IsError, KafkaError, KafkaResult};

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ops::Deref;
use std::fmt;
use std::slice;

pub const PARTITION_UNASSIGNED: i32 = -1;

pub const OFFSET_BEGINNING: i64 = rdsys::RD_KAFKA_OFFSET_BEGINNING as i64;
pub const OFFSET_END: i64 = rdsys::RD_KAFKA_OFFSET_END as i64;
pub const OFFSET_STORED: i64 = rdsys::RD_KAFKA_OFFSET_STORED as i64;
pub const OFFSET_INVALID: i64 = rdsys::RD_KAFKA_OFFSET_INVALID as i64;

/// A map of topic names to partitions.
pub type Topics = HashMap<String, Option<Vec<Partition>>>;

pub struct TopicPartitionListElem<'a> {
    ptr: *mut RDKafkaTopicPartition,
    _owner_list: &'a TopicPartitionList,
}

impl<'a> TopicPartitionListElem<'a> {
    fn from_ptr(list: &'a TopicPartitionList, ptr: *mut RDKafkaTopicPartition) -> TopicPartitionListElem {
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

/// The structure to store and manipulate a list of topics and partitions with optional offsets.
pub struct TopicPartitionList {
    ptr: *mut RDKafkaTopicPartitionList,
}

impl Clone for TopicPartitionList {
    fn clone(&self) -> Self {
        let new_tpl = unsafe { rdsys::rd_kafka_topic_partition_list_copy(self.ptr) };
        TopicPartitionList::from_ptr(new_tpl)
    }
}

impl TopicPartitionList {
    pub fn new() -> TopicPartitionList {
        TopicPartitionList::new_with_capacity(5)
    }

    pub fn new_with_capacity(capacity: usize) -> TopicPartitionList {
        let ptr = unsafe { rdsys::rd_kafka_topic_partition_list_new(capacity as i32) };
        TopicPartitionList::from_ptr(ptr)
    }

    pub fn from_ptr(ptr: *mut RDKafkaTopicPartitionList) -> TopicPartitionList {
        TopicPartitionList { ptr: ptr }
    }

    pub fn ptr(&self) -> *mut RDKafkaTopicPartitionList {
        self.ptr
    }

    pub fn count(&self) -> usize {
        unsafe { (*self.ptr).cnt as usize }
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.ptr).size as usize }
    }

    pub fn add_topic_unassigned<'a>(&'a mut self, topic: &str) -> TopicPartitionListElem<'a> {
        self.add_partition(topic, PARTITION_UNASSIGNED)
    }

    pub fn add_partition<'a>(&'a mut self, topic: &str, partition: i32) -> TopicPartitionListElem<'a> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let tp_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_add(self.ptr, topic_c.as_ptr(), partition)
        };
        TopicPartitionListElem::from_ptr(self, tp_ptr)
    }

    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        unsafe {
            rdsys::rd_kafka_topic_partition_list_add_range(self.ptr, topic_c.as_ptr(),
                start_partition, stop_partition);
        }
    }

    pub fn set_partition_offset(&mut self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
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

    pub fn add_partition_offset<'a>(&'a mut self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        self.add_partition(topic, partition);
        self.set_partition_offset(topic, partition, offset)
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

    pub fn set_all_offsets(&mut self, offset: i64) {
        let slice = unsafe { slice::from_raw_parts_mut((*self.ptr).elems, self.count()) };
        for elem_ptr in slice {
            let elem = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
            elem.set_offset(offset);
        }
    }

    pub fn elements<'a>(&'a self) -> Vec<TopicPartitionListElem<'a>> {
        let slice = unsafe { slice::from_raw_parts_mut((*self.ptr).elems, self.count()) };
        let mut vec = Vec::with_capacity(slice.len());
        for elem_ptr in slice {
            vec.push(TopicPartitionListElem::from_ptr(self, &mut *elem_ptr));
        }
        vec
    }

    pub fn create_topic_map(&self) -> HashMap<String, (i32, i64)> {
        self.elements().iter()
            .map(|elem| (elem.topic().to_owned(), (elem.partition(), elem.offset())))
            .collect::<HashMap<String, (i32, i64)>>()
    }
}

impl Drop for TopicPartitionList {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_topic_partition_list_destroy(self.ptr) }
    }
}

impl fmt::Debug for TopicPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TPL {{ ")?;
        for elem in self.elements() {
            write!(f, "({}, {}): {}", elem.topic(), elem.partition(), elem.offset())?
        }
        write!(f, "}}")
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
pub struct TopicPartitionList2 {
    pub topics: Topics
}

impl TopicPartitionList2 {
    /// Create list based on a list from the rdkafka side.
    pub fn from_rdkafka(tp_list: *const RDKafkaTopicPartitionList) -> TopicPartitionList2 {
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

        TopicPartitionList2 {
            topics: topics
        }
    }

    /// Create an empty list
    pub fn new() -> TopicPartitionList2 {
        TopicPartitionList2 {
            topics: HashMap::new()
        }
    }

    /// Create list with specified topics with default configuration
    pub fn with_topics(topic_names: &[&str]) -> TopicPartitionList2 {
        let mut topics: Topics = HashMap::with_capacity(topic_names.len());

        for topic_name in topic_names {
            topics.insert(topic_name.to_string(), None);
        }

        TopicPartitionList2 {
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

impl Deref for TopicPartitionList2 {
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
    fn add_partition_offset_find() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition("topic1", 0);
        tpl.add_partition("topic1", 1);
        tpl.add_partition("topic2", 0);
        tpl.add_partition("topic2", 1);

        tpl.set_partition_offset("topic1", 0, 0).unwrap();
        tpl.set_partition_offset("topic1", 1, 1).unwrap();
        tpl.set_partition_offset("topic2", 0, 2).unwrap();
        tpl.set_partition_offset("topic2", 1, 3).unwrap();

        assert_eq!(tpl.count(), 4);
        assert!(tpl.set_partition_offset("topic0", 3, 0).is_err());
        assert!(tpl.set_partition_offset("topic3", 0, 0).is_err());

        let tp0 = tpl.find_partition("topic1", 0).unwrap();
        let tp1 = tpl.find_partition("topic1", 1).unwrap();
        let tp2 = tpl.find_partition("topic2", 0).unwrap();
        let tp3 = tpl.find_partition("topic2", 1).unwrap();

        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), 0);
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), 1);
        assert_eq!(tp2.topic(), "topic2");
        assert_eq!(tp2.partition(), 0);
        assert_eq!(tp2.offset(), 2);
        assert_eq!(tp3.topic(), "topic2");
        assert_eq!(tp3.partition(), 1);
        assert_eq!(tp3.offset(), 3);

        tp3.set_offset(1234);
        assert_eq!(tp3.offset(), 1234);
    }

    #[test]
    fn add_partition_range() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition_range("topic1", 0, 3);

        tpl.set_partition_offset("topic1", 0, 0).unwrap();
        tpl.set_partition_offset("topic1", 1, 1).unwrap();
        tpl.set_partition_offset("topic1", 2, 2).unwrap();
        tpl.set_partition_offset("topic1", 3, 2).unwrap();
        assert!(tpl.set_partition_offset("topic1", 4, 2).is_err());
    }

    #[test]
    fn check_defaults() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition("topic1", 0);

        let tp = tpl.find_partition("topic1", 0).unwrap();
        assert_eq!(tp.offset(), OFFSET_INVALID);
    }

    #[test]
    fn test_add_partition_offset_clone() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("topic1", 0, 0).unwrap();
        tpl.add_partition_offset("topic1", 1, 1).unwrap();

        let tp0 = tpl.find_partition("topic1", 0).unwrap();
        let tp1 = tpl.find_partition("topic1", 1).unwrap();
        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), 0);
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), 1);

        let tpl_cloned = tpl.clone();
        let tp0 = tpl_cloned.find_partition("topic1", 0).unwrap();
        let tp1 = tpl_cloned.find_partition("topic1", 1).unwrap();
        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), 0);
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), 1);
    }

//    #[test]
//    fn test_topic_partition_list_no_configuration() {
//        let list = TopicPartitionList::with_topics(&vec!["topic_1", "topic_2"]);
//        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());
//
//        assert_eq!(list, through_rdkafka);
//    }

//    #[test]
//    fn test_topic_partition_list_with_partitions() {
//        let mut list = TopicPartitionList::new();
//        list.add_topic_with_partitions("topic_1", &vec![1, 2, 3]);
//        list.add_topic_with_partitions("topic_2", &vec![1, 3, 5]);
//
//        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());
//
//        assert_eq!(list, through_rdkafka);
//    }
//
//    #[test]
//    fn test_topic_partition_list_with_partitions_and_offsets() {
//        let mut list = TopicPartitionList::new();
//        list.add_topic_with_partitions_and_offsets("topic_1", &vec![(1, 1), (2, 1), (3, 1)]);
//        list.add_topic_with_partitions_and_offsets("topic_2", &vec![(1, 1), (3, 1), (5, 1)]);
//
//        let through_rdkafka = TopicPartitionList::from_rdkafka(list.create_native_topic_partition_list());
//
//        assert_eq!(list, through_rdkafka);
//    }
}
