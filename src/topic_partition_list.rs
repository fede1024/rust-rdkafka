//! A data structure representing topic, partitions and offsets, compatible with the
//! `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.
use rdsys;
use rdsys::types::*;

use error::{IsError, KafkaError, KafkaResult};

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr;
use std::slice;

const PARTITION_UNASSIGNED: i32 = -1;

const OFFSET_BEGINNING: i64 = rdsys::RD_KAFKA_OFFSET_BEGINNING as i64;
const OFFSET_END: i64 = rdsys::RD_KAFKA_OFFSET_END as i64;
const OFFSET_STORED: i64 = rdsys::RD_KAFKA_OFFSET_STORED as i64;
const OFFSET_INVALID: i64 = rdsys::RD_KAFKA_OFFSET_INVALID as i64;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Offset {
    Beginning,
    End,
    Stored,
    Invalid,
    Offset(i64)
}

impl Offset {
    pub fn from_raw(raw_offset: i64) -> Offset {
        match raw_offset {
            OFFSET_BEGINNING => Offset::Beginning,
            OFFSET_END => Offset::End,
            OFFSET_STORED => Offset::Stored,
            OFFSET_INVALID => Offset::Invalid,
            n => Offset::Offset(n)
        }
    }

    pub fn to_raw(&self) -> i64 {
        match *self {
            Offset::Beginning => OFFSET_BEGINNING,
            Offset::End => OFFSET_END,
            Offset::Stored => OFFSET_STORED,
            Offset::Invalid => OFFSET_INVALID,
            Offset::Offset(n) => n,
        }
    }
}

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

    pub fn offset(&self) -> Offset {
        let raw_offset = unsafe { (*self.ptr).offset };
        Offset::from_raw(raw_offset)
    }

    pub fn set_offset(&self, offset: Offset) {
        let raw_offset = offset.to_raw();
        unsafe { (*self.ptr).offset = raw_offset };
    }
}

impl<'a> PartialEq for TopicPartitionListElem<'a> {
    fn eq(&self, other: &TopicPartitionListElem<'a>) -> bool {
        self.topic() == other.topic() &&
            self.partition() == other.partition() &&
            self.offset() == other.offset()
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

    pub unsafe fn leak(mut self) {
        self.ptr = ptr::null_mut();
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

    pub fn set_partition_offset(&mut self, topic: &str, partition: i32, offset: Offset) -> KafkaResult<()> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let kafka_err = unsafe {
            rdsys::rd_kafka_topic_partition_list_set_offset(self.ptr, topic_c.as_ptr(), partition,
                offset.to_raw())
        };

        if kafka_err.is_error() {
            Err(KafkaError::SetPartitionOffset(kafka_err))
        } else {
            Ok(())
        }
    }

    pub fn add_partition_offset<'a>(&'a mut self, topic: &str, partition: i32, offset: Offset) -> KafkaResult<()> {
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

    pub fn set_all_offsets(&mut self, offset: Offset) {
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

    pub fn create_topic_map(&self) -> HashMap<String, (i32, Offset)> {
        self.elements().iter()
            .map(|elem| (elem.topic().to_owned(), (elem.partition(), elem.offset())))
            .collect::<HashMap<String, (i32, Offset)>>()
    }
}

impl Drop for TopicPartitionList {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(self.ptr) }
        }
    }
}

impl PartialEq for TopicPartitionList {
    fn eq(&self, other: &TopicPartitionList) -> bool {
        if self.count() != other.count() {
            return false;
        }
        self.elements().iter()
            .all(|elem| {
                if let Some(other_elem) = other.find_partition(elem.topic(), elem.partition()) {
                    elem == &other_elem
                } else {
                    false
                }
            })
    }
}

impl fmt::Debug for TopicPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TPL {{")?;
        for elem in self.elements() {
            write!(f, "({}, {}): {:?}, ", elem.topic(), elem.partition(), elem.offset())?
        }
        write!(f, "}}")
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

        tpl.set_partition_offset("topic1", 0, Offset::Offset(0)).unwrap();
        tpl.set_partition_offset("topic1", 1, Offset::Offset(1)).unwrap();
        tpl.set_partition_offset("topic2", 0, Offset::Offset(2)).unwrap();
        tpl.set_partition_offset("topic2", 1, Offset::Offset(3)).unwrap();

        assert_eq!(tpl.count(), 4);
        assert!(tpl.set_partition_offset("topic0", 3, Offset::Offset(0)).is_err());
        assert!(tpl.set_partition_offset("topic3", 0, Offset::Offset(0)).is_err());

        let tp0 = tpl.find_partition("topic1", 0).unwrap();
        let tp1 = tpl.find_partition("topic1", 1).unwrap();
        let tp2 = tpl.find_partition("topic2", 0).unwrap();
        let tp3 = tpl.find_partition("topic2", 1).unwrap();

        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), Offset::Offset(0));
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), Offset::Offset(1));
        assert_eq!(tp2.topic(), "topic2");
        assert_eq!(tp2.partition(), 0);
        assert_eq!(tp2.offset(), Offset::Offset(2));
        assert_eq!(tp3.topic(), "topic2");
        assert_eq!(tp3.partition(), 1);
        assert_eq!(tp3.offset(), Offset::Offset(3));

        tp3.set_offset(Offset::Offset(1234));
        assert_eq!(tp3.offset(), Offset::Offset(1234));
    }

    #[test]
    fn add_partition_range() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition_range("topic1", 0, 3);

        tpl.set_partition_offset("topic1", 0, Offset::Offset(0)).unwrap();
        tpl.set_partition_offset("topic1", 1, Offset::Offset(1)).unwrap();
        tpl.set_partition_offset("topic1", 2, Offset::Offset(2)).unwrap();
        tpl.set_partition_offset("topic1", 3, Offset::Offset(3)).unwrap();
        assert!(tpl.set_partition_offset("topic1", 4, Offset::Offset(2)).is_err());
    }

    #[test]
    fn check_defaults() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition("topic1", 0);

        let tp = tpl.find_partition("topic1", 0).unwrap();
        assert_eq!(tp.offset(), Offset::Invalid);
    }

    #[test]
    fn test_add_partition_offset_clone() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("topic1", 0, Offset::Offset(0)).unwrap();
        tpl.add_partition_offset("topic1", 1, Offset::Offset(1)).unwrap();

        let tp0 = tpl.find_partition("topic1", 0).unwrap();
        let tp1 = tpl.find_partition("topic1", 1).unwrap();
        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), Offset::Offset(0));
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), Offset::Offset(1));

        let tpl_cloned = tpl.clone();
        let tp0 = tpl_cloned.find_partition("topic1", 0).unwrap();
        let tp1 = tpl_cloned.find_partition("topic1", 1).unwrap();
        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), Offset::Offset(0));
        assert_eq!(tp1.topic(), "topic1");
        assert_eq!(tp1.partition(), 1);
        assert_eq!(tp1.offset(), Offset::Offset(1));
    }
}
