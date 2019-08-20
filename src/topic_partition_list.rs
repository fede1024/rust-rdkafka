//! A data structure representing topic, partitions and offsets, compatible with the
//! `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.
use crate::rdsys;
use crate::rdsys::types::*;

use crate::error::{IsError, KafkaError, KafkaResult};

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

/// A librdkafka offset.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Offset {
    /// Start consuming from the beginning of the partition.
    Beginning,
    /// Start consuming from the end of the partition.
    End,
    /// Start consuming from the stored offset.
    Stored,
    /// Offset not assigned or invalid.
    Invalid,
    /// A specific offset to consume from.
    Offset(i64),
}

impl Offset {
    /// Converts the integer representation of an offset use by librdkafka to an `Offset`.
    pub fn from_raw(raw_offset: i64) -> Offset {
        match raw_offset {
            OFFSET_BEGINNING => Offset::Beginning,
            OFFSET_END => Offset::End,
            OFFSET_STORED => Offset::Stored,
            OFFSET_INVALID => Offset::Invalid,
            n => Offset::Offset(n),
        }
    }

    /// Converts the `Offset` to the internal integer representation used by librdkafka.
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

// TODO: implement Debug
/// One element of the topic partition list.
pub struct TopicPartitionListElem<'a> {
    ptr: *mut RDKafkaTopicPartition,
    _owner_list: &'a TopicPartitionList,
}

impl<'a> TopicPartitionListElem<'a> {
    unsafe fn from_ptr(
        list: &'a TopicPartitionList,
        ptr: *mut RDKafkaTopicPartition,
    ) -> TopicPartitionListElem {
        TopicPartitionListElem {
            ptr,
            _owner_list: list,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        unsafe {
            let c_str = (*self.ptr).topic;
            CStr::from_ptr(c_str)
                .to_str()
                .expect("Topic name is not UTF-8")
        }
    }

    /// Returns the optional error associated to the specific entry in the TPL.
    pub fn error(&self) -> KafkaResult<()> {
        let kafka_err = unsafe { (*self.ptr).err };
        if kafka_err.is_error() {
            Err(KafkaError::OffsetFetch(kafka_err.into()))
        } else {
            Ok(())
        }
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        unsafe { (*self.ptr).partition }
    }

    /// Returns the offset.
    pub fn offset(&self) -> Offset {
        let raw_offset = unsafe { (*self.ptr).offset };
        Offset::from_raw(raw_offset)
    }

    /// Sets the offset.
    pub fn set_offset(&self, offset: Offset) {
        let raw_offset = offset.to_raw();
        unsafe { (*self.ptr).offset = raw_offset };
    }
}

impl<'a> PartialEq for TopicPartitionListElem<'a> {
    fn eq(&self, other: &TopicPartitionListElem<'a>) -> bool {
        self.topic() == other.topic()
            && self.partition() == other.partition()
            && self.offset() == other.offset()
    }
}

/// A structure to store and manipulate a list of topics and partitions with optional offsets.
pub struct TopicPartitionList {
    ptr: *mut RDKafkaTopicPartitionList,
}

impl Clone for TopicPartitionList {
    fn clone(&self) -> Self {
        let new_tpl = unsafe { rdsys::rd_kafka_topic_partition_list_copy(self.ptr) };
        unsafe { TopicPartitionList::from_ptr(new_tpl) }
    }
}

impl TopicPartitionList {
    /// Creates a new empty list with default capacity.
    pub fn new() -> TopicPartitionList {
        TopicPartitionList::with_capacity(5)
    }

    /// Creates a new empty list with the specified capacity.
    pub fn with_capacity(capacity: usize) -> TopicPartitionList {
        let ptr = unsafe { rdsys::rd_kafka_topic_partition_list_new(capacity as i32) };
        unsafe { TopicPartitionList::from_ptr(ptr) }
    }

    /// Transforms a pointer to the native librdkafka RDTopicPartitionList into a
    /// managed `TopicPartitionList` instance.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafkaTopicPartitionList) -> TopicPartitionList {
        TopicPartitionList { ptr }
    }

    /// Given a topic map, generates a new `TopicPartitionList`.
    pub fn from_topic_map(topic_map: &HashMap<(String, i32), Offset>) -> TopicPartitionList {
        topic_map.iter().fold(
            TopicPartitionList::with_capacity(topic_map.len()),
            |mut tpl, (&(ref topic_name, partition), offset)| {
                tpl.add_partition_offset(topic_name, partition, *offset);
                tpl
            },
        )
    }

    /// Returns the pointer to the internal librdkafka structure.
    pub fn ptr(&self) -> *mut RDKafkaTopicPartitionList {
        self.ptr
    }

    /// Capture the instance without calling the destructor on the internal librdkafka
    /// structure.
    pub(crate) unsafe fn leak(mut self) {
        self.ptr = ptr::null_mut();
    }

    /// Returns the number of elements in the list.
    pub fn count(&self) -> usize {
        unsafe { (*self.ptr).cnt as usize }
    }

    /// Returns the capacity of the list.
    pub fn capacity(&self) -> usize {
        unsafe { (*self.ptr).size as usize }
    }

    /// Adds a topic with unassigned partitions to the list.
    pub fn add_topic_unassigned<'a>(&'a mut self, topic: &str) -> TopicPartitionListElem<'a> {
        self.add_partition(topic, PARTITION_UNASSIGNED)
    }

    /// Adds a topic and partition to the list.
    pub fn add_partition<'a>(
        &'a mut self,
        topic: &str,
        partition: i32,
    ) -> TopicPartitionListElem<'a> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let tp_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_add(self.ptr, topic_c.as_ptr(), partition)
        };
        unsafe { TopicPartitionListElem::from_ptr(self, tp_ptr) }
    }

    /// Adds a topic and partition range to the list.
    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        unsafe {
            rdsys::rd_kafka_topic_partition_list_add_range(
                self.ptr,
                topic_c.as_ptr(),
                start_partition,
                stop_partition,
            );
        }
    }

    /// Sets the offset for an already created topic partition. It will fail if the topic partition
    /// isn't in the list.
    pub fn set_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> KafkaResult<()> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let kafka_err = unsafe {
            rdsys::rd_kafka_topic_partition_list_set_offset(
                self.ptr,
                topic_c.as_ptr(),
                partition,
                offset.to_raw(),
            )
        };

        if kafka_err.is_error() {
            Err(KafkaError::SetPartitionOffset(kafka_err.into()))
        } else {
            Ok(())
        }
    }

    /// Adds a topic and partition to the list, with the specified offset.
    pub fn add_partition_offset(&mut self, topic: &str, partition: i32, offset: Offset) {
        self.add_partition(topic, partition);
        self.set_partition_offset(topic, partition, offset)
            .expect("Should never fail");
    }

    /// Given a topic name and a partition number, returns the corresponding list element.
    pub fn find_partition(&self, topic: &str, partition: i32) -> Option<TopicPartitionListElem> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let elem_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_find(self.ptr, topic_c.as_ptr(), partition)
        };
        if elem_ptr.is_null() {
            None
        } else {
            Some(unsafe { TopicPartitionListElem::from_ptr(self, elem_ptr) })
        }
    }

    /// Sets all partitions in the list to the specified offset.
    pub fn set_all_offsets(&mut self, offset: Offset) {
        let slice = unsafe { slice::from_raw_parts_mut((*self.ptr).elems, self.count()) };
        for elem_ptr in slice {
            let elem = unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) };
            elem.set_offset(offset);
        }
    }

    /// Returns all the elements of the list.
    pub fn elements(&self) -> Vec<TopicPartitionListElem> {
        let slice = unsafe { slice::from_raw_parts_mut((*self.ptr).elems, self.count()) };
        let mut vec = Vec::with_capacity(slice.len());
        for elem_ptr in slice {
            vec.push(unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) });
        }
        vec
    }

    /// Returns all the elements of the list that belong to the specified topic.
    pub fn elements_for_topic<'a>(&'a self, topic: &str) -> Vec<TopicPartitionListElem<'a>> {
        let slice = unsafe { slice::from_raw_parts_mut((*self.ptr).elems, self.count()) };
        let mut vec = Vec::with_capacity(slice.len());
        for elem_ptr in slice {
            let tp = unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) };
            if tp.topic() == topic {
                vec.push(tp);
            }
        }
        vec
    }

    /// Returns a hashmap-based representation of the list.
    pub fn to_topic_map(&self) -> HashMap<(String, i32), Offset> {
        self.elements()
            .iter()
            .map(|elem| ((elem.topic().to_owned(), elem.partition()), elem.offset()))
            .collect()
    }
}

impl Drop for TopicPartitionList {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            trace!("Destroying TPL: {:?}", self.ptr);
            unsafe { rdsys::rd_kafka_topic_partition_list_destroy(self.ptr) }
            trace!("TPL destroyed: {:?}", self.ptr);
        }
    }
}

impl PartialEq for TopicPartitionList {
    fn eq(&self, other: &TopicPartitionList) -> bool {
        if self.count() != other.count() {
            return false;
        }
        self.elements().iter().all(|elem| {
            if let Some(other_elem) = other.find_partition(elem.topic(), elem.partition()) {
                elem == &other_elem
            } else {
                false
            }
        })
    }
}

impl Default for TopicPartitionList {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TopicPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TPL {{")?;
        for elem in self.elements() {
            write!(
                f,
                "({}, {}): {:?}, ",
                elem.topic(),
                elem.partition(),
                elem.offset()
            )?
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    #[test]
    fn add_partition_offset_find() {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition("topic1", 0);
        tpl.add_partition("topic1", 1);
        tpl.add_partition("topic2", 0);
        tpl.add_partition("topic2", 1);

        tpl.set_partition_offset("topic1", 0, Offset::Offset(0))
            .unwrap();
        tpl.set_partition_offset("topic1", 1, Offset::Offset(1))
            .unwrap();
        tpl.set_partition_offset("topic2", 0, Offset::Offset(2))
            .unwrap();
        tpl.set_partition_offset("topic2", 1, Offset::Offset(3))
            .unwrap();

        assert_eq!(tpl.count(), 4);
        assert!(tpl
            .set_partition_offset("topic0", 3, Offset::Offset(0))
            .is_err());
        assert!(tpl
            .set_partition_offset("topic3", 0, Offset::Offset(0))
            .is_err());

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

        tpl.set_partition_offset("topic1", 0, Offset::Offset(0))
            .unwrap();
        tpl.set_partition_offset("topic1", 1, Offset::Offset(1))
            .unwrap();
        tpl.set_partition_offset("topic1", 2, Offset::Offset(2))
            .unwrap();
        tpl.set_partition_offset("topic1", 3, Offset::Offset(3))
            .unwrap();
        assert!(tpl
            .set_partition_offset("topic1", 4, Offset::Offset(2))
            .is_err());
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
        tpl.add_partition_offset("topic1", 0, Offset::Offset(0));
        tpl.add_partition_offset("topic1", 1, Offset::Offset(1));

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

    #[test]
    fn test_topic_map() {
        let mut topic_map = HashMap::new();
        topic_map.insert(("topic1".to_string(), 0), Offset::Invalid);
        topic_map.insert(("topic1".to_string(), 1), Offset::Offset(123));
        topic_map.insert(("topic2".to_string(), 0), Offset::Beginning);

        let tpl = TopicPartitionList::from_topic_map(&topic_map);
        let topic_map2 = tpl.to_topic_map();
        let tpl2 = TopicPartitionList::from_topic_map(&topic_map2);

        assert_eq!(topic_map, topic_map2);
        assert_eq!(tpl, tpl2);
    }
}
