//! Data structures representing topic, partitions and offsets.
//!
//! Compatible with the `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::slice;
use std::str;

use libc::c_void;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::error::{IsError, KafkaError, KafkaResult};
use crate::util::{self, KafkaDrop, NativePtr};

const PARTITION_UNASSIGNED: i32 = -1;

const OFFSET_BEGINNING: i64 = rdsys::RD_KAFKA_OFFSET_BEGINNING as i64;
const OFFSET_END: i64 = rdsys::RD_KAFKA_OFFSET_END as i64;
const OFFSET_STORED: i64 = rdsys::RD_KAFKA_OFFSET_STORED as i64;
const OFFSET_INVALID: i64 = rdsys::RD_KAFKA_OFFSET_INVALID as i64;
const OFFSET_TAIL_BASE: i64 = rdsys::RD_KAFKA_OFFSET_TAIL_BASE as i64;

/// A Kafka offset.
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
    ///
    /// Note that while the offset is a signed integer, negative offsets will be
    /// rejected when passed to librdkafka.
    Offset(i64),
    /// An offset relative to the end of the partition.
    ///
    /// Note that while the offset is a signed integer, negative offsets will
    /// be rejected when passed to librdkafka.
    OffsetTail(i64),
}

impl Offset {
    /// Converts the integer representation of an offset used by librdkafka to
    /// an `Offset`.
    pub fn from_raw(raw_offset: i64) -> Offset {
        match raw_offset {
            OFFSET_BEGINNING => Offset::Beginning,
            OFFSET_END => Offset::End,
            OFFSET_STORED => Offset::Stored,
            OFFSET_INVALID => Offset::Invalid,
            n if n <= OFFSET_TAIL_BASE => Offset::OffsetTail(-(n - OFFSET_TAIL_BASE)),
            n => Offset::Offset(n),
        }
    }

    /// Converts the `Offset` to the internal integer representation used by
    /// librdkafka.
    ///
    /// Returns `None` if the offset cannot be represented in librdkafka's
    /// internal representation.
    pub fn to_raw(self) -> Option<i64> {
        match self {
            Offset::Beginning => Some(OFFSET_BEGINNING),
            Offset::End => Some(OFFSET_END),
            Offset::Stored => Some(OFFSET_STORED),
            Offset::Invalid => Some(OFFSET_INVALID),
            Offset::Offset(n) if n >= 0 => Some(n),
            Offset::OffsetTail(n) if n > 0 => Some(OFFSET_TAIL_BASE - n),
            Offset::Offset(_) | Offset::OffsetTail(_) => None,
        }
    }
}

// TODO: implement Debug
/// One element of the topic partition list.
pub struct TopicPartitionListElem<'a> {
    ptr: &'a mut RDKafkaTopicPartition,
}

impl<'a> TopicPartitionListElem<'a> {
    // _owner_list serves as a marker so that the lifetime isn't too long
    fn from_ptr(
        _owner_list: &'a TopicPartitionList,
        ptr: &'a mut RDKafkaTopicPartition,
    ) -> TopicPartitionListElem<'a> {
        TopicPartitionListElem { ptr }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        unsafe {
            let c_str = self.ptr.topic;
            CStr::from_ptr(c_str)
                .to_str()
                .expect("Topic name is not UTF-8")
        }
    }

    /// Returns the optional error associated to the specific entry in the TPL.
    pub fn error(&self) -> KafkaResult<()> {
        let kafka_err = self.ptr.err;
        if kafka_err.is_error() {
            Err(KafkaError::OffsetFetch(kafka_err.into()))
        } else {
            Ok(())
        }
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.ptr.partition
    }

    /// Returns the offset.
    pub fn offset(&self) -> Offset {
        let raw_offset = self.ptr.offset;
        Offset::from_raw(raw_offset)
    }

    /// Sets the offset.
    pub fn set_offset(&mut self, offset: Offset) -> KafkaResult<()> {
        match offset.to_raw() {
            Some(offset) => {
                self.ptr.offset = offset;
                Ok(())
            }
            None => Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::InvalidArgument,
            )),
        }
    }

    /// Returns the optional metadata associated with the entry.
    pub fn metadata(&self) -> &str {
        let bytes = unsafe { util::ptr_to_slice(self.ptr.metadata, self.ptr.metadata_size) };
        str::from_utf8(bytes).expect("Metadata is not UTF-8")
    }

    /// Sets the optional metadata associated with the entry.
    pub fn set_metadata<M>(&mut self, metadata: M)
    where
        M: AsRef<str>,
    {
        let metadata = metadata.as_ref();
        let buf = unsafe { libc::malloc(metadata.len()) };
        unsafe { libc::memcpy(buf, metadata.as_ptr() as *const c_void, metadata.len()) };
        self.ptr.metadata = buf;
        self.ptr.metadata_size = metadata.len();
    }
}

impl<'a> PartialEq for TopicPartitionListElem<'a> {
    fn eq(&self, other: &TopicPartitionListElem<'a>) -> bool {
        self.topic() == other.topic()
            && self.partition() == other.partition()
            && self.offset() == other.offset()
            && self.metadata() == other.metadata()
    }
}

/// A structure to store and manipulate a list of topics and partitions with optional offsets.
pub struct TopicPartitionList {
    ptr: NativePtr<RDKafkaTopicPartitionList>,
}

unsafe impl KafkaDrop for RDKafkaTopicPartitionList {
    const TYPE: &'static str = "topic partition list";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_topic_partition_list_destroy;
}

impl Clone for TopicPartitionList {
    fn clone(&self) -> Self {
        let new_tpl = unsafe { rdsys::rd_kafka_topic_partition_list_copy(self.ptr()) };
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
        TopicPartitionList {
            ptr: NativePtr::from_ptr(ptr).unwrap(),
        }
    }

    /// Given a topic map, generates a new `TopicPartitionList`.
    pub fn from_topic_map(
        topic_map: &HashMap<(String, i32), Offset>,
    ) -> KafkaResult<TopicPartitionList> {
        let mut tpl = TopicPartitionList::with_capacity(topic_map.len());
        for ((topic_name, partition), offset) in topic_map {
            tpl.add_partition_offset(topic_name, *partition, *offset)?;
        }
        Ok(tpl)
    }

    /// Returns the pointer to the internal librdkafka structure.
    pub fn ptr(&self) -> *mut RDKafkaTopicPartitionList {
        self.ptr.ptr()
    }

    /// Returns the number of elements in the list.
    pub fn count(&self) -> usize {
        self.ptr.cnt as usize
    }

    /// Returns the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.ptr.size as usize
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
            rdsys::rd_kafka_topic_partition_list_add(self.ptr(), topic_c.as_ptr(), partition)
        };
        unsafe { TopicPartitionListElem::from_ptr(self, &mut *tp_ptr) }
    }

    /// Adds a topic and partition range to the list.
    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        unsafe {
            rdsys::rd_kafka_topic_partition_list_add_range(
                self.ptr(),
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
        let kafka_err = match offset.to_raw() {
            Some(offset) => unsafe {
                rdsys::rd_kafka_topic_partition_list_set_offset(
                    self.ptr(),
                    topic_c.as_ptr(),
                    partition,
                    offset,
                )
            },
            None => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_ARG,
        };

        if kafka_err.is_error() {
            Err(KafkaError::SetPartitionOffset(kafka_err.into()))
        } else {
            Ok(())
        }
    }

    /// Adds a topic and partition to the list, with the specified offset.
    pub fn add_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> KafkaResult<()> {
        self.add_partition(topic, partition);
        self.set_partition_offset(topic, partition, offset)
    }

    /// Given a topic name and a partition number, returns the corresponding list element.
    pub fn find_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<TopicPartitionListElem<'_>> {
        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
        let elem_ptr = unsafe {
            rdsys::rd_kafka_topic_partition_list_find(self.ptr(), topic_c.as_ptr(), partition)
        };
        if elem_ptr.is_null() {
            None
        } else {
            Some(unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) })
        }
    }

    /// Sets all partitions in the list to the specified offset.
    pub fn set_all_offsets(&mut self, offset: Offset) -> Result<(), KafkaError> {
        if self.count() == 0 {
            return Ok(());
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            let mut elem = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
            elem.set_offset(offset)?;
        }
        Ok(())
    }

    /// Returns all the elements of the list.
    pub fn elements(&self) -> Vec<TopicPartitionListElem<'_>> {
        let mut vec = Vec::with_capacity(self.count());
        if self.count() == 0 {
            return vec;
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            vec.push(TopicPartitionListElem::from_ptr(self, &mut *elem_ptr));
        }
        vec
    }

    /// Returns all the elements of the list that belong to the specified topic.
    pub fn elements_for_topic<'a>(&'a self, topic: &str) -> Vec<TopicPartitionListElem<'a>> {
        let mut vec = Vec::with_capacity(self.count());
        if self.count() == 0 {
            return vec;
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            let tp = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TPL {{")?;
        for (i, elem) in self.elements().iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(
                f,
                "{}/{}: offset={:?} metadata={:?}, error={:?}",
                elem.topic(),
                elem.partition(),
                elem.offset(),
                elem.metadata(),
                elem.error(),
            )?;
        }
        write!(f, "}}")
    }
}

unsafe impl Send for TopicPartitionList {}
unsafe impl Sync for TopicPartitionList {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    #[test]
    fn offset_conversion() {
        assert_eq!(Offset::Offset(123).to_raw(), Some(123));
        assert_eq!(Offset::from_raw(123), Offset::Offset(123));

        assert_eq!(Offset::OffsetTail(10).to_raw(), Some(-2010));
        assert_eq!(Offset::from_raw(-2010), Offset::OffsetTail(10));
    }

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
        let mut tp3 = tpl.find_partition("topic2", 1).unwrap();

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

        tp3.set_offset(Offset::Offset(1234)).unwrap();
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
        tpl.add_partition_offset("topic1", 0, Offset::Offset(0))
            .unwrap();
        tpl.add_partition_offset("topic1", 1, Offset::Offset(1))
            .unwrap();

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

        let tpl = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        let topic_map2 = tpl.to_topic_map();
        let tpl2 = TopicPartitionList::from_topic_map(&topic_map2).unwrap();

        assert_eq!(topic_map, topic_map2);
        assert_eq!(tpl, tpl2);
    }
}
