//! Store and manipulate Kafka messages.
use rdsys;
use rdsys::types::*;

use std::slice;
use std::str;
use std::ffi::CStr;

/// Timestamp of a message
#[derive(Debug,PartialEq,Eq)]
pub enum Timestamp {
    NotAvailable,
    CreateTime(i64),
    LogAppendTime(i64)
}

/// A native librdkafka message.
#[derive(Debug)]
pub struct Message {
    ptr: *mut RDKafkaMessage,
}

unsafe impl Send for Message {}

impl<'a> Message {
    /// Creates a new Message that wraps the native Kafka message pointer.
    pub fn new(ptr: *mut RDKafkaMessage) -> Message {
        Message { ptr: ptr }
    }

    /// Returns a pointer to the RDKafkaMessage.
    pub fn ptr(&self) -> *mut RDKafkaMessage {
        self.ptr
    }

    /// Returns the length of the key field of the message.
    pub fn key_len(&self) -> usize {
        unsafe { (*self.ptr).key_len }
    }

    /// Returns the length of the payload field of the message.
    pub fn payload_len(&self) -> usize {
        unsafe { (*self.ptr).len }
    }

    /// Returns the key of the message, or None if there is no key.
    pub fn key(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.ptr).key.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.ptr).key as *const u8, (*self.ptr).key_len))
            }
        }
    }

    /// Returns the payload of the message, or None if there is no payload.
    pub fn payload(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.ptr).payload.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.ptr).payload as *const u8, (*self.ptr).len))
            }
        }
    }

    /// Returns the name of the topic.
    pub fn topic_name(&'a self) -> &'a str {
        unsafe {
            CStr::from_ptr(rdsys::rd_kafka_topic_name((*self.ptr).rkt))
                .to_str()
                .expect("Topic name is not valid UTF-8")
        }
    }

    /// Converts the raw bytes of the payload to a reference of type &P, pointing to the same data inside
    /// the message. The returned reference cannot outlive the message.
    pub fn payload_view<P: ?Sized + FromBytes>(&'a self) -> Option<Result<&'a P, P::Error>> {
        self.payload().map(P::from_bytes)
    }

    /// Converts the raw bytes of the key to a reference of type &K, pointing to the same data inside
    /// the message. The returned reference cannot outlive the message.
    pub fn key_view<K: ?Sized + FromBytes>(&'a self) -> Option<Result<&'a K, K::Error>> {
        self.key().map(K::from_bytes)
    }

    /// Returns the partition number where the message is stored.
    pub fn partition(&self) -> i32 {
        unsafe { (*self.ptr).partition }
    }

    /// Returns the offset of the message.
    pub fn offset(&self) -> i64 {
        unsafe { (*self.ptr).offset }
    }

    /// Returns a (topic, partition, offset) tuple with no reference to the original message.
    pub fn identifier(&self) -> (String, i32, i64) {
        (self.topic_name().to_owned(), self.partition(), self.offset())
    }

    /// Returns the message timestamp for a consumed message if available.
    pub fn timestamp(&self) -> Timestamp {
        let mut timestamp_type = rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let timestamp = unsafe {
            rdsys::rd_kafka_message_timestamp(
                self.ptr,
                &mut timestamp_type
            )

        };
        match timestamp_type {
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => Timestamp::NotAvailable,
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => Timestamp::CreateTime(timestamp),
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => Timestamp::LogAppendTime(timestamp)
        }
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        trace!("Destroying message {:?}", self);
        unsafe { rdsys::rd_kafka_message_destroy(self.ptr) };
    }
}

/// Given a reference to a byte array, returns a different view of the same data.
/// No copy of the data should be performed.
pub trait FromBytes {
    type Error;
    fn from_bytes(&[u8]) -> Result<&Self, Self::Error>;
}

impl FromBytes for [u8] {
    type Error = ();
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        Ok(bytes)
    }
}

impl FromBytes for str {
    type Error = str::Utf8Error;
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        str::from_utf8(bytes)
    }
}

/// Given some data, returns the byte representation of that data.
/// No copy of the data should be performed.
pub trait ToBytes {
    fn to_bytes(&self) -> &[u8];
}

impl<'a> ToBytes for &'a [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> ToBytes for &'a str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}
