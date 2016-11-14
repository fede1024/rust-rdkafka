//! Store and manipulate Kafka messages.
extern crate librdkafka_sys as rdkafka;

use std::slice;
use std::str;

/// Represents a native librdkafka message.
#[derive(Debug)]
pub struct Message {
    ptr: *mut rdkafka::rd_kafka_message_t,
}

unsafe impl Send for Message {}

impl<'a> Message {
    pub fn new(ptr: *mut rdkafka::rd_kafka_message_t) -> Message {
        Message { ptr: ptr }
    }

    pub fn get_payload(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.ptr).payload.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.ptr).payload as *const u8, (*self.ptr).len))
            }
        }
    }

    pub fn get_key(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.ptr).key.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.ptr).key as *const u8, (*self.ptr).key_len))
            }
        }
    }

    pub fn get_value_view<V: ?Sized + FromBytes>(&'a self) -> Option<Result<&'a V, V::Error>> {
        self.get_payload().map(V::from_bytes)
    }

    pub fn get_key_view<K: ?Sized + FromBytes>(&'a self) -> Option<Result<&'a K, K::Error>> {
        self.get_key().map(K::from_bytes)
    }

    pub fn get_partition(&self) -> i32 {
        unsafe { (*self.ptr).partition }
    }

    pub fn get_offset(&self) -> i64 {
        unsafe { (*self.ptr).offset }
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        trace!("Destroying message {:?}", self);
        unsafe { rdkafka::rd_kafka_message_destroy(self.ptr) };
    }
}

/// Given a reference to a byte array, returns a different view of the same data.
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

impl<'a> ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}
