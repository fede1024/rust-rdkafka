extern crate librdkafka_sys as rdkafka;

use std::slice;

#[derive(Debug)]
pub struct Message {
    // TODO need creator
    pub message_n: *mut rdkafka::rd_kafka_message_s,
}

unsafe impl Send for Message {}

impl<'a> Message {
    pub fn get_payload(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.message_n).payload.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.message_n).payload as *const u8, (*self.message_n).len))
            }
        }
    }

    pub fn get_key(&'a self) -> Option<&'a [u8]> {
        unsafe {
            if (*self.message_n).key.is_null() {
                None
            } else {
                Some(slice::from_raw_parts::<u8>((*self.message_n).key as *const u8, (*self.message_n).key_len))
            }
        }
    }

    pub fn get_partition(&self) -> i32 {
        unsafe { (*self.message_n).partition }
    }

    pub fn get_offset(&self) -> i64 {
        unsafe { (*self.message_n).offset }
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        trace!("Destroying {:?}", self);
        unsafe { rdkafka::rd_kafka_message_destroy(self.message_n) };
    }
}

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
