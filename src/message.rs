//! Store and manipulate Kafka messages.
extern crate rdkafka_sys as rdkafka;

use self::rdkafka::types::*;

use std::os::raw::c_void;
use std::ptr;
use std::slice;
use std::str;

/// A native librdkafka message.
#[derive(Debug)]
pub struct Message {
    ptr: *mut RDKafkaMessage,
    from_rdkafka: bool,
    // Key and payload can be added so we can make sure it's
    // contents don't get dropped during the lifetime of this
    // message.
    _payload: Option<Vec<u8>>,
    _key: Option<Vec<u8>>
}

unsafe impl Send for Message {}

impl<'a> Message {
    /// Creates a new Message that wraps the native Kafka message pointer.
    pub fn from_ptr(ptr: *mut RDKafkaMessage) -> Message {
        Message {
            ptr: ptr,
            from_rdkafka: true,
            _payload: None,
            _key: None
        }
    }

    /// Create a new message
    pub fn new(
        partition: Option<i32>,
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        offset: i64
    ) -> Message {
        let (payload_ptr, payload_len) = match payload {
            None => (ptr::null_mut(), 0),
            Some(ref p) => (p.as_ptr() as *mut c_void, p.len())
        };
        let (key_ptr, key_len) = match key {
            None => (ptr::null_mut(), 0),
            Some(ref k) => (k.as_ptr() as *mut c_void, k.len()),
        };
        let mut rdkafka_message = RDKafkaMessage {
            err: RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
            rkt: ptr::null_mut(),
            partition: partition.unwrap_or(-1),
            payload: payload_ptr,
            len: payload_len,
            key: key_ptr,
            key_len: key_len,
            offset: offset,
            _private: ptr::null_mut()
        };

        Message {
            ptr: &mut rdkafka_message,
            from_rdkafka: false,
            _payload: payload,
            _key: key
        }
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
}

impl Drop for Message {
    fn drop(&mut self) {
        trace!("Destroying message {:?}", self);
        if self.from_rdkafka {
            unsafe { rdkafka::rd_kafka_message_destroy(self.ptr) };
        }
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

#[cfg(test)]

mod tests {
    use super::*;
    #[test]
    fn test_message() {
        let message = Message::new(
            Some(1),
            Some(vec![1, 2, 3]),
            Some(vec![1, 2, 3]),
            1
        );

        assert!(!message.ptr().is_null());
        assert_eq!(1, message.partition());
        assert_eq!(&[1, 2, 3], message.payload().unwrap());
        assert_eq!(&[1, 2, 3], message.key().unwrap());
        assert!( message.offset() > 0);
    }
}
