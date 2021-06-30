//! Store and manipulate Kafka messages.

use std::ffi::{CStr, CString};
use std::fmt;
use std::marker::PhantomData;
use std::os::raw::c_void;
use std::ptr;
use std::str;
use std::time::SystemTime;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::error::{IsError, KafkaError, KafkaResult};
use crate::util::{self, millis_to_epoch, KafkaDrop, NativePtr};

/// Timestamp of a Kafka message.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Timestamp {
    /// Timestamp not available.
    NotAvailable,
    /// Message creation time.
    CreateTime(i64),
    /// Log append time.
    LogAppendTime(i64),
}

impl Timestamp {
    /// Convert the timestamp to milliseconds since epoch.
    pub fn to_millis(self) -> Option<i64> {
        match self {
            Timestamp::NotAvailable | Timestamp::CreateTime(-1) | Timestamp::LogAppendTime(-1) => {
                None
            }
            Timestamp::CreateTime(t) | Timestamp::LogAppendTime(t) => Some(t),
        }
    }

    /// Creates a new `Timestamp::CreateTime` representing the current time.
    pub fn now() -> Timestamp {
        Timestamp::from(SystemTime::now())
    }
}

impl From<i64> for Timestamp {
    fn from(system_time: i64) -> Timestamp {
        Timestamp::CreateTime(system_time)
    }
}

impl From<SystemTime> for Timestamp {
    fn from(system_time: SystemTime) -> Timestamp {
        Timestamp::CreateTime(millis_to_epoch(system_time))
    }
}

// Use TryFrom when stable
//impl From<Timestamp> for i64 {
//    fn from(timestamp: Timestamp) -> i64 {
//        timestamp.to_millis().unwrap()
//    }
//}

/// A generic representation of Kafka message headers.
///
/// This trait represents readable message headers. Headers are key-value pairs
/// that can be sent alongside every message. Only read-only methods are
/// provided by this trait, as the underlying storage might not allow
/// modification.
pub trait Headers {
    /// Returns the number of contained headers.
    fn count(&self) -> usize;

    /// Gets the specified header, where the first header corresponds to index
    /// 0. If the index is out of bounds, returns `None`.
    fn get(&self, idx: usize) -> Option<(&str, &[u8])>;

    /// Like [`Headers::get`], but the value of the header will be converted to
    /// the specified type. If the conversion fails, returns an error.
    fn get_as<V: FromBytes + ?Sized>(&self, idx: usize) -> Option<(&str, Result<&V, V::Error>)> {
        self.get(idx)
            .map(|(name, value)| (name, V::from_bytes(value)))
    }
}

/// A generic representation of a Kafka message.
///
/// Only read-only methods are provided by this trait, as the underlying storage
/// might not allow modification.
pub trait Message {
    /// The type of headers that this message contains.
    type Headers: Headers;

    /// Returns the key of the message, or `None` if there is no key.
    fn key(&self) -> Option<&[u8]>;

    /// Returns the payload of the message, or `None` if there is no payload.
    fn payload(&self) -> Option<&[u8]>;

    /// Returns a mutable reference to the payload of the message, or `None` if
    /// there is no payload.
    ///
    ///
    /// # Safety
    ///
    /// librdkafka does not formally guarantee that modifying the payload is
    /// safe. Calling this method may therefore result in undefined behavior.
    unsafe fn payload_mut(&mut self) -> Option<&mut [u8]>;

    /// Returns the source topic of the message.
    fn topic(&self) -> &str;

    /// Returns the partition number where the message is stored.
    fn partition(&self) -> i32;

    /// Returns the offset of the message within the partition.
    fn offset(&self) -> i64;

    /// Returns the message timestamp.
    fn timestamp(&self) -> Timestamp;

    /// Converts the raw bytes of the payload to a reference of the specified
    /// type, that points to the same data inside the message and without
    /// performing any memory allocation.
    fn payload_view<P: ?Sized + FromBytes>(&self) -> Option<Result<&P, P::Error>> {
        self.payload().map(P::from_bytes)
    }

    /// Converts the raw bytes of the key to a reference of the specified type,
    /// that points to the same data inside the message and without performing
    /// any memory allocation.
    fn key_view<K: ?Sized + FromBytes>(&self) -> Option<Result<&K, K::Error>> {
        self.key().map(K::from_bytes)
    }

    /// Returns the headers of the message, or `None` if there are no headers.
    fn headers(&self) -> Option<&Self::Headers>;
}

/// A zero-copy collection of Kafka message headers.
///
/// Provides a read-only access to headers owned by a Kafka consumer or producer
/// or by an [`OwnedHeaders`] struct.
pub struct BorrowedHeaders;

impl BorrowedHeaders {
    unsafe fn from_native_ptr<T>(
        _owner: &T,
        headers_ptr: *mut rdsys::rd_kafka_headers_t,
    ) -> &BorrowedHeaders {
        &*(headers_ptr as *mut BorrowedHeaders)
    }

    fn as_native_ptr(&self) -> *const RDKafkaHeaders {
        self as *const BorrowedHeaders as *const RDKafkaHeaders
    }

    /// Clones the content of `BorrowedHeaders` and returns an [`OwnedHeaders`]
    /// that can outlive the consumer.
    ///
    /// This operation requires memory allocation and can be expensive.
    pub fn detach(&self) -> OwnedHeaders {
        OwnedHeaders {
            ptr: unsafe {
                NativePtr::from_ptr(rdsys::rd_kafka_headers_copy(self.as_native_ptr())).unwrap()
            },
        }
    }
}

impl Headers for BorrowedHeaders {
    fn count(&self) -> usize {
        unsafe { rdsys::rd_kafka_header_cnt(self.as_native_ptr()) }
    }

    fn get(&self, idx: usize) -> Option<(&str, &[u8])> {
        let mut value_ptr = ptr::null();
        let mut name_ptr = ptr::null();
        let mut value_size = 0;
        let err = unsafe {
            rdsys::rd_kafka_header_get_all(
                self.as_native_ptr(),
                idx,
                &mut name_ptr,
                &mut value_ptr,
                &mut value_size,
            )
        };
        if err.is_error() {
            None
        } else {
            unsafe {
                Some((
                    CStr::from_ptr(name_ptr).to_str().unwrap(),
                    util::ptr_to_slice(value_ptr, value_size),
                ))
            }
        }
    }
}

/// A zero-copy Kafka message.
///
/// Provides a read-only access to headers owned by a Kafka consumer or producer
/// or by an [`OwnedMessage`] struct.
///
/// ## Consumers
///
/// `BorrowedMessage`s coming from consumers are removed from the consumer
/// buffer once they are dropped. Holding references to too many messages will
/// cause the memory of the consumer to fill up and the consumer to block until
/// some of the `BorrowedMessage`s are dropped.
///
/// ## Conversion to owned
///
/// To transform a `BorrowedMessage` into a [`OwnedMessage`], use the
/// [`detach`](BorrowedMessage::detach) method.
pub struct BorrowedMessage<'a> {
    ptr: NativePtr<RDKafkaMessage>,
    _owner: PhantomData<&'a u8>,
}

unsafe impl KafkaDrop for RDKafkaMessage {
    const TYPE: &'static str = "message";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_message_destroy;
}

impl<'a> fmt::Debug for BorrowedMessage<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Message {{ ptr: {:?} }}", self.ptr())
    }
}

impl<'a> BorrowedMessage<'a> {
    /// Creates a new `BorrowedMessage` that wraps the native Kafka message
    /// pointer returned by a consumer. The lifetime of the message will be
    /// bound to the lifetime of the consumer passed as parameter. This method
    /// should only be used with messages coming from consumers. If the message
    /// contains an error, only the error is returned and the message structure
    /// is freed.
    pub(crate) unsafe fn from_consumer<C>(
        ptr: NativePtr<RDKafkaMessage>,
        _consumer: &'a C,
    ) -> KafkaResult<BorrowedMessage<'a>> {
        if ptr.err.is_error() {
            let err = match ptr.err {
                rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    KafkaError::PartitionEOF((*ptr).partition)
                }
                e => KafkaError::MessageConsumption(e.into()),
            };
            Err(err)
        } else {
            Ok(BorrowedMessage {
                ptr,
                _owner: PhantomData,
            })
        }
    }

    /// Creates a new `BorrowedMessage` that wraps the native Kafka message
    /// pointer returned by the delivery callback of a producer. The lifetime of
    /// the message will be bound to the lifetime of the reference passed as
    /// parameter. This method should only be used with messages coming from the
    /// delivery callback. The message will not be freed in any circumstance.
    pub(crate) unsafe fn from_dr_callback<O>(
        ptr: *mut RDKafkaMessage,
        _owner: &'a O,
    ) -> DeliveryResult<'a> {
        let borrowed_message = BorrowedMessage {
            ptr: NativePtr::from_ptr(ptr).unwrap(),
            _owner: PhantomData,
        };
        if (*ptr).err.is_error() {
            Err((
                KafkaError::MessageProduction((*ptr).err.into()),
                borrowed_message,
            ))
        } else {
            Ok(borrowed_message)
        }
    }

    /// Returns a pointer to the [`RDKafkaMessage`].
    pub fn ptr(&self) -> *mut RDKafkaMessage {
        self.ptr.ptr()
    }

    /// Returns a pointer to the message's [`RDKafkaTopic`]
    pub fn topic_ptr(&self) -> *mut RDKafkaTopic {
        self.ptr.rkt
    }

    /// Returns the length of the key field of the message.
    pub fn key_len(&self) -> usize {
        self.ptr.key_len
    }

    /// Returns the length of the payload field of the message.
    pub fn payload_len(&self) -> usize {
        self.ptr.len
    }

    /// Clones the content of the `BorrowedMessage` and returns an
    /// [`OwnedMessage`] that can outlive the consumer.
    ///
    /// This operation requires memory allocation and can be expensive.
    pub fn detach(&self) -> OwnedMessage {
        OwnedMessage {
            key: self.key().map(|k| k.to_vec()),
            payload: self.payload().map(|p| p.to_vec()),
            topic: self.topic().to_owned(),
            timestamp: self.timestamp(),
            partition: self.partition(),
            offset: self.offset(),
            headers: self.headers().map(BorrowedHeaders::detach),
        }
    }
}

impl<'a> Message for BorrowedMessage<'a> {
    type Headers = BorrowedHeaders;

    fn key(&self) -> Option<&[u8]> {
        unsafe { util::ptr_to_opt_slice((*self.ptr).key, (*self.ptr).key_len) }
    }

    fn payload(&self) -> Option<&[u8]> {
        unsafe { util::ptr_to_opt_slice((*self.ptr).payload, (*self.ptr).len) }
    }

    unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
        util::ptr_to_opt_mut_slice((*self.ptr).payload, (*self.ptr).len)
    }

    fn topic(&self) -> &str {
        unsafe {
            CStr::from_ptr(rdsys::rd_kafka_topic_name((*self.ptr).rkt))
                .to_str()
                .expect("Topic name is not valid UTF-8")
        }
    }

    fn partition(&self) -> i32 {
        self.ptr.partition
    }

    fn offset(&self) -> i64 {
        self.ptr.offset
    }

    fn timestamp(&self) -> Timestamp {
        let mut timestamp_type = rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let timestamp =
            unsafe { rdsys::rd_kafka_message_timestamp(self.ptr.ptr(), &mut timestamp_type) };
        if timestamp == -1 {
            Timestamp::NotAvailable
        } else {
            match timestamp_type {
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => {
                    Timestamp::NotAvailable
                }
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => {
                    Timestamp::CreateTime(timestamp)
                }
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                    Timestamp::LogAppendTime(timestamp)
                }
            }
        }
    }

    fn headers(&self) -> Option<&BorrowedHeaders> {
        let mut native_headers_ptr = ptr::null_mut();
        unsafe {
            let err = rdsys::rd_kafka_message_headers(self.ptr.ptr(), &mut native_headers_ptr);
            match err.into() {
                RDKafkaErrorCode::NoError => {
                    Some(BorrowedHeaders::from_native_ptr(self, native_headers_ptr))
                }
                RDKafkaErrorCode::NoEnt => None,
                _ => None,
            }
        }
    }
}

unsafe impl<'a> Send for BorrowedMessage<'a> {}
unsafe impl<'a> Sync for BorrowedMessage<'a> {}

//
// ********** OWNED MESSAGE **********
//

/// A collection of Kafka message headers that owns its backing data.
///
/// Kafka supports associating an array of key-value pairs to every message,
/// called message headers. The `OwnedHeaders` can be used to create the desired
/// headers and to pass them to the producer. See also [`BorrowedHeaders`].
#[derive(Debug)]
pub struct OwnedHeaders {
    ptr: NativePtr<RDKafkaHeaders>,
}

unsafe impl KafkaDrop for RDKafkaHeaders {
    const TYPE: &'static str = "headers";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_headers_destroy;
}

unsafe impl Send for OwnedHeaders {}
unsafe impl Sync for OwnedHeaders {}

impl OwnedHeaders {
    /// Creates a new `OwnedHeaders` struct with initial capacity 5.
    pub fn new() -> OwnedHeaders {
        OwnedHeaders::new_with_capacity(5)
    }

    /// Creates a new `OwnedHeaders` struct with the desired initial capacity.
    /// The structure is automatically resized as more headers are added.
    pub fn new_with_capacity(initial_capacity: usize) -> OwnedHeaders {
        OwnedHeaders {
            ptr: unsafe {
                NativePtr::from_ptr(rdsys::rd_kafka_headers_new(initial_capacity)).unwrap()
            },
        }
    }

    /// Adds a new header.
    pub fn add<V: ToBytes + ?Sized>(self, name: &str, value: &V) -> OwnedHeaders {
        let name_cstring = CString::new(name.to_owned()).unwrap();
        let value_bytes = value.to_bytes();
        let err = unsafe {
            rdsys::rd_kafka_header_add(
                self.ptr(),
                name_cstring.as_ptr(),
                name_cstring.as_bytes().len() as isize,
                value_bytes.as_ptr() as *mut c_void,
                value_bytes.len() as isize,
            )
        };
        // OwnedHeaders should always represent writable instances of RDKafkaHeaders
        assert!(!err.is_error());
        self
    }

    pub(crate) fn ptr(&self) -> *mut RDKafkaHeaders {
        self.ptr.ptr()
    }

    /// Generates a read-only [`BorrowedHeaders`] reference.
    pub fn as_borrowed(&self) -> &BorrowedHeaders {
        unsafe { &*(self.ptr() as *mut RDKafkaHeaders as *mut BorrowedHeaders) }
    }
}

impl Default for OwnedHeaders {
    fn default() -> OwnedHeaders {
        OwnedHeaders::new()
    }
}

impl Headers for OwnedHeaders {
    fn count(&self) -> usize {
        unsafe { rdsys::rd_kafka_header_cnt(self.ptr()) }
    }

    fn get(&self, idx: usize) -> Option<(&str, &[u8])> {
        self.as_borrowed().get(idx)
    }
}

impl Clone for OwnedHeaders {
    fn clone(&self) -> Self {
        OwnedHeaders {
            ptr: unsafe { NativePtr::from_ptr(rdsys::rd_kafka_headers_copy(self.ptr())).unwrap() },
        }
    }
}

/// A Kafka message that owns its backing data.
///
/// An `OwnedMessage` can be created from a [`BorrowedMessage`] using the
/// [`BorrowedMessage::detach`] method. `OwnedMessage`s don't hold any reference
/// to the consumer and don't use any memory inside the consumer buffer.
#[derive(Debug, Clone)]
pub struct OwnedMessage {
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    topic: String,
    timestamp: Timestamp,
    partition: i32,
    offset: i64,
    headers: Option<OwnedHeaders>,
}

impl OwnedMessage {
    /// Creates a new message with the specified content.
    ///
    /// This function is mainly useful in tests of `rust-rdkafka` itself.
    pub fn new(
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        topic: String,
        timestamp: Timestamp,
        partition: i32,
        offset: i64,
        headers: Option<OwnedHeaders>,
    ) -> OwnedMessage {
        OwnedMessage {
            payload,
            key,
            topic,
            timestamp,
            partition,
            offset,
            headers,
        }
    }
}

impl Message for OwnedMessage {
    type Headers = OwnedHeaders;

    fn key(&self) -> Option<&[u8]> {
        match self.key {
            Some(ref k) => Some(k.as_slice()),
            None => None,
        }
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
        self.payload.as_deref_mut()
    }

    fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn headers(&self) -> Option<&OwnedHeaders> {
        self.headers.as_ref()
    }
}

/// The result of a message production.
///
/// If message production is successful `DeliveryResult` will contain the sent
/// message, which can be used to find which partition and offset the message
/// was sent to. If message production is not successful, the `DeliveryResult`
/// will contain an error and the message that failed to be sent. The partition
/// and offset, in this case, will default to -1 and 0 respectively.
///
/// ## Lifetimes
///
/// In both success or failure scenarios, the payload of the message resides in
/// the buffer of the producer and will be automatically removed once the
/// `delivery` callback finishes.
pub type DeliveryResult<'a> = Result<BorrowedMessage<'a>, (KafkaError, BorrowedMessage<'a>)>;

/// A cheap conversion from a byte slice to typed data.
///
/// Given a reference to a byte slice, returns a different view of the same
/// data. No allocation is performed, however the underlying data might be
/// checked for correctness (for example when converting to `str`).
///
/// See also the [`ToBytes`] trait.
pub trait FromBytes {
    /// The error type that will be returned if the conversion fails.
    type Error;
    /// Tries to convert the provided byte slice into a different type.
    fn from_bytes(_: &[u8]) -> Result<&Self, Self::Error>;
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

/// A cheap conversion from typed data to a byte slice.
///
/// Given some data, returns the byte representation of that data.
/// No copy of the data should be performed.
///
/// See also the [`FromBytes`] trait.
pub trait ToBytes {
    /// Converts the provided data to bytes.
    fn to_bytes(&self) -> &[u8];
}

impl ToBytes for [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a, T: ToBytes> ToBytes for &'a T {
    fn to_bytes(&self) -> &[u8] {
        (*self).to_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}

// Implement to_bytes for arrays - https://github.com/rust-lang/rfcs/issues/1038
macro_rules! array_impls {
    ($($N:expr)+) => {
        $(
            impl ToBytes for [u8; $N] {
                fn to_bytes(&self) -> &[u8] { self }
            }
         )+
    }
}

array_impls! {
     0  1  2  3  4  5  6  7  8  9
    10 11 12 13 14 15 16 17 18 19
    20 21 22 23 24 25 26 27 28 29
    30 31 32
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn test_timestamp_creation() {
        let now = SystemTime::now();
        let t1 = Timestamp::now();
        let t2 = Timestamp::from(now);
        let expected = Timestamp::CreateTime(util::millis_to_epoch(now));

        assert_eq!(t2, expected);
        assert!(t1.to_millis().unwrap() - t2.to_millis().unwrap() < 10);
    }

    #[test]
    fn test_timestamp_conversion() {
        assert_eq!(Timestamp::CreateTime(100).to_millis(), Some(100));
        assert_eq!(Timestamp::LogAppendTime(100).to_millis(), Some(100));
        assert_eq!(Timestamp::CreateTime(-1).to_millis(), None);
        assert_eq!(Timestamp::LogAppendTime(-1).to_millis(), None);
        assert_eq!(Timestamp::NotAvailable.to_millis(), None);
        let t: Timestamp = 100.into();
        assert_eq!(t, Timestamp::CreateTime(100));
    }

    #[test]
    fn test_headers() {
        let owned = OwnedHeaders::new()
            .add("key1", "value1")
            .add("key2", "value2");
        assert_eq!(
            owned.get(0),
            Some(("key1", &[118, 97, 108, 117, 101, 49][..]))
        );
        assert_eq!(owned.get_as::<str>(1), Some(("key2", Ok("value2"))));
    }
}
