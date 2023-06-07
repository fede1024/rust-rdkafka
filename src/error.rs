//! Error manipulations.

use std::error::Error;
use std::ffi::{self, CStr};
use std::fmt;
use std::ptr;
use std::sync::Arc;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::util::{KafkaDrop, NativePtr};

// Re-export rdkafka error code
pub use rdsys::types::RDKafkaErrorCode;

/// Kafka result.
pub type KafkaResult<T> = Result<T, KafkaError>;

/// Verify if the value represents an error condition.
///
/// Some librdkafka codes are informational, rather than true errors.
pub trait IsError {
    /// Reports whether the value represents an error.
    fn is_error(&self) -> bool;
}

impl IsError for RDKafkaRespErr {
    fn is_error(&self) -> bool {
        *self != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR
    }
}

impl IsError for RDKafkaConfRes {
    fn is_error(&self) -> bool {
        *self != RDKafkaConfRes::RD_KAFKA_CONF_OK
    }
}

impl IsError for RDKafkaError {
    fn is_error(&self) -> bool {
        self.0.is_some()
    }
}

/// Native rdkafka error.
#[derive(Clone)]
pub struct RDKafkaError(Option<Arc<NativePtr<rdsys::rd_kafka_error_t>>>);

unsafe impl KafkaDrop for rdsys::rd_kafka_error_t {
    const TYPE: &'static str = "error";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_error_destroy;
}

unsafe impl Send for RDKafkaError {}
unsafe impl Sync for RDKafkaError {}

impl RDKafkaError {
    pub(crate) unsafe fn from_ptr(ptr: *mut rdsys::rd_kafka_error_t) -> RDKafkaError {
        RDKafkaError(NativePtr::from_ptr(ptr).map(Arc::new))
    }

    fn ptr(&self) -> *const rdsys::rd_kafka_error_t {
        match &self.0 {
            None => ptr::null(),
            Some(p) => p.ptr(),
        }
    }

    /// Returns the error code or [`RDKafkaErrorCode::NoError`] if the error is
    /// null.
    pub fn code(&self) -> RDKafkaErrorCode {
        unsafe { rdsys::rd_kafka_error_code(self.ptr()).into() }
    }

    /// Returns the error code name, e.g., "ERR_UNKNOWN_MEMBER_ID" or an empty
    /// string if the error is null.
    pub fn name(&self) -> String {
        let cstr = unsafe { rdsys::rd_kafka_error_name(self.ptr()) };
        unsafe { CStr::from_ptr(cstr).to_string_lossy().into_owned() }
    }

    /// Returns a human readable error string or an empty string if the error is
    /// null.
    pub fn string(&self) -> String {
        let cstr = unsafe { rdsys::rd_kafka_error_string(self.ptr()) };
        unsafe { CStr::from_ptr(cstr).to_string_lossy().into_owned() }
    }

    /// Reports whether the error is a fatal error.
    ///
    /// A fatal error indicates that the client instance is no longer usable.
    pub fn is_fatal(&self) -> bool {
        unsafe { rdsys::rd_kafka_error_is_fatal(self.ptr()) != 0 }
    }

    /// Reports whether the operation that encountered the error can be retried.
    pub fn is_retriable(&self) -> bool {
        unsafe { rdsys::rd_kafka_error_is_retriable(self.ptr()) != 0 }
    }

    /// Reports whether the error is an abortable transaction error.
    pub fn txn_requires_abort(&self) -> bool {
        unsafe { rdsys::rd_kafka_error_txn_requires_abort(self.ptr()) != 0 }
    }
}

impl PartialEq for RDKafkaError {
    fn eq(&self, other: &RDKafkaError) -> bool {
        self.code() == other.code()
    }
}

impl Eq for RDKafkaError {}

impl fmt::Debug for RDKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RDKafkaError({})", self)
    }
}

impl fmt::Display for RDKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.string())
    }
}

impl Error for RDKafkaError {}

// TODO: consider using macro

/// Represents all possible Kafka errors.
///
/// If applicable, check the underlying [`RDKafkaErrorCode`] to get details.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaError {
    /// Creation of admin operation failed.
    AdminOpCreation(String),
    /// The admin operation itself failed.
    AdminOp(RDKafkaErrorCode),
    /// The client was dropped before the operation completed.
    Canceled,
    /// Invalid client configuration.
    ClientConfig(RDKafkaConfRes, String, String, String),
    /// Client creation failed.
    ClientCreation(String),
    /// Consumer commit failed.
    ConsumerCommit(RDKafkaErrorCode),
    /// Flushing failed
    Flush(RDKafkaErrorCode),
    /// Global error.
    Global(RDKafkaErrorCode),
    /// Group list fetch failed.
    GroupListFetch(RDKafkaErrorCode),
    /// Message consumption failed.
    MessageConsumption(RDKafkaErrorCode),
    /// Message production error.
    MessageProduction(RDKafkaErrorCode),
    /// Metadata fetch error.
    MetadataFetch(RDKafkaErrorCode),
    /// No message was received.
    NoMessageReceived,
    /// Unexpected null pointer
    Nul(ffi::NulError),
    /// Offset fetch failed.
    OffsetFetch(RDKafkaErrorCode),
    /// End of partition reached.
    PartitionEOF(i32),
    /// Pause/Resume failed.
    PauseResume(String),
    /// Rebalance failed.
    Rebalance(RDKafkaErrorCode),
    /// Seeking a partition failed.
    Seek(String),
    /// Setting partition offset failed.
    SetPartitionOffset(RDKafkaErrorCode),
    /// Offset store failed.
    StoreOffset(RDKafkaErrorCode),
    /// Subscription creation failed.
    Subscription(String),
    /// Transaction error.
    Transaction(RDKafkaError),
    /// Mock Cluster error
    MockCluster(RDKafkaErrorCode),
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::AdminOp(err) => write!(f, "KafkaError (Admin operation error: {})", err),
            KafkaError::AdminOpCreation(ref err) => {
                write!(f, "KafkaError (Admin operation creation error: {})", err)
            }
            KafkaError::Canceled => write!(f, "KafkaError (Client dropped)"),
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => write!(
                f,
                "KafkaError (Client config error: {} {} {})",
                desc, key, value
            ),
            KafkaError::ClientCreation(ref err) => {
                write!(f, "KafkaError (Client creation error: {})", err)
            }
            KafkaError::ConsumerCommit(err) => {
                write!(f, "KafkaError (Consumer commit error: {})", err)
            }
            KafkaError::Flush(err) => write!(f, "KafkaError (Flush error: {})", err),
            KafkaError::Global(err) => write!(f, "KafkaError (Global error: {})", err),
            KafkaError::GroupListFetch(err) => {
                write!(f, "KafkaError (Group list fetch error: {})", err)
            }
            KafkaError::MessageConsumption(err) => {
                write!(f, "KafkaError (Message consumption error: {})", err)
            }
            KafkaError::MessageProduction(err) => {
                write!(f, "KafkaError (Message production error: {})", err)
            }
            KafkaError::MetadataFetch(err) => {
                write!(f, "KafkaError (Metadata fetch error: {})", err)
            }
            KafkaError::NoMessageReceived => {
                write!(f, "No message received within the given poll interval")
            }
            KafkaError::Nul(_) => write!(f, "FFI null error"),
            KafkaError::OffsetFetch(err) => write!(f, "KafkaError (Offset fetch error: {})", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n),
            KafkaError::PauseResume(ref err) => {
                write!(f, "KafkaError (Pause/resume error: {})", err)
            }
            KafkaError::Rebalance(ref err) => write!(f, "KafkaError (Rebalance error: {})", err),
            KafkaError::Seek(ref err) => write!(f, "KafkaError (Seek error: {})", err),
            KafkaError::SetPartitionOffset(err) => {
                write!(f, "KafkaError (Set partition offset error: {})", err)
            }
            KafkaError::StoreOffset(err) => write!(f, "KafkaError (Store offset error: {})", err),
            KafkaError::Subscription(ref err) => {
                write!(f, "KafkaError (Subscription error: {})", err)
            }
            KafkaError::Transaction(err) => write!(f, "KafkaError (Transaction error: {})", err),
            KafkaError::MockCluster(err) => write!(f, "KafkaError (Mock cluster error: {})", err),
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::AdminOp(err) => write!(f, "Admin operation error: {}", err),
            KafkaError::AdminOpCreation(ref err) => {
                write!(f, "Admin operation creation error: {}", err)
            }
            KafkaError::Canceled => write!(f, "KafkaError (Client dropped)"),
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => {
                write!(f, "Client config error: {} {} {}", desc, key, value)
            }
            KafkaError::ClientCreation(ref err) => write!(f, "Client creation error: {}", err),
            KafkaError::ConsumerCommit(err) => write!(f, "Consumer commit error: {}", err),
            KafkaError::Flush(err) => write!(f, "Flush error: {}", err),
            KafkaError::Global(err) => write!(f, "Global error: {}", err),
            KafkaError::GroupListFetch(err) => write!(f, "Group list fetch error: {}", err),
            KafkaError::MessageConsumption(err) => write!(f, "Message consumption error: {}", err),
            KafkaError::MessageProduction(err) => write!(f, "Message production error: {}", err),
            KafkaError::MetadataFetch(err) => write!(f, "Meta data fetch error: {}", err),
            KafkaError::NoMessageReceived => {
                write!(f, "No message received within the given poll interval")
            }
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::OffsetFetch(err) => write!(f, "Offset fetch error: {}", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
            KafkaError::PauseResume(ref err) => write!(f, "Pause/resume error: {}", err),
            KafkaError::Rebalance(ref err) => write!(f, "Rebalance error: {}", err),
            KafkaError::Seek(ref err) => write!(f, "Seek error: {}", err),
            KafkaError::SetPartitionOffset(err) => write!(f, "Set partition offset error: {}", err),
            KafkaError::StoreOffset(err) => write!(f, "Store offset error: {}", err),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
            KafkaError::Transaction(err) => write!(f, "Transaction error: {}", err),
            KafkaError::MockCluster(err) => write!(f, "Mock cluster error: {}", err),
        }
    }
}

impl Error for KafkaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KafkaError::AdminOp(_) => None,
            KafkaError::AdminOpCreation(_) => None,
            KafkaError::Canceled => None,
            KafkaError::ClientConfig(..) => None,
            KafkaError::ClientCreation(_) => None,
            KafkaError::ConsumerCommit(err) => Some(err),
            KafkaError::Flush(err) => Some(err),
            KafkaError::Global(err) => Some(err),
            KafkaError::GroupListFetch(err) => Some(err),
            KafkaError::MessageConsumption(err) => Some(err),
            KafkaError::MessageProduction(err) => Some(err),
            KafkaError::MetadataFetch(err) => Some(err),
            KafkaError::NoMessageReceived => None,
            KafkaError::Nul(_) => None,
            KafkaError::OffsetFetch(err) => Some(err),
            KafkaError::PartitionEOF(_) => None,
            KafkaError::PauseResume(_) => None,
            KafkaError::Rebalance(err) => Some(err),
            KafkaError::Seek(_) => None,
            KafkaError::SetPartitionOffset(err) => Some(err),
            KafkaError::StoreOffset(err) => Some(err),
            KafkaError::Subscription(_) => None,
            KafkaError::Transaction(err) => Some(err),
            KafkaError::MockCluster(err) => Some(err),
        }
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}

impl KafkaError {
    /// Returns the [`RDKafkaErrorCode`] underlying this error, if any.
    #[allow(clippy::match_same_arms)]
    pub fn rdkafka_error_code(&self) -> Option<RDKafkaErrorCode> {
        match self {
            KafkaError::AdminOp(_) => None,
            KafkaError::AdminOpCreation(_) => None,
            KafkaError::Canceled => None,
            KafkaError::ClientConfig(..) => None,
            KafkaError::ClientCreation(_) => None,
            KafkaError::ConsumerCommit(err) => Some(*err),
            KafkaError::Flush(err) => Some(*err),
            KafkaError::Global(err) => Some(*err),
            KafkaError::GroupListFetch(err) => Some(*err),
            KafkaError::MessageConsumption(err) => Some(*err),
            KafkaError::MessageProduction(err) => Some(*err),
            KafkaError::MetadataFetch(err) => Some(*err),
            KafkaError::NoMessageReceived => None,
            KafkaError::Nul(_) => None,
            KafkaError::OffsetFetch(err) => Some(*err),
            KafkaError::PartitionEOF(_) => None,
            KafkaError::PauseResume(_) => None,
            KafkaError::Rebalance(err) => Some(*err),
            KafkaError::Seek(_) => None,
            KafkaError::SetPartitionOffset(err) => Some(*err),
            KafkaError::StoreOffset(err) => Some(*err),
            KafkaError::Subscription(_) => None,
            KafkaError::Transaction(err) => Some(err.code()),
            KafkaError::MockCluster(err) => Some(*err),
        }
    }
}
