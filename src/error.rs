//! Error manipulations.

use std::{error, ffi, fmt};

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

// Re-export rdkafka error code
pub use rdsys::types::RDKafkaErrorCode;

/// Kafka result.
pub type KafkaResult<T> = Result<T, KafkaError>;

/// Verify if the value represents an error condition.
///
/// Some librdkafka codes are informational, rather than true errors.
pub trait IsError {
    /// Reports whether the value represents an error.
    fn is_error(self) -> bool;
}

impl IsError for RDKafkaRespErr {
    fn is_error(self) -> bool {
        self as i32 != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as i32
    }
}

impl IsError for RDKafkaConfRes {
    fn is_error(self) -> bool {
        self as i32 != RDKafkaConfRes::RD_KAFKA_CONF_OK as i32
    }
}

// TODO: consider using macro

/// Represents all possible Kafka errors.
///
/// If applicable, check the underlying [`RDKafkaErrorCode`] to get details.
#[derive(Clone, PartialEq, Eq)]
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
    /// Seeking a partition failed.
    Seek(String),
    /// Setting partition offset failed.
    SetPartitionOffset(RDKafkaErrorCode),
    /// Offset store failed.
    StoreOffset(RDKafkaErrorCode),
    /// Subscription creation failed.
    Subscription(String),
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
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
            KafkaError::Seek(ref err) => write!(f, "KafkaError (Seek error: {})", err),
            KafkaError::SetPartitionOffset(err) => {
                write!(f, "KafkaError (Set partition offset error: {})", err)
            }
            KafkaError::StoreOffset(err) => write!(f, "KafkaError (Store offset error: {})", err),
            KafkaError::Subscription(ref err) => {
                write!(f, "KafkaError (Subscription error: {})", err)
            }
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
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
            KafkaError::Seek(ref err) => write!(f, "Seek error: {}", err),
            KafkaError::SetPartitionOffset(err) => write!(f, "Set partition offset error: {}", err),
            KafkaError::StoreOffset(err) => write!(f, "Store offset error: {}", err),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
        }
    }
}

impl error::Error for KafkaError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.rdkafka_error_code()
            .map(|e| e as &(dyn error::Error + 'static))
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}

impl KafkaError {
    /// Returns if an error is `Fatal` and requires reinitialisation.
    /// for details see https://docs.confluent.io/5.5.0/clients/librdkafka/rdkafka_8h.html
    pub fn is_fatal(&self) -> bool {
        match self.rdkafka_error_code() {
            Some(RDKafkaErrorCode::Fatal) => true,
            _ => false,
        }
    }

    /// Returns the [`RDKafkaErrorCode`] underlying this error, if any.
    #[allow(clippy::match_same_arms)]
    pub fn rdkafka_error_code(&self) -> Option<&RDKafkaErrorCode> {
        match self {
            KafkaError::AdminOp(_) => None,
            KafkaError::AdminOpCreation(_) => None,
            KafkaError::Canceled => None,
            KafkaError::ClientConfig(_, _, _, _) => None,
            KafkaError::ClientCreation(_) => None,
            KafkaError::ConsumerCommit(err) => Some(err),
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
            KafkaError::Seek(_) => None,
            KafkaError::SetPartitionOffset(err) => Some(err),
            KafkaError::StoreOffset(err) => Some(err),
            KafkaError::Subscription(_) => None,
        }
    }
}
