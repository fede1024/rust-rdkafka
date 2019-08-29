//! Error manipulations.
use crate::rdsys::types::*;

use std::{error, ffi, fmt};

// Re-export rdkafka error
pub use crate::rdsys::types::RDKafkaError;

/// Kafka result.
pub type KafkaResult<T> = Result<T, KafkaError>;

/// Verify if the value represents an error condition.
pub trait IsError {
    /// Return true if the value represents an error.
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

/// Represents all Kafka errors. Check the underlying `RDKafkaError` to get details.
#[derive(Clone, PartialEq, Eq)]
pub enum KafkaError {
    /// Creation of admin operation failed.
    AdminOpCreation(String),
    /// The admin operation itself failed.
    AdminOp(RDKafkaError),
    /// The client was dropped before the operation completed.
    Canceled,
    /// Invalid client configuration.
    ClientConfig(RDKafkaConfRes, String, String, String),
    /// Client creation failed.
    ClientCreation(String),
    /// Consumer commit failed.
    ConsumerCommit(RDKafkaError),
    /// Global error.
    Global(RDKafkaError),
    /// Group list fetch failed.
    GroupListFetch(RDKafkaError),
    /// Message consumption failed.
    MessageConsumption(RDKafkaError),
    /// Message production error.
    MessageProduction(RDKafkaError),
    /// Metadata fetch error.
    MetadataFetch(RDKafkaError),
    /// No message was received.
    NoMessageReceived,
    /// Unexpected null pointer
    Nul(ffi::NulError),
    /// Offset fetch failed.
    OffsetFetch(RDKafkaError),
    /// End of partition reached.
    PartitionEOF(i32),
    /// Setting partition offset failed.
    SetPartitionOffset(RDKafkaError),
    /// Offset store failed.
    StoreOffset(RDKafkaError),
    /// Subscription creation failed.
    Subscription(String),
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::AdminOp(err) => write!(f, "KafkaError (Admin operation error: {})", err),
            KafkaError::AdminOpCreation(ref err) => write!(f, "KafkaError (Admin operation creation error: {})", err),
            KafkaError::Canceled => write!(f, "KafkaError (Client dropped)"),
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => {
                write!(f, "KafkaError (Client config error: {} {} {})", desc, key, value)
            }
            KafkaError::ClientCreation(ref err) => write!(f, "KafkaError (Client creation error: {})", err),
            KafkaError::ConsumerCommit(err) => write!(f, "KafkaError (Consumer commit error: {})", err),
            KafkaError::Global(err) => write!(f, "KafkaError (Global error: {})", err),
            KafkaError::GroupListFetch(err) => write!(f, "KafkaError (Group list fetch error: {})", err),
            KafkaError::MessageConsumption(err) => write!(f, "KafkaError (Message consumption error: {})", err),
            KafkaError::MessageProduction(err) => write!(f, "KafkaError (Message production error: {})", err),
            KafkaError::MetadataFetch(err) => write!(f, "KafkaError (Metadata fetch error: {})", err),
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "FFI null error"),
            KafkaError::OffsetFetch(err) => write!(f, "KafkaError (Offset fetch error: {})", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n),
            KafkaError::SetPartitionOffset(err) => write!(f, "KafkaError (Set partition offset error: {})", err),
            KafkaError::StoreOffset(err) => write!(f, "KafkaError (Store offset error: {})", err),
            KafkaError::Subscription(ref err) => write!(f, "KafkaError (Subscription error: {})", err),
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::AdminOp(err) => write!(f, "Admin operation error: {}", err),
            KafkaError::AdminOpCreation(ref err) => write!(f, "Admin operation creation error: {}", err),
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
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::OffsetFetch(err) => write!(f, "Offset fetch error: {}", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
            KafkaError::SetPartitionOffset(err) => write!(f, "Set partition offset error: {}", err),
            KafkaError::StoreOffset(err) => write!(f, "Store offset error: {}", err),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
        }
    }
}

impl error::Error for KafkaError {
    fn description(&self) -> &str {
        match *self {
            KafkaError::AdminOp(_) => "Admin operation error",
            KafkaError::AdminOpCreation(_) => "Admin operation creation error",
            KafkaError::Canceled => "Client dropped",
            KafkaError::ClientConfig(_, _, _, _) => "Client config error",
            KafkaError::ClientCreation(_) => "Client creation error",
            KafkaError::ConsumerCommit(_) => "Consumer commit error",
            KafkaError::Global(_) => "Global error",
            KafkaError::GroupListFetch(_) => "Group list fetch error",
            KafkaError::MessageConsumption(_) => "Message consumption error",
            KafkaError::MessageProduction(_) => "Message production error",
            KafkaError::MetadataFetch(_) => "Meta data fetch error",
            KafkaError::NoMessageReceived => "No message received within the given poll interval",
            KafkaError::Nul(_) => "FFI nul error",
            KafkaError::OffsetFetch(_) => "Offset fetch error",
            KafkaError::PartitionEOF(_) => "Partition EOF error",
            KafkaError::SetPartitionOffset(_) => "Set partition offset error",
            KafkaError::StoreOffset(_) => "Store offset error",
            KafkaError::Subscription(_) => "Subscription error",
        }
    }

    #[allow(clippy::match_same_arms)]
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            KafkaError::AdminOp(_) => None,
            KafkaError::AdminOpCreation(_) => None,
            KafkaError::Canceled => None,
            KafkaError::ClientConfig(_, _, _, _) => None,
            KafkaError::ClientCreation(_) => None,
            KafkaError::ConsumerCommit(ref err) => Some(err),
            KafkaError::Global(ref err) => Some(err),
            KafkaError::GroupListFetch(ref err) => Some(err),
            KafkaError::MessageConsumption(ref err) => Some(err),
            KafkaError::MessageProduction(ref err) => Some(err),
            KafkaError::MetadataFetch(ref err) => Some(err),
            KafkaError::NoMessageReceived => None,
            KafkaError::Nul(_) => None,
            KafkaError::OffsetFetch(ref err) => Some(err),
            KafkaError::PartitionEOF(_) => None,
            KafkaError::SetPartitionOffset(ref err) => Some(err),
            KafkaError::StoreOffset(ref err) => Some(err),
            KafkaError::Subscription(_) => None,
        }
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}
