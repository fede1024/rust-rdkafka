//! Error manipulations.
use rdsys::types::*;

use std::{error, ffi, fmt};

// Re-export rdkafka error
pub use rdsys::types::RDKafkaError;

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

/// Represents all Kafka errors. Check the underlying `RDKafkaError` to get details.
pub enum KafkaError {
    ClientConfig(RDKafkaConfRes, String, String, String),
    ClientCreation(String),
    ConsumerCommit(RDKafkaError),
    StoreOffset(RDKafkaError),
    ConsumerCreation(String),
    GroupListFetch(RDKafkaError),
    MessageConsumption(RDKafkaError),
    MessageProduction(RDKafkaError),
    MetadataFetch(RDKafkaError),
    NoMessageReceived,
    Nul(ffi::NulError),
    PartitionEOF(i32),
    SetPartitionOffset(RDKafkaError),
    Subscription(String),
    TopicConfig(RDKafkaConfRes, String, String, String),
    TopicCreation(String),
    Global(RDKafkaError),
    FutureCanceled
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => write!(f, "KafkaError (Client config error: {} {} {})", desc, key, value),
            KafkaError::ClientCreation(ref err) => write!(f, "KafkaError (Client creation error: {})", err),
            KafkaError::ConsumerCommit(err) => write!(f, "KafkaError (Consumer commit error: {})", err),
            KafkaError::StoreOffset(err) => write!(f, "KafkaError (Store offset error: {})", err),
            KafkaError::ConsumerCreation(ref err) => write!(f, "KafkaError (Consumer creation error: {})", err),
            KafkaError::GroupListFetch(err) => write!(f, "KafkaError (Group list fetch error: {})", err),
            KafkaError::MessageConsumption(err) => write!(f, "KafkaError (Message consumption error: {})", err),
            KafkaError::MessageProduction(err) => write!(f, "KafkaError (Message production error: {})", err),
            KafkaError::MetadataFetch(err) => write!(f, "KafkaError (Metadata fetch error: {})", err),
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "KafkaError (FFI nul error)"),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n),
            KafkaError::SetPartitionOffset(err) => write!(f, "KafkaError (Set partition offset error: {})", err),
            KafkaError::Subscription(ref err) => write!(f, "KafkaError (Subscription error: {})", err),
            KafkaError::TopicConfig(_, ref desc, ref key, ref value) => write!(f, "KafkaError (Topic config error: {} {} {})", desc, key, value),
            KafkaError::TopicCreation(ref err) => write!(f, "KafkaError (Topic creation error: {})", err),
            KafkaError::Global(err) => write!(f, "KafkaError (Global error: {})", err),
            KafkaError::FutureCanceled => write!(f, "KafkaError (Future canceled)")
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => write!(f, "Client config error: {} {} {}", desc, key, value),
            KafkaError::ClientCreation(ref err) => write!(f, "Client creation error: {}", err),
            KafkaError::ConsumerCommit(err) => write!(f, "Consumer commit error: {}", err),
            KafkaError::StoreOffset(err) => write!(f, "Store offset error: {}", err),
            KafkaError::ConsumerCreation(ref err) => write!(f, "Consumer creation error: {}", err),
            KafkaError::GroupListFetch(err) => write!(f, "Group list fetch error: {}", err),
            KafkaError::MessageConsumption(err) => write!(f, "Message consumption error: {}", err),
            KafkaError::MessageProduction(err) => write!(f, "Message production error: {}", err),
            KafkaError::MetadataFetch(err) => write!(f, "Meta data fetch error: {}", err),
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
            KafkaError::SetPartitionOffset(err) => write!(f, "Set partition offset error: {}", err),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
            KafkaError::TopicConfig(_, ref desc, ref key, ref value) => write!(f, "Topic config error: {} {} {}", desc, key, value),
            KafkaError::TopicCreation(ref err) => write!(f, "Topic creation error: {}", err),
            KafkaError::Global(err) => write!(f, "Global error: {}", err),
            KafkaError::FutureCanceled => write!(f, "Future canceled")
        }
    }
}

impl error::Error for KafkaError {
    fn description(&self) -> &str {
        match *self {
            KafkaError::ClientConfig(_, _, _, _) => "Client config error",
            KafkaError::ClientCreation(_) => "Client creation error",
            KafkaError::ConsumerCommit(_) => "Consumer commit error",
            KafkaError::StoreOffset(_) => "Store offset error",
            KafkaError::ConsumerCreation(_) => "Consumer creation error",
            KafkaError::GroupListFetch(_) => "Group list fetch error",
            KafkaError::MessageConsumption(_) => "Message consumption error",
            KafkaError::MessageProduction(_) => "Message production error",
            KafkaError::MetadataFetch(_) => "Meta data fetch error",
            KafkaError::NoMessageReceived => "No message received within the given poll interval",
            KafkaError::Nul(_) => "FFI nul error",
            KafkaError::PartitionEOF(_) => "Partition EOF error",
            KafkaError::SetPartitionOffset(_) => "Set partition offset error",
            KafkaError::Subscription(_) => "Subscription error",
            KafkaError::TopicConfig(_, _, _, _) => "Topic config error",
            KafkaError::TopicCreation(_) => "Topic creation error",
            KafkaError::Global(_) => "Global error",
            KafkaError::FutureCanceled => "Future canceled"
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            KafkaError::ClientConfig(_, _, _, _) => None,
            KafkaError::ClientCreation(_) => None,
            KafkaError::ConsumerCommit(ref err) => Some(err),
            KafkaError::StoreOffset(ref err) => Some(err),
            KafkaError::ConsumerCreation(_) => None,
            KafkaError::GroupListFetch(ref err) => Some(err),
            KafkaError::MessageConsumption(ref err) => Some(err),
            KafkaError::MessageProduction(ref err) => Some(err),
            KafkaError::MetadataFetch(ref err) => Some(err),
            KafkaError::NoMessageReceived => None,
            KafkaError::Nul(_) => None,
            KafkaError::PartitionEOF(_) => None,
            KafkaError::SetPartitionOffset(ref err) => Some(err),
            KafkaError::Subscription(_) => None,
            KafkaError::TopicConfig(_, _, _, _) => None,
            KafkaError::TopicCreation(_) => None,
            KafkaError::Global(ref err) => Some(err),
            KafkaError::FutureCanceled => None
        }
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}
