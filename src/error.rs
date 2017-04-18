//! Error manipulations.
use rdsys;
use rdsys::types::*;

use std::{error, ffi, fmt};

use util::cstr_to_owned;

/// Kafka result.
pub type KafkaResult<T> = Result<T, KafkaError>;

/// Verify if the value represents an error condition.
pub trait IsError {
    /// Return true if the value represents an error.
    fn is_error(self) -> bool;
}

// TODO: Use type wrapper for RDKafkaRespErr
//impl PartialEq for RDKafkaRespErr {
//    fn eq(&self, other: &RDKafkaRespErr) -> bool {
//        self as i32 == *other as i32
//    }
//}

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

/// Represents all Kafka errors.
pub enum KafkaError {
    ClientConfig(RDKafkaConfRes, String, String, String),
    ClientCreation(String),
    ConsumerCommit(RDKafkaRespErr),
    ConsumerCreation(String),
    GroupListFetch(RDKafkaRespErr),
    MessageConsumption(RDKafkaRespErr),
    MessageProduction(RDKafkaRespErr),
    MetadataFetch(RDKafkaRespErr),
    NoMessageReceived,
    Nul(ffi::NulError),
    PartitionEOF(i32),
    Subscription(String),
    TopicConfig(RDKafkaConfRes, String, String, String),
    TopicCreation(String),
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => write!(f, "KafkaError (Client config error: {} {} {})", desc, key, value),
            KafkaError::ClientCreation(ref err) => write!(f, "KafkaError (Client creation error: {})", err),
            KafkaError::ConsumerCommit(err) => write!(f, "KafkaError (Consumer commit error: {})", resp_err_description(err)),
            KafkaError::ConsumerCreation(ref err) => write!(f, "KafkaError (Consumer creation error: {})", err),
            KafkaError::GroupListFetch(err) => write!(f, "KafkaError (Group list fetch error: {})", resp_err_description(err)),
            KafkaError::MessageConsumption(err) => write!(f, "KafkaError (Message consumption error: {})", resp_err_description(err)),
            KafkaError::MessageProduction(err) => write!(f, "KafkaError (Message production error: {})", resp_err_description(err)),
            KafkaError::MetadataFetch(err) => write!(f, "KafkaError (Metadata fetch error: {})", resp_err_description(err)),
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "KafkaError (FFI nul error)"),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n),
            KafkaError::Subscription(ref err) => write!(f, "KafkaError (Subscription error: {})", err),
            KafkaError::TopicConfig(_, ref desc, ref key, ref value) => write!(f, "KafkaError (Topic config error: {} {} {})", desc, key, value),
            KafkaError::TopicCreation(ref err) => write!(f, "KafkaError (Topic creation error: {})", err),
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KafkaError::ClientConfig(_, ref desc, ref key, ref value) => write!(f, "Client config error: {} {} {}", desc, key, value),
            KafkaError::ClientCreation(ref err) => write!(f, "Client creation error: {}", err),
            KafkaError::ConsumerCommit(err) => write!(f, "Consumer commit error: {}", resp_err_description(err)),
            KafkaError::ConsumerCreation(ref err) => write!(f, "Consumer creation error: {}", err),
            KafkaError::GroupListFetch(err) => write!(f, "Group list fetch error: {}", resp_err_description(err)),
            KafkaError::MessageConsumption(err) => write!(f, "Message consumption error: {}", resp_err_description(err)),
            KafkaError::MessageProduction(err) => write!(f, "Message production error: {}", resp_err_description(err)),
            KafkaError::MetadataFetch(err) => write!(f, "Meta data fetch error: {}", resp_err_description(err)),
            KafkaError::NoMessageReceived => write!(f, "No message received within the given poll interval"),
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
            KafkaError::TopicConfig(_, ref desc, ref key, ref value) => write!(f, "Topic config error: {} {} {}", desc, key, value),
            KafkaError::TopicCreation(ref err) => write!(f, "Topic creation error: {}", err),
        }
    }
}

impl error::Error for KafkaError {
    fn description(&self) -> &str {
        match *self {
            KafkaError::ClientConfig(_, _, _, _) => "Client config error",
            KafkaError::ClientCreation(_) => "Client creation error",
            KafkaError::ConsumerCommit(_) => "Consumer commit error",
            KafkaError::ConsumerCreation(_) => "Consumer creation error",
            KafkaError::GroupListFetch(_) => "Group list fetch error",
            KafkaError::MessageConsumption(_) => "Message consumption error",
            KafkaError::MessageProduction(_) => "Message production error",
            KafkaError::MetadataFetch(_) => "Meta data fetch error",
            KafkaError::NoMessageReceived => "No message received within the given poll interval",
            KafkaError::Nul(_) => "FFI nul error",
            KafkaError::PartitionEOF(_) => "Partition EOF error",
            KafkaError::Subscription(_) => "Subscription error",
            KafkaError::TopicConfig(_, _, _, _) => "Topic config error",
            KafkaError::TopicCreation(_) => "Topic creation error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            KafkaError::Nul(ref err) => Some(err),
            _ => None
        }
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}

/// Returns a string containing a description of the error.
pub fn resp_err_description(err: RDKafkaRespErr) -> String {
    unsafe {
        cstr_to_owned(rdsys::rd_kafka_err2str(err))
    }
}
