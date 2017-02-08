//! Error manipulations.
extern crate libc;
extern crate rdkafka_sys as rdkafka;
extern crate std;

use self::rdkafka::types::*;

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
    ClientConfig((RDKafkaConfRes, String, String, String)),
    ClientCreation(String),
    ConsumerCreation(String),
    MessageConsumption(RDKafkaRespErr),
    MessageProduction(RDKafkaRespErr),
    ConsumerCommit(RDKafkaRespErr),
    MetadataFetch(RDKafkaRespErr),
    Nul(std::ffi::NulError),
    Subscription(String),
    TopicConfig((RDKafkaConfRes, String, String, String)),
    TopicCreation(String),
    PartitionEOF(i32),
}

impl std::fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            KafkaError::ClientConfig(ref err) => write!(f, "KafkaError (Client config error: {} {} {})", err.1, err.2, err.3),
            KafkaError::ClientCreation(ref err) => write!(f, "KafkaError (Client creation error: {})", err),
            KafkaError::ConsumerCreation(ref err) => write!(f, "KafkaError (Consumer creation error: {})", err),
            KafkaError::MessageConsumption(err) => write!(f, "KafkaError (Message consumption error: {})", resp_err_description(err)),
            KafkaError::MessageProduction(err) => write!(f, "KafkaError (Message production error: {})", resp_err_description(err)),
            KafkaError::ConsumerCommit(err) => write!(f, "KafkaError (Consumer commit error: {})", resp_err_description(err)),
            KafkaError::MetadataFetch(err) => write!(f, "KafkaError (Metadata fetch error: {})", resp_err_description(err)),
            KafkaError::Nul(_) => write!(f, "KafkaError (FFI nul error)"),
            KafkaError::Subscription(ref err) => write!(f, "KafkaError (Subscription error: {})", err),
            KafkaError::TopicConfig(ref err) => write!(f, "KafkaError (Topic config error: {} {} {})", err.1, err.2, err.3),
            KafkaError::TopicCreation(ref err) => write!(f, "KafkaError (Topic creation error: {})", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n)
        }
    }
}

impl std::fmt::Display for KafkaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            KafkaError::ClientConfig(ref err) => write!(f, "Client config error: {} {} {}", err.1, err.2, err.3),
            KafkaError::ClientCreation(ref err) => write!(f, "Client creation error: {}", err),
            KafkaError::ConsumerCreation(ref err) => write!(f, "Consumer creation error: {}", err),
            KafkaError::MessageConsumption(err) => write!(f, "Message consumption error: {}", resp_err_description(err)),
            KafkaError::MessageProduction(err) => write!(f, "Message production error: {}", resp_err_description(err)),
            KafkaError::ConsumerCommit(err) => write!(f, "Consumer commit error: {}", resp_err_description(err)),
            KafkaError::MetadataFetch(err) => write!(f, "Meta data fetch error: {}", resp_err_description(err)),
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::Subscription(ref err) => write!(f, "Subscription error: {}", err),
            KafkaError::TopicConfig(ref err) => write!(f, "Topic config error: {} {} {}", err.1, err.2, err.3),
            KafkaError::TopicCreation(ref err) => write!(f, "Topic creation error: {}", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
        }
    }
}

impl std::error::Error for KafkaError {
    fn description(&self) -> &str {
        match *self {
            KafkaError::ClientConfig(_) => "Client config error",
            KafkaError::ClientCreation(_) => "Client creation error",
            KafkaError::ConsumerCreation(_) => "Consumer creation error",
            KafkaError::MessageConsumption(_) => "Message consumption error",
            KafkaError::MessageProduction(_) => "Message production error",
            KafkaError::ConsumerCommit(_) => "Consumer commit error",
            KafkaError::MetadataFetch(_) => "Meta data fetch error",
            KafkaError::Nul(_) => "FFI nul error",
            KafkaError::Subscription(_) => "Subscription error",
            KafkaError::TopicConfig(_) => "Topic config error",
            KafkaError::TopicCreation(_) => "Topic creation error",
            KafkaError::PartitionEOF(_) => "Partition EOF error"
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            KafkaError::Nul(ref err) => Some(err),
            _ => None
        }
    }
}

impl From<std::ffi::NulError> for KafkaError {
    fn from(err: std::ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}

/// Returns a string containng a description of the error.
pub fn resp_err_description(err: RDKafkaRespErr) -> String {
    unsafe {
        cstr_to_owned(rdkafka::rd_kafka_err2str(err))
    }
}
