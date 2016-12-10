//! Error manipulations.
extern crate libc;
extern crate rdkafka_sys as rdkafka;
extern crate std;

use self::rdkafka::types::*;

/// Kafka result
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

#[derive(Debug)]
/// Represents all Kafka errors.
pub enum KafkaError {
    ClientConfig((RDKafkaConfRes, String, String, String)),
    ClientCreation(String),
    ConsumerCreation(String),
    MessageConsumption(RDKafkaRespErr),
    MessageProduction(RDKafkaRespErr),
    MetadataFetch(RDKafkaRespErr),
    Nul(std::ffi::NulError),
    Subscription(String),
    TopicConfig((RDKafkaConfRes, String, String, String)),
    TopicCreation(String),
    PartitionEof
}

impl From<std::ffi::NulError> for KafkaError {
    fn from(err: std::ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}
