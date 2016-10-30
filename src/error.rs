extern crate libc;
extern crate librdkafka_sys as rdkafka;
extern crate std;

/// Response error
pub type RespError = rdkafka::rd_kafka_resp_err_t;

/// Configuration response
pub type ConfRes = rdkafka::rd_kafka_conf_res_t;

/// Verify if the return code or return value represents an error condition
pub trait IsError {
    /// Return true if the error code or return code represents an error
    fn is_error(self) -> bool;
}

impl IsError for RespError {
    fn is_error(self) -> bool {
        self as i32 != rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32
    }
}

impl IsError for ConfRes {
    fn is_error(self) -> bool {
        self as i32 != rdkafka::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK as i32
    }
}

#[derive(Debug)]
/// Represents all Kafka errors
pub enum KafkaError {
    ConfigError((ConfRes, String, String, String)),
    ConsumerCreationError(String),
    ProducerCreationError(String),
    MessageConsumptionError(RespError),
    MessageProductionError(RespError),
    SubscriptionError(String),
    TopicNameError(String),
    Nul(std::ffi::NulError),
}

impl From<std::ffi::NulError> for KafkaError {
    fn from(err: std::ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}
