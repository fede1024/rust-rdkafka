extern crate libc;
extern crate librdkafka_sys as rdkafka;
extern crate std;

pub fn is_kafka_error(kafka_error_code: rdkafka::rd_kafka_resp_err_t) -> bool {
    kafka_error_code as i32 != rdkafka::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as i32
}

pub fn is_config_error(config_error_code: rdkafka::rd_kafka_conf_res_t) -> bool {
    config_error_code as i32 != rdkafka::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK as i32
}

#[derive(Debug)]
pub enum KafkaError {
    ConfigError((rdkafka::rd_kafka_conf_res_t, String, String, String)),
    ConsumerCreationError(String),
    MessageConsumptionError(rdkafka::rd_kafka_resp_err_t),
    SubscriptionError(String),
    Nul(std::ffi::NulError),
}

impl From<std::ffi::NulError> for KafkaError {
    fn from(err: std::ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}
