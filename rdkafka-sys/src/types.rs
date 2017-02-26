//! This module contains type aliases for types defined in the auto-generated bindings.
use bindings;

// TYPES

/// Native rdkafka client
pub type RDKafka = bindings::rd_kafka_t;

/// Native rdkafka configuration
pub type RDKafkaConf = bindings::rd_kafka_conf_t;

/// Native rdkafka message
pub type RDKafkaMessage = bindings::rd_kafka_message_t;

/// Native rdkafka topic
pub type RDKafkaTopic = bindings::rd_kafka_topic_t;

/// Native rdkafka topic configuration
pub type RDKafkaTopicConf = bindings::rd_kafka_topic_conf_t;

/// Native rdkafka topic partition list
pub type RDKafkaTopicPartitionList = bindings::rd_kafka_topic_partition_list_t;

/// Native rdkafka metadata container
pub type RDKafkaMetadata = bindings::rd_kafka_metadata_t;

/// Native rdkafka topic information
pub type RDKafkaMetadataTopic = bindings::rd_kafka_metadata_topic_t;

/// Native rdkafka partition information
pub type RDKafkaMetadataPartition = bindings::rd_kafka_metadata_partition_t;

/// Native rdkafka broker information
pub type RDKafkaMetadataBroker = bindings::rd_kafka_metadata_broker_t;

/// Native rdkafka state
pub type RDKafkaState = bindings::rd_kafka_s;

/// Native rdkafka list of groups
pub type RDKafkaGroupList = bindings::rd_kafka_group_list;

/// Native rdkafka group information
pub type RDKafkaGroupInfo = bindings::rd_kafka_group_info;

/// Native rdkafka group member information
pub type RDKafkaGroupMemberInfo = bindings::rd_kafka_group_member_info;

// ENUMS

/// Client types
pub use bindings::rd_kafka_type_t as RDKafkaType;

/// Configuration result
pub use bindings::rd_kafka_conf_res_t as RDKafkaConfRes;

/// Response error
pub use bindings::rd_kafka_resp_err_t as RDKafkaRespErr;
