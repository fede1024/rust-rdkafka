//! This module contains type aliases for types defined in the auto-generated bindings.
use std::{error, fmt};
use std::ffi::CStr;

use bindings;
use helpers;

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

/// Native rdkafka topic partition
pub type RDKafkaTopicPartition = bindings::rd_kafka_topic_partition_t;

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

/// Native rdkafka group member information
pub type RDKafkaHeaders = bindings::rd_kafka_headers_t;

// ENUMS

/// Client types
pub use bindings::rd_kafka_type_t as RDKafkaType;

/// Configuration result
pub use bindings::rd_kafka_conf_res_t as RDKafkaConfRes;

/// Response error
pub use bindings::rd_kafka_resp_err_t as RDKafkaRespErr;

/// Errors enum

/// Error from the underlying rdkafka library.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RDKafkaError {
    #[doc(hidden)]
    Begin = -200,
    /// Received message is incorrect
    BadMessage = -199,
    /// Bad/unknown compression
    BadCompression = -198,
    /// Broker is going away
    BrokerDestroy = -197,
    /// Generic failure
    Fail = -196,
    /// Broker transport failure
    BrokerTransportFailure = -195,
    /// Critical system resource
    CriticalSystemResource = -194,
    /// Failed to resolve broker
    Resolve = -193,
    /// Produced message timed out
    MessageTimedOut = -192,
    /// Reached the end of the topic+partition queue on the broker. Not really an error.
    PartitionEOF = -191,
    /// Permanent: Partition does not exist in cluster.
    UnknownPartition = -190,
    /// File or filesystem error
    FileSystem = -189,
    /// Permanent: Topic does not exist in cluster.
    UnknownTopic = -188,
    /// All broker connections are down.
    AllBrokersDown = -187,
    /// Invalid argument, or invalid configuration
    InvalidArgument = -186,
    /// Operation timed out
    OperationTimedOut = -185,
    /// Queue is full
    QueueFull = -184,
    /// ISR count < required.acks
    ISRInsufficient = -183,
    /// Broker node update
    NodeUpdate = -182,
    /// SSL error
    SSL = -181,
    /// Waiting for coordinator to become available.
    WaitingForCoordinator = -180,
    /// Unknown client group
    UnknownGroup = -179,
    /// Operation in progress
    InProgress = -178,
    /// Previous operation in progress, wait for it to finish.
    PreviousInProgress = -177,
    /// This operation would interfere with an existing subscription
    ExistingSubscription = -176,
    /// Assigned partitions (rebalance_cb)
    AssignPartitions = -175,
    /// Revoked partitions (rebalance_cb)
    RevokePartitions = -174,
    /// Conflicting use
    Conflict = -173,
    /// Wrong state
    State = -172,
    /// Unknown protocol
    UnknownProtocol = -171,
    /// Not implemented
    NotImplemented = -170,
    /// Authentication failure
    Authentication = -169,
    /// No stored offset
    NoOffset = -168,
    /// Outdated
    Outdated = -167,
    /// Timed out in queue
    TimedOutQueue = -166,
    /// Feature not supported by broker
    UnsupportedFeature = -165,
    /// Awaiting cache update
    WaitCache = -164,
    /// Operation interrupted (e.g., due to yield))
    Interrupted = -163,
    /// Key serialization error
    KeySerialization = -162,
    /// Value serialization error
    ValueSerialization = -161,
    /// Key deserialization error
    KeyDeserialization = -160,
    /// Value deserialization error
    ValueDeserialization = -159,
    /// Partial response
    Partial = -158,
    /// Modification attempted on read-only object
    ReadOnly = -157,
    /// No such entry or item not found
    NoEnt = -156,
    /// Read underflow
    Underflow = -155,
    #[doc(hidden)]
    End = -100,
    /// Unknown broker error
    Unknown = -1,
    /// Success
    NoError = 0,
    /// Offset out of range
    OffsetOutOfRange = 1,
    /// Invalid message
    InvalidMessage = 2,
    /// Unknown topic or partition
    UnknownTopicOrPartition = 3,
    /// Invalid message size
    InvalidMessageSize = 4,
    /// Leader not available
    LeaderNotAvailable = 5,
    /// Not leader for partition
    NotLeaderForPartition = 6,
    /// Request timed out
    RequestTimedOut = 7,
    /// Broker not available
    BrokerNotAvailable = 8,
    /// Replica not available
    ReplicaNotAvailable = 9,
    /// Message size too large
    MessageSizeTooLarge = 10,
    /// Stale controller epoch code
    StaleControllerEpoch = 11,
    /// Offset metadata string too large
    OffsetMetadataTooLarge = 12,
    /// Broker disconnected before response received
    NetworkException = 13,
    /// Group coordinator load in progress
    GroupLoadInProgress = 14,
    /// Group coordinator not available
    GroupCoordinatorNotAvailable = 15,
    /// Not coordinator for group
    NotCoordinatorForGroup = 16,
    /// Invalid topic
    InvalidTopic = 17,
    /// Message batch larger than configured server segment size
    MessageBatchTooLarge = 18,
    /// Not enough in-sync replicas
    NotEnoughReplicas = 19,
    /// Message(s) written to insufficient number of in-sync replicas
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks value
    InvalidRequiredAcks = 21,
    /// Specified group generation id is not valid
    IllegalGeneration = 22,
    /// Inconsistent group protocol
    InconsistentGroupProtocol = 23,
    /// Invalid group.id
    InvalidGroupId = 24,
    /// Unknown member
    UnknownMemberId = 25,
    /// Invalid session timeout
    InvalidSessionTimeout = 26,
    /// Group rebalance in progress
    RebalanceInProgress = 27,
    /// Commit offset data size is not valid
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed
    TopicAuthorizationFailed = 29,
    /// Group authorization failed
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism
    UnsupportedSASLMechanism = 33,
    /// Illegal SASL state
    IllegalSASLState = 34,
    /// Unsupported version
    UnsupportedVersion = 35,
    /// Topic already exists
    TopicAlreadyExists = 36,
    /// Invalid number of partitions
    InvalidPartitions = 37,
    /// Invalid replication factor
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment
    InvalidReplicaAssignment = 39,
    /// Invalid config */
    InvalidConfig = 40,
    /// Not controller for cluster
    NotController = 41,
    /// Invalid request
    InvalidRequest = 42,
    /// Message format on broker does not support request
    UnsupportedForMessageFormat = 43,
    /// Policy violation
    PolicyViolation = 44,
    /// Broker received an out of order sequence number
    OutOfOrderSequenceNumber = 45,
    /// Broker received a duplicate sequence number
    DuplicateSequenceNumber = 46,
    /// Producer attempted an operation with an old epoch
    InvalidProducerEpoch = 47,
    /// Producer attempted a transactional operation in an invalid state
    InvalidTransactionalState = 48,
    /// Producer attempted to use a producer id which is currently assigned to its transactional id
    InvalidProducerIdMapping = 49,
    /// Transaction timeout is larger than the maxi value allowed by the broker's
    /// max.transaction.timeout.ms
    InvalidTransactionTimeout = 50,
    /// Producer attempted to update a transaction while another concurrent operation on the same
    /// transaction was ongoing
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current
    /// coordinator for a given producer
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled
    SecurityDisabled = 54,
    /// Operation not attempted
    OperationNotAttempted = 55,
    #[doc(hidden)]
    EndAll,
}

impl From<RDKafkaRespErr> for RDKafkaError {
    fn from(err: RDKafkaRespErr) -> RDKafkaError {
        helpers::rd_kafka_resp_err_t_to_rdkafka_error(err)
    }
}

impl fmt::Display for RDKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let description = match helpers::primitive_to_rd_kafka_resp_err_t(*self as i32) {
            Some(err) => {
                let cstr = unsafe { bindings::rd_kafka_err2str(err) };
                unsafe { CStr::from_ptr(cstr) }.to_string_lossy().into_owned()
            },
            None => "Unknown error".to_owned()
        };

        write!(f, "{:?} ({})", self, description)
    }
}

impl error::Error for RDKafkaError {
    fn description(&self) -> &str {
        "Error from underlying rdkafka library"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_error() {
        let error: RDKafkaError = RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF.into();
        assert_eq!("PartitionEOF (Broker: No more messages)", format!("{}", error));
        assert_eq!("PartitionEOF", format!("{:?}", error));
    }
}
