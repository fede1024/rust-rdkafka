//! Aliases for types defined in the auto-generated bindings.

use std::convert::TryFrom;
use std::error::Error;
use std::ffi::CStr;
use std::fmt;

use num_enum::IntoPrimitive;

use crate::bindings;
use crate::helpers;

// TYPES

/// Native rdkafka client.
pub type RDKafka = bindings::rd_kafka_t;

/// Native rdkafka configuration.
pub type RDKafkaConf = bindings::rd_kafka_conf_t;

/// Native rdkafka message.
pub type RDKafkaMessage = bindings::rd_kafka_message_t;

/// Native rdkafka topic.
pub type RDKafkaTopic = bindings::rd_kafka_topic_t;

/// Native rdkafka topic configuration.
pub type RDKafkaTopicConf = bindings::rd_kafka_topic_conf_t;

/// Native rdkafka topic partition.
pub type RDKafkaTopicPartition = bindings::rd_kafka_topic_partition_t;

/// Native rdkafka topic partition list.
pub type RDKafkaTopicPartitionList = bindings::rd_kafka_topic_partition_list_t;

/// Native rdkafka metadata container.
pub type RDKafkaMetadata = bindings::rd_kafka_metadata_t;

/// Native rdkafka topic information.
pub type RDKafkaMetadataTopic = bindings::rd_kafka_metadata_topic_t;

/// Native rdkafka partition information.
pub type RDKafkaMetadataPartition = bindings::rd_kafka_metadata_partition_t;

/// Native rdkafka broker information.
pub type RDKafkaMetadataBroker = bindings::rd_kafka_metadata_broker_t;

/// Native rdkafka consumer group metadata.
pub type RDKafkaConsumerGroupMetadata = bindings::rd_kafka_consumer_group_metadata_t;

/// Native rdkafka state.
pub type RDKafkaState = bindings::rd_kafka_s;

/// Native rdkafka list of groups.
pub type RDKafkaGroupList = bindings::rd_kafka_group_list;

/// Native rdkafka group information.
pub type RDKafkaGroupInfo = bindings::rd_kafka_group_info;

/// Native rdkafka group member information.
pub type RDKafkaGroupMemberInfo = bindings::rd_kafka_group_member_info;

/// Native rdkafka group member information.
pub type RDKafkaHeaders = bindings::rd_kafka_headers_t;

/// Native rdkafka queue.
pub type RDKafkaQueue = bindings::rd_kafka_queue_t;

/// Native rdkafka new topic object.
pub type RDKafkaNewTopic = bindings::rd_kafka_NewTopic_t;

/// Native rdkafka delete topic object.
pub type RDKafkaDeleteTopic = bindings::rd_kafka_DeleteTopic_t;

/// Native rdkafka delete records object.
pub type RDKafkaDeleteRecords = bindings::rd_kafka_DeleteRecords_t;

/// Native rdkafka delete group object.
pub type RDKafkaDeleteGroup = bindings::rd_kafka_DeleteGroup_t;

/// Native rdkafka new partitions object.
pub type RDKafkaNewPartitions = bindings::rd_kafka_NewPartitions_t;

/// Native rdkafka config resource.
pub type RDKafkaConfigResource = bindings::rd_kafka_ConfigResource_t;

/// Native rdkafka event.
pub type RDKafkaEvent = bindings::rd_kafka_event_t;

/// Native rdkafka admin options.
pub type RDKafkaAdminOptions = bindings::rd_kafka_AdminOptions_t;

/// Native rdkafka topic result.
pub type RDKafkaTopicResult = bindings::rd_kafka_topic_result_t;

/// Native rdkafka group result.
pub type RDKafkaGroupResult = bindings::rd_kafka_group_result_t;

/// Native rdkafka mock cluster.
pub type RDKafkaMockCluster = bindings::rd_kafka_mock_cluster_t;

// ENUMS

/// Client types.
pub use bindings::rd_kafka_type_t as RDKafkaType;

/// Configuration result.
pub use bindings::rd_kafka_conf_res_t as RDKafkaConfRes;

/// Response error.
pub use bindings::rd_kafka_resp_err_t as RDKafkaRespErr;

/// Admin operation.
pub use bindings::rd_kafka_admin_op_t as RDKafkaAdminOp;

/// Config resource type.
pub use bindings::rd_kafka_ResourceType_t as RDKafkaResourceType;

/// Config source.
pub use bindings::rd_kafka_ConfigSource_t as RDKafkaConfigSource;

// Errors enum

/// Native rdkafka error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RDKafkaErrorCode {
    #[doc(hidden)]
    Begin = -200,
    /// Received message is incorrect.
    BadMessage = -199,
    /// Bad/unknown compression.
    BadCompression = -198,
    /// Broker is going away.
    BrokerDestroy = -197,
    /// Generic failure.
    Fail = -196,
    /// Broker transport failure.
    BrokerTransportFailure = -195,
    /// Critical system resource.
    CriticalSystemResource = -194,
    /// Failed to resolve broker.
    Resolve = -193,
    /// Produced message timed out.
    MessageTimedOut = -192,
    /// Reached the end of the topic+partition queue on the broker. Not really an error.
    PartitionEOF = -191,
    /// Permanent: Partition does not exist in cluster.
    UnknownPartition = -190,
    /// File or filesystem error.
    FileSystem = -189,
    /// Permanent: Topic does not exist in cluster.
    UnknownTopic = -188,
    /// All broker connections are down.
    AllBrokersDown = -187,
    /// Invalid argument, or invalid configuration.
    InvalidArgument = -186,
    /// Operation timed out.
    OperationTimedOut = -185,
    /// Queue is full.
    QueueFull = -184,
    /// ISR count < required.acks.
    ISRInsufficient = -183,
    /// Broker node update.
    NodeUpdate = -182,
    /// SSL error.
    SSL = -181,
    /// Waiting for coordinator to become available.
    WaitingForCoordinator = -180,
    /// Unknown client group.
    UnknownGroup = -179,
    /// Operation in progress.
    InProgress = -178,
    /// Previous operation in progress, wait for it to finish.
    PreviousInProgress = -177,
    /// This operation would interfere with an existing subscription.
    ExistingSubscription = -176,
    /// Assigned partitions (rebalance_cb).
    AssignPartitions = -175,
    /// Revoked partitions (rebalance_cb).
    RevokePartitions = -174,
    /// Conflicting use.
    Conflict = -173,
    /// Wrong state.
    State = -172,
    /// Unknown protocol.
    UnknownProtocol = -171,
    /// Not implemented.
    NotImplemented = -170,
    /// Authentication failure.
    Authentication = -169,
    /// No stored offset.
    NoOffset = -168,
    /// Outdated.
    Outdated = -167,
    /// Timed out in queue.
    TimedOutQueue = -166,
    /// Feature not supported by broker.
    UnsupportedFeature = -165,
    /// Awaiting cache update.
    WaitCache = -164,
    /// Operation interrupted (e.g., due to yield).
    Interrupted = -163,
    /// Key serialization error.
    KeySerialization = -162,
    /// Value serialization error.
    ValueSerialization = -161,
    /// Key deserialization error.
    KeyDeserialization = -160,
    /// Value deserialization error.
    ValueDeserialization = -159,
    /// Partial response.
    Partial = -158,
    /// Modification attempted on read-only object.
    ReadOnly = -157,
    /// No such entry or item not found.
    NoEnt = -156,
    /// Read underflow.
    Underflow = -155,
    /// Invalid type.
    InvalidType = -154,
    /// Retry operation.
    Retry = -153,
    /// Purged in queue.
    PurgeQueue = -152,
    /// Purged in flight.
    PurgeInflight = -151,
    /// Fatal error: see rd_kafka_fatal_error().
    Fatal = -150,
    /// Inconsistent state.
    Inconsistent = -149,
    /// Gap-less ordering would not be guaranteed if proceeding.
    GaplessGuarantee = -148,
    /// Maximum poll interval exceeded.
    PollExceeded = -147,
    /// Unknown broker.
    UnknownBroker = -146,
    /// Functionality not configured.
    NotConfigured = -145,
    /// Instance has been fenced.
    Fenced = -144,
    /// Application generated error.
    Application = -143,
    /// Assignment lost.
    AssignmentLost = -142,
    /// No operation performed.
    Noop = -141,
    /// No offset to automatically reset to.
    AutoOffsetReset = -140,
    /// Partition log truncation detected
    LogTruncation = -139,
    #[doc(hidden)]
    End = -100,
    /// Unknown broker error.
    Unknown = -1,
    /// Success.
    NoError = 0,
    /// Offset out of range.
    OffsetOutOfRange = 1,
    /// Invalid message.
    InvalidMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size.
    InvalidMessageSize = 4,
    /// Leader not available.
    LeaderNotAvailable = 5,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out.
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Replica not available.
    ReplicaNotAvailable = 9,
    /// Message size too large.
    MessageSizeTooLarge = 10,
    /// Stale controller epoch code.
    StaleControllerEpoch = 11,
    /// Offset metadata string too large.
    OffsetMetadataTooLarge = 12,
    /// Broker disconnected before response received.
    NetworkException = 13,
    /// Coordinator load in progress.
    CoordinatorLoadInProgress = 14,
    /// Coordinator not available.
    CoordinatorNotAvailable = 15,
    /// Not coordinator.
    NotCoordinator = 16,
    /// Invalid topic.
    InvalidTopic = 17,
    /// Message batch larger than configured server segment size.
    MessageBatchTooLarge = 18,
    /// Not enough in-sync replicas.
    NotEnoughReplicas = 19,
    /// Message(s) written to insufficient number of in-sync replicas.
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks value.
    InvalidRequiredAcks = 21,
    /// Specified group generation id is not valid.
    IllegalGeneration = 22,
    /// Inconsistent group protocol.
    InconsistentGroupProtocol = 23,
    /// Invalid group.id.
    InvalidGroupId = 24,
    /// Unknown member.
    UnknownMemberId = 25,
    /// Invalid session timeout.
    InvalidSessionTimeout = 26,
    /// Group rebalance in progress.
    RebalanceInProgress = 27,
    /// Commit offset data size is not valid.
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp.
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism.
    UnsupportedSASLMechanism = 33,
    /// Illegal SASL state.
    IllegalSASLState = 34,
    /// Unsupported version.
    UnsupportedVersion = 35,
    /// Topic already exists.
    TopicAlreadyExists = 36,
    /// Invalid number of partitions.
    InvalidPartitions = 37,
    /// Invalid replication factor.
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment.
    InvalidReplicaAssignment = 39,
    /// Invalid config.
    InvalidConfig = 40,
    /// Not controller for cluster.
    NotController = 41,
    /// Invalid request.
    InvalidRequest = 42,
    /// Message format on broker does not support request.
    UnsupportedForMessageFormat = 43,
    /// Policy violation.
    PolicyViolation = 44,
    /// Broker received an out of order sequence number.
    OutOfOrderSequenceNumber = 45,
    /// Broker received a duplicate sequence number.
    DuplicateSequenceNumber = 46,
    /// Producer attempted an operation with an old epoch.
    InvalidProducerEpoch = 47,
    /// Producer attempted a transactional operation in an invalid state.
    InvalidTransactionalState = 48,
    /// Producer attempted to use a producer id which is currently assigned to
    /// its transactional id.
    InvalidProducerIdMapping = 49,
    /// Transaction timeout is larger than the maxi value allowed by the
    /// broker's max.transaction.timeout.ms.
    InvalidTransactionTimeout = 50,
    /// Producer attempted to update a transaction while another concurrent
    /// operation on the same transaction was ongoing.
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is
    /// no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled.
    SecurityDisabled = 54,
    /// Operation not attempted.
    OperationNotAttempted = 55,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
    /// Unknown Producer Id.
    UnknownProducerId = 59,
    /// Partition reassignment is in progress.
    ReassignmentInProgress = 60,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation Token requests are not allowed on this connection.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation Token is expired.
    DelegationTokenExpired = 66,
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67,
    /// The group is not empty.
    NonEmptyGroup = 68,
    /// The group id does not exist.
    GroupIdNotFound = 69,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71,
    /// No matching listener.
    ListenerNotFound = 72,
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73,
    /// Leader epoch is older than broker epoch.
    FencedLeaderEpoch = 74,
    /// Leader epoch is newer than broker epoch.
    UnknownLeaderEpoch = 75,
    /// Unsupported compression type.
    UnsupportedCompressionType = 76,
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77,
    /// Leader high watermark is not caught up.
    OffsetNotAvailable = 78,
    /// Group member needs a valid member ID.
    MemberIdRequired = 79,
    /// Preferred leader was not available.
    PreferredLeaderNotAvailable = 80,
    /// Consumer group has reached maximum size.
    GroupMaxSizeReached = 81,
    /// Static consumer fenced by other consumer with same group.instance.id.
    FencedInstanceId = 82,
    /// Eligible partition leaders are not available.
    EligibleLeadersNotAvailable = 83,
    /// Leader election not needed for topic partition.
    ElectionNotNeeded = 84,
    /// No partition reassignment is in progress.
    NoReassignmentInProgress = 85,
    /// Deleting offsets of a topic while the consumer group is subscribed to
    /// it.
    GroupSubscribedToTopic = 86,
    /// Broker failed to validate record.
    InvalidRecord = 87,
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit = 88,
    /// Throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89,
    /// There is a newer producer with the same transactional ID which fences
    /// the current one.
    ProducerFenced = 90,
    /// Request illegally referred to resource that does not exist.
    ResourceNotFound = 91,
    /// Request illegally referred to the same resource twice.
    DuplicateResource = 92,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93,
    /// Either the sender or recipient of a voter-only request is not one of the
    /// expected voters.
    InconsistentVoterSet = 94,
    /// Invalid update version.
    InvalidUpdateVersion = 95,
    /// Unable to update finalized features due to server error.
    FeatureUpdateFailed = 96,
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97,
    #[doc(hidden)]
    EndAll,
}

impl From<RDKafkaRespErr> for RDKafkaErrorCode {
    fn from(err: RDKafkaRespErr) -> RDKafkaErrorCode {
        helpers::rd_kafka_resp_err_t_to_rdkafka_error(err)
    }
}

impl fmt::Display for RDKafkaErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let description = match RDKafkaRespErr::try_from(*self as i32) {
            Ok(err) => {
                let cstr = unsafe { bindings::rd_kafka_err2str(err) };
                unsafe { CStr::from_ptr(cstr) }
                    .to_string_lossy()
                    .into_owned()
            }
            Err(_) => "Unknown error".to_owned(),
        };

        write!(f, "{:?} ({})", self, description)
    }
}

impl Error for RDKafkaErrorCode {}

/// Native rdkafka ApiKeys / protocol requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
#[repr(i16)]
#[non_exhaustive]
pub enum RDKafkaApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersion = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    Vote = 52,
    BeginQuorumEpoch = 53,
    EndQuorumEpoch = 54,
    DescribeQuorum = 55,
    AlterIsr = 56,
    UpdateFeatures = 57,
    Envelope = 58,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_error() {
        let error: RDKafkaErrorCode = RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF.into();
        assert_eq!(
            "PartitionEOF (Broker: No more messages)",
            format!("{}", error)
        );
        assert_eq!("PartitionEOF", format!("{:?}", error));
    }
}
