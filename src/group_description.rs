//! Data structures supporting the [AdminClient::describe_consumer_groups] operation.

use crate::error::{IsError, KafkaError, RDKafkaError};
use crate::group_description::ConsumerGroupState::Invalid;
use crate::util::cstr_to_owned;
use crate::TopicPartitionList;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

/// Result of the `AdminClient::describe_consumer_groups` method.
pub type ConsumerGroupDescriptionResult = Result<ConsumerGroupDescription, (String, KafkaError)>;

/// Given a `ConsumerGroupDescriptionResult` provides the identifier (the group_id) from either
/// the `Ok` or the `Err` branches.
pub fn group_description_result_key(result: &ConsumerGroupDescriptionResult) -> &str {
    match result {
        Ok(description) => description.group_id.as_str(),
        Err((group_id, _error)) => group_id.as_str(),
    }
}

///
/// Top level description of a consumer group
/// 
#[derive(Debug, PartialEq)]
pub struct ConsumerGroupDescription {
    /// The id of the consumer group.
    pub group_id: String,
    /// If consumer group is simple or not.
    pub is_simple_consumer_group: bool,
    /// The consumer group partition assignor.
    pub partition_assignor: String,
    /// authorizedOperations for this group, or null if that information is not known.
    pub authorized_operations: Vec<AclOperation>,
    /// The group state, or UNKNOWN if the state is too new for us to parse.
    pub state: ConsumerGroupState,
    /// The consumer group coordinator, or None if the coordinator is not known.
    pub coordinator: Option<Node>,
    /// The group type (or the protocol) of this consumer group. It defaults to Classic if not
    /// provided by the server.
    pub group_type: GroupType,
    /// A list of the members of the consumer group.
    pub members: Vec<MemberDescription>,
}

impl ConsumerGroupDescription {
    pub(crate) unsafe fn vec_from_ptr(
        ptr: *const RDKafkaDescribeConsumerGroupsResult,
    ) -> Vec<ConsumerGroupDescriptionResult> {
        let mut group_count = 0;

        let groups = rdsys::rd_kafka_DescribeConsumerGroups_result_groups(ptr, &mut group_count);

        let mut groups_out = Vec::with_capacity(group_count);

        // Copy the offsets from the C structure
        for i in 0..group_count {
            let group_ptr = *groups.add(i);

            let description = ConsumerGroupDescription::from_ptr(group_ptr);
            groups_out.push(description);
        }
        groups_out
    }

    pub(crate) unsafe fn from_ptr(
        ptr: *const RDKafkaConsumerGroupDescription,
    ) -> ConsumerGroupDescriptionResult {
        let group_id = cstr_to_owned(rdsys::rd_kafka_ConsumerGroupDescription_group_id(ptr));

        let kafka_error =
            RDKafkaError::from_ptr(rdsys::rd_kafka_ConsumerGroupDescription_error(ptr) as *mut _);
        if kafka_error.is_error() {
            Err((group_id.clone(),KafkaError::AdminOp(kafka_error.code())))
        } else {
            let is_simple_consumer_group: bool =
                rdsys::rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(ptr) != 0;

            let partition_assignor = cstr_to_owned(
                rdsys::rd_kafka_ConsumerGroupDescription_partition_assignor(ptr),
            );

            let mut acl_count = 0;
            let acl_list =
                rdsys::rd_kafka_ConsumerGroupDescription_authorized_operations(ptr, &mut acl_count);
            let mut authorized_operations = Vec::with_capacity(acl_count);
            for i in 0..acl_count {
                let native_acl = *acl_list.add(i);
                authorized_operations.push(AclOperation::from_native(native_acl));
            }
            let state =
                ConsumerGroupState::from_native(rdsys::rd_kafka_ConsumerGroupDescription_state(ptr));

            let coordinator_ptr = rdsys::rd_kafka_ConsumerGroupDescription_coordinator(ptr);
            let coordinator = if coordinator_ptr.is_null() {
                None
            } else {
                Some(Node::from_ptr(coordinator_ptr))
            };

            let group_type =
                GroupType::from_native(rdsys::rd_kafka_ConsumerGroupDescription_type(ptr));

            // rd_kafka_ConsumerGroupDescription_member_count
            let mut members = Vec::new();
            let member_count = rdsys::rd_kafka_ConsumerGroupDescription_member_count(ptr);
            for i in 0..member_count {
                let member_ptr = rdsys::rd_kafka_ConsumerGroupDescription_member(ptr, i);
                let member = MemberDescription::from_ptr(member_ptr);
                members.push(member);
            }
            Ok(ConsumerGroupDescription {
                group_id,
                is_simple_consumer_group,
                partition_assignor,
                authorized_operations,
                state,
                coordinator,
                group_type,
                members,
            })
        }
    }
}

///
/// Represents an operation which an ACL grants or denies permission to perform. Some operations
/// imply other operations:
///
/// - `ALLOW ALL` implies `ALLOW` everything
/// - `DENY ALL` implies `DENY` everything
/// - `ALLOW READ` implies `ALLOW DESCRIBE`
/// - `ALLOW WRITE` implies `ALLOW DESCRIBE`
/// - `ALLOW DELETE` implies `ALLOW DESCRIBE`
/// - `ALLOW ALTER` implies `ALLOW DESCRIBE`
/// - `ALLOW ALTER_CONFIGS` implies `ALLOW DESCRIBE_CONFIGS`
#[derive(Debug, PartialEq)]
pub enum AclOperation {
    /// Represents any AclOperation which this client cannot understand,
    /// perhaps because this client is too old.
    Unknown = 0,
    /// In a filter, matches any AclOperation.
    Any = 1,
    /// `ALL`  operations.
    All = 2,
    /// `READ` operation.
    Read = 3,
    /// `WRITE` operation.
    Write = 4,
    /// `CREATE` operation.
    Create = 5,
    /// `DELETE` operation.
    Delete = 6,
    /// `ALTE```  operation.
    Alter = 7,
    /// `DESCRIBE` operation.
    Describe = 8,
    /// `CLUSTER_ACTION` operation.
    ClusterAction = 9,
    /// `DESCRIBE_CONFIGS` operation.
    DescribeConfigs = 10,
    ///  ALTER_CONFIGS` operation.
    AlterConfigs = 11,
    /// IDEMPOTENT_WRITE operation.
    IdempotentWrite = 12,
    /// Invalid enum value.
    Invalid = 13,
}

impl AclOperation {
    pub(crate) fn from_native(native_value: RDKafkaAclOperation) -> AclOperation {
        match native_value {
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_UNKNOWN => AclOperation::Unknown,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_ANY => AclOperation::Any,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_ALL => AclOperation::All,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_READ => AclOperation::Read,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_WRITE => AclOperation::Write,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_CREATE => AclOperation::Create,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_DELETE => AclOperation::Delete,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_ALTER => AclOperation::Alter,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_DESCRIBE => AclOperation::Describe,
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION => {
                AclOperation::ClusterAction
            }
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS => {
                AclOperation::DescribeConfigs
            }
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS => {
                AclOperation::AlterConfigs
            }
            RDKafkaAclOperation::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE => {
                AclOperation::IdempotentWrite
            }
            _ => AclOperation::Invalid,
        }
    }
}

///
/// The consumer group state.
/// 
#[derive(Debug, PartialEq)]
pub enum ConsumerGroupState {
    /// Any state this client cannot understand.
    Unknown = 0,
    ///The group is preparing to rebalance. A rebalance is triggered whenever a consumer
    /// joins or leaves the group or when a consumer fails.
    PreparingRebalance = 1,
    /// The group is completing the rebalance process, assigning partitions to consumers.
    CompletingRebalance = 2,
    /// The group is stable with all consumers active and partitions assigned accordingly.
    Stable = 3,
    /// The group has been marked for deletion, or it does not exist.
    Dead = 4,
    /// All consumers in the group are inactive, and no partitions are assigned.
    Empty = 5,
    /// Invalid enum value.
    Invalid = 6,
}

impl ConsumerGroupState {
    pub(crate) fn from_native(
        native_value: RDKafkaConsumerGroupState,
    ) -> ConsumerGroupState {
        match native_value {
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN => ConsumerGroupState::Unknown,
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE => ConsumerGroupState::PreparingRebalance,
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE => ConsumerGroupState::CompletingRebalance,
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_STABLE => ConsumerGroupState::Stable,
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_DEAD => ConsumerGroupState::Dead,
            RDKafkaConsumerGroupState::RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY => ConsumerGroupState::Empty,
            _ => Invalid

        }
    }
}

///
/// Information about a Kafka node
/// 
#[derive(Debug, PartialEq)]
pub struct Node {
    /// The node id of this node
    pub id: i32,
    /// The host name for this node
    pub host: String,
    /// The port for this node
    pub port: u16,
    /// The rack for this node (if defined)
    pub rack: Option<String>,
}

impl Node {
    pub(crate) unsafe fn from_ptr(ptr: *const RDKafkaNode) -> Node {
        let id = rdsys::rd_kafka_Node_id(ptr);
        let host = cstr_to_owned(rdsys::rd_kafka_Node_host(ptr));
        let port = rdsys::rd_kafka_Node_port(ptr);
        let rack_ptr = rdsys::rd_kafka_Node_rack(ptr);
        let rack = if rack_ptr.is_null() {
            None
        } else {
            Some(cstr_to_owned(rack_ptr))
        };
        Node {
            id,
            host,
            port,
            rack,
        }
    }
}

///
/// The type of the consumer group.
/// 
#[derive(Debug, PartialEq)]
pub enum GroupType {
    /// Unknown or unsupported group type.
    Unknown = 0,
    /// The standard consumer group.
    Consumer = 1,
    /// Legacy consumer group.
    Classic = 2,
    /// Invalid enum value.
    Invalid = 3,
}

impl GroupType {
    pub(crate) fn from_native(native: RDKafkaConsumerGroupType) -> GroupType {
        match native {
            RDKafkaConsumerGroupType::RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN => {
                GroupType::Unknown
            }
            RDKafkaConsumerGroupType::RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER => {
                GroupType::Consumer
            }
            RDKafkaConsumerGroupType::RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC => {
                GroupType::Classic
            }
            _ => GroupType::Invalid,
        }
    }
}

///
/// A detailed description of a single group member in the cluster.
///
#[derive(Debug, PartialEq)]
pub struct MemberDescription {
    /// The client id of the group member.
    /// This is the client identifier string assigned by the Kafka client.
    /// It identifies the client application instance that is a member of the consumer group.
    /// The client_id is typically set in the consumer configuration and is used to
    /// differentiate clients in logs, metrics, and management tools.
    pub client_id: String,
    /// This is a unique identifier for a consumer instance within a consumer group,
    /// introduced to support static membership in Kafka. When set, it identifies the
    /// consumer instance persistently across restarts, enabling more stable group
    /// membership and reducing rebalances. If null, the member is considered a dynamic
    /// member with a broker-generated member ID.
    pub group_instance_id: Option<String>,
    /// This identifies the specific consumer member of the group and is generated by the
    /// broker upon joining the group. It is the broker-assigned ID used internally to track
    /// the group member and its state.
    pub consumer_id: String,
    /// The host where the group member is running.
    pub host: String,
    /// The assignment of the group member.
    pub assignment: Option<MemberAssignment>,
    /// The target assignment of the member.
    pub target_assignment: Option<MemberAssignment>,
}

impl MemberDescription {
    pub(crate) unsafe fn from_ptr(
        ptr: *const RDKafkaMemberDescription,
    ) -> MemberDescription {
        let client_id = cstr_to_owned(rdsys::rd_kafka_MemberDescription_client_id(ptr));
        let group_instance_id_ptr = rdsys::rd_kafka_MemberDescription_group_instance_id(ptr);
        let group_instance_id = if group_instance_id_ptr.is_null() {
            None
        } else {
            Some(cstr_to_owned(group_instance_id_ptr))
        };
        let consumer_id = cstr_to_owned(rdsys::rd_kafka_MemberDescription_consumer_id(ptr));
        let host = cstr_to_owned(rdsys::rd_kafka_MemberDescription_host(ptr));
        let assignment_ptr = rdsys::rd_kafka_MemberDescription_assignment(ptr);
        let assignment = if assignment_ptr.is_null() {
            None
        } else {
            Some(MemberAssignment::from_ptr(assignment_ptr))
        };
        let target_assignment_ptr = rdsys::rd_kafka_MemberDescription_target_assignment(ptr);
        let target_assignment = if target_assignment_ptr.is_null() {
            None
        } else {
            Some(MemberAssignment::from_ptr(target_assignment_ptr))
        };
        MemberDescription {
            client_id,
            group_instance_id,
            consumer_id,
            host,
            assignment,
            target_assignment,
        }
    }
}

///
/// A description of the assignments of a specific group member.
/// 
#[derive(Debug, PartialEq)]
pub struct MemberAssignment {
    /// The topic partitions assigned to a group member.
    pub partitions: TopicPartitionList,
}

impl MemberAssignment {
    pub(crate) unsafe fn from_ptr(
        ptr: *const RDKafkaMemberAssignment,
    ) -> MemberAssignment {
        let topic_partition_list_ptr = rdsys::rd_kafka_MemberAssignment_partitions(ptr);
        let topic_partition_list_cloned_ptr =
            rdsys::rd_kafka_topic_partition_list_copy(topic_partition_list_ptr);
        let partitions = TopicPartitionList::from_ptr(topic_partition_list_cloned_ptr);
        MemberAssignment { partitions }
    }
}
