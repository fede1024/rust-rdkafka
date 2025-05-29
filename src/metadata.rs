//! Cluster metadata.

use std::ffi::CStr;
use std::slice;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::error::IsError;
use crate::util::{KafkaDrop, NativePtr};

/// Broker metadata information.
pub struct MetadataBroker(RDKafkaMetadataBroker);

impl MetadataBroker {
    /// Returns the id of the broker.
    pub fn id(&self) -> i32 {
        self.0.id
    }

    /// Returns the host name of the broker.
    pub fn host(&self) -> &str {
        unsafe {
            CStr::from_ptr(self.0.host)
                .to_str()
                .expect("Broker host is not a valid UTF-8 string")
        }
    }

    /// Returns the port of the broker.
    pub fn port(&self) -> i32 {
        self.0.port
    }
}

/// Partition metadata information.
pub struct MetadataPartition(RDKafkaMetadataPartition);

impl MetadataPartition {
    /// Returns the id of the partition.
    pub fn id(&self) -> i32 {
        self.0.id
    }

    /// Returns the broker id of the leader broker for the partition.
    pub fn leader(&self) -> i32 {
        self.0.leader
    }

    // TODO: return result?
    /// Returns the metadata error for the partition, or `None` if there is no
    /// error.
    pub fn error(&self) -> Option<RDKafkaRespErr> {
        if self.0.err.is_error() {
            Some(self.0.err)
        } else {
            None
        }
    }

    /// Returns the broker IDs of the replicas.
    pub fn replicas(&self) -> &[i32] {
        unsafe { slice::from_raw_parts(self.0.replicas, self.0.replica_cnt as usize) }
    }

    /// Returns the broker IDs of the in-sync replicas.
    pub fn isr(&self) -> &[i32] {
        unsafe { slice::from_raw_parts(self.0.isrs, self.0.isr_cnt as usize) }
    }
}

/// Topic metadata information.
pub struct MetadataTopic(RDKafkaMetadataTopic);

impl MetadataTopic {
    /// Returns the name of the topic.
    pub fn name(&self) -> &str {
        unsafe {
            CStr::from_ptr(self.0.topic)
                .to_str()
                .expect("Topic name is not a valid UTF-8 string")
        }
    }

    /// Returns the partition metadata information for all the partitions.
    pub fn partitions(&self) -> &[MetadataPartition] {
        unsafe {
            slice::from_raw_parts(
                self.0.partitions as *const MetadataPartition,
                self.0.partition_cnt as usize,
            )
        }
    }

    /// Returns the metadata error for the topic, or `None` if there was no
    /// error.
    pub fn error(&self) -> Option<RDKafkaRespErr> {
        if self.0.err.is_error() {
            Some(self.0.err)
        } else {
            None
        }
    }
}

/// Metadata container.
///
/// This structure wraps the metadata pointer returned by rdkafka-sys, and
/// deallocates all the native resources when dropped.
pub struct Metadata(NativePtr<RDKafkaMetadata>);

unsafe impl KafkaDrop for RDKafkaMetadata {
    const TYPE: &'static str = "metadata";
    const DROP: unsafe extern "C" fn(*mut Self) = drop_metadata;
}

unsafe extern "C" fn drop_metadata(ptr: *mut RDKafkaMetadata) {
    rdsys::rd_kafka_metadata_destroy(ptr as *const _)
}

impl Metadata {
    /// Creates a new Metadata container given a pointer to the native rdkafka-sys metadata.
    pub(crate) unsafe fn from_ptr(ptr: *const RDKafkaMetadata) -> Metadata {
        Metadata(NativePtr::from_ptr(ptr as *mut _).unwrap())
    }

    /// Returns the ID of the broker originating this metadata.
    pub fn orig_broker_id(&self) -> i32 {
        self.0.orig_broker_id
    }

    /// Returns the hostname of the broker originating this metadata.
    pub fn orig_broker_name(&self) -> &str {
        unsafe {
            CStr::from_ptr(self.0.orig_broker_name)
                .to_str()
                .expect("Broker name is not a valid UTF-8 string")
        }
    }

    /// Returns the metadata information for all the brokers in the cluster.
    pub fn brokers(&self) -> &[MetadataBroker] {
        unsafe {
            slice::from_raw_parts(
                self.0.brokers as *const MetadataBroker,
                self.0.broker_cnt as usize,
            )
        }
    }

    /// Returns the metadata information for all the topics in the cluster.
    pub fn topics(&self) -> &[MetadataTopic] {
        unsafe {
            slice::from_raw_parts(
                self.0.topics as *const MetadataTopic,
                self.0.topic_cnt as usize,
            )
        }
    }
}

unsafe impl Send for Metadata {}
unsafe impl Sync for Metadata {}
