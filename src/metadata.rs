//! Metadata fetch API
extern crate rdkafka_sys as rdkafka;

use std::ffi::CStr;
use std::slice;

use self::rdkafka::types::*;

use error::IsError;


pub struct MetadataBroker(RDKafkaMetadataBroker);

impl MetadataBroker {
    pub fn id(&self) -> i32 {
        self.0.id
    }

    pub fn host<'a>(&'a self) -> &'a str {
        unsafe {
            CStr::from_ptr(self.0.host)
                .to_str()
                .expect("Broker host is not a valid UTF-8 string")
        }
    }

    pub fn port(&self) -> i32 {
        self.0.port
    }
}

pub struct MetadataPartition(RDKafkaMetadataPartition);

impl MetadataPartition {
    pub fn id(&self) -> i32 {
        self.0.id
    }

    pub fn leader(&self) -> i32 {
        self.0.leader
    }

    pub fn error(&self) -> Option<RDKafkaRespErr> {
        if self.0.err.is_error() {
            Some(self.0.err)
        } else {
            None
        }
    }

    pub fn replicas<'a>(&'a self) -> &'a [i32] {
        unsafe { slice::from_raw_parts(self.0.replicas, self.0.replica_cnt as usize) }
    }

    pub fn isr<'a>(&'a self) -> &'a [i32] {
        unsafe { slice::from_raw_parts(self.0.isrs, self.0.isr_cnt as usize) }
    }
}

pub struct MetadataTopic(RDKafkaMetadataTopic);

impl MetadataTopic {
    pub fn name<'a>(&'a self) -> &'a str {
        unsafe {
            CStr::from_ptr(self.0.topic)
                .to_str()
                .expect("Topic name is not a valid UTF-8 string")
        }
    }

    pub fn partitions<'a>(&'a self) -> &'a [MetadataPartition] {
        unsafe { slice::from_raw_parts(self.0.partitions as *const MetadataPartition, self.0.partition_cnt as usize) }
    }

    pub fn error(&self) -> Option<RDKafkaRespErr> {
        if self.0.err.is_error() {
            Some(self.0.err)
        } else {
            None
        }
    }
}

pub struct Metadata(*const RDKafkaMetadata);

impl Metadata {
    pub fn from_ptr(ptr: *const RDKafkaMetadata) -> Metadata {
        Metadata(ptr)
    }

    pub fn orig_broker_id(&self) -> i32 {
        unsafe { (*self.0).orig_broker_id }
    }

    pub fn orig_broker_name<'a>(&'a self) -> &'a str {
        unsafe {
            CStr::from_ptr((*self.0).orig_broker_name)
                .to_str()
                .expect("Broker name is not a valid UTF-8 string")
        }
    }

    pub fn brokers<'a>(&'a self) -> &'a [MetadataBroker] {
        unsafe { slice::from_raw_parts((*self.0).brokers as *const MetadataBroker, (*self.0).broker_cnt as usize) }
    }

    pub fn topics<'a>(&'a self) -> &'a [MetadataTopic] {
        unsafe { slice::from_raw_parts((*self.0).topics as *const MetadataTopic, (*self.0).topic_cnt as usize) }
    }
}

impl Drop for Metadata {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_metadata_destroy(self.0) };
    }
}
