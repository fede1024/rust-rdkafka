//! Group membership API
use std::ffi::CStr;
use std::slice;

use rdkafka::types::*;

use error::IsError;

/// Group membership information container. This structure wraps the  pointer returned by rdkafka-sys,
/// and deallocates all the native resources when dropped.
pub struct GroupList(*const RDKafkaGroupList);

impl GroupList {
    /// Creates a new Metadata container given a pointer to the native rdkafka-sys metadata.
    pub fn from_ptr(ptr: *const RDKafkaGroupList) -> GroupList {
        GroupList(ptr)
    }
}
    /// Returns the id of the broker originating this metadata.
    //pub fn orig_broker_id(&self) -> i32 {
    //    unsafe { (*self.0).orig_broker_id }
    //}

    /// Returns the hostname of the broker originating this metadata.
    //pub fn orig_broker_name<'a>(&'a self) -> &'a str {
    //    unsafe {
    //        CStr::from_ptr((*self.0).orig_broker_name)
    //            .to_str()
    //            .expect("Broker name is not a valid UTF-8 string")
    //    }
    //}

    /// Returns the metadata information for all the brokers in the cluster.
//    pub fn brokers<'a>(&'a self) -> &'a [MetadataBroker] {
//        unsafe { slice::from_raw_parts((*self.0).brokers as *const MetadataBroker, (*self.0).broker_cnt as usize) }
//    }

    /// Returns the metadata information for all the topics in the cluster.
//    pub fn topics<'a>(&'a self) -> &'a [MetadataTopic] {
//        unsafe { slice::from_raw_parts((*self.0).topics as *const MetadataTopic, (*self.0).topic_cnt as usize) }
//    }

impl Drop for GroupList {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_group_list_destroy(self.0) };
    }
}
