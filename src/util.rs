//! Utility functions.
extern crate librdkafka_sys as rdkafka;
extern crate std;

use std::ffi::CStr;

/// Return a tuple representing the version of `librdkafka` in
/// hexadecimal and string format.
pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdkafka::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdkafka::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

/// Convert a C string to a String.
pub fn cstr_to_owned(cstr: &[i8]) -> String {
    unsafe { CStr::from_ptr(cstr.as_ptr()).to_string_lossy().into_owned() }
}
