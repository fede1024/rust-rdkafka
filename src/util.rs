//! Utility functions.
extern crate rdkafka_sys as rdkafka;
extern crate std;

use std::ffi::CStr;

/// Return a tuple representing the version of `librdkafka` in
/// hexadecimal and string format.
pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdkafka::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdkafka::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

// TODO: check if the implementation returns a copy of the data and update the documentation
/// Converts a byte array representing a C string into a String.
pub unsafe fn bytes_cstr_to_owned(bytes_cstr: &[i8]) -> String {
    CStr::from_ptr(bytes_cstr.as_ptr()).to_string_lossy().into_owned()
}

/// Converts a C string into a String.
pub unsafe fn cstr_to_owned(cstr: *const i8) -> String {
    CStr::from_ptr(cstr).to_string_lossy().into_owned()
}
