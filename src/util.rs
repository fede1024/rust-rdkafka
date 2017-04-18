//! Utility functions.
use rdsys;

use std::ffi::CStr;
use std::time::Duration;

/// Return a tuple representing the version of `librdkafka` in
/// hexadecimal and string format.
pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdsys::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdsys::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

/// Converts a Duration into milliseconds
pub fn duration_to_millis(duration: Duration) -> u64 {
    let nanos = duration.subsec_nanos() as u64;
    duration.as_secs() * 1000 + nanos/1_000_000
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_duration_to_millis() {
        assert_eq!(duration_to_millis(Duration::from_secs(1)), 1000);
        assert_eq!(duration_to_millis(Duration::from_millis(1500)), 1500);
        assert_eq!(duration_to_millis(Duration::new(5, 123_000_000)), 5123);
    }
}

