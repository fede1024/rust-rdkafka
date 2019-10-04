//! Utility functions
use crate::rdsys;

use std::ffi::CStr;
use std::os::raw::c_void;
use std::ptr;
use std::slice;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::os::raw::c_char;

/// Return a tuple representing the version of `librdkafka` in
/// hexadecimal and string format.
pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdsys::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdsys::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

/// Converts a Duration into milliseconds
pub fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_secs() * 1000 + u64::from(duration.subsec_nanos()) / 1_000_000
}

/// Converts a timeout to the kafka's expected representation
pub(crate) fn timeout_to_ms<T: Into<Option<Duration>>>(timeout: T) -> i32 {
    timeout
        .into()
        .map(|t| duration_to_millis(t) as i32)
        .unwrap_or(-1)
}

/// Converts the given time to milliseconds since unix epoch.
pub fn millis_to_epoch(time: SystemTime) -> i64 {
    duration_to_millis(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))) as i64
}

/// Returns the current time in millis since unix epoch.
pub fn current_time_millis() -> i64 {
    millis_to_epoch(SystemTime::now())
}

/// Converts a pointer to an array to an optional slice. If the pointer is null, `None` will
/// be returned.
pub(crate) unsafe fn ptr_to_opt_slice<'a, T>(ptr: *const c_void, size: usize) -> Option<&'a[T]> {
    if ptr.is_null() {
        None
    } else {
        Some(slice::from_raw_parts::<T>(ptr as *const T, size))
    }
}

/// Converts a pointer to an array to a slice. If the pointer is null or the size is zero,
/// a zero-length slice will be returned.
pub(crate) unsafe fn ptr_to_slice<'a, T>(ptr: *const c_void, size: usize) -> &'a[T] {
    if ptr.is_null() || size == 0 {
        &[][..]
    } else {
        slice::from_raw_parts::<T>(ptr as *const T, size)
    }
}

/// A trait for the conversion of Rust data to raw pointers. This conversion is used
/// to pass opaque objects to the C library and vice versa.
pub trait IntoOpaque: Send + Sync {
    /// Converts the object into a raw pointer.
    fn as_ptr(&self) -> *mut c_void;
    /// Converts the raw pointer back to the original Rust object.
    unsafe fn from_ptr(_: *mut c_void) -> Self;
}

impl IntoOpaque for () {
    fn as_ptr(&self) -> *mut c_void {
        ptr::null_mut()
    }

    unsafe fn from_ptr(_: *mut c_void) -> Self {
    }
}

impl IntoOpaque for usize {
    fn as_ptr(&self) -> *mut c_void {
        *self as *mut usize as *mut c_void
    }

    unsafe fn from_ptr(ptr: *mut c_void) -> Self {
        ptr as usize
    }
}

impl<T: Send + Sync> IntoOpaque for Box<T> {
    fn as_ptr(&self) -> *mut c_void {
        self.as_ref() as *const T as *mut c_void
    }

    unsafe fn from_ptr(ptr: *mut c_void) -> Self {
        Box::from_raw(ptr as *mut T)
    }
}


// TODO: check if the implementation returns a copy of the data and update the documentation
/// Converts a byte array representing a C string into a String.
pub unsafe fn bytes_cstr_to_owned(bytes_cstr: &[c_char]) -> String {
    CStr::from_ptr(bytes_cstr.as_ptr() as *const c_char).to_string_lossy().into_owned()
}

/// Converts a C string into a String.
pub unsafe fn cstr_to_owned(cstr: *const c_char) -> String {
    CStr::from_ptr(cstr as *const c_char).to_string_lossy().into_owned()
}

pub(crate) struct ErrBuf {
    buf: [c_char; ErrBuf::MAX_ERR_LEN]
}

impl ErrBuf {
    const MAX_ERR_LEN: usize = 512;

    pub fn new() -> ErrBuf {
        ErrBuf { buf: [0; ErrBuf::MAX_ERR_LEN] }
    }

    pub fn as_mut_ptr(&mut self) -> *mut c_char {
        self.buf.as_mut_ptr()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn to_string(&self) -> String {
        unsafe { bytes_cstr_to_owned(&self.buf) }
    }
}

impl Default for ErrBuf {
    fn default() -> ErrBuf {
        ErrBuf::new()
    }
}

pub(crate) trait WrappedCPointer {
    type Target;

    fn ptr(&self) -> *mut Self::Target;

    fn is_null(&self) -> bool {
        self.ptr().is_null()
    }
}

/// Converts a container into a C array.
pub(crate) trait AsCArray<T: WrappedCPointer> {
    fn as_c_array(&self) -> *mut *mut T::Target;
}

impl<T: WrappedCPointer> AsCArray<T> for Vec<T> {
    fn as_c_array(&self) -> *mut *mut T::Target {
        self.as_ptr() as *mut *mut T::Target
    }
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

