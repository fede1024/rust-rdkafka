//! Utility functions and types.

use std::ffi::CStr;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::os::raw::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::channel::oneshot;
use futures::future::{FutureExt, Map};
use log::trace;

use rdkafka_sys as rdsys;

/// Returns a tuple representing the version of `librdkafka` in hexadecimal and
/// string format.
pub fn get_rdkafka_version() -> (u16, String) {
    let version_number = unsafe { rdsys::rd_kafka_version() } as u16;
    let c_str = unsafe { CStr::from_ptr(rdsys::rd_kafka_version_str()) };
    (version_number, c_str.to_string_lossy().into_owned())
}

/// Specifies a timeout for a Kafka operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Timeout {
    /// Time out after the specified duration elapses.
    After(Duration),
    /// Block forever.
    Never,
}

impl Timeout {
    /// Converts a timeout to Kafka's expected representation.
    pub(crate) fn as_millis(&self) -> i32 {
        match self {
            Timeout::After(d) => d.as_millis() as i32,
            Timeout::Never => -1,
        }
    }
}

impl std::ops::SubAssign for Timeout {
    fn sub_assign(&mut self, other: Self) {
        match (self, other) {
            (Timeout::After(lhs), Timeout::After(rhs)) => *lhs -= rhs,
            (Timeout::Never, Timeout::After(_)) => (),
            _ => panic!("subtraction of Timeout::Never is ill-defined"),
        }
    }
}

impl From<Duration> for Timeout {
    fn from(d: Duration) -> Timeout {
        Timeout::After(d)
    }
}

impl From<Option<Duration>> for Timeout {
    fn from(v: Option<Duration>) -> Timeout {
        match v {
            None => Timeout::Never,
            Some(d) => Timeout::After(d),
        }
    }
}

/// Converts the given time to the number of milliseconds since the Unix epoch.
pub fn millis_to_epoch(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as i64
}

/// Returns the current time in milliseconds since the Unix epoch.
pub fn current_time_millis() -> i64 {
    millis_to_epoch(SystemTime::now())
}

/// Converts a pointer to an array to an optional slice. If the pointer is null,
/// returns `None`.
pub(crate) unsafe fn ptr_to_opt_slice<'a, T>(ptr: *const c_void, size: usize) -> Option<&'a [T]> {
    if ptr.is_null() {
        None
    } else {
        Some(slice::from_raw_parts::<T>(ptr as *const T, size))
    }
}

pub(crate) unsafe fn ptr_to_opt_mut_slice<'a, T>(
    ptr: *const c_void,
    size: usize,
) -> Option<&'a mut [T]> {
    if ptr.is_null() {
        None
    } else {
        Some(slice::from_raw_parts_mut::<T>(ptr as *mut T, size))
    }
}

/// Converts a pointer to an array to a slice. If the pointer is null or the
/// size is zero, returns a zero-length slice..
pub(crate) unsafe fn ptr_to_slice<'a, T>(ptr: *const c_void, size: usize) -> &'a [T] {
    if ptr.is_null() || size == 0 {
        &[][..]
    } else {
        slice::from_raw_parts::<T>(ptr as *const T, size)
    }
}

/// Converts Rust data to and from raw pointers.
///
/// This conversion is used to pass opaque objects to the C library and vice
/// versa.
pub trait IntoOpaque: Send + Sync {
    /// Converts the object into a raw pointer.
    fn as_ptr(&self) -> *mut c_void;

    /// Converts the raw pointer back to the original Rust object.
    ///
    /// # Safety
    ///
    /// The pointer must be a valid pointer to the original Rust object.
    unsafe fn from_ptr(_: *mut c_void) -> Self;
}

impl IntoOpaque for () {
    fn as_ptr(&self) -> *mut c_void {
        ptr::null_mut()
    }

    unsafe fn from_ptr(_: *mut c_void) -> Self {}
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

impl<T: Send + Sync> IntoOpaque for Arc<T> {
    fn as_ptr(&self) -> *mut c_void {
        self.as_ref() as *const T as *mut c_void
    }

    unsafe fn from_ptr(ptr: *mut c_void) -> Self {
        Arc::from_raw(ptr as *mut T)
    }
}

/// Converts a C string into a [`String`].
///
/// # Safety
///
/// `cstr` must point to a valid, null-terminated C string.
pub unsafe fn cstr_to_owned(cstr: *const c_char) -> String {
    CStr::from_ptr(cstr as *const c_char)
        .to_string_lossy()
        .into_owned()
}

pub(crate) struct ErrBuf {
    buf: [u8; ErrBuf::MAX_ERR_LEN],
}

impl ErrBuf {
    const MAX_ERR_LEN: usize = 512;

    pub fn new() -> ErrBuf {
        ErrBuf {
            buf: [0; ErrBuf::MAX_ERR_LEN],
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut c_char {
        self.buf.as_mut_ptr() as *mut c_char
    }

    pub fn filled(&self) -> &[u8] {
        let i = self.buf.iter().position(|c| *c == 0).unwrap();
        &self.buf[..i + 1]
    }

    pub fn len(&self) -> usize {
        self.filled().len()
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
}

impl Default for ErrBuf {
    fn default() -> ErrBuf {
        ErrBuf::new()
    }
}

impl fmt::Display for ErrBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            CStr::from_bytes_with_nul(self.filled())
                .unwrap()
                .to_string_lossy()
        )
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

pub(crate) struct NativePtr<T>
where
    T: KafkaDrop,
{
    ptr: NonNull<T>,
}

impl<T> Drop for NativePtr<T>
where
    T: KafkaDrop,
{
    fn drop(&mut self) {
        trace!("Destroying {}: {:?}", T::TYPE, self.ptr);
        unsafe { T::DROP(self.ptr.as_ptr()) }
        trace!("Destroyed {}: {:?}", T::TYPE, self.ptr);
    }
}

pub(crate) unsafe trait KafkaDrop {
    const TYPE: &'static str;
    const DROP: unsafe extern "C" fn(*mut Self);
}

impl<T> WrappedCPointer for NativePtr<T>
where
    T: KafkaDrop,
{
    type Target = T;

    fn ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }
}

impl<T> Deref for NativePtr<T>
where
    T: KafkaDrop,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> fmt::Debug for NativePtr<T>
where
    T: KafkaDrop,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.ptr.fmt(f)
    }
}

impl<T> NativePtr<T>
where
    T: KafkaDrop,
{
    pub(crate) unsafe fn from_ptr(ptr: *mut T) -> Option<Self> {
        NonNull::new(ptr).map(|ptr| Self { ptr })
    }

    pub(crate) fn ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }
}

pub(crate) struct OnDrop<F>(pub F)
where
    F: Fn();

impl<F> Drop for OnDrop<F>
where
    F: Fn(),
{
    fn drop(&mut self) {
        (self.0)()
    }
}

/// An abstraction over asynchronous runtimes.
///
/// There are several asynchronous runtimes available for Rust. By default
/// rust-rdkafka uses Tokio, via the [`TokioRuntime`], but it has pluggable
/// support for any runtime that can satisfy this trait.
///
/// For an example of using the [smol] runtime with rust-rdkafka, see the
/// [runtime_smol] example.
///
/// For an example of using the [async-std] runtime with rust-rdkafka, see the
/// [runtime_async_std] example.
///
/// [smol]: https://docs.rs/smol
/// [async-std]: https://docs.rs/async-std
/// [runtime_smol]: https://github.com/fede1024/rust-rdkafka/tree/master/examples/runtime_smol.rs
/// [runtime_async_std]: https://github.com/fede1024/rust-rdkafka/tree/master/examples/runtime_async_std.rs
pub trait AsyncRuntime: Send + Sync + 'static {
    /// The type of the future returned by
    /// [`delay_for`](AsyncRuntime::delay_for).
    type Delay: Future<Output = ()> + Send;

    /// Spawns an asynchronous task.
    ///
    /// The task should be be polled to completion, unless the runtime exits
    /// first. With some runtimes this requires an explicit "detach" step.
    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static;

    /// Constructs a future that will resolve after `duration` has elapsed.
    fn delay_for(duration: Duration) -> Self::Delay;
}

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, and the [`NaiveRuntime`] otherwise.
#[cfg(not(feature = "tokio"))]
pub type DefaultRuntime = NaiveRuntime;

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, and the [`NaiveRuntime`] otherwise.
#[cfg(feature = "tokio")]
pub type DefaultRuntime = TokioRuntime;

/// An [`AsyncRuntime`] implementation backed by the executor in the
/// [futures](futures) crate.
///
/// This runtime should not be used when performance is a concern, as it makes
/// heavy use of threads to compenstate for the lack of a timer in the futures
/// executor.
pub struct NaiveRuntime;

impl AsyncRuntime for NaiveRuntime {
    type Delay = Map<oneshot::Receiver<()>, fn(Result<(), oneshot::Canceled>)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        thread::spawn(|| futures::executor::block_on(task));
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        let (tx, rx) = oneshot::channel();
        thread::spawn(move || {
            thread::sleep(duration);
            tx.send(())
        });
        rx.map(|_| ())
    }
}

/// An [`AsyncRuntime`] implementation backed by [Tokio](tokio).
///
/// This runtime is used by default throughout the crate, unless the `tokio`
/// feature is disabled.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct TokioRuntime;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl AsyncRuntime for TokioRuntime {
    type Delay = tokio::time::Sleep;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        tokio::time::sleep(duration)
    }
}
