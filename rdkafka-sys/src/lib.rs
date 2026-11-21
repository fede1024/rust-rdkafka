#![doc = include_str!("../README.md")]

#[cfg(feature = "openssl-sys")]
extern crate openssl_sys;

#[cfg(feature = "sasl2-sys")]
extern crate sasl2_sys;

#[cfg(feature = "libz-sys")]
extern crate libz_sys;

#[cfg(any(feature = "curl-sys", feature = "curl-static"))]
extern crate curl_sys;

#[cfg(feature = "zstd-sys")]
extern crate zstd_sys;

#[cfg(feature = "lz4-sys")]
extern crate lz4_sys;

/// FFI bindings.
///
/// These bindings are automatically generated
/// with [bindgen](https://github.com/rust-lang/rust-bindgen).
#[allow(
    non_camel_case_types,
    non_upper_case_globals,
    non_snake_case,
    clippy::all
)]
pub mod bindings;
pub mod helpers;
pub mod types;

pub use bindings::*;
pub use helpers::*;
pub use types::*;
