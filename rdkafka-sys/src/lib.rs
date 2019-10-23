//! # rdkafka-sys
//!
//! Low level bindings to [librdkafka](https://github.com/edenhill/librdkafka).
//!
//! ## Bindings
//!
//! To regenerate the bindings:
//!
//! ``` bash
//! git submodule update --init
//! cargo install bindgen --vers 0.30.0
//! bindgen --builtins --no-doc-comments librdkafka/src/rdkafka.h -o src/bindings/{platform}.rs
//! ```
//!
//! ## Version
//!
//! The rdkafka-sys version number is in the format `X.Y.Z-P`, where `X.Y.Z`
//! corresponds to the librdkafka version, and `P` indicates the version of the
//! rust bindings.
//!
//! ## Build
//!
//! By default a submodule with the librdkafka sources pinned to a specific commit will
//! be used to compile and statically link the library.
//!
//! The `dynamic_linking` feature can be used to link rdkafka to a locally installed
//! version of librdkafka: if the feature is enabled, the build script will use `pkg-config`
//! to check the version of the library installed in the system, and it will configure the
//! compiler to use dynamic linking.
//!
//! The build process is defined in [`build.rs`].
//!
//! [`build.rs`]: https://github.com/fede1024/rust-rdkafka/blob/master/rdkafka-sys/build.rs
//!
//! ## Updating
//!
//! To upgrade change the git submodule in `librdkafka`, check if new errors
//! need to be added to `helpers::primive_to_rd_kafka_resp_err_t` and update
//! the version in `Cargo.toml`.

#[cfg(feature = "ssl")]
extern crate openssl_sys;

extern crate libz_sys;

#[allow(non_camel_case_types, non_upper_case_globals, non_snake_case, clippy::all)]
pub mod bindings;
pub mod helpers;
pub mod types;

pub use bindings::*;
pub use helpers::*;
pub use types::*;
