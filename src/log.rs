//! A wrapper module to export logging functionality from
//! [`log`] or [`tracing`] depending on the `tracing` feature.
//!
//! [`log`]: https://docs.rs/log
//! [`tracing`]: https://docs.rs/tracing

#[cfg(not(feature = "tracing"))]
pub use log::Level::{Debug as DEBUG, Info as INFO, Warn as WARN};
#[cfg(not(feature = "tracing"))]
pub use log::{debug, error, info, log_enabled, trace, warn};

#[cfg(feature = "tracing")]
pub use tracing::{debug, enabled as log_enabled, error, info, trace, warn};
#[cfg(feature = "tracing")]
pub const DEBUG: tracing::Level = tracing::Level::DEBUG;
#[cfg(feature = "tracing")]
pub const INFO: tracing::Level = tracing::Level::INFO;
#[cfg(feature = "tracing")]
pub const WARN: tracing::Level = tracing::Level::WARN;
