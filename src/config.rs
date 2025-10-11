//! Producer and consumer configuration.
//!
//! ## C library configuration
//!
//! The Rust library will forward all the configuration to the C library. The
//! most frequently used parameters are listed here.
//!
//! ### Frequently used parameters
//!
//! For producer-specific and consumer-specific parameters check the producer
//! and consumer modules documentation. The full list of available parameters is
//! available in the [librdkafka documentation][librdkafka-config].
//!
//! - `client.id`: Client identifier. Default: `rdkafka`.
//! - `bootstrap.servers`: Initial list of brokers as a CSV list of broker host
//!   or host:port. Default: empty.
//! - `message.max.bytes`: Maximum message size. Default: 1000000.
//! - `debug`: A comma-separated list of debug contexts to enable. Use 'all' to
//!   print all the debugging information. Default: empty (off).
//! - `statistics.interval.ms`: how often the statistic callback
//!   specified in the [`ClientContext`] will be called. Default: 0 (disabled).
//!
//! [librdkafka-config]: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::os::raw::c_char;
use std::ptr;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::ClientContext;
use crate::config_map::Config;
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::log::{log_enabled, DEBUG, INFO, WARN};
use crate::util::{ErrBuf, KafkaDrop, NativePtr};

const SENSITIVE_CONFIG_KEYS: &[&str] = &[
    "sasl.password",
    "ssl.key.password",
    "ssl.keystore.password",
    "ssl.truststore.password",
    "sasl.oauthbearer.client.secret",
];

const SANITIZED_VALUE_PLACEHOLDER: &str = "[sanitized for safety]";

/// The log levels supported by librdkafka.
#[derive(Copy, Clone, Debug)]
pub enum RDKafkaLogLevel {
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Emerg = 0,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Alert = 1,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Critical = 2,
    /// Equivalent to [`Level::Error`](log::Level::Error) from the log crate.
    Error = 3,
    /// Equivalent to [`Level::Warn`](log::Level::Warn) from the log crate.
    Warning = 4,
    /// Higher priority then [`Level::Info`](log::Level::Info) from the log
    /// crate.
    Notice = 5,
    /// Equivalent to [`Level::Info`](log::Level::Info) from the log crate.
    Info = 6,
    /// Equivalent to [`Level::Debug`](log::Level::Debug) from the log crate.
    Debug = 7,
}

impl RDKafkaLogLevel {
    pub(crate) fn from_int(level: i32) -> RDKafkaLogLevel {
        match level {
            0 => RDKafkaLogLevel::Emerg,
            1 => RDKafkaLogLevel::Alert,
            2 => RDKafkaLogLevel::Critical,
            3 => RDKafkaLogLevel::Error,
            4 => RDKafkaLogLevel::Warning,
            5 => RDKafkaLogLevel::Notice,
            6 => RDKafkaLogLevel::Info,
            _ => RDKafkaLogLevel::Debug,
        }
    }
}

//
// ********** CLIENT CONFIG **********
//

/// A native rdkafka-sys client config.
pub struct NativeClientConfig {
    ptr: NativePtr<RDKafkaConf>,
}

unsafe impl KafkaDrop for RDKafkaConf {
    const TYPE: &'static str = "client config";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_conf_destroy;
}

impl NativeClientConfig {
    /// Wraps a pointer to an `RDKafkaConfig` object and returns a new `NativeClientConfig`.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafkaConf) -> NativeClientConfig {
        NativeClientConfig {
            ptr: NativePtr::from_ptr(ptr).unwrap(),
        }
    }

    /// Returns the pointer to the librdkafka RDKafkaConf structure.
    pub fn ptr(&self) -> *mut RDKafkaConf {
        self.ptr.ptr()
    }

    /// Gets the value of a parameter in the configuration.
    ///
    /// This method reflects librdkafka's view of the current value of the
    /// parameter. If the parameter was overridden by the user, it returns the
    /// user-specified value. Otherwise, it returns librdkafka's default value
    /// for the parameter.
    pub fn get(&self, key: &str) -> KafkaResult<String> {
        let make_err = |res| {
            KafkaError::ClientConfig(
                res,
                match res {
                    RDKafkaConfRes::RD_KAFKA_CONF_UNKNOWN => "Unknown configuration name",
                    RDKafkaConfRes::RD_KAFKA_CONF_INVALID => "Invalid configuration value",
                    RDKafkaConfRes::RD_KAFKA_CONF_OK => "OK",
                }
                .into(),
                key.into(),
                "".into(),
            )
        };
        let key_c = CString::new(key.to_string())?;

        // Call with a `NULL` buffer to determine the size of the string.
        let mut size = 0_usize;
        let res = unsafe {
            rdsys::rd_kafka_conf_get(self.ptr(), key_c.as_ptr(), ptr::null_mut(), &mut size)
        };
        if res.is_error() {
            return Err(make_err(res));
        }

        // Allocate a buffer of that size and call again to get the actual
        // string.
        let mut buf = vec![0_u8; size];
        let res = unsafe {
            rdsys::rd_kafka_conf_get(
                self.ptr(),
                key_c.as_ptr(),
                buf.as_mut_ptr() as *mut c_char,
                &mut size,
            )
        };
        if res.is_error() {
            return Err(make_err(res));
        }

        // Convert the C string to a Rust string.
        Ok(String::from_utf8_lossy(&buf)
            .trim_matches(char::from(0))
            .to_string())
    }

    pub(crate) fn set(&self, key: &str, value: &str) -> KafkaResult<()> {
        let mut err_buf = ErrBuf::new();
        let key_c = CString::new(key)?;
        let value_c = CString::new(value)?;
        let ret = unsafe {
            rdsys::rd_kafka_conf_set(
                self.ptr(),
                key_c.as_ptr(),
                value_c.as_ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        if ret.is_error() {
            return Err(KafkaError::ClientConfig(
                ret,
                err_buf.to_string(),
                key.to_string(),
                value.to_string(),
            ));
        }
        Ok(())
    }
}

/// Client configuration.
#[derive(Clone)]
pub struct ClientConfig {
    conf_map: HashMap<String, String>,
    /// The librdkafka logging level. Refer to [`RDKafkaLogLevel`] for the list
    /// of available levels.
    pub log_level: RDKafkaLogLevel,
}

impl Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sanitized: BTreeMap<&str, &str> = self
            .conf_map
            .iter()
            .filter_map(|(key, value)| {
                if SENSITIVE_CONFIG_KEYS.contains(&key.as_str()) {
                    None
                } else {
                    Some((key.as_str(), value.as_str()))
                }
            })
            .collect();

        let mut debug_struct = f.debug_struct("ClientConfig");
        debug_struct.field("log_level", &self.log_level);
        debug_struct.field("conf_map", &sanitized);
        debug_struct.finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientConfig {
    /// Creates a new empty configuration.
    pub fn new() -> ClientConfig {
        ClientConfig {
            conf_map: HashMap::new(),
            log_level: log_level_from_global_config(),
        }
    }

    /// Returns a sanitized view of the underlying config map.
    ///
    /// Sensitive keys have their values replaced with a placeholder string so they never appear in
    /// clear text when inspected.
    pub fn config_map(&self) -> BTreeMap<&str, &str> {
        self.conf_map
            .iter()
            .map(|(key, value)| {
                if SENSITIVE_CONFIG_KEYS.contains(&key.as_str()) {
                    (key.as_str(), SANITIZED_VALUE_PLACEHOLDER)
                } else {
                    (key.as_str(), value.as_str())
                }
            })
            .collect()
    }

    /// Gets the value of a parameter in the configuration.
    ///
    /// Returns the current value set for `key`, or `None` if no value for `key`
    /// exists.
    ///
    /// Note that this method will only ever return values that were installed
    /// by a call to [`ClientConfig::set`]. To retrieve librdkafka's default
    /// value for a parameter, build a [`NativeClientConfig`] and then call
    /// [`NativeClientConfig::get`] on the resulting object.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.conf_map.get(key).map(|val| val.as_str())
    }

    /// Sets a parameter in the configuration.
    ///
    /// If there is an existing value for `key` in the configuration, it is
    /// overridden with the new `value`.
    pub fn set<K, V>(&mut self, map: Config) -> &mut ClientConfig
    where
        K: Into<String>,
        V: Into<String>,
    {
        match map {
            Config::Producer(producer) => match producer {
                crate::config_map::Producer::Acks(ack) => {
                    let acks = match ack {
                        crate::config_map::Acks::All => "all",
                        crate::config_map::Acks::Balanced => "1",
                        crate::config_map::Acks::FastRisky => "0",
                    };
                    self.conf_map.insert("acks".into(), acks.into());
                    self
                }
                crate::config_map::Producer::BatchSize(size) => {
                    self.conf_map.insert("batch.size".into(), size.into());
                    self
                }
                crate::config_map::Producer::Bootstrap(port) => {
                    self.conf_map
                        .insert("bootstrap.servers".into(), port.into());
                    self
                }
                crate::config_map::Producer::BufferMaxMemory(size) => {
                    self.conf_map.insert("buffer.memory".into(), size.into());
                    self
                }
                crate::config_map::Producer::Compression(compression) => {
                    let b = match compression {
                        crate::config_map::Compression::Gzip => "gzip",
                        crate::config_map::Compression::Lz4 => "lz4",
                        crate::config_map::Compression::None => "none",
                        crate::config_map::Compression::Snappy => "snappy",
                        crate::config_map::Compression::Zstd => "zstd",
                    };
                    self.conf_map.insert("compression.type".into(), b.into());
                    self
                }
                crate::config_map::Producer::Delay(b) => {
                    self.conf_map.insert("linger.ms".into(), b.into());
                    self
                }
                crate::config_map::Producer::Idempotence(b) => {
                    self.conf_map.insert("enable.idempotence".into(), b.into());
                    self
                }
                crate::config_map::Producer::MaxMessageSize(size) => {
                    self.conf_map.insert("max.message.size".into(), size.into());
                    self
                }
                crate::config_map::Producer::MultipleBootstrap(port) => {
                    self.conf_map
                        .insert("bootstrap.servers".into(), port.into());
                    self
                }
                crate::config_map::Producer::Retries(b) => {
                    self.conf_map.insert("retries".into(), b.into());
                    self
                }
                crate::config_map::Producer::RetryBackoff(b) => {
                    self.conf_map.insert("retry.backoff.ms".into(), b.into());
                    self
                }
            },
            Config::Consumer(consumer) => match consumer {
                crate::config_map::Producer::Acks(ack) => {
                    let acks = match ack {
                        crate::config_map::Acks::All => "all",
                        crate::config_map::Acks::Balanced => "1",
                        crate::config_map::Acks::FastRisky => "0",
                    };
                    self.conf_map.insert("acks".into(), acks.into());
                    self
                }
                crate::config_map::Consumer::AutoCommit(enable) => {
                    self.conf_map
                        .insert("enable.auto.commit".into(), enable.into());
                    self
                }
                crate::config_map::Consumer::AutoOffsetReset(position) => {
                    let b = match position {
                        crate::config_map::AutoOffsetReset::Earliest => "earliest",
                        crate::config_map::AutoOffsetReset::Latest => "latest",
                        crate::config_map::AutoOffsetReset::None => "none",
                    };
                    self.conf_map.insert("auto.offset.reset".into(), b.into());
                    self
                }
                crate::config_map::Consumer::Bootstrap(port) => {
                    self.conf_map
                        .insert("bootstrap.servers".into(), port.into());
                    self
                }
                crate::config_map::Consumer::CommitInterval(ms) => {
                    self.conf_map
                        .insert("auto.commit.interval.ms".into(), ms.into());
                    self
                }
                crate::config_map::Consumer::Compression(b) => {
                    let b = match compression {
                        crate::config_map::Compression::Gzip => "gzip",
                        crate::config_map::Compression::Lz4 => "lz4",
                        crate::config_map::Compression::None => "none",
                        crate::config_map::Compression::Snappy => "snappy",
                        crate::config_map::Compression::Zstd => "zstd",
                    };
                    self.conf_map.insert("compression.type".into(), b.into());
                    self
                }
                crate::config_map::Consumer::GroupId(id) => {
                    self.conf_map.insert("group.id".into(), id.into());
                    self
                }
                crate::config_map::Consumer::Heartbeat(beat) => {
                    self.conf_map
                        .insert("heartbeat.interval.ms".into(), beat.into());
                    self
                }
                crate::config_map::Consumer::MaxMessagePerPool(msg) => {
                    self.conf_map.insert("max.poll.records".into(), msg.into());
                    self
                }
                crate::config_map::Consumer::MaxTimeWait(time) => {
                    self.conf_map
                        .insert("fetch.max.wait.ms".into(), time.into());
                    self
                }
                crate::config_map::Consumer::MinFetch(msg) => {
                    self.conf_map.insert("fetch.min.bytes".into(), msg.into());
                    self
                }
                crate::config_map::Consumer::SessionTimeout(msg) => {
                    self.conf_map
                        .insert("session.timeout.ms".into(), msg.into());
                    self
                }
            },
        }
    }

    /// Removes a parameter from the configuration.
    pub fn remove<'a>(&'a mut self, key: &str) -> &'a mut ClientConfig {
        self.conf_map.remove(key);
        self
    }

    /// Sets the log level of the client. If not specified, the log level will be calculated based
    /// on the global log level of the log crate.
    pub fn set_log_level(&mut self, log_level: RDKafkaLogLevel) -> &mut ClientConfig {
        self.log_level = log_level;
        self
    }

    /// Builds a native librdkafka configuration.
    pub fn create_native_config(&self) -> KafkaResult<NativeClientConfig> {
        let conf = unsafe { NativeClientConfig::from_ptr(rdsys::rd_kafka_conf_new()) };
        for (key, value) in &self.conf_map {
            conf.set(key, value)?;
        }
        Ok(conf)
    }

    /// Uses the current configuration to create a new Consumer or Producer.
    pub fn create<T: FromClientConfig>(&self) -> KafkaResult<T> {
        T::from_config(self)
    }

    /// Uses the current configuration and the provided context to create a new Consumer or Producer.
    pub fn create_with_context<C, T>(&self, context: C) -> KafkaResult<T>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<C>,
    {
        T::from_config_and_context(self, context)
    }
}

impl FromIterator<(String, String)> for ClientConfig {
    fn from_iter<I>(iter: I) -> ClientConfig
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut config = ClientConfig::new();
        config.extend(iter);
        config
    }
}

impl Extend<(String, String)> for ClientConfig {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.conf_map.extend(iter)
    }
}

/// Return the log level
fn log_level_from_global_config() -> RDKafkaLogLevel {
    if log_enabled!(target: "librdkafka", DEBUG) {
        RDKafkaLogLevel::Debug
    } else if log_enabled!(target: "librdkafka", INFO) {
        RDKafkaLogLevel::Info
    } else if log_enabled!(target: "librdkafka", WARN) {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    }
}

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    /// Creates a client from a client configuration. The default client context
    /// will be used.
    fn from_config(_: &ClientConfig) -> KafkaResult<Self>;
}

/// Create a new client based on the provided configuration and context.
pub trait FromClientConfigAndContext<C: ClientContext>: Sized {
    /// Creates a client from a client configuration and a client context.
    fn from_config_and_context(_: &ClientConfig, _: C) -> KafkaResult<Self>;
}

#[cfg(test)]
mod tests {
    use super::ClientConfig;

    #[test]
    fn test_client_config_set_map() {
        let mut config: ClientConfig = vec![("a".into(), "1".into()), ("b".into(), "1".into())]
            .into_iter()
            .collect();
        config.extend([("b".into(), "2".into()), ("c".into(), "3".into())]);

        assert_eq!(config.get("a").unwrap(), "1");
        assert_eq!(config.get("b").unwrap(), "2");
        assert_eq!(config.get("c").unwrap(), "3");
    }
}
