pub enum Config {
    Producer(Producer),
    Consumer(Consumer),
}

/// congif for producer
pub enum Producer {
    /// Kafka brokers to connect to (comma-separated)
    Bootstrap(&str),
    /// Acknowledgment level (0, 1, all)
    Acks(Acks),
    /// Message compression (none, gzip, snappy, lz4, zstd)
    Compression(Compression),
    /// 	Max batch size in bytes before sending
    BatchSize(BatchSize),
    /// Delay before sending batch (reduce for low latency)
    Delay(u64),
    /// Max memory for buffering messages
    BufferMaxMemory(u64),
    /// Max message size allowed
    MaxMessageSize(u64),
    /// Number of retries for failed sends
    Retries(u64),
    /// Time between retry attempts
    RetryBackoff(u64),
    /// Ensure exactly-once semantics (true, false)
    Idempotence(bool),
    /// Required for transactional messaging
    TrasactionId(&str),
}

/// config for consumer
pub enum Consumer {
    /// Kafka brokers to connect to
    Bootstrap(u64),
    /// Consumer group identifier
    GroupId(&str),
    /// Start position (earliest, latest, none)
    AutoOffsetReset(AutoOffsetReset),
    /// Auto commit offsets (true, false)
    AutoCommit(bool),
    /// Time between auto commits
    CommitInterval(u64),
    /// Max messages returned per poll
    MaxMessagePerPool(u64),
    /// Min data to fetch per request
    MinFetch(u64),
    /// Max time to wait for data
    MaxTimeWait(u64),
    /// Heartbeat interval to the broker
    Heartbeat(u64),
    /// Consumer session timeout
    SessionTimeout(u64),
}

pub enum AutoOffsetReset {
    Earliest,
    Latest,
    None,
}
pub enum Acks {
    FastRisky,
    Balanced,
    All,
}

pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

pub enum BatchSize {
    Default,
    Custom(u64),
}
