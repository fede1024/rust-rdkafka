//! Client and broker statistics.
//!
//! These statistics are collected automatically by librdkafka when the client
//! is configured with a non-zero `statistics.interval.ms`. They are made
//! available via the [`ClientContext::stats`] callback.
//!
//! Refer to the [librdkafka statistics documentation][librdkafka-stats] for
//! details.
//!
//! [`ClientContext::stats`]: crate::ClientContext::stats
//! [librdkafka-stats]: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md

use std::collections::HashMap;

use serde::Deserialize;

/// Overall statistics.
#[derive(Deserialize, Debug, Default)]
pub struct Statistics {
    /// The name of the librdkafka handle.
    pub name: String,
    /// The configured `client.id`.
    pub client_id: String,
    /// The instance type (producer or consumer).
    #[serde(rename = "type")]
    pub client_type: String,
    /// The current value of librdkafka's internal monotonic clock, in
    // microseconds since start.
    pub ts: i64,
    /// Wall clock time, in seconds since the Unix epoch.
    pub time: i64,
    /// Time since this client instance was created, in microseconds.
    pub age: i64,
    /// The number of operations (callbacks, events, etc.) waiting in queue.
    pub replyq: i64,
    /// The current number of messages in producer queues.
    pub msg_cnt: u64,
    /// The current total size of messages in producer queues.
    pub msg_size: u64,
    /// The maximum number of messages allowed in the producer queues.
    pub msg_max: u64,
    /// The maximum total size of messages allowed in the producer queues.
    pub msg_size_max: u64,
    /// The total number of requests sent to brokers.
    pub tx: i64,
    /// The total number of bytes transmitted to brokers.
    pub tx_bytes: i64,
    /// The total number of responses received from brokers.
    pub rx: i64,
    /// The total number of bytes received from brokers.
    pub rx_bytes: i64,
    /// The total number of messages transmitted (produced) to brokers.
    pub txmsgs: i64,
    /// The total number of bytes transmitted (produced) to brokers.
    pub txmsg_bytes: i64,
    /// The total number of messages consumed from brokers, not including
    /// ignored messages.
    pub rxmsgs: i64,
    /// The total number of bytes (including framing) consumed from brokers.
    pub rxmsg_bytes: i64,
    /// Internal tracking of legacy vs. new consumer API state.
    pub simple_cnt: i64,
    /// Number of topics in the metadata cache.
    pub metadata_cache_cnt: i64,
    /// Per-broker statistics.
    pub brokers: HashMap<String, Broker>,
    /// Per-topic statistics.
    pub topics: HashMap<String, Topic>,
    /// Consumer group statistics.
    pub cgrp: Option<ConsumerGroup>,
    /// Exactly-once semantics and idempotent producer statistics.
    pub eos: Option<ExactlyOnceSemantics>,
}

/// Per-broker statistics.
#[derive(Deserialize, Debug, Default)]
pub struct Broker {
    /// The broker hostname, port, and ID, in the form `HOSTNAME:PORT/ID`.
    pub name: String,
    /// The broker ID (-1 for bootstraps).
    pub nodeid: i32,
    /// The broker hostname and port.
    pub nodename: String,
    /// The broker source (learned, configured, internal, or logical).
    pub source: String,
    /// The broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY,
    /// AUTH_HANDSHAKE, UP, UPDATE).
    pub state: String,
    /// The time since the last broker state change, in microseconds.
    pub stateage: i64,
    /// The number of requests awaiting transmission to the broker.
    pub outbuf_cnt: i64,
    /// The number of messages awaiting transmission to the broker.
    pub outbuf_msg_cnt: i64,
    /// The number of requests in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_cnt: i64,
    /// The number of messages in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_msg_cnt: i64,
    /// The total number of requests sent to the broker.
    pub tx: u64,
    /// The total number of bytes sent to the broker.
    pub txbytes: u64,
    /// The total number of transmission errors.
    pub txerrs: u64,
    /// The total number of request retries.
    pub txretries: u64,
    /// Microseconds since last socket send, or -1 if no sends yet for the
    /// current connection.
    pub txidle: i64,
    /// The total number of requests that timed out.
    pub req_timeouts: u64,
    /// The total number of responses received from the broker.
    pub rx: u64,
    /// The total number of bytes received from the broker.
    pub rxbytes: u64,
    /// The total number of receive errors.
    pub rxerrs: u64,
    /// The number of unmatched correlation IDs in response, typically for
    /// timed out requests.
    pub rxcorriderrs: u64,
    /// The total number of partial message sets received. The broker may return
    /// partial responses if the full message set could not fit in the remaining
    /// fetch response size.
    pub rxpartial: u64,
    /// Microseconds since last socket receive, or -1 if no receives yet for the
    /// current connection.
    pub rxidle: i64,
    /// Request type counters. The object key is the name of the request type
    /// and the value is the number of requests of that type that have been
    /// sent.
    pub req: HashMap<String, i64>,
    /// The total number of decompression buffer size increases.
    pub zbuf_grow: u64,
    /// The total number of buffer size increases (deprecated and unused).
    pub buf_grow: u64,
    /// The number of broker thread poll wakeups.
    pub wakeups: Option<u64>,
    /// The number of connection attempts, including successful and failed
    /// attempts, and name resolution failures.
    pub connects: Option<i64>,
    /// The number of disconnections, whether triggered by the broker, the
    /// network, the load balancer, or something else.
    pub disconnects: Option<i64>,
    /// Rolling window statistics for the internal producer queue latency, in
    /// microseconds.
    pub int_latency: Option<Window>,
    /// Rolling window statistics for the internal request queue latency, in
    /// microseconds.
    ///
    /// This is the time between when a request is enqueued on the transmit
    /// (outbuf) queue and the time the request is written to the TCP socket.
    /// Additional buffering and latency may be incurred by the TCP stack and
    /// network.
    pub outbuf_latency: Option<Window>,
    /// Rolling window statistics for the broker latency/round-trip time,
    /// in microseconds.
    pub rtt: Option<Window>,
    /// Rolling window statistics for the broker throttling time, in
    /// milliseconds.
    pub throttle: Option<Window>,
    /// The partitions that are handled by this broker handle.
    pub toppars: HashMap<String, TopicPartition>,
}

/// Rolling window statistics.
///
/// These values are not exact; they are sampled estimates maintained by an
/// HDR histogram in librdkafka.
#[derive(Deserialize, Debug, Default)]
pub struct Window {
    /// The smallest value.
    pub min: i64,
    /// The largest value.
    pub max: i64,
    /// The mean value.
    pub avg: i64,
    /// The sum of all values.
    pub sum: i64,
    /// The total number of values.
    pub cnt: i64,
    /// The standard deviation.
    pub stddev: i64,
    /// The memory size of the underlying HDR histogram.
    pub hdrsize: i64,
    /// The 50th percentile.
    pub p50: i64,
    /// The 75th percentile.
    pub p75: i64,
    /// The 90th percentile.
    pub p90: i64,
    /// The 95th percentile.
    pub p95: i64,
    /// The 99th percentile.
    pub p99: i64,
    /// The 99.99th percentile.
    pub p99_99: i64,
    /// The number of values not included in the underlying histogram because
    /// they were out of range.
    pub outofrange: i64,
}

/// A topic and partition specifier.
#[derive(Deserialize, Debug, Default)]
pub struct TopicPartition {
    /// The name of the topic.
    pub topic: String,
    /// The ID of the partition.
    pub partition: i32,
}

/// Per-topic statistics.
#[derive(Deserialize, Debug, Default)]
pub struct Topic {
    /// The name of the topic.
    pub topic: String,
    /// The age of the client's metadata for this topic, in milliseconds.
    pub metadata_age: i64,
    /// Rolling window statistics for batch sizes, in bytes.
    pub batchsize: Window,
    /// Rolling window statistics for batch message counts.
    pub batchcnt: Window,
    /// Per-partition statistics.
    pub partitions: HashMap<i32, Partition>,
}

/// Per-partition statistics.
#[derive(Deserialize, Debug, Default)]
pub struct Partition {
    /// The partition ID.
    pub partition: i32,
    /// The ID of the broker from which messages are currently being fetched.
    pub broker: i32,
    /// The broker ID of the leader.
    pub leader: i32,
    /// Whether the partition is explicitly desired by the application.
    pub desired: bool,
    /// Whether the partition is not seen in the topic metadata from the broker.
    pub unknown: bool,
    /// The number of messages waiting to be produced in the first-level queue.
    pub msgq_cnt: i64,
    /// The number of bytes waiting to be produced in the first-level queue.
    pub msgq_bytes: u64,
    /// The number of messages ready to be produced in the transmit queue.
    pub xmit_msgq_cnt: i64,
    /// The number of bytes ready to be produced in the transmit queue.
    pub xmit_msgq_bytes: u64,
    /// The number of prefetched messages in the fetch queue.
    pub fetchq_cnt: i64,
    /// The number of bytes in the fetch queue.
    pub fetchq_size: u64,
    /// The consumer fetch state for this partition (none, stopping, stopped,
    /// offset-query, offset-wait, active).
    pub fetch_state: String,
    /// The current/last logical offset query.
    pub query_offset: i64,
    /// The next offset to fetch.
    pub next_offset: i64,
    /// The offset of the last message passed to the application, plus one.
    pub app_offset: i64,
    /// The offset to be committed.
    pub stored_offset: i64,
    /// The last committed offset.
    pub committed_offset: i64,
    /// The last offset for which partition EOF was signaled.
    pub eof_offset: i64,
    /// The low watermark offset on the broker.
    pub lo_offset: i64,
    /// The high watermark offset on the broker.
    pub hi_offset: i64,
    /// The last stable offset on the broker.
    pub ls_offset: i64,
    /// The difference between `hi_offset` and `committed_offset`.
    pub consumer_lag: i64,
    /// The difference between `hi_offset` and `stored_offset`.
    pub consumer_lag_stored: i64,
    /// The total number of messages transmitted (produced).
    pub txmsgs: u64,
    /// The total number of bytes transmitted (produced).
    pub txbytes: u64,
    /// The total number of messages consumed, not included ignored messages.
    pub rxmsgs: u64,
    /// The total bytes consumed.
    pub rxbytes: u64,
    /// The total number of messages received, for consumers, or the total
    /// number of messages produced, for producers.
    pub msgs: u64,
    /// The number of dropped outdated messages.
    pub rx_ver_drops: u64,
    /// The current number of messages in flight to or from the broker.
    pub msgs_inflight: i64,
    /// The next expected acked sequence number, for idempotent producers.
    pub next_ack_seq: i64,
    /// The next expected errored sequence number, for idempotent producers.
    pub next_err_seq: i64,
    /// The last acked internal message ID, for idempotent producers.
    pub acked_msgid: u64,
}

/// Consumer group manager statistics.
#[derive(Deserialize, Debug, Default)]
pub struct ConsumerGroup {
    /// The local consumer group handler's state.
    pub state: String,
    /// The time elapsed since the last state change, in milliseconds.
    pub stateage: i64,
    /// The local consumer group hander's join state.
    pub join_state: String,
    /// The time elapsed since the last rebalance (assign or revoke), in
    /// milliseconds.
    pub rebalance_age: i64,
    /// The total number of rebalances (assign or revoke).
    pub rebalance_cnt: i64,
    /// The reason for the last rebalance.
    ///
    /// This string will be empty if no rebalances have occurred.
    pub rebalance_reason: String,
    /// The partition count for the current assignment.
    pub assignment_size: i32,
}

/// Exactly-once semantics statistics.
#[derive(Deserialize, Debug, Default)]
pub struct ExactlyOnceSemantics {
    /// The current idempotent producer state.
    pub idemp_state: String,
    /// THe time elapsed since the last idempotent producer state change, in
    /// milliseconds.
    pub idemp_stateage: i64,
    /// The current transactional producer state.
    pub txn_state: String,
    /// The time elapsed since the last transactional producer state change, in
    /// milliseconds.
    pub txn_stateage: i64,
    /// Whether the transactional state allows enqueing (producing) new
    /// messages.
    pub txn_may_enq: bool,
    /// The currently assigned producer ID, or -1.
    pub producer_id: i64,
    /// The current epoch, or -1.
    pub producer_epoch: i64,
    /// The number of producer ID assignments.
    pub epoch_cnt: i64,
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn test_statistics() {
        let stats: Statistics = serde_json::from_str(EXAMPLE).unwrap();

        assert_eq!(stats.name, "rdkafka#producer-1");
        assert_eq!(stats.client_type, "producer");
        assert_eq!(stats.ts, 1163982743268);
        assert_eq!(stats.time, 1589652530);
        assert_eq!(stats.replyq, 0);
        assert_eq!(stats.msg_cnt, 320);
        assert_eq!(stats.msg_size, 9920);
        assert_eq!(stats.msg_max, 500000);
        assert_eq!(stats.msg_size_max, 1073741824);
        assert_eq!(stats.simple_cnt, 0);

        assert_eq!(stats.brokers.len(), 1);

        let broker = stats.brokers.values().into_iter().collect::<Vec<_>>()[0];

        assert_eq!(
            broker.req,
            hashmap! {
                "Produce".to_string() => 31307,
                "Offset".to_string() => 0,
                "Metadata".to_string() => 2,
                "FindCoordinator".to_string() => 0,
                "SaslHandshake".to_string() => 0,
                "ApiVersion".to_string() => 2,
                "InitProducerId".to_string() => 0,
                "AddPartitionsToTxn".to_string() => 0,
                "AddOffsetsToTxn".to_string() => 0,
                "EndTxn".to_string() => 0,
                "TxnOffsetCommit".to_string() => 0,
                "SaslAuthenticate".to_string() => 0,
            }
        );

        assert_eq!(stats.topics.len(), 1);
    }

    // Example from https://github.com/edenhill/librdkafka/wiki/Statistics
    const EXAMPLE: &'static str = r#"
      {
        "name": "rdkafka#producer-1",
        "client_id": "rdkafka",
        "type": "producer",
        "ts": 1163982743268,
        "time": 1589652530,
        "age": 5,
        "replyq": 0,
        "msg_cnt": 320,
        "msg_size": 9920,
        "msg_max": 500000,
        "msg_size_max": 1073741824,
        "simple_cnt": 0,
        "metadata_cache_cnt": 1,
        "brokers": {
          "localhost:9092/0": {
            "name": "localhost:9092/0",
            "nodeid": 0,
            "nodename": "localhost:9092",
            "source": "configured",
            "state": "UP",
            "stateage": 8005652,
            "outbuf_cnt": 0,
            "outbuf_msg_cnt": 0,
            "waitresp_cnt": 1,
            "waitresp_msg_cnt": 126,
            "tx": 31311,
            "txbytes": 463869957,
            "txerrs": 0,
            "txretries": 0,
            "txidle": 5,
            "req_timeouts": 0,
            "rx": 31310,
            "rxbytes": 1753668,
            "rxerrs": 0,
            "rxcorriderrs": 0,
            "rxpartial": 0,
            "rxidle": 5,
            "zbuf_grow": 0,
            "buf_grow": 0,
            "wakeups": 131568,
            "connects": 1,
            "disconnects": 0,
            "int_latency": {
              "min": 2,
              "max": 9193,
              "avg": 605,
              "sum": 874202325,
              "stddev": 1080,
              "p50": 319,
              "p75": 481,
              "p90": 1135,
              "p95": 3023,
              "p99": 5919,
              "p99_99": 9087,
              "outofrange": 0,
              "hdrsize": 15472,
              "cnt": 1443154
            },
            "outbuf_latency": {
              "min": 1,
              "max": 308,
              "avg": 22,
              "sum": 107311,
              "stddev": 21,
              "p50": 22,
              "p75": 29,
              "p90": 36,
              "p95": 44,
              "p99": 111,
              "p99_99": 309,
              "outofrange": 0,
              "hdrsize": 11376,
              "cnt": 4740
            },
            "rtt": {
              "min": 94,
              "max": 3279,
              "avg": 237,
              "sum": 1124867,
              "stddev": 198,
              "p50": 193,
              "p75": 245,
              "p90": 329,
              "p95": 393,
              "p99": 1183,
              "p99_99": 3279,
              "outofrange": 0,
              "hdrsize": 13424,
              "cnt": 4739
            },
            "throttle": {
              "min": 0,
              "max": 0,
              "avg": 0,
              "sum": 0,
              "stddev": 0,
              "p50": 0,
              "p75": 0,
              "p90": 0,
              "p95": 0,
              "p99": 0,
              "p99_99": 0,
              "outofrange": 0,
              "hdrsize": 17520,
              "cnt": 4739
            },
            "req": {
              "Produce": 31307,
              "Offset": 0,
              "Metadata": 2,
              "FindCoordinator": 0,
              "SaslHandshake": 0,
              "ApiVersion": 2,
              "InitProducerId": 0,
              "AddPartitionsToTxn": 0,
              "AddOffsetsToTxn": 0,
              "EndTxn": 0,
              "TxnOffsetCommit": 0,
              "SaslAuthenticate": 0
            },
            "toppars": {
              "test-0": {
                "topic": "test",
                "partition": 0
              },
              "test-1": {
                "topic": "test",
                "partition": 1
              },
              "test-2": {
                "topic": "test",
                "partition": 2
              }
            }
          }
        },
        "topics": {
          "test": {
            "topic": "test",
            "metadata_age": 7014,
            "batchsize": {
              "min": 99,
              "max": 240276,
              "avg": 11871,
              "sum": 56260370,
              "stddev": 13137,
              "p50": 10431,
              "p75": 11583,
              "p90": 12799,
              "p95": 13823,
              "p99": 72191,
              "p99_99": 240639,
              "outofrange": 0,
              "hdrsize": 14448,
              "cnt": 4739
            },
            "batchcnt": {
              "min": 1,
              "max": 6161,
              "avg": 304,
              "sum": 1442353,
              "stddev": 336,
              "p50": 267,
              "p75": 297,
              "p90": 329,
              "p95": 353,
              "p99": 1847,
              "p99_99": 6175,
              "outofrange": 0,
              "hdrsize": 8304,
              "cnt": 4739
            },
            "partitions": {
              "0": {
                "partition": 0,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 845,
                "msgq_bytes": 26195,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3950967,
                "txbytes": 122479977,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3951812,
                "rx_ver_drops": 0,
                "msgs_inflight": 1067,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "1": {
                "partition": 1,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 229,
                "msgq_bytes": 7099,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3950656,
                "txbytes": 122470336,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3952618,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "2": {
                "partition": 2,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 1816,
                "msgq_bytes": 56296,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3952027,
                "txbytes": 122512837,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3953855,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "-1": {
                "partition": -1,
                "broker": -1,
                "leader": -1,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 0,
                "msgq_bytes": 0,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 0,
                "txbytes": 0,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 500000,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              }
            }
          }
        },
        "tx": 31311,
        "tx_bytes": 463869957,
        "rx": 31310,
        "rx_bytes": 1753668,
        "txmsgs": 11853650,
        "txmsg_bytes": 367463150,
        "rxmsgs": 0,
        "rxmsg_bytes": 0
      }"#;
}
