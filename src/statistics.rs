// TODO: add documentation for all fields
#![allow(missing_docs)]

use std::collections::HashMap;

/// Statistics from librdkafka. Refer to the [librdkafka documentation](https://github.com/edenhill/librdkafka/wiki/Statistics)
/// for details.
#[derive(Deserialize, Debug)]
pub struct Statistics {
    pub name: String,
    #[serde(rename = "type")]
    pub client_type: String,
    pub ts: i64,
    pub time: i64,
    pub replyq: i64,
    pub msg_cnt: i64,
    pub msg_size: i64,
    pub msg_max: i64,
    pub msg_size_max: i64,
    pub simple_cnt: i64,
    pub brokers: HashMap<String, Broker>,
    pub topics: HashMap<String, Topic>,
    pub cgrp: Option<ConsumerGroup>,
}

#[derive(Deserialize, Debug)]
pub struct Broker {
    pub name: String,
    pub nodeid: i32,
    pub state: String,
    pub stateage: i64,
    pub outbuf_cnt: i64,
    pub outbuf_msg_cnt: i64,
    pub waitresp_cnt: i64,
    pub waitresp_msg_cnt: i64,
    pub tx: i64,
    pub txbytes: i64,
    pub txerrs: i64,
    pub txretries: i64,
    pub req_timeouts: i64,
    pub rx: i64,
    pub rxbytes: i64,
    pub rxerrs: i64,
    pub rxcorriderrs: i64,
    pub rxpartial: i64,
    pub zbuf_grow: i64,
    pub buf_grow: i64,
    pub wakeups: Option<i64>,
    pub int_latency: Option<Window>,
    pub rtt: Option<Window>,
    pub throttle: Option<Window>,
    pub toppars: HashMap<String, TopicPartition>,
}

#[derive(Deserialize, Debug)]
pub struct Window {
    pub min: i64,
    pub max: i64,
    pub avg: i64,
    pub sum: i64,
    pub cnt: i64,
}

#[derive(Deserialize, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Deserialize, Debug)]
pub struct Topic {
    pub topic: String,
    pub metadata_age: i64,
    pub partitions: HashMap<i32, Partition>,
}

#[derive(Deserialize, Debug)]
pub struct Partition {
    pub partition: i32,
    pub leader: i32,
    pub desired: bool,
    pub unknown: bool,
    pub msgq_cnt: i64,
    pub msgq_bytes: i64,
    pub xmit_msgq_cnt: i64,
    pub xmit_msgq_bytes: i64,
    pub fetchq_cnt: i64,
    pub fetchq_size: i64,
    pub fetch_state: String,
    pub query_offset: i64,
    pub next_offset: i64,
    pub app_offset: i64,
    pub stored_offset: i64,
    pub committed_offset: i64,
    pub eof_offset: i64,
    pub lo_offset: i64,
    pub hi_offset: i64,
    pub consumer_lag: i64,
    pub txmsgs: i64,
    pub txbytes: i64,
    pub msgs: i64,
    pub rx_ver_drops: i64,
}

#[derive(Deserialize, Debug)]
pub struct ConsumerGroup {
    pub rebalance_age: i64,
    pub rebalance_cnt: i64,
    pub assignment_size: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_statistics() {
        let stats: Statistics = serde_json::from_str(EXAMPLE).unwrap();

        assert_eq!(stats.name, "rdkafka#consumer-1");
        assert_eq!(stats.client_type, "consumer");
        assert_eq!(stats.ts, 895747604205);
        assert_eq!(stats.time, 1479659343);
        assert_eq!(stats.replyq, 0);
        assert_eq!(stats.msg_cnt, 0);
        assert_eq!(stats.msg_size, 0);
        assert_eq!(stats.msg_max, 0);
        assert_eq!(stats.msg_size_max, 0);
        assert_eq!(stats.simple_cnt, 0);

        assert_eq!(stats.brokers.len(), 4);
        assert_eq!(stats.topics.len(), 1);
    }

    // Example from https://github.com/edenhill/librdkafka/wiki/Statistics
    const EXAMPLE: &'static str = r#"
        {
          "name": "rdkafka#consumer-1",
          "type": "consumer",
          "ts": 895747604205,
          "time": 1479659343,
          "replyq": 0,
          "msg_cnt": 0,
          "msg_size": 0,
          "msg_max": 0,
          "msg_size_max": 0,
          "simple_cnt": 0,
          "brokers": {
            "0:9092/bootstrap": {
              "name": "0:9092/bootstrap",
              "nodeid": -1,
              "state": "UP",
              "stateage": 5989882,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 0,
              "waitresp_msg_cnt": 0,
              "tx": 2,
              "txbytes": 56,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 2,
              "rxbytes": 31692,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 0,
              "int_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "toppars": {}
            },
            "localhost:9092/2": {
              "name": "localhost:9092/2",
              "nodeid": 2,
              "state": "UP",
              "stateage": 5958663,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 1,
              "waitresp_msg_cnt": 0,
              "tx": 54,
              "txbytes": 3650,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 53,
              "rxbytes": 89546,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 0,
              "int_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 721,
                "max": 106064,
                "avg": 87530,
                "sum": 1925664,
                "cnt": 22
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 19
              },
              "toppars": {
                "test-1": {
                  "topic": "test",
                  "partition": 1
                }
              }
            },
            "localhost:9094/4": {
              "name": "localhost:9094/4",
              "nodeid": 4,
              "state": "UP",
              "stateage": 5958663,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 1,
              "waitresp_msg_cnt": 0,
              "tx": 40,
              "txbytes": 3042,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 39,
              "rxbytes": 87058,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 0,
              "int_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 100169,
                "max": 101198,
                "avg": 100730,
                "sum": 2014600,
                "cnt": 20
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 20
              },
              "toppars": {
                "test-3": {
                  "topic": "test",
                  "partition": 3
                },
                "test-0": {
                  "topic": "test",
                  "partition": 0
                }
              }
            },
            "localhost:9093/3": {
              "name": "localhost:9093/3",
              "nodeid": 3,
              "state": "UP",
              "stateage": 5958647,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 1,
              "waitresp_msg_cnt": 0,
              "tx": 44,
              "txbytes": 2688,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 43,
              "rxbytes": 90161,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 0,
              "int_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 99647,
                "max": 101254,
                "avg": 100612,
                "sum": 2012247,
                "cnt": 20
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "cnt": 20
              },
              "toppars": {
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
              "metadata_age": 4957,
              "partitions": {
                "0": {
                  "partition": 0,
                  "leader": 4,
                  "desired": true,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "active",
                  "query_offset": -2,
                  "next_offset": 427,
                  "app_offset": 427,
                  "stored_offset": 427,
                  "commited_offset": 427,
                  "committed_offset": 427,
                  "eof_offset": 427,
                  "lo_offset": -1001,
                  "hi_offset": 427,
                  "consumer_lag": 0,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "msgs": 0,
                  "rx_ver_drops": 0
                },
                "1": {
                  "partition": 1,
                  "leader": 2,
                  "desired": true,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "active",
                  "query_offset": -2,
                  "next_offset": 436,
                  "app_offset": 436,
                  "stored_offset": 436,
                  "commited_offset": 436,
                  "committed_offset": 436,
                  "eof_offset": 436,
                  "lo_offset": -1001,
                  "hi_offset": 436,
                  "consumer_lag": 0,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "msgs": 0,
                  "rx_ver_drops": 0
                },
                "2": {
                  "partition": 2,
                  "leader": 3,
                  "desired": true,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "active",
                  "query_offset": -2,
                  "next_offset": 458,
                  "app_offset": 458,
                  "stored_offset": 458,
                  "commited_offset": 458,
                  "committed_offset": 458,
                  "eof_offset": 458,
                  "lo_offset": -1001,
                  "hi_offset": 458,
                  "consumer_lag": 0,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "msgs": 0,
                  "rx_ver_drops": 0
                },
                "3": {
                  "partition": 3,
                  "leader": 4,
                  "desired": true,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "active",
                  "query_offset": -2,
                  "next_offset": 497,
                  "app_offset": 497,
                  "stored_offset": 497,
                  "commited_offset": 497,
                  "committed_offset": 497,
                  "eof_offset": 497,
                  "lo_offset": -1001,
                  "hi_offset": 497,
                  "consumer_lag": 0,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "msgs": 0,
                  "rx_ver_drops": 0
                },
                "-1": {
                  "partition": -1,
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
                  "query_offset": 0,
                  "next_offset": 0,
                  "app_offset": -1001,
                  "stored_offset": -1001,
                  "commited_offset": -1001,
                  "committed_offset": -1001,
                  "eof_offset": -1001,
                  "lo_offset": -1001,
                  "hi_offset": -1001,
                  "consumer_lag": -1,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "msgs": 0,
                  "rx_ver_drops": 0
                }
              }
            }
          },
          "cgrp": {
            "rebalance_age": 5251,
            "rebalance_cnt": 2,
            "assignment_size": 4
          }
        }
        "#;
}
