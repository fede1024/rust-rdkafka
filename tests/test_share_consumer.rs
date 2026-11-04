//! Public-surface tests for the KIP-932 share consumer scaffolding.
//!
//! These tests deliberately do not require a broker: the
//! `BaseShareConsumer` is a stub until librdkafka exposes a public
//! share consumer C API. The goal is to lock in the shape of the public
//! API and prove that runtime methods uniformly return
//! `KafkaError::Unsupported`, so the future FFI swap is mechanical.

#![cfg(feature = "kip-932")]

use std::time::Duration;

use rdkafka::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::share::{
    AcknowledgeType, AcknowledgementMode, AutoOffsetReset, BaseShareConsumer,
    DefaultShareConsumerContext, IsolationLevel, ShareConsumer, ShareConsumerConfig,
};
use rdkafka::error::KafkaError;

fn make_config() -> ClientConfig {
    let mut config = ShareConsumerConfig::new();
    config
        .group_id("share-consumer-smoke")
        .acknowledgement_mode(AcknowledgementMode::Explicit)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .set("bootstrap.servers", "localhost:9092");
    config.into_client_config().expect("group.id is set")
}

fn assert_unsupported<T: std::fmt::Debug>(result: Result<T, KafkaError>) {
    match result {
        Err(KafkaError::Unsupported(reason)) => {
            assert!(
                reason.contains("KIP-932"),
                "expected KIP-932 marker in reason, got {reason:?}"
            );
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[test]
fn config_writes_canonical_keys() {
    let config = make_config();
    let map = config.config_map();
    assert_eq!(map.get("group.id").copied(), Some("share-consumer-smoke"));
    assert_eq!(
        map.get("share.acknowledgement.mode").copied(),
        Some("explicit")
    );
    assert_eq!(
        map.get("share.auto.offset.reset").copied(),
        Some("earliest")
    );
    assert_eq!(
        map.get("share.isolation.level").copied(),
        Some("read_committed")
    );
    assert_eq!(
        map.get("group.share.delivery.attempt.limit").copied(),
        Some("5")
    );
    assert_eq!(
        map.get("group.share.record.lock.duration.ms").copied(),
        Some("30000")
    );
    assert_eq!(
        map.get("bootstrap.servers").copied(),
        Some("localhost:9092")
    );
}

#[test]
fn construction_round_trip() {
    let config = make_config();
    let consumer = BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&config).unwrap();
    assert_eq!(consumer.group_id(), "share-consumer-smoke");
}

#[test]
fn construction_with_context() {
    let config = make_config();
    let consumer =
        BaseShareConsumer::from_config_and_context(&config, DefaultShareConsumerContext).unwrap();
    assert_eq!(consumer.group_id(), "share-consumer-smoke");
}

#[test]
fn runtime_methods_return_unsupported() {
    let config = make_config();
    let consumer = BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&config).unwrap();

    assert_unsupported(consumer.subscribe(&["topic-a", "topic-b"]));
    assert_unsupported(consumer.unsubscribe());
    assert_unsupported(consumer.subscription());
    assert_unsupported(consumer.poll(Duration::from_millis(10)));
    assert_unsupported(consumer.acknowledge_offset("topic-a", 0, 42, AcknowledgeType::Accept));
    assert_unsupported(consumer.commit_sync(Duration::from_millis(10)));
    assert_unsupported(consumer.commit_async());
}

#[test]
fn close_and_wakeup_are_safe_no_ops() {
    let config = make_config();
    let consumer = BaseShareConsumer::<DefaultShareConsumerContext>::from_config(&config).unwrap();
    consumer.wakeup();
    consumer.close().expect("close stub returns Ok");
}

#[test]
fn acknowledge_type_string_round_trip() {
    for ack in [
        AcknowledgeType::Accept,
        AcknowledgeType::Release,
        AcknowledgeType::Reject,
    ] {
        let s = ack.to_string();
        let parsed: AcknowledgeType = s.parse().unwrap();
        assert_eq!(parsed, ack);
    }
}
