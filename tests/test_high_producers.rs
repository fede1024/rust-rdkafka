//! Test data production using high level producers.
extern crate futures;
extern crate rand;
extern crate rdkafka;

use futures::Future;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::future_producer::FutureRecord;
use rdkafka::producer::FutureProducer;

use std::error::Error;

#[test]
fn test_future_producer_send_fail() {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create::<FutureProducer>()
        .expect("Failed to create producer");

    let future = producer.send(
        FutureRecord::to("topic")
            .payload("payload")
            .key("key")
            .partition(100) // Fail
            .headers(
                OwnedHeaders::new()
                    .add("0", "A")
                    .add("1", "B")
                    .add("2", "C"),
            ),
        10000,
    );

    match future.wait() {
        Ok(Err((kafka_error, owned_message))) => {
            assert_eq!(kafka_error.description(), "Message production error");
            assert_eq!(owned_message.topic(), "topic");
            let headers = owned_message.headers().unwrap();
            assert_eq!(headers.count(), 3);
            assert_eq!(headers.get_as::<str>(0).unwrap(), ("0", Ok("A")));
            assert_eq!(headers.get_as::<str>(1).unwrap(), ("1", Ok("B")));
            assert_eq!(headers.get_as::<str>(2).unwrap(), ("2", Ok("C")));
        }
        e => {
            panic!("Unexpected return value: {:?}", e);
        }
    }
}
