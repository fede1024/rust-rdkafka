#[macro_use]
extern crate log;
extern crate clap;
extern crate rand;
extern crate rdkafka;

use clap::{App, Arg};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::BaseProducer;
use rdkafka::Message;

use std::collections::binary_heap::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

mod example_utils;
use example_utils::setup_logger;

use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::ProducerContext;
use rdkafka::topic_partition_list::Offset;
use rdkafka::ClientContext;
use rdkafka::TopicPartitionList;

struct LoggingContext;

impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?} {:?}", offsets, result);
    }
}

type LoggingConsumer = BaseConsumer<LoggingContext>;

fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> LoggingConsumer {
    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set("debug", "consumer,cgrp")
        .create_with_context::<LoggingContext, LoggingConsumer>(LoggingContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}

struct MirrorProducerContext {
    topic_map: TopicOffsetMap,
    source_consumer: Arc<LoggingConsumer>,
}

impl ClientContext for MirrorProducerContext {}
impl ProducerContext for MirrorProducerContext {
    type DeliveryOpaque = TopicPartitionList;

    fn delivery(
        &mut self,
        delivery_result: &Result<BorrowedMessage, (KafkaError, BorrowedMessage)>,
        delivery_opaque: TopicPartitionList,
    ) {
        match delivery_result {
            Ok(m) => {
                debug!("Delivered copy of {:?} to {}", delivery_opaque, m.topic());
                // NOTE(blt) -- Our ambition here is to avoid committing
                // out-of-order offsets for any given topic. The approach taken
                // is to dissect the passed TopicPartitionList, compare against
                // ProducerContext's TopicOffsetMap and use that map to build a
                // `commit_tpl` of contiguous offsets that are safe to commit.
                for ((ref mut topic, ref mut partition), ref mut offset_run) in self.topic_map.iter_mut() {
                    if let Some(tpl_elem) = delivery_opaque.find_partition(topic, *partition) {
                        let offset = tpl_elem.offset();
                        offset_run.offsets.push(offset);
                        if offset < offset_run.floor {
                            offset_run.floor = offset;
                        }
                    }
                }
                // By this point, the `self.topic_map` is updated with the
                // offsets that came in via `delivery_opaque`. We now,
                // potentially, have 'runs' that can be comitted to the
                // consumer, which we build up into `commit_tpl`.
                let mut commit_tpl = TopicPartitionList::new();
                for ((ref topic, ref partition), ref mut offset_run) in self.topic_map.iter_mut() {
                    loop {
                        let mut do_pop = false;
                        let mut do_break = false;
                        if let Some(min) = offset_run.offsets.peek_mut() {
                            if *min == offset_run.floor {
                                offset_run.floor = *min;
                                let elem = commit_tpl.add_partition(topic, *partition);
                                elem.set_offset(*min);
                                do_pop = true;
                            }
                        } else {
                            do_break = true;
                        }
                        // These kind of wacky clauses down here are to avoid
                        // double-borrowing on offset_run.
                        if do_pop {
                            let _ = offset_run.offsets.pop().unwrap();
                        }
                        if do_break {
                            break;
                        }
                    }
                }
                while let Err(_) = self.source_consumer.store_offset_list(&commit_tpl) {
                    // TODO(blt) -- backoff
                }
            }
            Err((e, m)) => {
                // Question(blt) -- Okay, we've failed to deliver here. Now
                // what? The rustdocs say this:
                //
                // > In both success or failure scenarios, the payload of the
                // > message resides in the buffer of the producer and will be
                // > automatically removed once the delivery callback finishes.
                //
                // The storage of `m` is toast once we exit this
                // function. Re-allocate the thing and then call
                // `self.send`?
                error!("Failed to deliver to {}: {:?}", m.topic(), e);
            }
        };
    }
}

type MirrorProducer = BaseProducer<MirrorProducerContext>;
struct OffsetRun {
    floor: Offset,
    offsets: BinaryHeap<Offset>, // a minheap
}
type TopicOffsetMap = HashMap<(String, i32), OffsetRun>;
type TopicMap = HashMap<(String, i32), Offset>;

fn create_producer(brokers: &str, mut topic_map: TopicMap, consumer: Arc<LoggingConsumer>) -> MirrorProducer {
    let topic_offset_map = topic_map.drain().fold(HashMap::default(), |mut acc, (k, v)| {
        let mut minheap = BinaryHeap::new();
        minheap.push(v);
        acc.insert(
            k,
            OffsetRun {
                floor: v,
                offsets: minheap,
            },
        );
        acc
    });
    let mirror_context = MirrorProducerContext {
        topic_map: topic_offset_map,
        source_consumer: consumer,
    };
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create_with_context(mirror_context)
        .expect("Producer creation failed")
}

fn mirroring(consumer: &LoggingConsumer, producer: &MirrorProducer, output_topic: &str) {
    loop {
        while producer.poll(Duration::from_secs(0)) > 0 {}
        let message = match consumer.poll(Duration::from_millis(1000)) {
            Some(Ok(message)) => message,
            Some(Err(err)) => {
                error!("Error {:?}", err);
                continue;
            }
            None => continue,
        };

        loop {
            let mut tpl = TopicPartitionList::new();
            tpl.add_message_offset(&message);

            let mut record = BaseRecord::with_opaque_to(output_topic, tpl);
            if message.key().is_some() {
                record = record.key(message.key().unwrap());
            }
            if message.payload().is_some() {
                record = record.payload(message.payload().unwrap());
            }

            // NOTE(blt) -- The `record` is constructed on every iteration of
            // the loop since send takes ownership of the record. I wonder if
            // the allocation of `message` couldn't be reused more fully?
            if let Err((e, _)) = producer.send(record) {
                // TODO(blt) -- backoff
                error!("Failed to enqueue message for production: {}", e);
            } else {
                break;
            }
        }
    }
}

fn main() {
    let matches = App::new("topic-mirror")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("At-least-once delivery example")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("topic_mirror_example"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic name")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic name")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let consumer = Arc::new(create_consumer(brokers, group_id, input_topic));
    let topic_map = consumer.position().unwrap().to_topic_map();
    let producer = create_producer(brokers, topic_map, Arc::clone(&consumer));

    mirroring(consumer.as_ref(), &producer, &output_topic);
}
