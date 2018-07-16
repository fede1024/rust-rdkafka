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
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod example_utils;
use example_utils::setup_logger;

use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::Rebalance;
use rdkafka::error::KafkaError;
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::ProducerContext;
use rdkafka::topic_partition_list::Offset;
use rdkafka::ClientContext;
use rdkafka::TopicPartitionList;

struct OffsetRun {
    floor: Offset,
    offsets: BinaryHeap<Offset>, // a minheap
}
type TopicOffsetMap = HashMap<(String, i32), OffsetRun>;
type TopicMap = HashMap<(String, i32), Offset>;

struct OffsetStore {
    offset_map: TopicOffsetMap,
}

impl OffsetStore {
    fn new() -> OffsetStore {
        OffsetStore {
            offset_map: HashMap::default(),
        }
    }

    fn prime(&mut self, mut topic_map: TopicMap) -> () {
        self.offset_map.clear();
        for (k, v) in topic_map.drain() {
            let mut minheap = BinaryHeap::new();
            minheap.push(v);
            self.offset_map.insert(
                k,
                OffsetRun {
                    floor: v,
                    offsets: minheap,
                },
            );
        }
    }

    fn rebalance(&mut self, rebalance: &Rebalance) -> () {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let topic_map = tpl.to_topic_map();
                self.prime(topic_map);
            }
            _ => unreachable!(), // TODO(blt) must actuall handle this
        }
    }

    fn commit(&mut self, tpl: TopicPartitionList) -> Option<TopicPartitionList> {
        // NOTE(blt) -- Our ambition here is to avoid committing out-of-order
        // offsets for any given topic. The approach taken is to dissect the
        // passed TopicPartitionList, compare against the stored offset_map and
        // use that map to build a `commit_tpl` of contiguous offsets that are
        // safe to commit.
        for ((ref topic, ref partition), ref mut offset_run) in self.offset_map.iter_mut() {
            if let Some(tpl_elem) = tpl.find_partition(topic, *partition) {
                let offset = tpl_elem.offset();
                offset_run.offsets.push(offset);
                if offset < offset_run.floor {
                    offset_run.floor = offset;
                }
            }
        }
        // By this point, the `self.offset_map` is updated with the offsets that
        // came in via `tpl`. We now, potentially, have 'runs' that can be
        // comitted to the consumer, which we build up into `commit_tpl`.
        let mut commit_tpl = TopicPartitionList::new();
        for ((ref topic, ref partition), ref mut offset_run) in self.offset_map.iter_mut() {
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
        if commit_tpl.count() == 0 {
            None
        } else {
            Some(commit_tpl)
        }
    }
}

struct LoggingContext {
    offset_store: Arc<Mutex<OffsetStore>>,
}

impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?} {:?}", offsets, result);
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        let mut lk = self.offset_store.lock().unwrap();
        lk.rebalance(rebalance);
    }
}

type LoggingConsumer = BaseConsumer<LoggingContext>;

fn create_consumer(brokers: &str, group_id: &str, topics: &[&str]) -> (Arc<LoggingConsumer>, Arc<Mutex<OffsetStore>>) {
    let offset_store = Arc::new(Mutex::new(OffsetStore::new()));
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
        .create_with_context::<LoggingContext, LoggingConsumer>(LoggingContext {
            offset_store: Arc::clone(&offset_store)
        })
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topic");
    let topic_map = consumer.position().unwrap().to_topic_map();
    {
        let mut lk = offset_store.lock().unwrap();
        lk.prime(topic_map);
    }
    (Arc::new(consumer), offset_store)
}

struct MirrorProducerContext {
    consumer: Arc<LoggingConsumer>,
    offset_store: Arc<Mutex<OffsetStore>>,
}

impl ClientContext for MirrorProducerContext {}
impl ProducerContext for MirrorProducerContext {
    type DeliveryOpaque = TopicPartitionList;

    fn delivery(
        &self,
        delivery_result: &Result<BorrowedMessage, (KafkaError, BorrowedMessage)>,
        delivery_opaque: TopicPartitionList,
    ) {
        match delivery_result {
            Ok(m) => {
                debug!("Delivered copy of {:?} to {}", delivery_opaque, m.topic());
                let mut offset_store = self.offset_store.lock().unwrap();
                if let Some(commit_tpl) = offset_store.commit(delivery_opaque) {
                    while let Err(_) = self.consumer.store_offset_list(&commit_tpl) {
                        // TODO(blt) -- backoff
                    }
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

fn create_producer(
    brokers: &str,
    consumer: Arc<LoggingConsumer>,
    offset_store: Arc<Mutex<OffsetStore>>,
) -> MirrorProducer {
    let mirror_context = MirrorProducerContext { offset_store, consumer };
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create_with_context(mirror_context)
        .expect("Producer creation failed")
}

fn mirroring(consumer: &LoggingConsumer, producer: &MirrorProducer, _offset_store: Arc<Mutex<OffsetStore>>) {
    // `offset_store` is not in use in this function but it could, potentially,
    // be used by a filter program: just call `commit` without passing to the
    // producer to avoid mirroring
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

            let mut record = BaseRecord::with_opaque_to(message.topic(), tpl);
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
            Arg::with_name("topic")
                .long("topic")
                .help("Topic names")
                .multiple(true)
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let topics: Vec<&str> = matches.values_of("topic").unwrap().collect();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let (consumer, offset_store) = create_consumer(brokers, group_id, &topics);
    let producer = create_producer(brokers, Arc::clone(&consumer), Arc::clone(&offset_store));

    info!("Starting mirroring of topics {:?}", topics);
    mirroring(consumer.as_ref(), &producer, offset_store);
}
