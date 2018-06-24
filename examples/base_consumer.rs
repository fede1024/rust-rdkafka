#[macro_use] extern crate log;
extern crate clap;
extern crate rdkafka;
extern crate rdkafka_sys as rdsys;
extern crate tokio;
extern crate futures;

use clap::{App, Arg};
use futures::task::Task;
use futures::Async;
use futures::task;
use futures::{Future, Stream};

use rdkafka::message::{Message, Headers, ToBytes, FromBytes};
use rdkafka::client::ClientContext;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, CommitMode, Rebalance};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::util::get_rdkafka_version;
use rdkafka::error::KafkaResult;
use rdkafka::consumer::DefaultConsumerContext;

mod example_utils;
use example_utils::setup_logger;

use std::time::Duration;
use std::fs::File;
use std::os::raw::c_void;
use std::ffi::CStr;
use std::ffi::CString;
use std::mem;
use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::Instant;
use tokio::timer::Interval;
use tokio::timer::Delay;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut rdsys::RDKafkaTopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = BaseConsumer<CustomContext>;

//unsafe extern "C" fn event_cb(opaque: *mut c_void) {
unsafe extern "C" fn event_cb(_client: *mut rdsys::rd_kafka_s, opaque: *mut c_void) {
//    let opaque_str = CStr::from_ptr(_opaque as *const i8).to_string_lossy();
//    info!("YOLOOOOO '{}' {:?}", opaque_str, _opaque);
    let tasks_box = Box::from_raw(opaque as *mut RwLock<VecDeque<Task>>);
    {
        let mut tasks = tasks_box.write().unwrap();
        let tasks_count = tasks.len();
        let first_task = tasks.pop_front();
        info!("Tasks in queue: {}, receiving task: {:?}", tasks_count, first_task);
        if let Some(task) = first_task {
            task.notify();
        }
    }

    mem::forget(tasks_box);
}


struct StreamConsumer2 {
    consumer: LoggingConsumer,
    tasks: Box<RwLock<VecDeque<Task>>>,
}

impl StreamConsumer2 {
    fn new(topics: &[&str]) -> StreamConsumer2 {
        let context = CustomContext;

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", "test_group")
            .set("bootstrap.servers", "localhost:9092")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        consumer.subscribe(&topics.to_vec())
            .expect("Can't subscribe to specified topics");

        let client_ptr = consumer.client.native_ptr();
//        let main_queue = unsafe { rdsys::rd_kafka_queue_get_main(client_ptr) };
        // Can i take the queue after having started the consumer?
        let consumer_queue = unsafe { rdsys::rd_kafka_queue_get_consumer(client_ptr) };

        let tasks = Box::new(RwLock::new(VecDeque::new()));

        let cstring2 = CString::new("consumer").unwrap();
        let tasks_ptr = Box::into_raw(tasks);
        unsafe {
            rdsys::rd_kafka_queue_io_event_cb_enable(consumer_queue, Some(event_cb), tasks_ptr as *mut c_void);
        };

        // TODO: delete consumer_queue

        StreamConsumer2 {
            consumer,
            tasks: unsafe { Box::from_raw(tasks_ptr) }
        }
    }
}

impl Stream for StreamConsumer2 {
    type Item = u32;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        info!("About to poll");
        match self.consumer.poll(Duration::from_millis(0)) {
            Some(_message) => {
                let mut tasks = self.tasks.write().unwrap();
//                if let Some(task) = (*tasks).pop_front() {
//                    task.notify();  // Notify next task
//                }
                info!("Ready");
                Ok(Async::Ready(Some(42)))
            },
            None => {
                let current_task = task::current();
                info!("NotReady: {:?}", current_task);
                (*self.tasks.write().unwrap()).push_back(current_task);
                Ok(Async::NotReady)
            }
        }
    }
}


fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let consumer = StreamConsumer2::new(topics);

    let compl = consumer.for_each(|m| {
        info!("Received: {:?}", m);
        Ok(())
    });

    tokio::run(compl);
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(Arg::with_name("brokers")
             .short("b")
             .long("brokers")
             .help("Broker list in kafka format")
             .takes_value(true)
             .default_value("localhost:9092"))
        .arg(Arg::with_name("group-id")
             .short("g")
             .long("group-id")
             .help("Consumer group id")
             .takes_value(true)
             .default_value("example_consumer_group_id"))
        .arg(Arg::with_name("log-conf")
             .long("log-conf")
             .help("Configure the logging format (example: 'rdkafka=trace')")
             .takes_value(true))
        .arg(Arg::with_name("topics")
             .short("t")
             .long("topics")
             .help("Topic list")
             .takes_value(true)
             .multiple(true)
             .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, &topics);
}
