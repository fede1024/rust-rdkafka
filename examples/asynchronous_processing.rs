#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate rand;
extern crate rdkafka;
extern crate tokio;

use clap::{App, Arg};
use futures::{FutureExt, StreamExt};
use futures::future::{lazy, ready};
use tokio::runtime::current_thread;

use rdkafka::Message;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};

use std::thread;
use std::time::Duration;

mod example_utils;
use crate::example_utils::setup_logger;


// Emulates an expensive, synchronous computation.
fn expensive_computation<'a>(msg: OwnedMessage) -> String {
    info!("Starting expensive computation on message {}", msg.offset());
    thread::sleep(Duration::from_millis(rand::random::<u64>() % 5000));
    info!("Expensive computation completed on message {}", msg.offset());
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
        Some(Err(_)) => "Message payload is not a string".to_owned(),
        None => "No payload".to_owned(),
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// Moving each message from one stage of the pipeline to next one is handled by the event loop,
// that runs on a single thread. The expensive CPU-bound computation is handled by the `ThreadPool`,
// without blocking the event loop.
fn run_async_processor(brokers: &str, group_id: &str, input_topic: &str, output_topic: &str) {

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[input_topic]).expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Create the runtime where the expensive computation will be performed.
    let thread_pool = tokio::runtime::Builder::new()
        .name_prefix("pool-")
        .core_threads(4)
        .build()
        .unwrap();

    // Use the current thread as IO thread to drive consumer and producer.
    let mut io_thread = current_thread::Runtime::new().unwrap();
    let io_thread_handle = io_thread.handle();

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.start()
        .filter_map(|result| {  // Filter out errors
            ready(match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            })
        }).for_each(move |borrowed_message| {     // Process each message
            info!("Message received: {}", borrowed_message.offset());
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            let output_topic = output_topic.to_string();
            let producer = producer.clone();
            let io_thread_handle = io_thread_handle.clone();
            let message_future = lazy(move |_| {
                // The body of this closure will be executed in the thread pool.
                let computation_result = expensive_computation(owned_message);
                let producer_future = producer.send(
                    FutureRecord::to(&output_topic)
                        .key("some key")
                        .payload(&computation_result),
                    0).then(|result| {
                        match result {
                            Ok(delivery) => println!("Sent: {:?}", delivery),
                            Err(e) => println!("Error: {:?}", e),
                        }
                        ready(())
                    });
                let _ = io_thread_handle.spawn(producer_future);
                ()
            });
            thread_pool.spawn(message_future);
            ready(())
        });

    info!("Starting event loop");
    let _ = io_thread.block_on(stream_processor);
    info!("Stream processing terminated");
}

fn main() {
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
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
        .arg(Arg::with_name("input-topic")
             .long("input-topic")
             .help("Input topic")
             .takes_value(true)
             .required(true))
        .arg(Arg::with_name("output-topic")
            .long("output-topic")
            .help("Output topic")
            .takes_value(true)
            .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();

    run_async_processor(brokers, group_id, input_topic, output_topic);
}
