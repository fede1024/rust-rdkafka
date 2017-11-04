#[macro_use] extern crate log;
extern crate clap;
extern crate futures;
extern crate futures_cpupool;
extern crate rand;
extern crate rdkafka;
extern crate tokio_core;

use clap::{App, Arg};
use futures::Future;
use futures::stream::Stream;
use futures_cpupool::Builder;
use tokio_core::reactor::Core;

use rdkafka::Message;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::FutureProducer;

use std::thread;
use std::time::Duration;

mod example_utils;
use example_utils::setup_logger;

// Emulates an expensive, synchronous computation. This function returns a string with the length
// of the message payload, if any.
fn expensive_computation(msg: OwnedMessage) -> String {
    info!("Starting expensive computation on message");
    thread::sleep(Duration::from_millis(rand::random::<u64>() % 5000));
    info!("Expensive computation completed");
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
        Some(Err(_)) => "Error processing message payload".to_owned(),
        None => "No payload".to_owned(),
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// Moving each message from one stage of the pipeline to next one is handled by the event loop,
// that runs on a single thread. The expensive CPU-bound computation is handled by the `CpuPool`,
// without blocking the event loop.
fn run_async_processor(brokers: &str, group_id: &str, input_topic: &str, output_topic: &str) {
    // Create the event loop. The event loop will run on a single thread and drive the pipeline.
    let mut core = Core::new().unwrap();

    // Create the CPU pool, for CPU-intensive message processing.
    let cpu_pool = Builder::new().pool_size(4).create();

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer.subscribe(&[input_topic]).expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .create::<FutureProducer<_>>()
        .expect("Producer creation error");

    // Create a handle to the core, that will be used to provide additional asynchronous work
    // to the event loop.
    let handle = core.handle();

    // Create the outer pipeline on the message stream.
    let processed_stream = consumer.start()
        .filter_map(|result| {  // Filter out errors
            match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            }
        }).for_each(|msg| {     // Process each message
            info!("Enqueuing message for computation");
            let producer = producer.clone();
            let topic_name = output_topic.to_owned();
            let owned_message = msg.detach();
            // Create the inner pipeline, that represents the processing of a single event.
            let process_message = cpu_pool.spawn_fn(move || {
                // Take ownership of the message, and runs an expensive computation on it,
                // using one of the threads of the `cpu_pool`.
                Ok(expensive_computation(owned_message))
            }).and_then(move |computation_result| {
                // Send the result of the computation to Kafka, asynchronously.
                info!("Sending result");
                producer.send_copy::<String, ()>(&topic_name, None, Some(&computation_result), None, None, 1000)
            }).and_then(|d_report| {
                // Once the message has been produced, print the delivery report and terminate
                // the pipeline.
                info!("Delivery report for result: {:?}", d_report);
                Ok(())
            }).or_else(|err| {
                // In case of error, this closure will be executed instead.
                warn!("Error while processing message: {:?}", err);
                Ok(())
            });
            // Spawns the inner pipeline in the same event pool.
            handle.spawn(process_message);
            Ok(())
        });

    info!("Starting event loop");
    // Runs the event pool until the consumer terminates.
    core.run(processed_stream).unwrap();
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
