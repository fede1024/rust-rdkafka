use std::future::Future;
use std::process;
use std::time::{Duration, Instant};

use clap::{App, Arg};
use futures::future::{self, FutureExt};
use futures::stream::StreamExt;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::AsyncRuntime;

use crate::example_utils::setup_logger;

mod example_utils;

pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        smol::spawn(task).detach()
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}

fn main() {
    let matches = App::new("smol runtime example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Demonstrates using rust-rdkafka with a custom async runtime")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .help("topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let topic = matches.value_of("topic").unwrap().to_owned();

    smol::block_on(async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        let delivery_status = producer
            .send::<Vec<u8>, _, _>(
                FutureRecord::to(&topic).payload("hello from smol"),
                Duration::from_secs(0),
            )
            .await;
        if let Err((e, _)) = delivery_status {
            eprintln!("unable to send message: {}", e);
            process::exit(1);
        }

        let consumer: StreamConsumer<_, SmolRuntime> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "rust-rdkafka-smol-runtime-example")
            .create()
            .expect("Consumer creation failed");
        consumer.subscribe(&[&topic]).unwrap();

        let mut stream = consumer.stream();
        let message = stream.next().await;
        match message {
            Some(Ok(message)) => println!(
                "Received message: {}",
                match message.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(_)) => "<invalid utf-8>",
                }
            ),
            Some(Err(e)) => {
                eprintln!("Error receiving message: {}", e);
                process::exit(1);
            }
            None => {
                eprintln!("Consumer unexpectedly returned no messages");
                process::exit(1);
            }
        }
    })
}
