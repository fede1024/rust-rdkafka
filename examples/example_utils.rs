extern crate log;
extern crate env_logger;

use std::thread;
use self::log::{LogRecord, LogLevelFilter};
use self::env_logger::LogBuilder;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |record: &LogRecord| {
        let thread_name = if log_thread {
            format!("({}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };
        format!("{}{} - {} - {}", thread_name, record.level(), record.target(), record.args())
    };

    let mut builder = LogBuilder::new();
    builder.format(output_format).filter(None, LogLevelFilter::Info);

    rust_log.map(|conf| builder.parse(conf));

    builder.init().unwrap();
}

#[allow(dead_code)]
fn main() {
    println!("This is not an example");
}
