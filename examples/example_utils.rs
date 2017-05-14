extern crate log;
extern crate env_logger;
extern crate chrono;

use self::chrono::prelude::*;

use std::thread;
use self::log::{LogRecord, LogLevelFilter};
use self::env_logger::LogBuilder;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |record: &LogRecord| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        format!("{} {}{} - {} - {}", time_str, thread_name, record.level(), record.target(), record.args())
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
