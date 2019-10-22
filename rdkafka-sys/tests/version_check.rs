extern crate rdkafka_sys as rdsys;

use std::ffi::CStr;

const PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn remove_pre(version: &str) -> &str {
    version
        .split('-')
        .next()
        .expect("Version format is not valid")
}

#[test]
fn check_version() {
    let version_str_c = unsafe { CStr::from_ptr(rdsys::rd_kafka_version_str()) };
    let rdsys_version = version_str_c.to_string_lossy();
    println!("librdkafka version: {}", rdsys_version);

    assert_eq!(remove_pre(&rdsys_version), remove_pre(PKG_VERSION))
}
