use std::ffi::CStr;

#[test]
fn check_version() {
    let cargo_version = match env!("CARGO_PKG_VERSION")
        .split('+')
        .collect::<Vec<_>>()
        .as_slice()
    {
        [_rdsys_version, librdkafka_version] => *librdkafka_version,
        _ => panic!("Version format is not valid"),
    };

    let librdkafka_version =
        unsafe { CStr::from_ptr(rdkafka_sys::rd_kafka_version_str()).to_string_lossy() };
    println!("librdkafka version: {}", librdkafka_version);

    assert_eq!(cargo_version, &librdkafka_version);
}
