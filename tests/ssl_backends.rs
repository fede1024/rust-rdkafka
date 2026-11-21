#[cfg(feature = "aws-lc")]
#[test]
fn initializes_aws_lc_backed_client() {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::BaseProducer;

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:1")
        .set("security.protocol", "ssl")
        .create()
        .expect("AWS-LC-backed SSL initialization should succeed");
    drop(producer);
}
