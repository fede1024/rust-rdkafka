use rand::distr::{Alphanumeric, SampleString};

pub fn rand_test_topic(test_name: &str) -> String {
    let id = Alphanumeric.sample_string(&mut rand::rng(), 10);
    format!("{}_{}", test_name, id)
}

pub fn rand_test_group() -> String {
    let id = Alphanumeric.sample_string(&mut rand::rng(), 10);
    format!("__test_{}", id)
}

pub fn rand_test_transactional_id() -> String {
    let id = Alphanumeric.sample_string(&mut rand::rng(), 10);
    format!("__test_{}", id)
}
