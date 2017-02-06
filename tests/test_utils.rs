extern crate rand;

use rand::Rng;

pub fn rand_test_topic() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_group() -> String {
    let id = rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}
