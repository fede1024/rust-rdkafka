#[macro_use] extern crate log;

pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod message;
pub mod producer;
pub mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
