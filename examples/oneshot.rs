extern crate futures;

use std::thread;
use futures::Future;

fn main() {
    let (tx, rx) = futures::oneshot();

    let handle = thread::spawn(move || {
        for _ in 0..10 {
            println!("ehehe");
            thread::sleep_ms(200);
        }
        tx.complete(42);
        println!("I'm dead");
    });

    rx.map(|i| {
        println!("got: {}", i);
    }).wait();
}
