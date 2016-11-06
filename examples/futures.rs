extern crate futures;
use std::thread;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use futures::{Future, Poll, Async};
use futures::task;
use futures::task::Task;
use std::cell::Cell;

struct FutureStatus {
    ready: bool,
    task: Option<Task>
}

impl FutureStatus {
    fn set_ready(&mut self) {
        self.ready = true;
        match self.task.as_ref() {
            Some(ref task) => task.unpark(),
            None => {}
        };
    }
}

struct MyFuture {
    stuff: i32,
    status: Arc<Mutex<FutureStatus>>,
}

impl MyFuture {
    fn new(stuff: i32) -> MyFuture {
        let status = FutureStatus { ready: false, task: None };
        MyFuture { stuff: stuff, status: Arc::new(Mutex::new(status)) }
    }
}

impl Future for MyFuture {
    type Item = i32;
    type Error = String;

    fn poll(&mut self) -> Poll<i32, String> {
        let mut status = self.status.lock().unwrap();  // FIXME no unwrap
        if status.ready {
            Ok(Async::Ready(self.stuff))
        } else {
            let task = task::park();
            println!("pack {:?}", task);
            if status.task.is_none() {
                status.task = Some(task);
            };
            Ok(Async::NotReady)
        }
    }
}

fn main() {
    let future = MyFuture::new(42);

    let mut status_clone = future.status.clone();

    let handle = thread::spawn(move || {
        for _ in 0..10 {
            println!("ehehe");
            thread::sleep_ms(200);
        }
        let mut status_clone = status_clone.lock().unwrap();
        status_clone.set_ready();
        println!("I'm dead");
    });

    let result = future
        .map(|val| {
            println!("> {:?}", val);
            val
        })
        .map(|val| val + 1)
        .map(|val| {
            println!("> {:?}", val);
            val
        })
        .wait();

    println!(">>> {:?}", result);
}
