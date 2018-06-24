//! Stream-based consumer implementation.
use futures::Async;
use futures::task::{self, Task};
use futures::{Poll, Stream};
use rdsys;

use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::base_consumer::BaseConsumer;
use consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use error::KafkaResult;
use message::BorrowedMessage;

use std::collections::VecDeque;
use std::mem;
use std::os::raw::c_void;
use std::sync::{Arc, RwLock};
use std::time::Duration;


pub struct BorrowedMessageStream<'a, C: ConsumerContext + 'a> {
    stream_consumer: &'a StreamConsumer<C>,
    task_queue: Arc<TaskQueue>,
}

impl<'a, C: ConsumerContext> BorrowedMessageStream<'a, C> {
    fn new(stream_consumer: &'a StreamConsumer<C>, task_queue: Arc<TaskQueue>) -> BorrowedMessageStream<'a, C> {
        BorrowedMessageStream { stream_consumer, task_queue }
    }
}

impl<'a, C: ConsumerContext> Stream for BorrowedMessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        println!("Poll");
        match self.stream_consumer.base_consumer.poll(Duration::from_millis(0)) {
            Some(message) => {
                println!("Message ready");
                Ok(Async::Ready(Some(message)))
            },
            None => {
                println!("Task enqueue");
                self.task_queue.enqueue_task(task::current());
                println!("Task enqueue done");
                Ok(Async::NotReady)
            }
        }
    }
}

/// A Kafka Consumer providing a `futures::Stream` interface.
///
/// This consumer doesn't need to be polled since it has a separate polling thread. Due to the
/// asynchronous nature of the stream, some messages might be consumed by the consumer without being
/// processed on the other end of the stream. If auto commit is used, it might cause message loss
/// after consumer restart. Manual offset storing should be used, see the `store_offset` function on
/// `Consumer`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext = DefaultConsumerContext> {
    base_consumer: Arc<BaseConsumer<C>>,
    task_queue: Arc<TaskQueue>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.base_consumer)
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

#[derive(Debug)]
struct TaskQueue(Box<RwLock<TaskQueueInner>>);

#[derive(Debug)]
struct TaskQueueInner {
    queue: VecDeque<Task>,
    events_ready: bool,
}

impl TaskQueue {
    fn new() -> TaskQueue {
        let inner = TaskQueueInner {
            queue: VecDeque::new(),
            events_ready: false,
        };
        TaskQueue(Box::new(RwLock::new(inner)))
    }

    fn enqueue_task(&self, task: Task) {
        println!("About to take write in enqueue_task");
        let mut inner_guard = self.0.write().unwrap();
        println!("enqueue_task locking done");
        if (*inner_guard).events_ready {
            (*inner_guard).events_ready = false;
            task.notify();
        } else {
            (*inner_guard).queue.push_back(task);
        }
        println!("enqueue_task locking finish");
    }

    fn notify_task(&self) {
        println!("About to take write in notify_task");
        let mut inner_guard = self.0.write().unwrap();
        println!(">>> {:?}", inner_guard);
        match (*inner_guard).queue.pop_front() {
            Some(task) => task.notify(),
            None => (*inner_guard).events_ready = true,
        };
    }

    fn to_opaque(self) -> *mut c_void {
        Box::into_raw(self.0) as *mut c_void
    }

    unsafe fn from_opaque(ptr: *mut c_void) -> TaskQueue {
        TaskQueue(Box::from_raw(ptr as *mut RwLock<TaskQueueInner>))
    }
}

unsafe extern "C" fn event_cb(_client: *mut rdsys::rd_kafka_s, opaque: *mut c_void) {
    println!("Event cb {:?} {:?}", _client, opaque);
    let task_queue = TaskQueue::from_opaque(opaque);
    task_queue.notify_task();
    mem::forget(task_queue);
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let base_consumer = BaseConsumer::from_config_and_context(config, context)?;

        let task_queue_ptr = TaskQueue::new().to_opaque();
        let task_queue = unsafe {
            // TODO: main queue?
            let consumer_queue = rdsys::rd_kafka_queue_get_consumer(base_consumer.client().native_ptr());
            println!("Setting up cb {:?}", task_queue_ptr);
            rdsys::rd_kafka_queue_io_event_cb_enable(consumer_queue, Some(event_cb), task_queue_ptr);
            TaskQueue::from_opaque(task_queue_ptr)
        };

        Ok(StreamConsumer {
            base_consumer: Arc::new(base_consumer),
            task_queue: Arc::new(task_queue),
        })
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    pub fn borrowed_stream(&self) -> BorrowedMessageStream<C> {
        println!("Creating borrowed stream");
        BorrowedMessageStream::new(self, Arc::clone(&self.task_queue))
    }
}
