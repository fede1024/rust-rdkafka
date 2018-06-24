//! Stream-based consumer implementation.
use futures::{Future, Poll, Stream};
use rdsys;

use config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use consumer::base_consumer::BaseConsumer;
use consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use error::{KafkaError, KafkaResult};
use message::BorrowedMessage;

use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use futures::task::{self, Task};
use futures::Async;
use std::os::raw::c_void;
use core::mem;
use std::time::Duration;


//type TaskQueue = Box<RwLock<VecDeque<Task>>>;

/// A Kafka consumer implementing Stream.
///
/// It can be used to receive messages as they are consumed from Kafka. Note: there might be
/// buffering between the actual Kafka consumer and the receiving end of this stream, so it is not
/// advised to use automatic commit, as some messages might have been consumed by the internal Kafka
/// consumer but not processed. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
pub struct BorrowedMessageStream<'a, C: ConsumerContext + 'a> {
    stream_consumer: &'a StreamConsumer2<C>,
    task_queue: Arc<TaskQueue>,
}

impl<'a, C: ConsumerContext> BorrowedMessageStream<'a, C> {
    fn new(stream_consumer: &'a StreamConsumer2<C>, task_queue: Arc<TaskQueue>) -> BorrowedMessageStream<'a, C> {
        BorrowedMessageStream { stream_consumer, task_queue }
    }
}

impl<'a, C: ConsumerContext> Stream for BorrowedMessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;
    type Error = ();  // TODO: error?

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        info!("About to poll");
        match self.stream_consumer.base_consumer.poll(Duration::from_millis(0)) {
            Some(message) => {
                info!("Ready");
                Ok(Async::Ready(Some(message)))
            },
            None => {
                info!("NotReady");
                self.task_queue.enqueue_task(task::current());
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
pub struct StreamConsumer2<C: ConsumerContext = DefaultConsumerContext> {
    base_consumer: Arc<BaseConsumer<C>>,
    task_queue: Arc<TaskQueue>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer2<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.base_consumer)
    }
}

impl FromClientConfig for StreamConsumer2 {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer2> {
        StreamConsumer2::from_config_and_context(config, DefaultConsumerContext)
    }
}

struct TaskQueue(Box<RwLock<TaskQueueInner>>);

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
        info!("Before enqueue");
        let mut inner_guard = self.0.write().unwrap();
        if (*inner_guard).events_ready {
            (*inner_guard).events_ready = false;
            task.notify();
            info!("Notified");
        } else {
            (*inner_guard).queue.push_back(task);
            info!("Enqueued");
        }
        info!("After enqueue");
    }

    fn notify_task(&self) {
        info!("Notify Task");
        let mut inner_guard = self.0.write().unwrap();
        match (*inner_guard).queue.pop_front() {
            Some(task) => {
                task.notify();
                info!("Task notified");
            },
            None => {
                (*inner_guard).events_ready = true;
                info!("Events ready");
            },
        };
        info!("After notify task");
    }

    fn to_opaque(self) -> *mut c_void {
        Box::into_raw(self.0) as *mut c_void
    }

    unsafe fn from_opaque(ptr: *mut c_void) -> TaskQueue {
        TaskQueue(Box::from_raw(ptr as *mut RwLock<TaskQueueInner>))
    }
}

unsafe extern "C" fn event_cb(_client: *mut rdsys::rd_kafka_s, opaque: *mut c_void) {
    let task_queue = TaskQueue::from_opaque(opaque);
    task_queue.notify_task();
    mem::forget(task_queue);
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer2<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer2<C>> {
        let base_consumer = BaseConsumer::from_config_and_context(config, context)?;

        let task_queue_ptr = TaskQueue::new().to_opaque();
        let task_queue = unsafe {
            // TODO: main queue?
            let consumer_queue = rdsys::rd_kafka_queue_get_consumer(base_consumer.client().native_ptr());
            rdsys::rd_kafka_queue_io_event_cb_enable(consumer_queue, Some(event_cb), task_queue_ptr);
            TaskQueue::from_opaque(task_queue_ptr)
        };

        Ok(StreamConsumer2 {
            base_consumer: Arc::new(base_consumer),
            task_queue: Arc::new(task_queue),
        })
    }
}

impl<C: ConsumerContext> StreamConsumer2<C> {
    pub fn borrowed_stream(&self) -> BorrowedMessageStream<C> {
        BorrowedMessageStream::new(self, Arc::clone(&self.task_queue))
    }
}
