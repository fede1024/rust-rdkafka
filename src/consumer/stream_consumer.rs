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
use message::OwnedMessage;

pub struct BorrowedMessageStream<'a, C: ConsumerContext + 'a> {
    stream_consumer: &'a StreamConsumer<C>,
}

impl<'a, C: ConsumerContext> BorrowedMessageStream<'a, C> {
    fn new(stream_consumer: &'a StreamConsumer<C>) -> BorrowedMessageStream<'a, C> {
        BorrowedMessageStream { stream_consumer }
    }
}

impl<'a, C: ConsumerContext> Stream for BorrowedMessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.stream_consumer.base_consumer.poll(Duration::from_millis(0)) {
            Some(message) => Ok(Async::Ready(Some(message))),
            None => {
                self.stream_consumer.tasks.enqueue_task(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}

pub struct OwnedMessageStream<C: ConsumerContext> {
    stream_consumer: Arc<StreamConsumer<C>>,
}

impl<C: ConsumerContext> OwnedMessageStream<C> {
    fn new(stream_consumer: Arc<StreamConsumer<C>>) -> OwnedMessageStream<C> {
        OwnedMessageStream { stream_consumer }
    }
}

impl<C: ConsumerContext> Stream for OwnedMessageStream<C> {
    type Item = KafkaResult<OwnedMessage>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.stream_consumer.base_consumer.poll(Duration::from_millis(0)) {
            Some(result) => {
                let owned_result = result.map(|message| message.detach());
                Ok(Async::Ready(Some(owned_result)))
            },
            None => {
                self.stream_consumer.tasks.enqueue_task(task::current());
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
    tasks: Arc<TaskQueue>,
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
        let mut inner_guard = self.0.write().unwrap();
        if (*inner_guard).events_ready {
            (*inner_guard).events_ready = false;
            task.notify();
        } else {
            (*inner_guard).queue.push_back(task);
        }
    }

    fn notify_task(&self) {
        let mut inner_guard = self.0.write().unwrap();
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
            let consumer_queue = rdsys::rd_kafka_queue_get_consumer(base_consumer.client().native_ptr());
            rdsys::rd_kafka_queue_io_event_cb_enable(consumer_queue, Some(event_cb), task_queue_ptr);
            // TODO: queue should be destroyed on StreamConsumer::drop?
            TaskQueue::from_opaque(task_queue_ptr)
        };

        Ok(StreamConsumer {
            base_consumer: Arc::new(base_consumer),
            tasks: Arc::new(task_queue),
        })
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    pub fn borrowed_stream(&self) -> BorrowedMessageStream<C> {
        BorrowedMessageStream::new(self)
    }

    pub fn owned_stream(self) -> OwnedMessageStream<C> {
        OwnedMessageStream::new(Arc::new(self))
    }
}
