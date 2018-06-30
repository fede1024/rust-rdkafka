//! Stream-based consumer implementation.
use futures::Async;
use futures::task::{self, Task};
use futures::{Poll, Stream};
use rdsys;

use client::NativeQueue;
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

//
// ********** STREAM CONSUMER **********
//

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

    fn into_opaque(self) -> *mut c_void {
        Box::into_raw(self.0) as *mut c_void
    }

    unsafe fn from_opaque(ptr: *mut c_void) -> TaskQueue {
        TaskQueue(Box::from_raw(ptr as *mut RwLock<TaskQueueInner>))
    }
}

/// A Kafka consumer providing a [futures::Stream] interface.
///
/// The [StreamConsumer] is a higher level consumer that can generate message streams. Message
/// streams don't need to be polled explicitly, since polling will be driven by the executor
/// the stream will be given to. The consumer will take care of waking up the event loop when
/// there are new events to be processed (new messages, assignment changes etc.).
///
/// Two streams are provided: [BorrowedMessageStream] and [OwnedMessageStream]. Refer to their
/// documentation for more details.
pub struct StreamConsumer<C: ConsumerContext = DefaultConsumerContext> {
    base_consumer: Arc<BaseConsumer<C>>,
    tasks: Arc<TaskQueue>,
    // The queue should be dropped before BaseConsumer.
    queue: Arc<NativeQueue>,
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

/// This function will be called by librdkafka every time the number of events waiting on the
/// queue goes from 0 to a non-0 value. When that happens, a task will be woken up.
unsafe extern "C" fn event_cb(_client: *mut rdsys::rd_kafka_s, opaque: *mut c_void) {
    let task_queue = TaskQueue::from_opaque(opaque);
    task_queue.notify_task();
    mem::forget(task_queue);
}

impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<StreamConsumer<C>> {
        let base_consumer = BaseConsumer::from_config_and_context(config, context)?;

        let task_queue_ptr = TaskQueue::new().into_opaque();
        let consumer_queue = base_consumer.client().get_consumer_queue();
        let task_queue = unsafe {
            // let consumer_queue = rdsys::rd_kafka_queue_get_consumer(base_consumer.client().native_ptr());
            rdsys::rd_kafka_queue_cb_event_enable(consumer_queue.ptr(), Some(event_cb), task_queue_ptr);
            TaskQueue::from_opaque(task_queue_ptr)
        };

        Ok(StreamConsumer {
            base_consumer: Arc::new(base_consumer),
            tasks: Arc::new(task_queue),
            queue: Arc::new(consumer_queue),
        })
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Create a new [BorrowedMessageStream]. The returned stream has a reference to the
    /// [StreamConsumer] and cannot outlive it.
    pub fn borrowed_stream(&self) -> BorrowedMessageStream<C> {
        BorrowedMessageStream::new(self)
    }

    /// Returns a new [OwnedMessageStream]. The [OwnedMessageStream] will take ownership of the
    /// consumer.
    pub fn owned_stream(self) -> OwnedMessageStream<C> {
        OwnedMessageStream::new(Arc::new(self))
    }
}

//
// ********** BORROWED MESSAGE STREAM **********
//

/// A stream of borrowed messages.
///
/// The [BorrowedMessageStreams] allows streaming access to messages. To reduce the amount of
/// data being copied, key and payload of the returned messages live in the buffer memory of the
/// consumer, meaning that the returned message cannot outlive it.
/// For more information refer to the [BorrowedMessage] documentation.
#[derive(Clone)]
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

//
// ********** OWNED MESSAGE STREAM **********
//

#[derive(Clone)]
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
