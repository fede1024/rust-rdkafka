//! Deterministic regression test for the split-partition-queue fallback wakeup.
//!
//! No broker required: a parked partition stream with no data and no non-empty
//! callback edge can only be woken by the periodic fallback loop. With the fix
//! it is woken within ~max.poll.interval.ms; without it, never.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use futures::stream::Stream;
use tokio::time::{sleep, Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

// Waker that records when it is woken, so we can observe the fallback wake.
struct SignalWaker(AtomicBool);

impl Wake for SignalWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_split_partition_queue_fallback_wakeup() {
    let _r = env_logger::try_init();

    // poll_interval = 500ms => fallback fires every 250ms. Dummy bootstrap:
    // we never connect, the test is purely about local waker mechanics.
    let consumer: Arc<StreamConsumer> = Arc::new(
        ClientConfig::new()
            .set("group.id", "test-fallback-wakeup")
            .set("bootstrap.servers", "localhost:9092")
            .set("session.timeout.ms", "1000")
            .set("max.poll.interval.ms", "1000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer creation failed"),
    );

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("nonexistent-topic", 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let pq = consumer
        .split_partition_queue("nonexistent-topic", 0)
        .expect("split_partition_queue returned None");

    let signal = Arc::new(SignalWaker(AtomicBool::new(false)));
    let waker = Waker::from(signal.clone());

    // Park the stream: first poll registers our waker and returns Pending
    // (empty queue, no broker -> no data).
    let mut stream = pin!(pq.stream());
    let mut cx = Context::from_waker(&waker);
    match stream.as_mut().poll_next(&mut cx) {
        Poll::Pending => {}
        Poll::Ready(_) => panic!("expected Pending on empty queue"),
    }

    // The only thing that can wake us now is the fallback loop. With the fix it
    // fires within ~250ms; without it, this never trips and we time out.
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if signal.0.load(Ordering::SeqCst) {
            return; // woken by the fallback loop — pass
        }
        sleep(Duration::from_millis(50)).await;
    }

    panic!("partition queue stream was never woken — split queue is not covered by the fallback wakeup");
}
