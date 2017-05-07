use consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use topic_partition_list::TopicPartitionList;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type OffsetMap = HashMap<(String, i32), i64>;

struct AutoCommitRegistryInner {
    offsets: OffsetMap,
    last_commit_time: Instant,
}

pub struct AutoCommitRegistry<C>
    where C: ConsumerContext
{
    inner: Arc<Mutex<AutoCommitRegistryInner>>,
    commit_interval: Duration,
    commit_mode: CommitMode,
    consumer: BaseConsumer<C>,
}

impl<C> Clone for AutoCommitRegistry<C>
    where C: ConsumerContext
{
    fn clone(&self) -> Self {
        AutoCommitRegistry {
            inner: Arc::clone(&self.inner),
            commit_interval: self.commit_interval,
            commit_mode: self.commit_mode,
            consumer: self.consumer.clone(),
        }
    }
}

impl<C> AutoCommitRegistry<C>
    where C: ConsumerContext
{
    pub fn new(
        commit_interval: Duration,
        commit_mode: CommitMode,
        consumer: &Consumer<C>,
    ) -> AutoCommitRegistry<C> {
        let inner = AutoCommitRegistryInner {
            offsets: HashMap::new(),
            last_commit_time: Instant::now(),
        };
        AutoCommitRegistry {
            inner: Arc::new(Mutex::new(inner)),
            commit_interval: commit_interval,
            commit_mode: commit_mode,
            consumer: consumer.get_base_consumer().clone(),
        }
    }

    pub fn register_message(&self, message_id: (String, i32, i64)) -> bool {
        {
            let mut inner = self.inner.lock().unwrap();
            (*inner).offsets.insert((message_id.0, message_id.1), message_id.2);
        }
        self.maybe_commit()
    }

    pub fn maybe_commit(&self) -> bool {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        if now.duration_since((*inner).last_commit_time) >= self.commit_interval {
            self.commit_inner(&mut inner);
            return true;
        }
        false
    }

    pub fn commit(&self) {
        self.commit_inner(&mut self.inner.lock().unwrap())
    }

    fn commit_inner(&self, inner: &mut AutoCommitRegistryInner) {
        inner.last_commit_time = Instant::now();
        let _ = self.consumer.commit(&offset_map_to_tpl(&inner.offsets), self.commit_mode);
    }
}

impl<C> Drop for AutoCommitRegistry<C>
    where C: ConsumerContext {

    fn drop(&mut self) {
        // Force commit before drop
        let _ = self.commit();
    }
}

fn offset_map_to_tpl(map: &OffsetMap) -> TopicPartitionList { let mut groups = HashMap::new();
    for (&(ref topic, ref partition), offset) in map {
        let mut partitions = groups.entry(topic.to_owned()).or_insert(Vec::new());
        partitions.push((*partition, *offset));
    }

    let mut tpl = TopicPartitionList::new();
    for (topic, partitions) in groups {
        tpl.add_topic_with_partitions_and_offsets(&topic, &partitions);
    }

    tpl
}

#[cfg(test)]
mod test {
    extern crate rdkafka_sys as rdsys;
    use super::*;
    use std::thread;

    use config::ClientConfig;
    use consumer::base_consumer::BaseConsumer;

    #[test]
    fn test_offset_map_to_tpl() {
        let mut map = HashMap::new();
        map.insert(("t1".to_owned(), 0), 0);
        map.insert(("t1".to_owned(), 1), 1);
        map.insert(("t2".to_owned(), 0), 2);

        let tpl = offset_map_to_tpl(&map);
        let mut tpl2 = TopicPartitionList::new();
        tpl2.add_topic_with_partitions_and_offsets("t1", &vec![(0, 0), (1, 1)]);
        tpl2.add_topic_with_partitions_and_offsets("t2", &vec![(0, 2)]);

        assert_eq!(tpl, tpl2);
    }

    #[test]
    fn test_auto_commit_registry() {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", "1.2.3.4")
            .create::<BaseConsumer<_>>()
            .unwrap();

        let reg = AutoCommitRegistry::new(
            Duration::from_secs(2),
            CommitMode::Async,
            &consumer,
        );

        assert_eq!(reg.register_message(("a".to_owned(), 0, 0)), false);
        assert_eq!(reg.register_message(("a".to_owned(), 1, 1)), false);
        assert_eq!(reg.register_message(("b".to_owned(), 0, 2)), false);
        thread::sleep(Duration::from_millis(1200));
        assert_eq!(reg.register_message(("a".to_owned(), 0, 3)), false);
        assert_eq!(reg.register_message(("a".to_owned(), 1, 4)), false);
        assert_eq!(reg.register_message(("b".to_owned(), 0, 5)), false);
        thread::sleep(Duration::from_millis(1200));
        assert_eq!(reg.register_message(("a".to_owned(), 0, 6)), true);  // Commit
        assert_eq!(reg.register_message(("a".to_owned(), 1, 7)), false);
        assert_eq!(reg.register_message(("b".to_owned(), 0, 8)), false);

        // Check AutoCommitRegistry is send and sync
        let reg_clone = reg.clone();
        let t = thread::spawn(move || {
            reg_clone.register_message(("b".to_owned(), 0, 8));
        });
        t.join().expect("Thread should exit correctly");

        // TODO: move test to integration tests and verify commit callback
    }
}