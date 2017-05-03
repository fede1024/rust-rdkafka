use consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use error::KafkaResult;
use topic_partition_list::TopicPartitionList;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type OffsetMap = HashMap<(String, i32), i64>;
// pub type CommitCb = Fn(&OffsetMap, KafkaResult<()>);

struct AutoCommitRegistryInner<F> {
    offsets: OffsetMap,
    last_commit_time: Instant,
    callback: F,
}

pub struct AutoCommitRegistry<F, C>
    where F: Fn(&OffsetMap, KafkaResult<()>), C: ConsumerContext
{
    inner: Arc<Mutex<AutoCommitRegistryInner<F>>>,
    commit_interval: Duration,
    commit_mode: CommitMode,
    consumer: BaseConsumer<C>,
}

impl<F, C> Clone for AutoCommitRegistry<F, C>
    where F: Fn(&OffsetMap, KafkaResult<()>), C: ConsumerContext
{
    fn clone(&self) -> Self {
        AutoCommitRegistry {
            inner: Arc::clone(&self.inner),
            commit_interval: self.commit_interval,
            commit_mode: self.commit_mode.clone(),
            consumer: self.consumer.clone(),
        }
    }
}

impl<F, C> AutoCommitRegistry<F, C>
    where F: Fn(&OffsetMap, KafkaResult<()>), C: ConsumerContext
{
    pub fn new(
        commit_interval: Duration,
        commit_mode: CommitMode,
        consumer: &Consumer<C>,
        callback: F
    ) -> AutoCommitRegistry<F, C> {
        let inner = AutoCommitRegistryInner {
            offsets: HashMap::new(),
            last_commit_time: Instant::now(),
            callback: callback,
        };
        AutoCommitRegistry {
            inner: Arc::new(Mutex::new(inner)),
            commit_interval: commit_interval,
            commit_mode: commit_mode,
            consumer: consumer.get_base_consumer().clone(),
        }
    }

    pub fn register_message(&self, message_id: (String, i32, i64)) {
        {
            let mut inner = self.inner.lock().unwrap();
            (*inner).offsets.insert((message_id.0, message_id.1), message_id.2);
        }
        self.maybe_commit();
    }

    pub fn maybe_commit(&self) {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        if now.duration_since((*inner).last_commit_time) >= self.commit_interval {
            let commit_result = self.consumer.commit(
                &offset_map_to_tpl(&(*inner).offsets),
                self.commit_mode);
            ((*inner).callback)(&(*inner).offsets, commit_result);
            (*inner).last_commit_time = now;
        }
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
    use super::*;
    use std::thread;

    use config::ClientConfig;
    use consumer::base_consumer::BaseConsumer;
    use topic_partition_list::TopicPartitionList;

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

        let committed = Arc::new(Mutex::new(None));
        let committed_clone = Arc::clone(&committed);
        let reg = AutoCommitRegistry::new(
            Duration::from_secs(2),
            CommitMode::Async,
            &consumer,
            move |offsets, _| {
                let mut c = committed_clone.lock().unwrap();
                (*c) = Some(offsets.clone());
            },
        );

        let reg_clone = reg.clone();
        let t = thread::spawn(move || {
            for i in 0..4 {
                reg_clone.register_message(("a".to_owned(), 0, i as i64));
                reg_clone.register_message(("a".to_owned(), 1, i + 1 as i64));
                reg_clone.register_message(("b".to_owned(), 0, i as i64));
                thread::sleep(Duration::from_millis(800));
            }
        });
        let _ = t.join();

        let mut expected = HashMap::new();
        expected.insert(("a".to_owned(), 0), 3);
        expected.insert(("a".to_owned(), 1), 3);
        expected.insert(("b".to_owned(), 0), 2);

        assert_eq!(Some(expected), *committed.lock().unwrap());
    }
}