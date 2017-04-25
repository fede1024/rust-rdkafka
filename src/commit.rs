use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type OffsetMap = HashMap<(String, i32), i64>;

struct AutoCommitRegistryInner<F: Fn(&OffsetMap)> {
    offsets: OffsetMap,
    commit_interval: Duration,
    last_commit_time: Instant,
    callback: F,
}

pub struct AutoCommitRegistry<F: Fn(&OffsetMap)> {
    inner: Arc<Mutex<AutoCommitRegistryInner<F>>>
}

impl<F: Fn(&OffsetMap)> Clone for AutoCommitRegistry<F> {
    fn clone(&self) -> Self {
        AutoCommitRegistry {
            inner: Arc::clone(&self.inner)
        }
    }
}

impl<F: Fn(&OffsetMap)> AutoCommitRegistry<F> {
    pub fn new(commit_interval: Duration, callback: F) -> AutoCommitRegistry<F> {
        let inner = AutoCommitRegistryInner {
            offsets: HashMap::new(),
            commit_interval: commit_interval,
            last_commit_time: Instant::now(),
            callback: callback,
        };
        AutoCommitRegistry {
            inner: Arc::new(Mutex::new(inner))
        }
    }

    pub fn register_message(&mut self, message_id: (String, i32, i64)) {
        {
            let mut inner = self.inner.lock().unwrap();
            (*inner).offsets.insert((message_id.0, message_id.1), message_id.2);
        }
        self.maybe_commit();
    }

    pub fn maybe_commit(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        if now.duration_since((*inner).last_commit_time) >= (*inner).commit_interval {
            println!("Do commit");
            println!("?? {:?}", inner.offsets);
            ((*inner).callback)(&(*inner).offsets);
            (*inner).last_commit_time = now;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;
    use std::time;

    #[test]
    fn test_auto_commit_registry() {
        let reg = AutoCommitRegistry::new(
            Duration::from_secs(2),
            |m| println!(">>> {:?}", m),
        );

        let mut reg_clone = reg.clone();
        let t = thread::spawn(move || {
            for i in 0..4 {
                reg_clone.register_message(("a".to_owned(), 0, i as i64));
                reg_clone.register_message(("a".to_owned(), 1, i + 1 as i64));
                reg_clone.register_message(("b".to_owned(), 0, i as i64));
                thread::sleep(Duration::from_millis(800));
            }
        });
        let _ = t.join();
    }

}