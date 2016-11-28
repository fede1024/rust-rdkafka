pub mod base_consumer;
pub mod stream_consumer;

use message::Message;
use error::KafkaResult;

use consumer::base_consumer::BaseConsumer;
use topic_partition_list::TopicPartitionList;

/// Specifies if the commit should be performed synchronously
/// or asynchronously.
pub enum CommitMode {
    /// Synchronous commit.
    Sync,
    /// Asynchronous commit.
    Async,
}

/// Common trait for all consumers
pub trait Consumer {
    /// Returns a reference to the BaseConsumer.
    fn get_base_consumer(&self) -> &BaseConsumer;
    /// Returns a mutable reference to the BaseConsumer.
    fn get_base_consumer_mut(&mut self) -> &mut BaseConsumer;

    // Default implementations

    /// Subscribe the consumer to a list of topics.
    fn subscribe(&mut self, topics: &TopicPartitionList) -> KafkaResult<()> {
        self.get_base_consumer_mut().subscribe(topics)
    }

    /// Commit a specific message. If mode is set to CommitMode::Sync,
    /// the call will block until the message has been succesfully
    /// committed.
    fn commit_message(&self, message: &Message, mode: CommitMode) {
        self.get_base_consumer().commit_message(message, mode);
    }
}
