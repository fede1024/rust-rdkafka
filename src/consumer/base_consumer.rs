//! Low level consumer wrapper.
use crate::rdsys;
use crate::rdsys::types::*;

use crate::client::{Client, NativeClient};
use crate::config::{FromClientConfig, FromClientConfigAndContext, ClientConfig};
use crate::consumer::{Consumer, ConsumerContext, CommitMode, DefaultConsumerContext};
use crate::error::{KafkaError, KafkaResult, IsError};
use crate::groups::GroupList;
use crate::message::{Message, BorrowedMessage};
use crate::metadata::Metadata;
use crate::topic_partition_list::TopicPartitionList;
use crate::topic_partition_list::Offset::Offset;
use crate::util::{cstr_to_owned, timeout_to_ms};

use std::os::raw::c_void;
use std::str;
use std::mem;
use std::ptr;
use std::time::Duration;

pub(crate) unsafe extern "C" fn native_commit_cb<C: ConsumerContext>(
    _conf: *mut RDKafka,
    err: RDKafkaRespErr,
    offsets: *mut RDKafkaTopicPartitionList,
    opaque_ptr: *mut c_void,
) {
    let context = Box::from_raw(opaque_ptr as *mut C);

    let commit_error = if err.is_error() {
        Err(KafkaError::ConsumerCommit(err.into()))
    } else {
        Ok(())
    };
    (*context).commit_callback(commit_error, offsets);

    mem::forget(context); // Do not free the context
}

/// Native rebalance callback. This callback will run on every rebalance, and it will call the
/// rebalance method defined in the current `Context`.
unsafe extern "C" fn native_rebalance_cb<C: ConsumerContext>(
    rk: *mut RDKafka,
    err: RDKafkaRespErr,
    native_tpl: *mut RDKafkaTopicPartitionList,
    opaque_ptr: *mut c_void,
) {
    // let context: &C = &*(opaque_ptr as *const C);
    let context = Box::from_raw(opaque_ptr as *mut C);
    let native_client = NativeClient::from_ptr(rk);
    let tpl = TopicPartitionList::from_ptr(native_tpl);

    context.rebalance(&native_client, err, &tpl);

    mem::forget(context); // Do not free the context
    mem::forget(native_client); // Do not free native client
    tpl.leak() // Do not free native topic partition list
}


/// Low level wrapper around the librdkafka consumer. This consumer requires to be periodically polled
/// to make progress on rebalance, callbacks and to receive messages.
pub struct BaseConsumer<C: ConsumerContext= DefaultConsumerContext> {
    client: Client<C>,
}

impl FromClientConfig for BaseConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer> {
        BaseConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `BaseConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseConsumer<C>> {
        let native_config = config.create_native_config()?;
        unsafe {
            rdsys::rd_kafka_conf_set_rebalance_cb(native_config.ptr(), Some(native_rebalance_cb::<C>));
            rdsys::rd_kafka_conf_set_offset_commit_cb(native_config.ptr(), Some(native_commit_cb::<C>));
        }
        let client = Client::new(config, native_config, RDKafkaType::RD_KAFKA_CONSUMER, context)?;
        unsafe { rdsys::rd_kafka_poll_set_consumer(client.native_ptr()) };
        Ok(BaseConsumer { client })
    }
}

impl<C: ConsumerContext> BaseConsumer<C> {
    /// Polls the consumer for messages and returns a pointer to the native rdkafka-sys struct.
    /// This method is for internal use only. Use poll instead.
    pub(crate) fn poll_raw(&self, timeout_ms: i32) -> Option<*mut RDKafkaMessage> {
        let message_ptr = unsafe { rdsys::rd_kafka_consumer_poll(self.client.native_ptr(), timeout_ms) };
        if message_ptr.is_null() {
            None
        } else {
            Some(message_ptr)
        }
    }

    /// Polls the consumer for new messages.
    ///
    /// It won't block for more than the specified timeout. Use zero `Duration` for non-blocking
    /// call. With no timeout it blocks until an event is received.
    ///
    /// This method should be called at regular intervals, even if no message is expected,
    /// to serve any queued callbacks waiting to be called. This is especially important for
    /// automatic consumer rebalance, as the rebalance function will be executed by the thread
    /// calling the poll() function.
    ///
    /// # Lifetime
    ///
    /// The returned message lives in the memory of the consumer and cannot outlive it.
    pub fn poll<T: Into<Option<Duration>>>(&self, timeout: T)
        -> Option<KafkaResult<BorrowedMessage>>
    {
        self.poll_raw(timeout_to_ms(timeout))
            .map(|ptr| unsafe { BorrowedMessage::from_consumer(ptr, self) })
    }

    /// Returns an iterator over the available messages.
    ///
    /// It repeatedly calls [`poll`](#method.poll) with no timeout.
    ///
    /// Note that it's also possible to iterate over the consumer directly.
    ///
    /// # Examples
    ///
    /// All these are equivalent and will receive messages without timing out.
    ///
    /// ```rust,no_run
    /// # extern crate rdkafka;
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// loop {
    ///   let message = consumer.poll(None);
    ///   // Handle the message
    /// }
    /// ```
    ///
    /// ```rust,no_run
    /// # extern crate rdkafka;
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// for message in consumer.iter() {
    ///   // Handle the message
    /// }
    /// ```
    ///
    /// ```rust,no_run
    /// # extern crate rdkafka;
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// for message in &consumer {
    ///   // Handle the message
    /// }
    /// ```
    pub fn iter(&self) -> Iter<C> {
        Iter(self)
    }
}

impl<C: ConsumerContext> Consumer<C> for BaseConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        self
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();
        for topic in topics {
            tpl.add_topic_unassigned(topic);
        }
        let ret_code = unsafe { rdsys::rd_kafka_subscribe(self.client.native_ptr(), tpl.ptr()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn unsubscribe(&self) {
        unsafe { rdsys::rd_kafka_unsubscribe(self.client.native_ptr()) };
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let ret_code = unsafe { rdsys::rd_kafka_assign(self.client.native_ptr(), assignment.ptr()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn commit(&self, topic_partition_list: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_commit(self.client.native_ptr(), topic_partition_list.ptr(), mode as i32)
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_commit(self.client.native_ptr(), ptr::null_mut(), mode as i32)
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn commit_message(&self, message: &BorrowedMessage, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe { rdsys::rd_kafka_commit_message(self.client.native_ptr(), message.ptr(), mode as i32) };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn store_offset(&self, message: &BorrowedMessage) -> KafkaResult<()> {
        let error = unsafe { rdsys::rd_kafka_offset_store(message.topic_ptr(), message.partition(), message.offset()) };
        if error.is_error() {
            Err(KafkaError::StoreOffset(error.into()))
        } else {
            Ok(())
        }
    }

    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        let error = unsafe { rdsys::rd_kafka_offsets_store(self.client.native_ptr(), tpl.ptr()) };
        if error.is_error() {
            Err(KafkaError::StoreOffset(error.into()))
        } else {
            Ok(())
        }
    }

    fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let error = unsafe { rdsys::rd_kafka_subscription(self.client.native_ptr(), &mut tpl_ptr) };

        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };

        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn committed<T: Into<Option<Duration>>>(&self, timeout: T) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let assignment_error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };
        if assignment_error.is_error() {
            return Err(KafkaError::MetadataFetch(assignment_error.into()));
        }

        let committed_error = unsafe {
            rdsys::rd_kafka_committed(self.client.native_ptr(), tpl_ptr, timeout_to_ms(timeout))
        };

        if committed_error.is_error() {
            Err(KafkaError::MetadataFetch(committed_error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn offsets_for_timestamp<T: Into<Option<Duration>>>(&self, timestamp: i64, timeout: T)
        -> KafkaResult<TopicPartitionList>
    {
        let mut tpl_ptr = ptr::null_mut();
        let assignment_error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };
        if assignment_error.is_error() {
            return Err(KafkaError::MetadataFetch(assignment_error.into()));
        }
        let mut tpl = unsafe { TopicPartitionList::from_ptr(tpl_ptr) };

        // Set the timestamp we want in the offset field for every partition as librdkafka expects.
        tpl.set_all_offsets(Offset(timestamp));

        // This call will then put the offset in the offset field of this topic partition list.
        let offsets_for_times_error = unsafe {
            rdsys::rd_kafka_offsets_for_times(
                self.client.native_ptr(),
                tpl.ptr(),
                timeout_to_ms(timeout)
            )
        };

        if offsets_for_times_error.is_error() {
            Err(KafkaError::MetadataFetch(offsets_for_times_error.into()))
        } else {
            Ok(tpl)
        }
    }

    fn offsets_for_times<T: Into<Option<Duration>>>(&self, mut timestamps: TopicPartitionList, timeout: T)
                                                    -> KafkaResult<TopicPartitionList>
    {
        // This call will then put the offset in the offset field of this topic partition list.
        let offsets_for_times_error = unsafe {
            rdsys::rd_kafka_offsets_for_times(
                self.client.native_ptr(),
                timestamps.ptr(),
                timeout_to_ms(timeout)
            )
        };

        if offsets_for_times_error.is_error() {
            Err(KafkaError::MetadataFetch(offsets_for_times_error.into()))
        } else {
            Ok(timestamps)
        }
    }

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let error = unsafe {
            // TODO: improve error handling
            rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr);
            rdsys::rd_kafka_position(self.client.native_ptr(), tpl_ptr)
        };

        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn fetch_metadata<T: Into<Option<Duration>>>(&self, topic: Option<&str>, timeout: T)
        -> KafkaResult<Metadata>
    {
        self.client.fetch_metadata(topic, timeout)
    }

    fn fetch_watermarks<T: Into<Option<Duration>>>(&self, topic: &str, partition: i32, timeout:T)
        -> KafkaResult<(i64, i64)>
    {
        self.client
            .fetch_watermarks(topic, partition, timeout)
    }

    fn fetch_group_list<T: Into<Option<Duration>>>(&self, group: Option<&str>, timeout: T)
        -> KafkaResult<GroupList>
    {
        self.client.fetch_group_list(group, timeout)
    }
}

impl<C: ConsumerContext> Drop for BaseConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroying consumer: {:?}", self.client.native_ptr()); // TODO: fix me (multiple executions ?)
        unsafe { rdsys::rd_kafka_consumer_close(self.client.native_ptr()) };
        trace!("Consumer destroyed: {:?}", self.client.native_ptr());
    }
}

/// Iterator for more convenient interface.
///
/// It simply repeatedly calls [`BaseConsumer::poll`](struct.BaseConsumer.html#method.poll).
pub struct Iter<'a, C: ConsumerContext + 'a>(&'a BaseConsumer<C>);

impl<'a, C: ConsumerContext + 'a> Iterator for Iter<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.0.poll(None) {
                return Some(item)
            }
        }
    }
}

impl<'a, C: ConsumerContext + 'a> IntoIterator for &'a BaseConsumer<C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;
    type IntoIter = Iter<'a, C>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
