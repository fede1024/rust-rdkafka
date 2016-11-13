//! Events and event queues.
extern crate librdkafka_sys as rdkafka;

#[derive(Debug)]
pub struct EventQueue {
    pub ptr: *mut rdkafka::rd_kafka_queue_t
}

impl EventQueue {
    pub fn poll(&self, timeout_ms: i32) -> Option<Event> {
        let ptr = unsafe { rdkafka::rd_kafka_queue_poll(self.ptr, timeout_ms) };
        if ptr.is_null() {
            None
        } else {
            Some(Event { ptr: ptr })
        }
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_queue_destroy(self.ptr) };
    }
}

#[derive(Debug)]
pub struct Event {
    pub ptr: *mut rdkafka::rd_kafka_event_t
}

impl Drop for Event {
    fn drop(&mut self) {
        unsafe { rdkafka::rd_kafka_event_destroy(self.ptr) };
    }
}
