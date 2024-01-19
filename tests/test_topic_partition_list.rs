use rdkafka::{Offset, TopicPartitionList};

/// Test topic partition list API and wrappers.

#[test]
fn test_fmt_debug() {
    {
        let tpl = TopicPartitionList::new();
        assert_eq!(format!("{tpl:?}"), "[]");
    }

    {
        let mut tpl = TopicPartitionList::new();
        tpl.add_topic_unassigned("foo");
        tpl.add_partition("bar", 8);
        tpl.add_partition_offset("bar", 7, Offset::Offset(42))
            .unwrap();
        assert_eq!(
            format!("{tpl:?}"),
            "[TopicPartitionListElem { topic: \"foo\", partition: -1, offset: Invalid, metadata: \"\", error: Ok(()) }, \
              TopicPartitionListElem { topic: \"bar\", partition: 8, offset: Invalid, metadata: \"\", error: Ok(()) }, \
              TopicPartitionListElem { topic: \"bar\", partition: 7, offset: Offset(42), metadata: \"\", error: Ok(()) }]");
    }
}
