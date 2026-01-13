//! Kafka Offset Commit Integration Tests
//!
//! Tests for offset tracking and commit functionality

use std::collections::HashMap;
use std::sync::Arc;

use dfe_loader::buffer::KafkaOffset;

// ============================================================================
// Unit Tests for Offset Tracking Logic
// ============================================================================

#[test]
fn test_kafka_offset_creation() {
    let offset = KafkaOffset {
        topic: Arc::from("test-topic"),
        partition: 0,
        offset: 100,
    };

    assert_eq!(offset.topic.as_ref(), "test-topic");
    assert_eq!(offset.partition, 0);
    assert_eq!(offset.offset, 100);
}

#[test]
fn test_kafka_offset_arc_sharing() {
    // Multiple offsets from same topic should share Arc
    let topic: Arc<str> = Arc::from("shared-topic");

    let offset1 = KafkaOffset {
        topic: topic.clone(),
        partition: 0,
        offset: 100,
    };

    let offset2 = KafkaOffset {
        topic: topic.clone(),
        partition: 1,
        offset: 200,
    };

    // Verify Arc sharing (same pointer)
    assert!(Arc::ptr_eq(&offset1.topic, &offset2.topic));
}

#[test]
fn test_offset_grouping_by_partition() {
    // Simulate grouping logic from commit_kafka_offsets
    let offsets = vec![
        KafkaOffset {
            topic: Arc::from("topic-a"),
            partition: 0,
            offset: 100,
        },
        KafkaOffset {
            topic: Arc::from("topic-a"),
            partition: 0,
            offset: 150,
        },
        KafkaOffset {
            topic: Arc::from("topic-a"),
            partition: 1,
            offset: 50,
        },
        KafkaOffset {
            topic: Arc::from("topic-b"),
            partition: 0,
            offset: 200,
        },
    ];

    // Group by topic/partition, keeping highest offset
    let mut max_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();

    for off in &offsets {
        let key = (off.topic.clone(), off.partition);
        max_offsets
            .entry(key)
            .and_modify(|existing| {
                if off.offset > *existing {
                    *existing = off.offset;
                }
            })
            .or_insert(off.offset);
    }

    // Verify grouping
    assert_eq!(max_offsets.len(), 3); // 3 unique topic/partition pairs

    let topic_a: Arc<str> = Arc::from("topic-a");
    let topic_b: Arc<str> = Arc::from("topic-b");

    assert_eq!(max_offsets.get(&(topic_a.clone(), 0)), Some(&150)); // Max of 100, 150
    assert_eq!(max_offsets.get(&(topic_a.clone(), 1)), Some(&50));
    assert_eq!(max_offsets.get(&(topic_b, 0)), Some(&200));
}

#[test]
fn test_offset_ordering() {
    // Offsets should be orderable for processing
    let mut offsets = vec![
        KafkaOffset {
            topic: Arc::from("topic"),
            partition: 0,
            offset: 300,
        },
        KafkaOffset {
            topic: Arc::from("topic"),
            partition: 0,
            offset: 100,
        },
        KafkaOffset {
            topic: Arc::from("topic"),
            partition: 0,
            offset: 200,
        },
    ];

    // Sort by offset
    offsets.sort_by_key(|o| o.offset);

    assert_eq!(offsets[0].offset, 100);
    assert_eq!(offsets[1].offset, 200);
    assert_eq!(offsets[2].offset, 300);
}

#[test]
fn test_empty_offset_list() {
    let offsets: Vec<KafkaOffset> = vec![];

    // Group by topic/partition
    let mut max_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();

    for off in &offsets {
        let key = (off.topic.clone(), off.partition);
        max_offsets.entry(key).or_insert(off.offset);
    }

    assert!(max_offsets.is_empty());
}

#[test]
fn test_offset_plus_one_for_commit() {
    // Kafka commits the NEXT offset to consume
    let offset = KafkaOffset {
        topic: Arc::from("topic"),
        partition: 0,
        offset: 99,
    };

    // When committing, we add 1 to get the next offset to consume
    let commit_offset = offset.offset + 1;
    assert_eq!(commit_offset, 100);
}

#[test]
fn test_multi_topic_offset_tracking() {
    let topic1: Arc<str> = Arc::from("events");
    let topic2: Arc<str> = Arc::from("logs");
    let topic3: Arc<str> = Arc::from("metrics");

    let offsets = vec![
        KafkaOffset {
            topic: topic1.clone(),
            partition: 0,
            offset: 1000,
        },
        KafkaOffset {
            topic: topic1.clone(),
            partition: 1,
            offset: 2000,
        },
        KafkaOffset {
            topic: topic2.clone(),
            partition: 0,
            offset: 500,
        },
        KafkaOffset {
            topic: topic3.clone(),
            partition: 0,
            offset: 100,
        },
        KafkaOffset {
            topic: topic3.clone(),
            partition: 1,
            offset: 200,
        },
        KafkaOffset {
            topic: topic3.clone(),
            partition: 2,
            offset: 300,
        },
    ];

    // Group by topic/partition
    let mut max_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();
    for off in &offsets {
        let key = (off.topic.clone(), off.partition);
        max_offsets
            .entry(key)
            .and_modify(|existing| {
                if off.offset > *existing {
                    *existing = off.offset;
                }
            })
            .or_insert(off.offset);
    }

    // 6 unique topic/partition combinations
    assert_eq!(max_offsets.len(), 6);

    // Verify each
    assert_eq!(max_offsets.get(&(topic1.clone(), 0)), Some(&1000));
    assert_eq!(max_offsets.get(&(topic1.clone(), 1)), Some(&2000));
    assert_eq!(max_offsets.get(&(topic2.clone(), 0)), Some(&500));
    assert_eq!(max_offsets.get(&(topic3.clone(), 0)), Some(&100));
    assert_eq!(max_offsets.get(&(topic3.clone(), 1)), Some(&200));
    assert_eq!(max_offsets.get(&(topic3.clone(), 2)), Some(&300));
}

#[test]
fn test_offset_batch_collection() {
    // Simulate collecting offsets during batch processing
    let mut batch_offsets: Vec<KafkaOffset> = Vec::new();

    // Simulate processing 10 messages from a batch
    let topic: Arc<str> = Arc::from("events");
    for i in 0..10 {
        batch_offsets.push(KafkaOffset {
            topic: topic.clone(),
            partition: i % 3, // Distribute across 3 partitions
            offset: 1000 + i as i64,
        });
    }

    assert_eq!(batch_offsets.len(), 10);

    // Group and find max per partition
    let mut max_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();
    for off in &batch_offsets {
        let key = (off.topic.clone(), off.partition);
        max_offsets
            .entry(key)
            .and_modify(|existing| {
                if off.offset > *existing {
                    *existing = off.offset;
                }
            })
            .or_insert(off.offset);
    }

    // 3 partitions
    assert_eq!(max_offsets.len(), 3);

    // Partition 0: messages 0, 3, 6, 9 -> offsets 1000, 1003, 1006, 1009 -> max 1009
    // Partition 1: messages 1, 4, 7 -> offsets 1001, 1004, 1007 -> max 1007
    // Partition 2: messages 2, 5, 8 -> offsets 1002, 1005, 1008 -> max 1008
    assert_eq!(max_offsets.get(&(topic.clone(), 0)), Some(&1009));
    assert_eq!(max_offsets.get(&(topic.clone(), 1)), Some(&1007));
    assert_eq!(max_offsets.get(&(topic.clone(), 2)), Some(&1008));
}

// ============================================================================
// Buffer Offset Tracking Tests
// ============================================================================

#[test]
fn test_buffer_offset_accumulation() {
    use dfe_loader::buffer::BufferManager;
    use dfe_loader::config::BufferConfig;
    use serde_json::json;

    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 5,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("events");

    // Push messages with offsets
    for i in 0..5 {
        let data = json!({"id": i}).as_object().unwrap().clone();
        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: 0,
            offset: 100 + i as i64,
        };
        buffer_manager.push("test.events", data, Some(offset));
    }

    // Flush and check offsets are included
    let batches = buffer_manager.get_ready_for_flush().unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.batch.num_rows(), 5);
    assert_eq!(batch.offsets.len(), 5);

    // Verify offsets
    assert_eq!(batch.offsets[0].offset, 100);
    assert_eq!(batch.offsets[4].offset, 104);
}

#[test]
fn test_buffer_mixed_offset_tracking() {
    use dfe_loader::buffer::BufferManager;
    use dfe_loader::config::BufferConfig;
    use serde_json::json;

    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    // Push messages to different tables with different offsets
    let topic: Arc<str> = Arc::from("events");

    for i in 0..5 {
        let data = json!({"id": i, "table": "auth"}).as_object().unwrap().clone();
        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: 0,
            offset: i as i64,
        };
        buffer_manager.push("default.auth", data, Some(offset));
    }

    for i in 0..5 {
        let data = json!({"id": i, "table": "api"}).as_object().unwrap().clone();
        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: 1,
            offset: 100 + i as i64,
        };
        buffer_manager.push("default.api", data, Some(offset));
    }

    // Both tables should have 5 rows but not trigger flush (threshold is 10)
    assert_eq!(buffer_manager.pending_rows(), 10);

    // Force flush all
    let batches = buffer_manager.flush_all().unwrap();
    assert_eq!(batches.len(), 2);

    // Check offsets are partitioned correctly
    for batch in &batches {
        assert_eq!(batch.offsets.len(), 5);
        let partition = batch.offsets[0].partition;
        for offset in &batch.offsets {
            assert_eq!(offset.partition, partition);
        }
    }
}
