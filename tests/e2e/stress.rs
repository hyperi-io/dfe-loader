// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! High-Volume Stress Tests
//!
//! Tests for high-throughput scenarios and performance under load

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;

use dfe_loader::buffer::{BufferManager, KafkaOffset};
use dfe_loader::config::BufferConfig;

use crate::common::{
    check_clickhouse_reachable, create_test_client, drop_test_table, load_dotenv, unique_table_name,
};

// ============================================================================
// Skip Helper
// ============================================================================

fn skip_if_no_clickhouse() -> bool {
    load_dotenv();
    if !crate::common::has_clickhouse() {
        return true;
    }
    !check_clickhouse_reachable()
}

// ============================================================================
// High-Volume Insert Tests
// ============================================================================

/// Test inserting 10,000 rows in a single batch
#[tokio::test]
async fn test_stress_10k_single_batch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_10k");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            event String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let row_count: usize = 10_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<u64> = (0..row_count as u64).collect();
    let events: Vec<String> = (0..row_count)
        .map(|i| format!("event_{}", i % 100))
        .collect();
    let values: Vec<f64> = (0..row_count).map(|i| i as f64 * 0.1).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(events)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();

    let start = Instant::now();
    let result = client.insert(&table_name, batch).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "10k insert failed: {:?}", result.err());
    let inserted = result.unwrap();
    assert_eq!(inserted, row_count);

    let rows_per_sec = row_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Stress 10k: {} rows in {:?} ({:.0} rows/sec)",
        inserted, elapsed, rows_per_sec
    );

    drop_test_table(&client, &table_name).await;
}

/// Test inserting 50,000 rows in a single batch
#[tokio::test]
async fn test_stress_50k_single_batch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_50k");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            category String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let row_count = 50_000;
    let categories = ["auth", "api", "web", "mobile", "backend"];

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<u64> = (0..row_count as u64).collect();
    let cats: Vec<&str> = (0..row_count)
        .map(|i| categories[i % categories.len()])
        .collect();
    let values: Vec<f64> = (0..row_count).map(|i| i as f64 * 0.01).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(cats)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();

    let start = Instant::now();
    let result = client.insert(&table_name, batch).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "50k insert failed: {:?}", result.err());
    let inserted = result.unwrap();
    assert_eq!(inserted, row_count as usize);

    let rows_per_sec = row_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Stress 50k: {} rows in {:?} ({:.0} rows/sec)",
        inserted, elapsed, rows_per_sec
    );

    drop_test_table(&client, &table_name).await;
}

/// Test multiple sequential batch inserts
#[tokio::test]
async fn test_stress_multiple_batches() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_multi");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            batch_id UInt64,
            data String
        ) ENGINE = MergeTree()
        ORDER BY (batch_id, id)",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let batch_count = 10;
    let rows_per_batch = 5_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("batch_id", DataType::UInt64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let start = Instant::now();
    let mut total_inserted = 0;

    for batch_id in 0..batch_count {
        let ids: Vec<u64> = (0..rows_per_batch).collect();
        let batch_ids: Vec<u64> = vec![batch_id as u64; rows_per_batch as usize];
        let data: Vec<String> = (0..rows_per_batch)
            .map(|i| format!("data_{}_{}", batch_id, i))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(UInt64Array::from(batch_ids)) as ArrayRef,
                Arc::new(StringArray::from(data)) as ArrayRef,
            ],
        )
        .unwrap();

        let result = client.insert(&table_name, batch).await;
        assert!(result.is_ok(), "Batch {} insert failed", batch_id);
        total_inserted += result.unwrap();
    }

    let elapsed = start.elapsed();
    let rows_per_sec = total_inserted as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Stress multi-batch: {} rows in {} batches, {:?} ({:.0} rows/sec)",
        total_inserted, batch_count, elapsed, rows_per_sec
    );

    assert_eq!(total_inserted, (batch_count * rows_per_batch) as usize);

    drop_test_table(&client, &table_name).await;
}

/// Test concurrent batch inserts
#[tokio::test]
async fn test_stress_concurrent_inserts() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = Arc::new(match create_test_client().await {
        Some(c) => c,
        None => return,
    });

    let table_name = unique_table_name("stress_concurrent");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            thread_id UInt64,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY (thread_id, id)",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let concurrent_count = 8;
    let rows_per_task = 2_000;

    let start = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..concurrent_count {
        let client = client.clone();
        let table_name = table_name.clone();
        let schema = schema.clone();

        let handle = tokio::spawn(async move {
            let ids: Vec<u64> = (0..rows_per_task).collect();
            let thread_ids: Vec<u64> = vec![thread_id as u64; rows_per_task as usize];
            let values: Vec<f64> = (0..rows_per_task).map(|i| i as f64 * 0.5).collect();

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(UInt64Array::from(ids)) as ArrayRef,
                    Arc::new(UInt64Array::from(thread_ids)) as ArrayRef,
                    Arc::new(Float64Array::from(values)) as ArrayRef,
                ],
            )
            .unwrap();

            client.insert(&table_name, batch).await
        });

        handles.push(handle);
    }

    // Wait for all inserts
    let mut total_inserted = 0;
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.expect("Task panicked");
        assert!(result.is_ok(), "Task {} failed: {:?}", i, result.err());
        total_inserted += result.unwrap();
    }

    let elapsed = start.elapsed();
    let rows_per_sec = total_inserted as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Stress concurrent: {} rows from {} tasks in {:?} ({:.0} rows/sec)",
        total_inserted, concurrent_count, elapsed, rows_per_sec
    );

    assert_eq!(total_inserted, (concurrent_count * rows_per_task) as usize);

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Buffer Manager Stress Tests
// ============================================================================

/// Test buffer manager with high message volume
#[test]
fn test_stress_buffer_high_volume() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 1000,
        flush_bytes: 10_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("events");
    let message_count = 10_000;

    let start = Instant::now();

    for i in 0..message_count {
        let data = json!({
            "id": i,
            "event": format!("event_{}", i % 100),
            "value": i as f64 * 0.1
        })
        .as_object()
        .unwrap()
        .clone();

        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: (i % 10) as i32,
            offset: i as i64,
        };

        buffer_manager.push("test.events", data, Some(offset), None);

        // Periodically flush
        if i > 0 && i % 1000 == 0 {
            let batches = buffer_manager.get_ready_for_flush().unwrap();
            if !batches.is_empty() {
                // In real scenario, we'd insert to ClickHouse here
            }
        }
    }

    // Final flush
    let batches = buffer_manager.flush_all().unwrap();
    let elapsed = start.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();
    let msgs_per_sec = message_count as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Buffer stress: {} messages → {} rows in {:?} ({:.0} msg/sec)",
        message_count, total_rows, elapsed, msgs_per_sec
    );
}

/// Test buffer manager with multi-table routing
#[test]
fn test_stress_buffer_multi_table() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 500,
        flush_bytes: 1_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("events");
    let tables = ["auth", "api", "web", "mobile", "metrics"];
    let message_count = 10_000;

    let start = Instant::now();

    for i in 0..message_count {
        let table = tables[i % tables.len()];
        let destination = format!("default.{}", table);

        let data = json!({
            "id": i,
            "table": table,
            "value": i as f64
        })
        .as_object()
        .unwrap()
        .clone();

        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: (i % 5) as i32,
            offset: i as i64,
        };

        buffer_manager.push(&destination, data, Some(offset), None);
    }

    let batches = buffer_manager.flush_all().unwrap();
    let elapsed = start.elapsed();

    let stats = buffer_manager.stats();
    let total_rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();

    eprintln!(
        "✓ Buffer multi-table stress: {} messages → {} batches in {:?}",
        message_count,
        batches.len(),
        elapsed
    );
    eprintln!(
        "  Total rows: {}, Tables: {}",
        total_rows, stats.table_count
    );
}

/// Test offset tracking under load
#[test]
fn test_stress_offset_tracking() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 1000,
        flush_bytes: 10_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("high-volume-topic");
    let partition_count = 16;
    let message_count = 50_000;

    let start = Instant::now();

    for i in 0..message_count {
        let data = json!({"id": i}).as_object().unwrap().clone();
        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: (i % partition_count) as i32,
            offset: (i / partition_count) as i64, // Offset per partition
        };
        buffer_manager.push("test.events", data, Some(offset), None);
    }

    let batches = buffer_manager.flush_all().unwrap();
    let elapsed = start.elapsed();

    // Verify all offsets are tracked
    let total_offsets: usize = batches.iter().map(|b| b.offsets.len()).sum();
    assert_eq!(total_offsets, message_count as usize);

    let msgs_per_sec = message_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Offset tracking stress: {} messages, {} partitions in {:?} ({:.0} msg/sec)",
        message_count, partition_count, elapsed, msgs_per_sec
    );
}

// ============================================================================
// Memory Stress Tests
// ============================================================================

/// Test memory usage with large payloads
#[test]
fn test_stress_large_payloads() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100, // Low threshold to trigger flushes
        flush_bytes: 50_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("large-payload");
    let message_count = 500;
    let payload_size = 10_000; // 10KB per message

    let large_value = "x".repeat(payload_size);

    let start = Instant::now();
    let mut flush_count = 0;

    for i in 0..message_count {
        let data = json!({
            "id": i,
            "large_data": &large_value
        })
        .as_object()
        .unwrap()
        .clone();

        let offset = KafkaOffset {
            topic: topic.clone(),
            partition: 0,
            offset: i as i64,
        };

        buffer_manager.push("test.large", data, Some(offset), None);

        let batches = buffer_manager.get_ready_for_flush().unwrap();
        if !batches.is_empty() {
            flush_count += batches.len();
        }
    }

    let final_batches = buffer_manager.flush_all().unwrap();
    flush_count += final_batches.len();
    let elapsed = start.elapsed();

    eprintln!(
        "✓ Large payload stress: {} messages ({} KB each), {} flushes in {:?}",
        message_count,
        payload_size / 1000,
        flush_count,
        elapsed
    );
}

/// Test rapid buffer flush cycles
#[test]
fn test_stress_rapid_flush_cycles() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10, // Very low threshold
        flush_bytes: 1_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("rapid");
    let cycle_count = 1000;

    let start = Instant::now();
    let mut total_batches = 0;

    for cycle in 0..cycle_count {
        // Push 10 messages to trigger flush
        for i in 0..10 {
            let data = json!({"cycle": cycle, "id": i})
                .as_object()
                .unwrap()
                .clone();
            let offset = KafkaOffset {
                topic: topic.clone(),
                partition: 0,
                offset: (cycle * 10 + i) as i64,
            };
            buffer_manager.push("test.rapid", data, Some(offset), None);
        }

        let batches = buffer_manager.get_ready_for_flush().unwrap();
        total_batches += batches.len();
    }

    let elapsed = start.elapsed();
    let cycles_per_sec = cycle_count as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Rapid flush stress: {} cycles, {} batches in {:?} ({:.0} cycles/sec)",
        cycle_count, total_batches, elapsed, cycles_per_sec
    );

    // Should have roughly cycle_count batches (one per cycle)
    assert!(
        total_batches >= cycle_count as usize * 9 / 10,
        "Expected ~{} batches, got {}",
        cycle_count,
        total_batches
    );
}
