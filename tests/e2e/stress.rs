// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! High-Volume Stress Tests
//!
//! Tests for high-throughput scenarios and performance under load

use std::sync::Arc;
use std::time::Instant;

use serde_json::json;

use dfe_loader::buffer::{BufferManager, KafkaOffset};
use dfe_loader::config::BufferConfig;

use crate::common::{
    check_clickhouse_reachable, create_http_test_client, drop_http_test_table, load_dotenv,
    unique_table_name,
};

fn skip_if_no_clickhouse() -> bool {
    load_dotenv();
    if !crate::common::has_clickhouse() {
        return true;
    }
    !check_clickhouse_reachable()
}

fn make_stress_rows(
    count: usize,
    columns: &[&str],
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    (0..count)
        .map(|i| {
            let mut row = serde_json::Map::new();
            for &col in columns {
                match col {
                    "id" => {
                        row.insert("id".into(), json!(i as u64));
                    }
                    "event" => {
                        row.insert("event".into(), json!(format!("event_{}", i % 100)));
                    }
                    "category" => {
                        let categories = ["auth", "api", "web", "mobile", "backend"];
                        row.insert("category".into(), json!(categories[i % categories.len()]));
                    }
                    "value" => {
                        row.insert("value".into(), json!(i as f64 * 0.1));
                    }
                    "batch_id" => {
                        row.insert("batch_id".into(), json!(0_u64));
                    }
                    "data" => {
                        row.insert("data".into(), json!(format!("data_{}", i)));
                    }
                    "thread_id" => {
                        row.insert("thread_id".into(), json!(0_u64));
                    }
                    _ => {}
                }
            }
            row
        })
        .collect()
}

#[tokio::test]
async fn test_stress_10k_single_batch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_10k");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            event String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let row_count: usize = 10_000;
    let rows = make_stress_rows(row_count, &["id", "event", "value"]);

    let start = Instant::now();
    let result = client.insert_json_rows(&table_name, &rows).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "10k insert failed: {:?}", result.err());
    let inserted = result.unwrap();
    assert_eq!(inserted, row_count);

    let rows_per_sec = row_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Stress 10k: {} rows in {:?} ({:.0} rows/sec)",
        inserted, elapsed, rows_per_sec
    );

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_stress_50k_single_batch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_50k");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            category String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let row_count = 50_000;
    let rows = make_stress_rows(row_count, &["id", "category", "value"]);

    let start = Instant::now();
    let result = client.insert_json_rows(&table_name, &rows).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "50k insert failed: {:?}", result.err());
    let inserted = result.unwrap();
    assert_eq!(inserted, row_count);

    let rows_per_sec = row_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Stress 50k: {} rows in {:?} ({:.0} rows/sec)",
        inserted, elapsed, rows_per_sec
    );

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_stress_multiple_batches() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("stress_multi");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            batch_id UInt64,
            data String
        ) ENGINE = MergeTree()
        ORDER BY (batch_id, id)",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let batch_count = 10_u64;
    let rows_per_batch = 5_000_usize;

    let start = Instant::now();
    let mut total_inserted = 0;

    for batch_id in 0..batch_count {
        let rows: Vec<serde_json::Map<String, serde_json::Value>> = (0..rows_per_batch)
            .map(|i| {
                json!({
                    "id": i as u64,
                    "batch_id": batch_id,
                    "data": format!("data_{}_{}", batch_id, i)
                })
                .as_object()
                .unwrap()
                .clone()
            })
            .collect();

        let result = client.insert_json_rows(&table_name, &rows).await;
        assert!(result.is_ok(), "Batch {} insert failed", batch_id);
        total_inserted += result.unwrap();
    }

    let elapsed = start.elapsed();
    let rows_per_sec = total_inserted as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Stress multi-batch: {} rows in {} batches, {:?} ({:.0} rows/sec)",
        total_inserted, batch_count, elapsed, rows_per_sec
    );

    assert_eq!(total_inserted, (batch_count as usize) * rows_per_batch);

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_stress_concurrent_inserts() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping stress test: no ClickHouse available");
        return;
    }

    let client = Arc::new(match create_http_test_client() {
        Some(c) => c,
        None => return,
    });

    let table_name = unique_table_name("stress_concurrent");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            thread_id UInt64,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY (thread_id, id)",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let concurrent_count = 8_u64;
    let rows_per_task = 2_000_usize;

    let start = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..concurrent_count {
        let client = client.clone();
        let table_name = table_name.clone();

        let handle = tokio::spawn(async move {
            let rows: Vec<serde_json::Map<String, serde_json::Value>> = (0..rows_per_task)
                .map(|i| {
                    json!({
                        "id": i as u64,
                        "thread_id": thread_id,
                        "value": i as f64 * 0.5
                    })
                    .as_object()
                    .unwrap()
                    .clone()
                })
                .collect();

            client.insert_json_rows(&table_name, &rows).await
        });

        handles.push(handle);
    }

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

    assert_eq!(total_inserted, (concurrent_count as usize) * rows_per_task);

    drop_http_test_table(&client, &table_name).await;
}

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
            partition: (i % 10),
            offset: i as i64,
        };

        buffer_manager.push("test.events", data, Some(offset));

        if i > 0 && i % 1000 == 0 {
            let batches = buffer_manager.get_ready_for_flush();
            if !batches.is_empty() {
                // In real scenario, we'd insert to ClickHouse here
            }
        }
    }

    let batches = buffer_manager.flush_all();
    let elapsed = start.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
    let msgs_per_sec = message_count as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Buffer stress: {} messages → {} rows in {:?} ({:.0} msg/sec)",
        message_count, total_rows, elapsed, msgs_per_sec
    );
}

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

        buffer_manager.push(&destination, data, Some(offset));
    }

    let batches = buffer_manager.flush_all();
    let elapsed = start.elapsed();

    let stats = buffer_manager.stats();
    let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();

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
            partition: (i % partition_count),
            offset: (i / partition_count) as i64,
        };
        buffer_manager.push("test.events", data, Some(offset));
    }

    let batches = buffer_manager.flush_all();
    let elapsed = start.elapsed();

    let total_offsets: usize = batches.iter().map(|b| b.offsets.len()).sum();
    assert_eq!(total_offsets, message_count as usize);

    let msgs_per_sec = message_count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "✓ Offset tracking stress: {} messages, {} partitions in {:?} ({:.0} msg/sec)",
        message_count, partition_count, elapsed, msgs_per_sec
    );
}

#[test]
fn test_stress_large_payloads() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 50_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("large-payload");
    let message_count = 500;
    let payload_size = 10_000;

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

        buffer_manager.push("test.large", data, Some(offset));

        let batches = buffer_manager.get_ready_for_flush();
        if !batches.is_empty() {
            flush_count += batches.len();
        }
    }

    let final_batches = buffer_manager.flush_all();
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

#[test]
fn test_stress_rapid_flush_cycles() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 1_000_000,
        flush_age_secs: 60,
    });

    let topic: Arc<str> = Arc::from("rapid");
    let cycle_count = 1000;

    let start = Instant::now();
    let mut total_batches = 0;

    for cycle in 0..cycle_count {
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
            buffer_manager.push("test.rapid", data, Some(offset));
        }

        let batches = buffer_manager.get_ready_for_flush();
        total_batches += batches.len();
    }

    let elapsed = start.elapsed();
    let cycles_per_sec = cycle_count as f64 / elapsed.as_secs_f64();

    eprintln!(
        "✓ Rapid flush stress: {} cycles, {} batches in {:?} ({:.0} cycles/sec)",
        cycle_count, total_batches, elapsed, cycles_per_sec
    );

    assert!(
        total_batches >= cycle_count as usize * 9 / 10,
        "Expected ~{} batches, got {}",
        cycle_count,
        total_batches
    );
}
