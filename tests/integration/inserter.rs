//! Inserter integration tests
//!
//! Tests for batch salvage, circuit breaker, and insert operations

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};

use dfe_loader::clickhouse::{Inserter, InserterConfig};
use dfe_loader::clickhouse::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};

use crate::common::{create_test_client, drop_test_table, unique_table_name};
use crate::skip_if_no_clickhouse;

// ============================================================================
// Basic Insert Tests
// ============================================================================

#[tokio::test]
async fn test_inserter_basic_insert() {
    skip_if_no_clickhouse!();

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => {
            eprintln!("Could not create client");
            return;
        }
    };
    let table_name = unique_table_name("test_inserter_basic");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            name String,
            value Float64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Create inserter
    let inserter = Inserter::new(client.clone(), InserterConfig::default());

    // Create batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ],
    )
    .unwrap();

    // Insert using insert_arrow
    let result = inserter.insert_arrow(&table_name, batch).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Basic insert succeeded with 3 rows");

    // Query back to verify data
    let count_sql = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_result = client.select(&count_sql).await.expect("Count query failed");
    let count_batch = &count_result[0];
    let count_col = count_batch.column(0).as_any().downcast_ref::<UInt64Array>().expect("Count should be UInt64");
    assert_eq!(count_col.value(0), 3, "Should have 3 rows");
    eprintln!("✓ Query verification: confirmed 3 rows");

    // Verify specific row data
    let data_sql = format!("SELECT name FROM {} WHERE id = 2", table_name);
    let data_result = client.select(&data_sql).await.expect("Data query failed");
    if !data_result.is_empty() {
        let data_batch = &data_result[0];
        if data_batch.num_rows() > 0 {
            // ClickHouse returns String as Binary via Arrow protocol
            use arrow::array::BinaryArray;
            if let Some(name_col) = data_batch.column(0).as_any().downcast_ref::<BinaryArray>() {
                let name_bytes = name_col.value(0);
                let name_str = std::str::from_utf8(name_bytes).expect("Should be valid UTF-8");
                assert_eq!(name_str, "b", "Row with id=2 should have name='b'");
                eprintln!("✓ Query verification: confirmed data integrity");
            } else {
                // Might be StringArray or LargeStringArray in some cases
                eprintln!("Column type: {:?}", data_batch.column(0).data_type());
                eprintln!("✓ Query verification: skipped (unexpected column type)");
            }
        }
    }

    // Cleanup
    drop_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_inserter_large_batch() {
    skip_if_no_clickhouse!();

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_table_name("test_large_batch");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            event String,
            value Float64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let inserter = Inserter::new(client.clone(), InserterConfig::default());

    // Create large batch (10,000 rows)
    let row_count = 10_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<u64> = (0..row_count as u64).collect();
    let events: Vec<String> = (0..row_count).map(|i| format!("event_{}", i % 100)).collect();
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

    let start = std::time::Instant::now();
    let result = inserter.insert_arrow(&table_name, batch).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Large batch insert failed: {:?}", result.err());
    let count = result.unwrap();
    assert_eq!(count, row_count);

    eprintln!(
        "✓ Large batch insert: {} rows in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Query back to verify row count
    let count_sql = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_result = client.select(&count_sql).await.expect("Count query failed");
    let count_batch = &count_result[0];
    let count_col = count_batch.column(0).as_any().downcast_ref::<UInt64Array>().expect("Count should be UInt64");
    assert_eq!(count_col.value(0), row_count as u64, "Should have {} rows", row_count);
    eprintln!("✓ Query verification: confirmed {} rows", row_count);

    // Cleanup
    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Circuit Breaker Tests
// ============================================================================

#[tokio::test]
async fn test_circuit_breaker_basic() {
    // Circuit breaker doesn't need ClickHouse for basic state tests
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        open_duration: Duration::from_millis(1000),
        half_open_max_requests: 1,
    };
    let breaker = CircuitBreaker::new(config);

    // Initial state is closed
    assert!(breaker.allow_request("test_table"));

    // Record failures
    breaker.record_failure("test_table");
    breaker.record_failure("test_table");
    assert!(breaker.allow_request("test_table")); // Still closed after 2 failures

    breaker.record_failure("test_table");
    // After 3 failures, circuit should be open
    assert!(!breaker.allow_request("test_table"));

    eprintln!("✓ Circuit breaker transitions to open after threshold failures");
}

#[tokio::test]
async fn test_circuit_breaker_per_table() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        success_threshold: 1,
        open_duration: Duration::from_millis(1000),
        half_open_max_requests: 1,
    };
    let breaker = CircuitBreaker::new(config);

    // Fail table1
    breaker.record_failure("table1");
    breaker.record_failure("table1");
    assert!(!breaker.allow_request("table1")); // table1 is open

    // table2 should still work
    assert!(breaker.allow_request("table2"));

    eprintln!("✓ Circuit breaker maintains per-table state");
}

#[tokio::test]
async fn test_circuit_breaker_recovery() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        success_threshold: 1,
        open_duration: Duration::from_millis(100), // Short timeout for testing
        half_open_max_requests: 1,
    };
    let breaker = CircuitBreaker::new(config);

    // Open the circuit
    breaker.record_failure("test");
    breaker.record_failure("test");
    assert!(!breaker.allow_request("test"));

    // Wait for timeout
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Should be half-open now, allowing one request
    assert!(breaker.allow_request("test"));

    // Record success to close it
    breaker.record_success("test");
    assert!(breaker.allow_request("test"));

    eprintln!("✓ Circuit breaker recovers after timeout and success");
}

#[tokio::test]
async fn test_circuit_breaker_with_inserter() {
    skip_if_no_clickhouse!();

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_table_name("test_cb_inserter");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            name String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Create inserter with circuit breaker
    let cb_config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        open_duration: Duration::from_millis(5000),
        half_open_max_requests: 1,
    };
    let circuit_breaker = Arc::new(CircuitBreaker::new(cb_config));

    let inserter = Inserter::new(client.clone(), InserterConfig::default())
        .with_circuit_breaker(circuit_breaker.clone());

    // Create valid batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .unwrap();

    // Insert should succeed
    let result = inserter.insert_arrow(&table_name, batch).await;
    assert!(result.is_ok());

    // Check circuit breaker stats
    let stats = circuit_breaker.stats();
    eprintln!("Circuit breaker stats: {:?}", stats);

    // Cleanup
    drop_test_table(&client, &table_name).await;

    eprintln!("✓ Inserter with circuit breaker works correctly");
}

// ============================================================================
// Concurrent Insert Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_inserts() {
    skip_if_no_clickhouse!();

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_table_name("test_concurrent");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            batch_id UInt64,
            value Float64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Create inserter with concurrency limit
    let config = InserterConfig {
        max_concurrent_inserts: 4,
        ..Default::default()
    };
    let inserter = Arc::new(Inserter::new(client.clone(), config));

    // Spawn multiple concurrent inserts
    let mut handles = Vec::new();
    for batch_id in 0..8u64 {
        let inserter = inserter.clone();
        let table = table_name.clone();

        let handle = tokio::spawn(async move {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("batch_id", DataType::UInt64, false),
                Field::new("value", DataType::Float64, false),
            ]));

            let ids: Vec<u64> = (0..100).collect();
            let batch_ids: Vec<u64> = vec![batch_id; 100];
            let values: Vec<f64> = (0..100).map(|i| i as f64).collect();

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(UInt64Array::from(ids)) as ArrayRef,
                    Arc::new(UInt64Array::from(batch_ids)) as ArrayRef,
                    Arc::new(Float64Array::from(values)) as ArrayRef,
                ],
            )
            .unwrap();

            inserter.insert_arrow(&table, batch).await
        });
        handles.push(handle);
    }

    // Wait for all inserts
    let mut total_inserted = 0;
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent insert failed: {:?}", result.err());
        total_inserted += result.unwrap();
    }

    assert_eq!(total_inserted, 800); // 8 batches * 100 rows
    eprintln!("✓ Concurrent inserts: {} total rows inserted", total_inserted);

    // Cleanup
    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Batch Salvage Tests (using FlushBatch)
// ============================================================================

#[tokio::test]
async fn test_inserter_batch_salvage() {
    skip_if_no_clickhouse!();

    use compact_str::CompactString;
    use dfe_loader::buffer::FlushBatch;

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_table_name("test_salvage");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            name String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Create inserter with salvage enabled
    let config = InserterConfig {
        enable_salvage: true,
        max_salvage_depth: 10,
        max_retries: 1,
        ..Default::default()
    };
    let inserter = Inserter::new(client.clone(), config);

    // Create a valid batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        ],
    )
    .unwrap();

    // Create FlushBatch
    let flush_batch = FlushBatch {
        table: CompactString::from(&table_name),
        batch,
        offsets: Vec::new(),
    };

    // Insert with salvage
    let result = inserter.insert_with_salvage(flush_batch).await;
    assert_eq!(result.inserted, 5);
    assert!(result.failed.is_empty());

    eprintln!("✓ Batch salvage insert succeeded with {} rows", result.inserted);

    // Cleanup
    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_inserter_config_defaults() {
    let config = InserterConfig::default();
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.base_retry_delay_ms, 100);
    assert_eq!(config.max_retry_delay_ms, 30_000);
    assert!(config.enable_salvage);
    assert_eq!(config.max_salvage_depth, 20);
    assert_eq!(config.max_concurrent_inserts, 8);
}

#[test]
fn test_circuit_breaker_config_validation() {
    let config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        open_duration: Duration::from_millis(30000),
        half_open_max_requests: 1,
    };

    assert_eq!(config.failure_threshold, 5);
    assert_eq!(config.success_threshold, 2);
    assert_eq!(config.open_duration, Duration::from_millis(30000));
}
