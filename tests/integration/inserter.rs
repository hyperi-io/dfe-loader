// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Inserter integration tests
//!
//! Tests for batch salvage, circuit breaker, and insert operations

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};

use dfe_loader::clickhouse::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use dfe_loader::clickhouse::config::InsertFormat;
use dfe_loader::clickhouse::{Inserter, InserterConfig};

use crate::common::{
    create_ch_test_client, create_http_test_client, drop_http_test_table,
    unique_qualified_table_name, unique_table_name,
};
use crate::{skip_if_no_clickhouse, skip_if_not_replicated};

/// Helper: create JSON rows for testing
fn make_test_rows(count: usize) -> Vec<Map<String, Value>> {
    (0..count)
        .map(|i| {
            json!({
                "id": i as u64,
                "name": format!("row_{}", i),
                "value": i as f64 * 1.1
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect()
}

// ============================================================================
// Basic Insert Tests
// ============================================================================

#[tokio::test]
async fn test_inserter_basic_insert() {
    skip_if_no_clickhouse!();

    let client = if let Some(c) = create_http_test_client() {
        Arc::new(c)
    } else {
        eprintln!("Could not create HTTP client");
        return;
    };
    let table_name = unique_qualified_table_name("test_inserter_basic");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Create inserter
    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::JsonEachRow);

    // Create rows
    let rows = make_test_rows(3);

    // Insert using insert_rows
    let result = inserter.insert_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Basic insert succeeded with 3 rows");

    // Cleanup
    drop_http_test_table(&client, &table_name).await;
}

/// config -> ACTION: a MULTI-ROW RowBinary batch must be accepted by a real
/// ClickHouse. A successful end() (the server returns 200 after parsing the
/// RowBinary body) proves the dynamic encoder + the FORMAT RowBinary insert
/// path actually land data -- the offline byte tests cannot.
///
/// RowBinary goes over HTTP. The TCP/native dynamic insert would use the
/// fork's FORMAT Native path, which this server rejects (Native block
/// mis-frame, even single-row) -- tracked as a fork issue -- so it is not
/// exercised here.
#[tokio::test]
async fn rowbinary_multirow_lands_over_http() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => Arc::new(c),
        None => return,
    };
    let oc = crate::common::on_cluster_clause();
    // Qualify with the configured database: in remote mode it is `benchmark`,
    // and a bare table name defaults the insert path to the `default` db.
    let db = crate::common::ClickHouseTestConfig::from_env().database;

    let table = format!("{db}.{}", unique_table_name("rb_multirow"));
    let ddl = format!(
        "CREATE TABLE {table}{oc} (id UInt64, name String, value Float64) \
         ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("create table");

    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::RowBinary);

    let n = inserter
        .insert_rows(&table, &make_test_rows(5), &[])
        .await
        .expect("multi-row RowBinary insert must be accepted by the server");
    assert_eq!(n, 5, "RowBinary insert must report 5 rows written");

    drop_http_test_table(&client, &table).await;
}

#[tokio::test]
async fn test_inserter_large_batch() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_qualified_table_name("test_large_batch");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::JsonEachRow);

    // Create large batch (10,000 rows)
    let row_count = 10_000usize;
    let rows = make_test_rows(row_count);

    let start = std::time::Instant::now();
    let result = inserter.insert_rows(&table_name, &rows, &[]).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "Large batch insert failed: {:?}",
        result.err()
    );
    let count = result.unwrap();
    assert_eq!(count, row_count);

    eprintln!(
        "✓ Large batch insert: {} rows in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Cleanup
    drop_http_test_table(&client, &table_name).await;
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

    let client = match create_http_test_client() {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_qualified_table_name("test_cb_inserter");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Create inserter with circuit breaker
    let cb_config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        open_duration: Duration::from_millis(5000),
        half_open_max_requests: 1,
    };
    let circuit_breaker = Arc::new(CircuitBreaker::new(cb_config));

    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::JsonEachRow)
    .with_circuit_breaker(circuit_breaker.clone());

    // Create valid rows
    let rows: Vec<Map<String, Value>> = (0..3)
        .map(|i| {
            json!({"id": i as u64, "name": format!("row_{}", i)})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();

    // Insert should succeed
    let result = inserter.insert_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok());

    // Check circuit breaker stats
    let stats = circuit_breaker.stats();
    eprintln!("Circuit breaker stats: {stats:?}");

    // Cleanup
    drop_http_test_table(&client, &table_name).await;

    eprintln!("✓ Inserter with circuit breaker works correctly");
}

// ============================================================================
// Concurrent Insert Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_inserts() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_qualified_table_name("test_concurrent");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            batch_id UInt64,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Create inserter with concurrency limit
    let config = InserterConfig {
        max_concurrent_inserts: 4,
        ..Default::default()
    };
    let inserter = Arc::new(
        Inserter::new(client.clone(), create_ch_test_client().unwrap(), config)
            .with_insert_format(InsertFormat::JsonEachRow),
    );

    // Spawn multiple concurrent inserts
    let mut handles = Vec::new();
    for batch_id in 0..8u64 {
        let inserter = inserter.clone();
        let table = table_name.clone();

        let handle = tokio::spawn(async move {
            let rows: Vec<Map<String, Value>> = (0..100)
                .map(|i| {
                    json!({
                        "id": i as u64,
                        "batch_id": batch_id,
                        "value": i as f64
                    })
                    .as_object()
                    .unwrap()
                    .clone()
                })
                .collect();

            inserter.insert_rows(&table, &rows, &[]).await
        });
        handles.push(handle);
    }

    // Wait for all inserts
    let mut total_inserted = 0;
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Concurrent insert failed: {:?}",
            result.err()
        );
        total_inserted += result.unwrap();
    }

    assert_eq!(total_inserted, 800); // 8 batches * 100 rows
    eprintln!("✓ Concurrent inserts: {total_inserted} total rows inserted");

    // Cleanup
    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// Batch Salvage Tests (using FlushBatch)
// ============================================================================

#[tokio::test]
async fn test_inserter_batch_salvage() {
    skip_if_no_clickhouse!();

    use compact_str::CompactString;
    use dfe_loader::buffer::FlushBatch;

    let client = match create_http_test_client() {
        Some(c) => Arc::new(c),
        None => return,
    };
    let table_name = unique_qualified_table_name("test_salvage");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Create inserter with salvage enabled
    let config = InserterConfig {
        enable_salvage: true,
        max_salvage_depth: 10,
        max_retries: 1,
        ..Default::default()
    };
    let inserter = Inserter::new(client.clone(), create_ch_test_client().unwrap(), config)
        .with_insert_format(InsertFormat::JsonEachRow);

    // Create rows
    let rows: Vec<Map<String, Value>> = (0..5)
        .map(|i| {
            json!({"id": i as u64, "name": format!("row_{}", i)})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();

    // Create FlushBatch
    let flush_batch = FlushBatch {
        table: CompactString::from(&table_name),
        rows,
        offsets: Vec::new(),
        raw_payloads: Vec::new(),
    };

    // Insert with salvage
    let result = inserter.insert_with_salvage(flush_batch).await;
    assert_eq!(result.inserted, 5);
    assert!(result.failed.is_empty());

    eprintln!(
        "✓ Batch salvage insert succeeded with {} rows",
        result.inserted
    );

    // Cleanup
    drop_http_test_table(&client, &table_name).await;
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

// ============================================================================
// RowBinary _json Zero-Copy Tests
// ============================================================================

#[tokio::test]
async fn test_rowbinary_json_from_raw_payload() {
    skip_if_no_clickhouse!();
    skip_if_not_replicated!();

    let client = if let Some(c) = create_http_test_client() {
        Arc::new(c)
    } else {
        eprintln!("Could not create HTTP client");
        return;
    };
    let oc = crate::common::on_cluster_clause();

    let table_name = unique_table_name("test_rb_json_raw");
    let full_name = format!("default.{table_name}");

    // Create table with Nullable(JSON) _json column — matches Common Header v2.
    let ddl = format!(
        "CREATE TABLE {full_name} (
            _timestamp DateTime64(3),
            _timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            severity String,
            _json Nullable(JSON)
        )
        ENGINE = ReplicatedMergeTree()
        ORDER BY (_timestamp, _org_id)"
    );
    client.execute(&ddl).await.expect("Failed to create table");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create RowBinary inserter
    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::RowBinary);

    // Extractor-path rows: _json is NOT in the map — raw payload passed separately.
    // This is the zero-copy path: raw Kafka bytes → Arc<[u8]> → RowBinary wire.
    let rows: Vec<Map<String, Value>> = vec![
        json!({"_timestamp": "2026-04-09 10:00:00.000", "_org_id": "acme", "severity": "high"})
            .as_object()
            .unwrap()
            .clone(),
        json!({"_timestamp": "2026-04-09 10:01:00.000", "_org_id": "acme", "severity": "low"})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let raw_payloads: Vec<Arc<[u8]>> = vec![
        Arc::from(
            br#"{"severity":"high","src_ip":"10.0.0.1","detail":"login attempt"}"#.as_slice(),
        ),
        Arc::from(br#"{"severity":"low","src_ip":"10.0.0.2","detail":"heartbeat"}"#.as_slice()),
    ];

    let result = inserter.insert_rows(&full_name, &rows, &raw_payloads).await;
    assert!(
        result.is_ok(),
        "RowBinary insert failed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 2);

    // Sync replicas then query back
    client
        .execute(&format!("SYSTEM SYNC REPLICA{oc} {full_name}"))
        .await
        .expect("Failed to sync replicas");

    let count = client
        .query_count(&full_name, None)
        .await
        .expect("Failed to query count");
    assert_eq!(count, 2, "Should have 2 rows");

    // Verify _json was stored and is queryable via path syntax
    let json_count = client
        .query_count(
            &full_name,
            Some("_json.src_ip = '10.0.0.1' AND severity = 'high'"),
        )
        .await
        .expect("Failed to query _json path");
    assert_eq!(json_count, 1, "_json.src_ip path query should match 1 row");

    // Cleanup
    drop_http_test_table(&client, &full_name).await;
}

/// raw_only mode: _raw populated with full payload, _json stays NULL.
#[tokio::test]
async fn test_raw_only_mode_raw_populated_json_null() {
    skip_if_no_clickhouse!();
    skip_if_not_replicated!();

    let client = if let Some(c) = create_http_test_client() {
        Arc::new(c)
    } else {
        return;
    };
    let oc = crate::common::on_cluster_clause();
    let table_name = unique_table_name("test_raw_only");
    let full_name = format!("default.{table_name}");

    let ddl = format!(
        "CREATE TABLE {full_name} (
            _timestamp DateTime64(3),
            _timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            severity String,
            _raw Nullable(String),
            _json Nullable(JSON(max_dynamic_paths = 2048))
        )
        ENGINE = ReplicatedMergeTree()
        ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("Failed to create table");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Simulate raw_only mode: _raw contains full payload as String, _json not set.
    let rows: Vec<Map<String, Value>> = vec![
        json!({
            "_timestamp": "2026-04-10 10:00:00.000",
            "_org_id": "acme",
            "severity": "high",
            "_raw": r#"{"severity":"high","src_ip":"10.0.0.1","detail":"login"}"#
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    // No raw_payloads — raw_only mode puts payload in _raw directly, no _json splice
    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::RowBinary);

    let result = inserter.insert_rows(&full_name, &rows, &[]).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());

    client
        .execute(&format!("SYSTEM SYNC REPLICA{oc} {full_name}"))
        .await
        .expect("Failed to sync replicas");

    // _raw should contain the full payload string
    let raw_count = client
        .query_count(&full_name, Some("_raw IS NOT NULL AND _raw != ''"))
        .await
        .expect("Failed to query _raw");
    assert_eq!(raw_count, 1, "_raw should be populated");

    // _json should be NULL (not populated in raw_only mode)
    let json_null_count = client
        .query_count(&full_name, Some("_json IS NULL"))
        .await
        .expect("Failed to query _json NULL");
    assert_eq!(json_null_count, 1, "_json should be NULL in raw_only mode");

    // Text search on _raw should work
    let search_count = client
        .query_count(&full_name, Some("_raw LIKE '%login%'"))
        .await
        .expect("Failed to text search _raw");
    assert_eq!(search_count, 1, "Text search on _raw should find the row");

    drop_http_test_table(&client, &full_name).await;
}

/// extracted_only mode: neither _json nor _raw populated, only promoted fields.
#[tokio::test]
async fn test_extracted_only_mode_both_null() {
    skip_if_no_clickhouse!();
    skip_if_not_replicated!();

    let client = if let Some(c) = create_http_test_client() {
        Arc::new(c)
    } else {
        return;
    };
    let oc = crate::common::on_cluster_clause();
    let table_name = unique_table_name("test_extracted_only");
    let full_name = format!("default.{table_name}");

    let ddl = format!(
        "CREATE TABLE {full_name} (
            _timestamp DateTime64(3),
            _timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            severity String,
            _raw Nullable(String),
            _json Nullable(JSON(max_dynamic_paths = 2048))
        )
        ENGINE = ReplicatedMergeTree()
        ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("Failed to create table");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Simulate extracted_only mode: only promoted fields, no _raw or _json
    let rows: Vec<Map<String, Value>> = vec![
        json!({
            "_timestamp": "2026-04-10 10:00:00.000",
            "_org_id": "acme",
            "severity": "critical"
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let inserter = Inserter::new(
        client.clone(),
        create_ch_test_client().unwrap(),
        InserterConfig::default(),
    )
    .with_insert_format(InsertFormat::RowBinary);

    let result = inserter.insert_rows(&full_name, &rows, &[]).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());

    client
        .execute(&format!("SYSTEM SYNC REPLICA{oc} {full_name}"))
        .await
        .expect("Failed to sync replicas");

    // Promoted field should be present
    let severity_count = client
        .query_count(&full_name, Some("severity = 'critical'"))
        .await
        .expect("Failed to query severity");
    assert_eq!(severity_count, 1, "Promoted field should be stored");

    // Both _raw and _json should be NULL
    let both_null = client
        .query_count(&full_name, Some("_raw IS NULL AND _json IS NULL"))
        .await
        .expect("Failed to query NULLs");
    assert_eq!(
        both_null, 1,
        "Both _raw and _json should be NULL in extracted_only mode"
    );

    drop_http_test_table(&client, &full_name).await;
}
