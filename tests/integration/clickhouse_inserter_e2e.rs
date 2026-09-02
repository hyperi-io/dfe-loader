// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! End-to-end integration tests for `Inserter` and `ClickHouseQueryClient`
//! using scope-local testcontainers.
//!
//! Each test spins up its own isolated `ClickHouse` container via
//! `TestInfrastructure`. Containers drop automatically at end of scope
//! (`ContainerAsync` has a blocking-drop that stops & removes the container).
//!
//! Gated behind `#[cfg(feature = "testcontainers")]` — enable with:
//! ```bash
//! cargo test --features testcontainers --test integration_tests
//! ```

#![cfg(feature = "testcontainers")]

use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use serde_json::{Map, Value, json};

use dfe_loader::buffer::FlushBatch;
use dfe_loader::clickhouse::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use dfe_loader::clickhouse::config::{ClickHouseConfig, InsertFormat, Transport};
use dfe_loader::clickhouse::{ClickHouseQueryClient, Inserter, InserterConfig, SchemaCache};

use clickhouse_dfe::UnifiedClient;

use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

// ============================================================================
// Helpers
// ============================================================================

/// Bring up a single-node ClickHouse container and build HTTP-based clients
/// wired to it.
///
/// Returns `(infra, http_query_client, unified_client)`. Keep `infra` in scope
/// until the end of the test — dropping it stops/removes the container.
///
/// `test` names the container. Pass `test_name!()` from the calling test: each
/// of these tests gets its own container (nextest runs them in separate
/// processes), so they must not share a name.
async fn spin_up(
    test: &str,
) -> (TestInfrastructure, Arc<ClickHouseQueryClient>, UnifiedClient) {
    let infra = TestInfrastructure::new(test, true, false).await;
    let container = infra
        .clickhouse
        .as_ref()
        .expect("ClickHouse container must be running");

    let host = container
        .get_host()
        .await
        .expect("container get_host")
        .to_string();
    let http_port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("HTTP port mapping");

    // HTTP-based ClickHouseQueryClient (for DDL/queries).
    let cfg = ClickHouseConfig {
        hosts: vec![format!("{host}:{http_port}")],
        transport: Transport::Http,
        database: "default".to_string(),
        username: "default".to_string(),
        password: String::new(),
        tls: false,
        ..Default::default()
    };
    let query_client = Arc::new(
        ClickHouseQueryClient::new(&cfg).expect("ClickHouseQueryClient must build for HTTP"),
    );

    // The insert path takes a UnifiedClient so it can dispatch per transport.
    // HTTP is the arm these tests want: it carries the JSONEachRow path and
    // serves RowBinary just as well.
    let url = format!("http://{host}:{http_port}");
    let unified = UnifiedClient::Http(
        clickhouse::Client::default()
            .with_url(&url)
            .with_user("default")
            .with_database("default"),
    );

    // Wait briefly for the server to be fully responsive (the image reports
    // ready before the default user is fully usable on some builds).
    for _ in 0..50 {
        if query_client.health_check().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    (infra, query_client, unified)
}

/// Create a simple `(id, name, value)` MergeTree test table.
async fn create_simple_table(client: &ClickHouseQueryClient, table: &str) {
    let ddl = format!(
        "CREATE TABLE {table} (
            id UInt64,
            name String,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client
        .execute(&ddl)
        .await
        .expect("simple table create must succeed");
}

/// Build a batch of simple rows.
fn simple_rows(count: usize) -> Vec<Map<String, Value>> {
    (0..count)
        .map(|i| {
            json!({
                "id": i as u64,
                "name": format!("row_{i}"),
                "value": i as f64 * 1.5
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect()
}

/// Default inserter config with small retry budget (tests should fail fast).
fn fast_fail_config() -> InserterConfig {
    InserterConfig {
        max_retries: 1,
        base_retry_delay_ms: 10,
        max_retry_delay_ms: 100,
        enable_salvage: true,
        max_salvage_depth: 20,
        max_concurrent_inserts: 8,
    }
}

// ============================================================================
// Inserter: JSONEachRow path
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_basic_insert() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_basic");
    create_simple_table(&client, &table).await;

    let inserter = Inserter::new(client.clone(), ch, InserterConfig::default())
        .with_insert_format(InsertFormat::JsonEachRow);

    let rows = simple_rows(5);
    let inserted = inserter
        .insert_rows(&table, &rows, &[])
        .await
        .expect("basic insert should succeed");

    assert_eq!(inserted, 5, "insert_rows should report 5 rows written");

    let count = client.query_count(&table, None).await.expect("count query");
    assert_eq!(count, 5, "table should contain 5 rows after insert");

    let sum_count = client
        .query_count(&table, Some("value >= 0"))
        .await
        .expect("predicate count");
    assert_eq!(sum_count, 5, "all 5 rows should match value >= 0");
}

// ============================================================================
// Inserter: RowBinary path (DynamicInsert)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_rowbinary_insert() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_rowbin");
    create_simple_table(&client, &table).await;

    let inserter = Inserter::new(client.clone(), ch, InserterConfig::default())
        .with_insert_format(InsertFormat::RowBinary);

    let rows = simple_rows(7);
    let inserted = inserter
        .insert_rows(&table, &rows, &[])
        .await
        .expect("RowBinary insert should succeed");

    assert_eq!(inserted, 7);

    let count = client.query_count(&table, None).await.expect("count");
    assert_eq!(count, 7, "RowBinary insert must land all rows");
}

// ============================================================================
// Inserter: salvage on bad row
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_batch_salvage() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_salvage");
    // id must be UInt64 — a non-numeric string will fail type coercion.
    create_simple_table(&client, &table).await;

    let config = InserterConfig {
        enable_salvage: true,
        max_salvage_depth: 10,
        max_retries: 0, // fail fast so salvage kicks in immediately
        base_retry_delay_ms: 10,
        max_retry_delay_ms: 50,
        max_concurrent_inserts: 4,
    };
    let inserter =
        Inserter::new(client.clone(), ch, config).with_insert_format(InsertFormat::JsonEachRow);

    // Valid rows 0, 1, 2, 4, 5 and one bad row at index 3.
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    for i in 0..6u64 {
        if i == 3 {
            // Deliberately bad row — id is an object not a UInt64.
            rows.push(
                json!({"id": {"nested": "bad"}, "name": "bad", "value": 3.0})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        } else {
            rows.push(
                json!({"id": i, "name": format!("row_{i}"), "value": i as f64})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
    }

    let batch = FlushBatch {
        table: CompactString::from(&table),
        rows,
        offsets: Vec::new(),
        raw_payloads: Vec::new(),
    };

    let result = inserter.insert_with_salvage(batch).await;
    eprintln!(
        "salvage result: inserted={} failed={}",
        result.inserted,
        result.failed.len()
    );

    // Salvage should isolate at least the bad row.
    assert!(
        !result.failed.is_empty(),
        "expected at least one failed row from salvage; got inserted={} failed={}",
        result.inserted,
        result.failed.len()
    );
    // And still insert the good rows.
    assert!(
        result.inserted >= 1,
        "salvage should rescue at least one good row"
    );
    assert_eq!(
        result.inserted + result.failed.len(),
        6,
        "all rows must be accounted for"
    );
}

// ============================================================================
// Inserter: retry on missing table (should fail after retries exhausted)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_retry_on_transient_error() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    // Deliberately do NOT create the table — insert must fail.
    let table = unique_table_name("tc_missing");

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::JsonEachRow);

    let rows = simple_rows(3);
    let err = inserter
        .insert_rows(&table, &rows, &[])
        .await
        .expect_err("insert into non-existent table must fail");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unknown") || msg.contains("table") || msg.contains("insert"),
        "error should mention table/insert failure, got: {err}"
    );
}

// ============================================================================
// Inserter: concurrent inserts via semaphore
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_concurrent_inserts() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_concurrent");
    let ddl = format!(
        "CREATE TABLE {table} (
            id UInt64,
            batch_id UInt64,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("create table");

    let config = InserterConfig {
        max_concurrent_inserts: 4,
        ..InserterConfig::default()
    };
    let inserter = Arc::new(
        Inserter::new(client.clone(), ch, config).with_insert_format(InsertFormat::JsonEachRow),
    );

    let rows_per_batch = 25usize;
    let batches = 10u64;

    let mut handles = Vec::new();
    for batch_id in 0..batches {
        let inserter = inserter.clone();
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            let rows: Vec<Map<String, Value>> = (0..rows_per_batch)
                .map(|i| {
                    json!({
                        "id": i as u64,
                        "batch_id": batch_id,
                        "value": (i as f64) + (batch_id as f64) * 100.0
                    })
                    .as_object()
                    .unwrap()
                    .clone()
                })
                .collect();
            inserter.insert_rows(&table, &rows, &[]).await
        }));
    }

    let mut total = 0usize;
    for h in handles {
        let count = h.await.expect("join").expect("concurrent insert");
        total += count;
    }

    let expected = rows_per_batch * batches as usize;
    assert_eq!(total, expected, "report-count must equal expected");

    let persisted = client.query_count(&table, None).await.expect("count");
    assert_eq!(
        persisted, expected,
        "all concurrent rows must land in the table"
    );
}

// ============================================================================
// Inserter: circuit breaker opens after failures
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_with_circuit_breaker() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let missing_table = unique_table_name("tc_cb_missing");

    let cb_cfg = CircuitBreakerConfig {
        failure_threshold: 2,
        success_threshold: 1,
        open_duration: Duration::from_secs(5),
        half_open_max_requests: 1,
    };
    let breaker = Arc::new(CircuitBreaker::new(cb_cfg));

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::JsonEachRow)
        .with_circuit_breaker(breaker.clone());

    let rows = simple_rows(2);

    // Simulate per-table failures to drive the breaker into Open state.
    // Inserter methods don't automatically record via the breaker — so we
    // drive it directly using the same API the loader uses elsewhere.
    assert!(
        breaker.allow_request(&missing_table),
        "closed breaker should allow first request"
    );
    let first = inserter.insert_rows(&missing_table, &rows, &[]).await;
    assert!(first.is_err(), "insert into missing table must fail");
    breaker.record_failure(&missing_table);

    assert!(
        breaker.allow_request(&missing_table),
        "one failure should not yet open the breaker"
    );
    let second = inserter.insert_rows(&missing_table, &rows, &[]).await;
    assert!(second.is_err(), "second insert must also fail");
    breaker.record_failure(&missing_table);

    // After reaching the failure threshold, breaker must reject.
    assert!(
        !breaker.allow_request(&missing_table),
        "breaker must open after {} failures",
        2
    );

    // Another table is unaffected — state is per-table.
    assert!(
        breaker.allow_request("unrelated.table"),
        "circuit breaker state must be isolated per-table"
    );
}

// ============================================================================
// Inserter: schema drift recovery — ALTER mid-batch invalidates cache
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_schema_drift_recovery() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_drift");

    // Initial schema: id + name only.
    let ddl = format!(
        "CREATE TABLE {table} (
            id UInt64,
            name String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("create table");

    let schema_cache = Arc::new(SchemaCache::new(300));
    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::RowBinary)
        .with_schema_cache(schema_cache.clone());

    // First insert — caches original schema.
    let rows1: Vec<Map<String, Value>> = (0..3u64)
        .map(|i| {
            json!({"id": i, "name": format!("before_{i}")})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();
    inserter
        .insert_rows(&table, &rows1, &[])
        .await
        .expect("first insert");

    // Prime the cache directly (simulates any prior fetch) so we can
    // observe invalidation.
    let fetched = client
        .fetch_table_schema(&table)
        .await
        .expect("fetch schema");
    schema_cache.insert(table.clone(), fetched);
    assert!(
        schema_cache.get(&table).is_some(),
        "cache should have the schema"
    );

    // Alter the table — add a required column. This will cause rows without
    // `extra` to fail type coercion against the new schema.
    let alter = format!("ALTER TABLE {table} ADD COLUMN extra Int64 DEFAULT 0");
    client.execute(&alter).await.expect("alter");

    // Cache is now stale. Invalidate explicitly to simulate the drift path
    // (the schema_cache API exposes this as a public test surface).
    schema_cache.invalidate(&table);
    assert!(
        schema_cache.get(&table).is_none(),
        "invalidate() must drop the cached entry"
    );

    // Subsequent insert works — fork re-fetches schema on write.
    let rows2: Vec<Map<String, Value>> = (10..13u64)
        .map(|i| {
            json!({"id": i, "name": format!("after_{i}"), "extra": i as i64})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();
    inserter
        .insert_rows(&table, &rows2, &[])
        .await
        .expect("post-alter insert");

    let count = client.query_count(&table, None).await.expect("count");
    assert_eq!(count, 6, "3 rows before + 3 rows after ALTER");
}

// ============================================================================
// Inserter: empty batch is a no-op
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_empty_batch() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_empty");
    create_simple_table(&client, &table).await;

    let inserter = Inserter::new(client.clone(), ch, InserterConfig::default())
        .with_insert_format(InsertFormat::JsonEachRow);

    let empty: Vec<Map<String, Value>> = Vec::new();
    let n = inserter
        .insert_rows(&table, &empty, &[])
        .await
        .expect("empty insert must be a no-op");
    assert_eq!(n, 0, "empty batch reports 0 rows");

    let count = client.query_count(&table, None).await.expect("count");
    assert_eq!(count, 0, "no rows should have been written");
}

// ============================================================================
// Inserter: huge batch
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_huge_batch() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_huge");
    create_simple_table(&client, &table).await;

    let inserter = Inserter::new(client.clone(), ch, InserterConfig::default())
        .with_insert_format(InsertFormat::JsonEachRow);

    let row_count: usize = 10_000;
    let rows = simple_rows(row_count);

    let start = std::time::Instant::now();
    let inserted = inserter
        .insert_rows(&table, &rows, &[])
        .await
        .expect("huge insert must succeed");
    let elapsed = start.elapsed();

    assert_eq!(inserted, row_count);
    eprintln!(
        "huge batch: {row_count} rows in {elapsed:?} ({:.0} rows/sec)",
        row_count as f64 / elapsed.as_secs_f64()
    );

    let count = client.query_count(&table, None).await.expect("count");
    assert_eq!(count, row_count, "all 10k rows must land");
}

// ============================================================================
// Inserter: offset accounting via FlushBatch
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_inserter_with_offset_commit() {
    use dfe_loader::buffer::KafkaOffset;

    let (_infra, client, ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_offsets");
    create_simple_table(&client, &table).await;

    let inserter = Inserter::new(client.clone(), ch, InserterConfig::default())
        .with_insert_format(InsertFormat::JsonEachRow);

    let rows = simple_rows(4);
    let offsets: Vec<KafkaOffset> = (0..4i64)
        .map(|i| KafkaOffset {
            topic: Arc::from("events"),
            partition: 0,
            offset: 1000 + i,
        })
        .collect();

    let batch = FlushBatch {
        table: CompactString::from(&table),
        rows,
        offsets: offsets.clone(),
        raw_payloads: Vec::new(),
    };

    let result = inserter.insert_with_salvage(batch).await;
    assert_eq!(result.inserted, 4, "all rows should be inserted");
    assert!(
        result.failed.is_empty(),
        "no failures on a well-formed batch"
    );

    // The caller (BufferManager) uses `offsets` to commit — verify our batch
    // had the expected structure by reading back from CH.
    let count = client.query_count(&table, None).await.expect("count");
    assert_eq!(count, 4);
}

// ============================================================================
// ClickHouseQueryClient: DDL & query surface
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_client_http_create_database() {
    let (_infra, client, _ch) = spin_up(test_name!()).await;
    let db = format!("testdb_{}", chrono::Utc::now().timestamp_millis());

    client
        .execute(&format!("CREATE DATABASE {db}"))
        .await
        .expect("CREATE DATABASE must succeed");

    // Sanity: creating again should fail without IF NOT EXISTS.
    let dup = client.execute(&format!("CREATE DATABASE {db}")).await;
    assert!(
        dup.is_err(),
        "duplicate CREATE DATABASE should error without IF NOT EXISTS"
    );

    client
        .execute(&format!("DROP DATABASE {db}"))
        .await
        .expect("DROP DATABASE must succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_http_create_table() {
    let (_infra, client, _ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_ct");

    // Table with various ClickHouse types — covers the type parser path.
    let ddl = format!(
        "CREATE TABLE {table} (
            id UInt64,
            name String,
            count Int32,
            price Decimal(10, 2),
            tags Array(String),
            props Map(String, String),
            when DateTime64(3),
            maybe Nullable(Float64)
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    assert!(
        client.table_exists(&table).await.expect("exists"),
        "table_exists must return true"
    );

    let schema = client
        .fetch_table_schema(&table)
        .await
        .expect("fetch schema");
    assert_eq!(schema.columns.len(), 8, "all 8 columns must be reported");

    // Column names present and in order.
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "id", "name", "count", "price", "tags", "props", "when", "maybe"
        ]
    );

    // "id" is in the sorting key (ORDER BY id).
    let id_col = schema
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column");
    assert!(
        id_col.is_in_sorting_key,
        "id must be flagged as sorting key member"
    );

    // Nullable detection.
    let maybe = schema
        .columns
        .iter()
        .find(|c| c.name == "maybe")
        .expect("maybe column");
    assert!(
        maybe.is_nullable(),
        "Nullable(Float64) must be detected as nullable"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_http_query_system_columns() {
    let (_infra, client, _ch) = spin_up(test_name!()).await;
    let table = unique_table_name("tc_syscols");

    let ddl = format!(
        "CREATE TABLE {table} (
            a UInt64,
            b String COMMENT 'the b column',
            c Float64
        ) ENGINE = MergeTree() ORDER BY a"
    );
    client.execute(&ddl).await.expect("create table");

    // fetch_table_schema goes through system.columns and TableSchema parsing.
    let schema = client
        .fetch_table_schema(&table)
        .await
        .expect("schema fetch");
    assert_eq!(schema.database, "default");
    assert_eq!(schema.table, table);
    assert_eq!(schema.columns.len(), 3);

    // Positions should be 1-based and monotonic.
    let positions: Vec<u64> = schema.columns.iter().map(|c| c.position).collect();
    assert_eq!(positions, vec![1, 2, 3]);

    // Column-comment fetch.
    let comments = client
        .fetch_column_comments(&table)
        .await
        .expect("comments");
    assert_eq!(
        comments.get("b").map(String::as_str),
        Some("the b column"),
        "b's comment must be returned"
    );

    // list_tables must include our fresh table.
    let tables = client.list_tables().await.expect("list");
    assert!(
        tables.iter().any(|t| t == &table),
        "list_tables should include {table}"
    );

    // query_count on an empty table.
    let empty = client.query_count(&table, None).await.expect("count");
    assert_eq!(empty, 0, "empty table count must be zero");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_http_invalid_query() {
    let (_infra, client, _ch) = spin_up(test_name!()).await;

    // Malformed SQL.
    let err = client
        .execute("SELECT not valid sql at all;;")
        .await
        .expect_err("invalid SQL must return an error");
    let msg = err.to_string();
    assert!(
        !msg.is_empty(),
        "error must carry a message from ClickHouse, got empty"
    );

    // Nonexistent table.
    let missing = unique_table_name("does_not_exist");
    let err2 = client
        .fetch_table_schema(&missing)
        .await
        .expect_err("fetch_table_schema on a missing table must return an error");
    let msg2 = err2.to_string().to_lowercase();
    assert!(
        msg2.contains("not found")
            || msg2.contains("no columns")
            || msg2.contains("doesn't exist")
            || msg2.contains("does not exist"),
        "error for missing table should mention not-found; got: {err2}"
    );

    // table_exists returns Ok(false), not Err, for missing tables.
    let exists = client
        .table_exists(&missing)
        .await
        .expect("table_exists must not propagate not-found as an error");
    assert!(!exists, "missing table exists() must be false");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_http_unicode_table_name() {
    let (_infra, client, _ch) = spin_up(test_name!()).await;
    // Backtick-quoted identifier with unicode characters.
    let table = "événements_тест";

    // Use backticks to quote unicode identifiers.
    let ddl = format!(
        "CREATE TABLE `{table}` (
            id UInt64,
            label String
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client
        .execute(&ddl)
        .await
        .expect("unicode identifier DDL must work");

    // The query client accepts the bare name and will backtick-escape on its
    // end. table_exists internally parses db.table.
    assert!(
        client.table_exists(table).await.expect("exists lookup"),
        "unicode table must be detected as existing"
    );

    let schema = client
        .fetch_table_schema(table)
        .await
        .expect("unicode schema fetch");
    assert_eq!(schema.columns.len(), 2, "two columns on unicode table");

    // Cleanup.
    client
        .execute(&format!("DROP TABLE `{table}`"))
        .await
        .expect("drop unicode table");
}
