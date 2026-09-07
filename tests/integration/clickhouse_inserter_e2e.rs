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
) -> (
    TestInfrastructure,
    Arc<ClickHouseQueryClient>,
    clickhouse::Client,
) {
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

    // Fork Client (HTTP transport — needed for the JSONEachRow inserter path
    // and sufficient for RowBinary over HTTP).
    let url = format!("http://{host}:{http_port}");
    let unified = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default")
        .with_database("default");

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
// Inserter: the typed meta-schema table over RowBinary (#134)
// ============================================================================

/// One JSON column read back as text.
#[derive(clickhouse::Row, serde::Deserialize)]
struct JsonText {
    json: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rowbinary_parameterised_json_column_lands() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let reader = ch.clone();
    let table = unique_table_name("tc_metajson");

    // The shipped filebeat meta schema in miniature: typed columns plus the
    // parameterised `_json` that `system.columns` reports with its parameters.
    let ddl = format!(
        "CREATE TABLE {table} (
            _timestamp DateTime64(3),
            _timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            _source LowCardinality(String),
            message String,
            log_file_path String,
            _json JSON(max_dynamic_paths=2048)
        ) ENGINE = MergeTree() ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("create table");

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::RowBinary);

    let rows: Vec<Map<String, Value>> = vec![
        json!({
            "_timestamp": "2026-09-03 10:00:00.000",
            "_org_id": "acme",
            "_source": "filebeat",
            "message": "Accepted publickey for derek",
            "log_file_path": "/var/log/auth.log"
        })
        .as_object()
        .unwrap()
        .clone(),
    ];
    let raw: Vec<Arc<[u8]>> = vec![Arc::from(
        br#"{"message":"Accepted publickey for derek","source":{"ip":"172.17.3.4"}}"#.as_slice(),
    )];

    let inserted = inserter
        .insert_rows(&table, &rows, &raw)
        .await
        .expect("a parameterised JSON column must not fail the RowBinary insert");
    assert_eq!(inserted, 1);

    let stored: Vec<String> = reader
        .query(&format!("SELECT toString(_json) AS json FROM {table}"))
        .fetch_all::<JsonText>()
        .await
        .expect("read back")
        .into_iter()
        .map(|r| r.json)
        .collect();

    assert_eq!(stored.len(), 1, "the row must land");
    assert!(
        stored[0].contains("172.17.3.4") && stored[0].contains("Accepted publickey"),
        "_json must hold the payload as a JSON value, got: {}",
        stored[0]
    );
}

// ============================================================================
// Inserter: an IPv6 column takes an IPv4 literal (#127)
// ============================================================================

/// One `toString(ip)` per row, for reading a `Nullable(IPv6)` back as text.
#[derive(clickhouse::Row, serde::Deserialize)]
struct IpText {
    ip: Option<String>,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rowbinary_ipv6_column_takes_an_ipv4_literal() {
    let (_infra, client, ch) = spin_up(test_name!()).await;
    let reader = ch.clone();
    let table = unique_table_name("tc_ipv6");

    let ddl = format!(
        "CREATE TABLE {table} (
            id UInt64,
            source_ip Nullable(IPv6)
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::RowBinary);

    // The three forms a source.ip field arrives in.
    let rows: Vec<Map<String, Value>> = [
        json!({"id": 1u64, "source_ip": "172.17.3.4"}),
        json!({"id": 2u64, "source_ip": "2001:db8::1"}),
        json!({"id": 3u64, "source_ip": "::ffff:172.17.3.4"}),
    ]
    .into_iter()
    .map(|v| v.as_object().unwrap().clone())
    .collect();

    let inserted = inserter
        .insert_rows(&table, &rows, &[])
        .await
        .expect("an IPv4 literal must not be rejected by an IPv6 column");
    assert_eq!(inserted, 3);

    let stored: Vec<String> = reader
        .query(&format!(
            "SELECT toString(source_ip) AS ip FROM {table} ORDER BY id"
        ))
        .fetch_all::<IpText>()
        .await
        .expect("read back")
        .into_iter()
        .map(|r| r.ip.unwrap_or_default())
        .collect();

    assert_eq!(
        stored,
        vec!["::ffff:172.17.3.4", "2001:db8::1", "::ffff:172.17.3.4"],
        "the v4 literal must store the same address ClickHouse's toIPv6 gives it"
    );
}

// ============================================================================
// Inserter: an ECS tags array lands in the JSON _tags column (#139)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_rowbinary_ecs_tags_array_lands_in_the_json_tags_column() {
    use dfe_loader::config::{FieldSanitizationConfig, MetadataConfig, TimestampDqConfig};
    use dfe_loader::transform::Transformer;

    let (_infra, client, ch) = spin_up(test_name!()).await;
    let reader = ch.clone();
    let table = unique_table_name("tc_ecstags");

    let ddl = format!(
        "CREATE TABLE {table} (
            _timestamp DateTime64(3),
            _org_id String,
            _source LowCardinality(String),
            message String,
            _tags JSON
        ) ENGINE = MergeTree() ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("create table");

    // The shape the bundled filebeat pipeline emits: ECS `tags` is an array of
    // keywords, and the `tags_fields` hoist puts it in the JSON `_tags` column.
    let transformer = Transformer::new(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
    );
    let event = json!({
        "timestamp": "2026-09-03 10:00:00.000",
        "message": "Accepted publickey for derek",
        "tags": ["preserve_original_event", "forwarded"]
    });
    let row = transformer
        .transform_with_raw(event, Some("acme"), Some("filebeat"))
        .expect("transform must succeed")
        .data;

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::RowBinary);

    let inserted = inserter
        .insert_rows(&table, &[row], &[])
        .await
        .expect("an ECS tags array must not be rejected by the JSON _tags column");
    assert_eq!(inserted, 1, "the row must land, not fail the batch");

    let stored: Vec<String> = reader
        .query(&format!("SELECT toString(_tags) AS json FROM {table}"))
        .fetch_all::<JsonText>()
        .await
        .expect("read back")
        .into_iter()
        .map(|r| r.json)
        .collect();

    assert_eq!(stored.len(), 1, "the row must be readable back");
    let tags: Value = serde_json::from_str(&stored[0]).expect("_tags must read back as JSON");
    assert_eq!(
        tags,
        json!({"list": ["preserve_original_event", "forwarded"]}),
        "every tag must survive the hoist, in order"
    );
}

// ============================================================================
// Inserter: the JSONEachRow path shapes the JSON _tags column too (#139)
// ============================================================================

/// `insert_format = "json"` is a live config value, and this path serialises
/// the row map as it stands -- so a hoisted ECS `tags` array reaches the JSON
/// `_tags` column unshaped unless the inserter shapes it against the schema.
/// Backing `shape_json_row` out fails this test with `code 117: Cannot insert
/// data into JSON column`.
#[tokio::test(flavor = "multi_thread")]
async fn test_jsoneachrow_ecs_tags_array_lands_in_the_json_tags_column() {
    use dfe_loader::config::{FieldSanitizationConfig, MetadataConfig, TimestampDqConfig};
    use dfe_loader::transform::Transformer;

    let (_infra, client, ch) = spin_up(test_name!()).await;
    let reader = ch.clone();
    let table = unique_table_name("tc_jsontags");

    // The filebeat-shaped table, with the JSON _tags column the meta schemas
    // declare.
    let ddl = format!(
        "CREATE TABLE {table} (
            _timestamp DateTime64(3),
            _org_id String,
            _source LowCardinality(String),
            message String,
            _tags JSON
        ) ENGINE = MergeTree() ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("create table");

    let transformer = Transformer::new(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
    );
    let event = json!({
        "timestamp": "2026-09-03 10:00:00.000",
        "message": "Accepted publickey for derek",
        "tags": ["preserve_original_event", "forwarded"]
    });
    let row = transformer
        .transform_with_raw(event, Some("acme"), Some("filebeat"))
        .expect("transform must succeed")
        .data;
    assert_eq!(
        row.get("_tags"),
        Some(&json!(["preserve_original_event", "forwarded"])),
        "the hoist carries the array verbatim — the insert path is what shapes it"
    );

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::JsonEachRow);

    let inserted = inserter
        .insert_rows(&table, &[row], &[])
        .await
        .expect("an ECS tags array must not be rejected by the JSON _tags column");
    assert_eq!(inserted, 1, "the row must land, not fail the batch");

    let stored: Vec<String> = reader
        .query(&format!("SELECT toString(_tags) AS json FROM {table}"))
        .fetch_all::<JsonText>()
        .await
        .expect("read back")
        .into_iter()
        .map(|r| r.json)
        .collect();

    assert_eq!(stored.len(), 1, "the row must be readable back");
    let tags: Value = serde_json::from_str(&stored[0]).expect("_tags must read back as JSON");
    assert_eq!(
        tags,
        json!({"list": ["preserve_original_event", "forwarded"]}),
        "JSONEachRow must store the shape RowBinary stores"
    );
}

// ============================================================================
// Inserter: the `@source` column comment fills _tags on the wire (#139)
// ============================================================================

/// The path the deployed filebeat table actually uses: `_tags` is filled from
/// the column's own `@source` comment, which the loader compiles into a
/// `FieldMapping` rule and the extractor reads directly. Both copy the source
/// value verbatim, so an ECS `tags` array only survives if the JSON column's
/// encoder shapes it. #140 shaped `Transformer::extract_tags` instead, which
/// this path never calls, and the array still failed with code 117.
#[tokio::test(flavor = "multi_thread")]
async fn test_rowbinary_tags_source_comment_lands_in_the_json_tags_column() {
    use dfe_loader::column_meta::{ColumnDirectivesConfig, ColumnMetaCache, parse_directives};
    use dfe_loader::config::{FieldMappingConfig, MetadataConfig, RoutingConfig};
    use dfe_loader::transform::field_mapping::RuleOrigin;
    use dfe_loader::transform::{HeaderExtractor, MappingBuilder};
    use rustc_hash::FxHashMap;

    let (_infra, client, ch) = spin_up(test_name!()).await;
    let reader = ch.clone();
    let table = unique_table_name("tc_tagsrc");

    // The filebeat meta schema in miniature: `_tags` carries the `@source`
    // comment dfe-schemas emits, verbatim.
    let ddl = format!(
        "CREATE TABLE {table} (
            _timestamp DateTime64(3),
            _org_id LowCardinality(String),
            _source LowCardinality(String),
            message String,
            _tags JSON COMMENT '@source: first(tags/_tags/meta/metadata.tags)'
        ) ENGINE = MergeTree() ORDER BY (_org_id, _timestamp)"
    );
    client.execute(&ddl).await.expect("create table");

    // Read the schema and the column comments back out of ClickHouse, exactly
    // as the background schema resolver does.
    let schema = client
        .fetch_table_schema(&table)
        .await
        .expect("fetch schema");
    let comments = client
        .fetch_column_comments(&table)
        .await
        .expect("fetch column comments");
    assert!(
        comments
            .iter()
            .any(|(col, c)| col == "_tags" && c.contains("@source")),
        "ClickHouse must return the _tags @source comment, got {comments:?}"
    );

    // The directive cache is keyed by `db.table`, the form the pipeline routes
    // to; the bare name is what the DDL and the inserter use here.
    let qualified = format!("{}.{}", schema.database, schema.table);
    let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
    let directives: FxHashMap<String, _> = comments
        .into_iter()
        .map(|(col, comment)| (col, parse_directives(&comment)))
        .collect();
    col_meta.apply_ddl(&qualified, directives);

    // The comment compiles into a FieldMapping rule over the four sources.
    let mapping = MappingBuilder::from_config(&FieldMappingConfig::default())
        .expect("mapping builder must build")
        .build_for_table(&schema, &col_meta);
    let tags_rule = mapping
        .rules()
        .iter()
        .find(|r| r.destination == "_tags")
        .expect("the @source comment must compile into a _tags rule");
    assert_eq!(tags_rule.origin, RuleOrigin::ColumnComment);
    assert_eq!(
        tags_rule.source_fields,
        vec!["tags", "_tags", "meta", "metadata.tags"]
    );

    let event = br#"{"timestamp":"2026-09-03 10:00:00.000","org_id":"acme","source":"filebeat","message":"Accepted publickey for derek","tags":["preserve_original_event","forwarded"]}"#;

    // Row 1: the json_primary extractor, which resolves @source itself.
    let extractor = HeaderExtractor::new(&MetadataConfig::default(), &RoutingConfig::default());
    let extracted = extractor.extract(event, &qualified, &schema, &col_meta);
    assert_eq!(
        extracted.get("_tags"),
        Some(&json!(["preserve_original_event", "forwarded"])),
        "the extractor copies the array verbatim — nothing shapes it before the write"
    );

    // Row 2: the FieldMapping rule, applied to a row that still carries `tags`.
    let mut mapped: Map<String, Value> = json!({
        "_timestamp": "2026-09-03 10:00:00.000",
        "_org_id": "acme",
        "_source": "filebeat",
        "message": "Accepted publickey for derek",
        "tags": ["preserve_original_event", "forwarded"]
    })
    .as_object()
    .unwrap()
    .clone();
    mapping.apply(&mut mapped);
    assert_eq!(
        mapped.get("_tags"),
        Some(&json!(["preserve_original_event", "forwarded"])),
        "FieldMapping::apply copies the array verbatim too"
    );

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config())
        .with_insert_format(InsertFormat::RowBinary);

    let inserted = inserter
        .insert_rows(&table, &[extracted, mapped], &[])
        .await
        .expect("an ECS tags array must not be rejected by the JSON _tags column");
    assert_eq!(inserted, 2, "both rows must land, not fail the batch");

    let stored: Vec<String> = reader
        .query(&format!("SELECT toString(_tags) AS json FROM {table}"))
        .fetch_all::<JsonText>()
        .await
        .expect("read back")
        .into_iter()
        .map(|r| r.json)
        .collect();

    assert_eq!(stored.len(), 2, "both rows must be readable back");
    for text in &stored {
        let tags: Value = serde_json::from_str(text).expect("_tags must read back as JSON");
        assert_eq!(
            tags,
            json!({"list": ["preserve_original_event", "forwarded"]}),
            "every tag must survive, in order"
        );
    }
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

// ============================================================================
// Inserter: the whole common header survives an RFC3339 event timestamp (#144)
// ============================================================================

/// The common header read back as text.
#[derive(clickhouse::Row, serde::Deserialize)]
struct CommonHeader {
    timestamp: String,
    source: String,
    raw: String,
    json: String,
    tags: String,
    message: String,
}

/// The per-source table dfe-engine builds: the timeseries header profile
/// verbatim (types, `@source` / `@captured` comments and all) plus two columns
/// off the filebeat meta schema. Hyphenated, because a source name is DNS-1123
/// and the table takes it verbatim.
async fn create_per_source_table(client: &ClickHouseQueryClient, table: &str) {
    let ddl = format!(
        "CREATE TABLE `{table}` (
            `_timestamp_load` DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '@generated: now64(3) - Insertion timestamp (ms precision)',
            `_timestamp` DateTime64(3, 'UTC') COMMENT '@source: timestamp | now() - Event timestamp from source data',
            `_timestamp_received` DateTime64(3, 'UTC') COMMENT '@source: first(timestamp_received/received_at) - When the event was first received',
            `_uuid` Nullable(UUID) DEFAULT generateUUIDv7() COMMENT '@generated: generateUUIDv7() - Time-ordered unique event identifier',
            `_org_id` LowCardinality(String) COMMENT '@source: org_id - Tenant/organisation identifier',
            `_source` LowCardinality(Nullable(String)) COMMENT '@source: first(_source) | topic_name - Data source label',
            `_raw` Nullable(String) COMMENT '@captured: raw_payload - Original event payload as text',
            `_json` JSON(max_dynamic_paths = 2048) COMMENT '@captured: raw_payload as JSON - Original event payload as structured JSON',
            `_tags` JSON COMMENT '@source: first(tags/_tags/meta/metadata.tags) - Event metadata tags',
            `message` String COMMENT '@source: message - Log line content',
            `source_ip` Nullable(IPv6) COMMENT '@source: source.ip - Source address (ECS source.ip)'
        ) ENGINE = MergeTree() ORDER BY (_timestamp_load, _timestamp, _org_id)"
    );
    client
        .execute(&ddl)
        .await
        .expect("per-source table create must succeed");
}

/// Drive the real header pass over #144's event and assert every common-header
/// column lands. The event's `timestamp` is RFC3339 with a `Z`, which is what
/// both transforms and the receiver emit.
async fn common_header_survives(test: &str, format: InsertFormat) {
    use dfe_loader::column_meta::{ColumnDirectivesConfig, ColumnMetaCache, parse_directives};
    use dfe_loader::config::{MetadataConfig, RoutingConfig};
    use dfe_loader::transform::HeaderExtractor;
    use rustc_hash::FxHashMap;

    let (_infra, client, ch) = spin_up(test).await;
    let reader = ch.clone();
    let table = "filebeat-vector";
    create_per_source_table(&client, table).await;

    // Read the schema and comments back out of ClickHouse, as the background
    // schema resolver does.
    let schema = client
        .fetch_table_schema(table)
        .await
        .expect("fetch schema");
    let comments = client
        .fetch_column_comments(table)
        .await
        .expect("fetch column comments");
    let qualified = format!("{}.{}", schema.database, schema.table);
    let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
    let directives: FxHashMap<String, _> = comments
        .into_iter()
        .map(|(col, comment)| (col, parse_directives(&comment)))
        .collect();
    col_meta.apply_ddl(&qualified, directives);

    // #144's event, verbatim off the topic.
    let payload = br#"{"_source":"filebeat-vector","_timestamp_receiver":1788760433385,"message":"ws21 probe: a line no filebeat module claims","tags":["filebeat_unmatched"],"timestamp":"2026-09-07T05:53:53.385Z","topic":"filebeat-vector_land"}"#;

    let extractor = HeaderExtractor::new(&MetadataConfig::default(), &RoutingConfig::default());
    let mut row = extractor.extract(payload, &qualified, &schema, &col_meta);
    // The processor's capture step, which the extractor does not do.
    row.insert(
        "_raw".to_string(),
        Value::String(String::from_utf8(payload.to_vec()).expect("utf-8 payload")),
    );

    let inserter = Inserter::new(client.clone(), ch, fast_fail_config()).with_insert_format(format);
    let raw: Arc<[u8]> = Arc::from(payload.as_slice());
    let inserted = inserter
        .insert_rows(&qualified, &[row], &[raw])
        .await
        .expect("an RFC3339 event timestamp must not reject the batch");
    assert_eq!(inserted, 1, "the row must land, not fail the batch");

    let stored = reader
        .query(&format!(
            "SELECT toString(_timestamp) AS timestamp, \
             ifNull(toString(_source), '') AS source, ifNull(toString(_raw), '') AS raw, \
             toString(_json) AS json, toString(_tags) AS tags, message FROM `{table}`"
        ))
        .fetch_all::<CommonHeader>()
        .await
        .expect("read back");

    assert_eq!(stored.len(), 1, "the row must be readable back");
    let got = &stored[0];
    assert_eq!(
        got.timestamp, "2026-09-07 05:53:53.385",
        "_timestamp must carry the event's own time, not epoch zero"
    );
    assert_eq!(
        got.source, "filebeat-vector",
        "_source must say where it came from"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&got.tags).expect("_tags is JSON"),
        json!({"list": ["filebeat_unmatched"]}),
        "_tags must carry the ECS tags list"
    );
    assert_eq!(
        got.raw.as_bytes(),
        payload.as_slice(),
        "_raw must be the payload"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&got.json).expect("_json is JSON"),
        serde_json::from_slice::<Value>(payload).expect("payload is JSON"),
        "_json must be the payload as structured JSON"
    );
    assert_eq!(got.message, "ws21 probe: a line no filebeat module claims");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_common_header_survives_rfc3339_timestamp_rowbinary_144() {
    common_header_survives(test_name!(), InsertFormat::RowBinary).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_common_header_survives_rfc3339_timestamp_jsoneachrow_144() {
    common_header_survives(test_name!(), InsertFormat::JsonEachRow).await;
}
