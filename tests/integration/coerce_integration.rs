// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Coercion integration tests against a real ClickHouse cluster.
//!
//! Exercises the `Coercer` end-to-end: verify that ambiguous input values
//! (epoch ms for DateTime64, string booleans, uppercase UUIDs) are coerced
//! before they reach ClickHouse, then round-trip verified with a query.
//!
//! Uses the dual-mode test infrastructure:
//! - `TEST_MODE=docker` → local Docker cluster (single-node)
//! - `TEST_MODE=remote` (default) → devex cluster
//!
//! Tests skip automatically when ClickHouse is not reachable.

use serde_json::{Map, Value, json};

use dfe_loader::clickhouse::types::ColumnInfo;
use dfe_loader::clickhouse::{ParsedType, TableSchema};
use dfe_loader::config::CoercionConfig;
use dfe_loader::transform::Coercer;

use crate::common::{create_http_test_client, drop_http_test_table, unique_table_name};
use crate::skip_if_no_clickhouse;

/// Build a minimal TableSchema from (name, type) pairs for coercion tests.
fn build_schema(columns: &[(&str, &str)]) -> TableSchema {
    let cols: Vec<ColumnInfo> = columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            name: (*name).to_string(),
            type_name: (*ty).to_string(),
            parsed_type: ParsedType::parse(ty),
            position: (i + 1) as u64,
            default_kind: String::new(),
            default_expression: String::new(),
            comment: String::new(),
            is_in_primary_key: false,
            is_in_sorting_key: false,
        })
        .collect();

    TableSchema {
        database: "test".to_string(),
        table: "coerce".to_string(),
        columns: cols,
        comment: String::new(),
    }
}

// ============================================================================
// DateTime64 coercion: epoch ms → ClickHouse-accepted string
// ============================================================================

#[tokio::test]
async fn datetime64_epoch_ms_coerced_and_inserted() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_dt64");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            ts DateTime64(3)
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    // Build a schema description matching the real table for the coercer.
    let schema = build_schema(&[("id", "UInt64"), ("ts", "DateTime64(3)")]);
    let coercer = Coercer::new(CoercionConfig::default());

    // Raw input: epoch milliseconds as an integer — coercer turns this into
    // a "YYYY-MM-DD HH:MM:SS.mmm" string accepted by ClickHouse.
    let now_ms: i64 = 1_700_000_000_000; // 2023-11-14T22:13:20Z
    let iso_with_t = "2024-12-25T10:30:00.123Z"; // ISO 8601 with T — coercer normalises to space-separated

    let mut row1: Map<String, Value> = json!({"id": 1, "ts": now_ms}).as_object().unwrap().clone();
    let mut row2: Map<String, Value> = json!({"id": 2, "ts": iso_with_t})
        .as_object()
        .unwrap()
        .clone();

    coercer.coerce_row(&mut row1, &schema).expect("coerce row1");
    coercer.coerce_row(&mut row2, &schema).expect("coerce row2");

    // Post-coercion: both should be strings acceptable to ClickHouse
    let ts1 = row1
        .get("ts")
        .and_then(Value::as_str)
        .expect("ts after coerce should be string");
    assert!(
        ts1.contains("2023-11-14") || ts1.contains("2023"),
        "epoch ms should coerce to dated string, got: {ts1}"
    );

    let ts2 = row2
        .get("ts")
        .and_then(Value::as_str)
        .expect("ts after coerce should be string");
    assert!(
        !ts2.contains('T'),
        "ISO 8601 'T' separator should be normalised to space, got: {ts2}"
    );

    let result = client
        .insert_json_rows(&table_name, &[row1, row2], &[])
        .await;
    assert!(
        result.is_ok(),
        "Coerced DateTime64 rows should insert cleanly: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 2);

    // Round-trip count verification
    let count = client
        .query_count(&table_name, None)
        .await
        .expect("count query");
    assert_eq!(count, 2, "Should have 2 rows in table");

    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// Bool coercion: string "true"/"yes"/"1" → Bool
// ============================================================================

#[tokio::test]
async fn bool_string_coerced_and_inserted() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_bool");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            flag Bool
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    let schema = build_schema(&[("id", "UInt64"), ("flag", "Bool")]);
    let coercer = Coercer::new(CoercionConfig::default());

    // Mix truthy strings, numeric booleans and falsy strings
    let raw_rows = vec![
        json!({"id": 1, "flag": "true"}),
        json!({"id": 2, "flag": "yes"}),
        json!({"id": 3, "flag": "1"}),
        json!({"id": 4, "flag": "false"}),
        json!({"id": 5, "flag": "no"}),
        json!({"id": 6, "flag": 0}),
        json!({"id": 7, "flag": 1}),
    ];

    let mut coerced: Vec<Map<String, Value>> = Vec::with_capacity(raw_rows.len());
    for raw in &raw_rows {
        let mut row = raw.as_object().unwrap().clone();
        coercer.coerce_row(&mut row, &schema).expect("coerce");
        // Post-coercion, flag must be a Bool (not a string)
        assert!(
            row.get("flag").is_some_and(Value::is_boolean),
            "flag should be coerced to Bool, got: {:?}",
            row.get("flag")
        );
        coerced.push(row);
    }

    let result = client.insert_json_rows(&table_name, &coerced, &[]).await;
    assert!(
        result.is_ok(),
        "Coerced Bool rows should insert cleanly: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 7);

    // Verify truthy rows are stored as true
    let true_count = client
        .query_count(&table_name, Some("flag = true"))
        .await
        .expect("count");
    assert_eq!(
        true_count, 4,
        "Should have 4 true rows (true, yes, 1, 1), got {true_count}"
    );

    let false_count = client
        .query_count(&table_name, Some("flag = false"))
        .await
        .expect("count");
    assert_eq!(
        false_count, 3,
        "Should have 3 false rows (false, no, 0), got {false_count}"
    );

    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// UUID normalisation: uppercase / hyphenless → canonical lowercase
// ============================================================================

#[tokio::test]
async fn uuid_normalised_and_inserted() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_uuid");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            uid UUID
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    let schema = build_schema(&[("id", "UInt64"), ("uid", "UUID")]);
    let coercer = Coercer::new(CoercionConfig::default());

    // Uppercase RFC 4122 UUID — coercer should normalise to lowercase
    let uppercase_uuid = "550E8400-E29B-41D4-A716-446655440000";
    // Hyphenless — coercer inserts hyphens
    let hyphenless_uuid = "550e8400e29b41d4a716446655440001";
    // Already canonical
    let canonical_uuid = "550e8400-e29b-41d4-a716-446655440002";

    let raw_rows = vec![
        json!({"id": 1, "uid": uppercase_uuid}),
        json!({"id": 2, "uid": hyphenless_uuid}),
        json!({"id": 3, "uid": canonical_uuid}),
    ];

    let mut coerced = Vec::with_capacity(raw_rows.len());
    for raw in &raw_rows {
        let mut row = raw.as_object().unwrap().clone();
        coercer.coerce_row(&mut row, &schema).expect("coerce uuid");

        let uid = row
            .get("uid")
            .and_then(Value::as_str)
            .expect("uid should remain string after coerce");
        // Normalised: lowercase, hyphens in standard positions (36 chars)
        assert_eq!(
            uid.len(),
            36,
            "Normalised UUID should be 36 chars (with hyphens), got: {uid} ({} chars)",
            uid.len()
        );
        assert_eq!(
            uid,
            uid.to_lowercase(),
            "Normalised UUID should be all lowercase, got: {uid}"
        );
        coerced.push(row);
    }

    let result = client.insert_json_rows(&table_name, &coerced, &[]).await;
    assert!(
        result.is_ok(),
        "Coerced UUID rows should insert cleanly: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 3);

    let count = client.query_count(&table_name, None).await.expect("count");
    assert_eq!(count, 3);

    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// Strict mode: invalid UUID should fail coercion
// ============================================================================

#[tokio::test]
async fn strict_mode_rejects_garbage_uuid() {
    skip_if_no_clickhouse!();

    // Skip if ClickHouse not reachable — even though we don't insert, the
    // test is part of the integration suite which enforces real-infra testing.
    let schema = build_schema(&[("uid", "UUID")]);
    let strict_config = CoercionConfig {
        strict: true,
        ..Default::default()
    };
    let coercer = Coercer::new(strict_config);

    let mut row: Map<String, Value> = json!({"uid": "definitely_not_a_uuid_at_all"})
        .as_object()
        .unwrap()
        .clone();

    let result = coercer.coerce_row(&mut row, &schema);
    assert!(
        result.is_err(),
        "Strict mode should reject garbage UUID, got Ok({:?})",
        row.get("uid")
    );
    let err = result.unwrap_err();
    let err_msg = format!("{err}");
    assert!(
        err_msg.to_lowercase().contains("uuid") || err_msg.to_lowercase().contains("coercion"),
        "Error should mention UUID/coercion, got: {err_msg}"
    );
}

// ============================================================================
// Combined: full row with multiple coerced columns
// ============================================================================

#[tokio::test]
async fn combined_coercion_round_trip() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_combined");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            ts DateTime64(3),
            flag Bool,
            uid UUID,
            count Int32
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&ddl).await.expect("create table");

    let schema = build_schema(&[
        ("id", "UInt64"),
        ("ts", "DateTime64(3)"),
        ("flag", "Bool"),
        ("uid", "UUID"),
        ("count", "Int32"),
    ]);
    let coercer = Coercer::new(CoercionConfig::default());

    // Mixed ambiguous input: epoch ms, string bool, uppercase UUID, numeric int
    let mut row: Map<String, Value> = json!({
        "id": 42,
        "ts": 1_700_000_000_123_i64,
        "flag": "yes",
        "uid": "550E8400-E29B-41D4-A716-446655440000",
        "count": 100
    })
    .as_object()
    .unwrap()
    .clone();

    coercer.coerce_row(&mut row, &schema).expect("coerce row");

    // Post-coercion assertions
    assert_eq!(row["flag"], Value::Bool(true), "flag should coerce to true");
    assert!(
        row["ts"].is_string(),
        "ts should coerce to string, got: {:?}",
        row["ts"]
    );
    let uid = row["uid"].as_str().expect("uid string");
    assert_eq!(uid, uid.to_lowercase(), "uid should be lowercase");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Combined coercion row should insert: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    // Round-trip: verify the row made it into ClickHouse
    let count = client
        .query_count(&table_name, Some("id = 42 AND flag = true AND count = 100"))
        .await
        .expect("count");
    assert_eq!(
        count, 1,
        "Coerced row should round-trip with correct values"
    );

    drop_http_test_table(&client, &table_name).await;
}
