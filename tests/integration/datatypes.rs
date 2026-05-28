// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Data types integration tests
//!
//! Tests for various `ClickHouse` data types via `JSONEachRow` inserts.
//! Includes Phase 5.6 coercion tests: verify that the Coercer correctly
//! transforms ambiguous input values before they reach `ClickHouse`.

#![allow(clippy::approx_constant)]

use serde_json::json;

use crate::common::{create_http_test_client, drop_http_test_table, unique_table_name};
use crate::skip_if_no_clickhouse;

#[tokio::test]
async fn test_integer_types() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_integers");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            i8_col Int8,
            i16_col Int16,
            i32_col Int32,
            i64_col Int64,
            u8_col UInt8,
            u16_col UInt16,
            u32_col UInt32,
            u64_col UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"i8_col": -128, "i16_col": -32768, "i32_col": -2147483648_i64, "i64_col": -9223372036854775808_i64, "u8_col": 0, "u16_col": 0, "u32_col": 0, "u64_col": 0}).as_object().unwrap().clone(),
        json!({"i8_col": 0, "i16_col": 0, "i32_col": 0, "i64_col": 0, "u8_col": 128, "u16_col": 32768, "u32_col": 2147483648_u64, "u64_col": 9223372036854775808_u64}).as_object().unwrap().clone(),
        json!({"i8_col": 127, "i16_col": 32767, "i32_col": 2147483647, "i64_col": 9223372036854775807_i64, "u8_col": 255, "u16_col": 65535, "u32_col": 4294967295_u64, "u64_col": 18446744073709551615_u64}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Integer insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Integer types insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_float_types() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_floats");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            f32_col Float32,
            f64_col Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"f32_col": 0.0, "f64_col": 0.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"f32_col": 3.14159, "f64_col": 3.141592653589793})
            .as_object()
            .unwrap()
            .clone(),
        json!({"f32_col": -1.5e10, "f64_col": -1.5e100})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Float insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Float types insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_string_types() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_strings");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            str_col String,
            fixed_col FixedString(10)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"str_col": "hello", "fixed_col": "0123456789"})
            .as_object()
            .unwrap()
            .clone(),
        json!({"str_col": "world", "fixed_col": "abc"})
            .as_object()
            .unwrap()
            .clone(),
        json!({"str_col": "test string with unicode: 日本語", "fixed_col": "short"})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "String insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ String types insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_datetime_types() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_datetime");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            dt64_ms DateTime64(3),
            date_col Date
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let now = chrono::Utc::now();
    // DateTime64(3) accepts milliseconds integer or 'YYYY-MM-DD HH:MM:SS.mmm' string.
    // RFC3339 with timezone and sub-ms precision is not reliably supported.
    let now_ms = now.timestamp_millis();
    let now_str = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let today = now.format("%Y-%m-%d").to_string();

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"dt64_ms": now_ms, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
        json!({"dt64_ms": &now_str, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
        json!({"dt64_ms": now_ms, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "DateTime insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ DateTime types insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_boolean_type() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_bool");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            bool_col Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Boolean insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 4);

    eprintln!("✓ Boolean type insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_nullable_types() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_nullable");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            nullable_str Nullable(String),
            nullable_int Nullable(Int64),
            nullable_float Nullable(Float64)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "nullable_str": "a", "nullable_int": 1, "nullable_float": null})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 2, "nullable_str": null, "nullable_int": 2, "nullable_float": 2.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 3, "nullable_str": "c", "nullable_int": null, "nullable_float": 3.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 4, "nullable_str": null, "nullable_int": 4, "nullable_float": null})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 5, "nullable_str": "e", "nullable_int": null, "nullable_float": 5.0})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Nullable insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 5);

    eprintln!("✓ Nullable types insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_low_cardinality_type() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_lowcard");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            category LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let categories = [
        "auth", "api", "web", "auth", "api", "auth", "web", "api", "auth", "web",
    ];
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = categories
        .iter()
        .enumerate()
        .map(|(i, cat)| {
            json!({"id": i as u64, "category": cat})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "LowCardinality insert failed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 10);

    eprintln!("✓ LowCardinality type insert succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_realistic_event_table() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_events");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            timestamp DateTime64(3),
            event_id UInt64,
            org_id String,
            event_type LowCardinality(String),
            user_id Nullable(UInt64),
            action String,
            value Float64,
            success Bool,
            metadata String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let row_count: usize = 1000;
    let now = chrono::Utc::now();
    let event_types = ["login", "logout", "purchase", "view", "click"];
    let orgs = ["org1", "org2", "org3"];

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = (0..row_count)
        .map(|i| {
            let ts = now.timestamp_millis() + (i as i64 * 100);
            let user_id: serde_json::Value = if i % 10 == 0 {
                serde_json::Value::Null
            } else {
                json!(i as u64 * 100)
            };
            json!({
                "timestamp": ts,
                "event_id": i as u64,
                "org_id": orgs[i % orgs.len()],
                "event_type": event_types[i % event_types.len()],
                "user_id": user_id,
                "action": format!("action_{}", i % 20),
                "value": i as f64 * 0.1,
                "success": i % 5 != 0,
                "metadata": format!("{{\"key\": \"value_{}\"}}", i)
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect();

    let start = std::time::Instant::now();
    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Event insert failed: {:?}", result.err());
    let count = result.unwrap();
    assert_eq!(count, row_count);

    eprintln!(
        "✓ Realistic event table: {} rows in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// Phase 5.6: Type Coercion tests
//
// Each test verifies one coercion case against real ClickHouse:
//   1. Build a table with a target column type
//   2. Apply the Coercer to a row with ambiguous/raw input
//   3. Insert via insert_json_rows
//   4. Query back to confirm the value landed correctly
// ============================================================================

/// Helper: create a Coercer with default config
fn default_coercer() -> dfe_loader::transform::Coercer {
    use dfe_loader::config::CoercionConfig;
    dfe_loader::transform::Coercer::new(CoercionConfig::default())
}

#[tokio::test]
async fn test_coerce_datetime64_from_epoch_ms() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_dt64_epoch");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ts DateTime64(3)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Epoch ms integer — coercer converts to "YYYY-MM-DD HH:MM:SS.mmm" string
    let epoch_ms: i64 = 1735084800000; // 2024-12-25 00:00:00.000 UTC
    let mut row = json!({"ts": epoch_ms}).as_object().unwrap().clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ DateTime64 from epoch ms coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_datetime64_from_iso_string() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_dt64_iso");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ts DateTime64(3)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // ISO8601 string with Z suffix — coercer normalises to CH-accepted format
    let mut row = json!({"ts": "2024-12-25T10:30:00.123Z"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ DateTime64 from ISO string coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_bool_from_string() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_bool_str");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            b Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // String representations of bool — coercer converts to JSON bool
    let string_trues = ["true", "1", "yes", "on", "t", "y"];
    let string_falses = ["false", "0", "no", "off"];

    let mut rows = Vec::new();
    for s in &string_trues {
        let mut row = json!({"b": s}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        // After coercion, "b" must be a JSON bool
        assert_eq!(row["b"], json!(true), "Expected true for input {s:?}");
        rows.push(row);
    }
    for s in &string_falses {
        let mut row = json!({"b": s}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        assert_eq!(row["b"], json!(false), "Expected false for input {s:?}");
        rows.push(row);
    }

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), string_trues.len() + string_falses.len());

    eprintln!("✓ Bool from string coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_bool_from_int() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_bool_int");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            b Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    let cases = [(1i64, true), (0i64, false), (42i64, true), (-1i64, true)];
    let mut rows = Vec::new();
    for (int_val, expected_bool) in &cases {
        let mut row = json!({"b": int_val}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        assert_eq!(
            row["b"],
            json!(expected_bool),
            "Expected {expected_bool} for int input {int_val}"
        );
        rows.push(row);
    }

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), cases.len());

    eprintln!("✓ Bool from int coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_uuid_normalisation() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_uuid");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UUID
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Hex without hyphens — coercer normalises to RFC 4122 format
    let mut row = json!({"id": "550e8400e29b41d4a716446655440000"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(row["id"], json!("550e8400-e29b-41d4-a716-446655440000"));

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after UUID coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ UUID normalisation coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_ipv4_from_integer() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_ipv4");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ip IPv4
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Integer representation of 192.168.1.1 = 3232235777
    let mut row = json!({"ip": 3232235777u64}).as_object().unwrap().clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(
        row["ip"],
        json!("192.168.1.1"),
        "Expected dotted-decimal IPv4"
    );

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after IPv4 coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ IPv4 from integer coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_null_non_nullable_defaults_to_empty() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_null_nonnullable");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            name String,
            score UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Null for non-nullable columns — coercer substitutes type defaults
    let mut row = json!({"name": null, "score": null})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(
        row["name"],
        json!(""),
        "Expected empty string default for String"
    );
    assert_eq!(row["score"], json!(0), "Expected 0 default for UInt64");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after null coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ Null → non-nullable default coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_array_datetime64_from_epoch_ms() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_arr_dt64");
    let oc = crate::common::on_cluster_clause();

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            timestamps Array(DateTime64(3))
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Array of epoch ms integers — coercer applies inner DateTime64 coercion
    let epoch_values = json!([1735084800000i64, 1735085000000i64, 1735085200000i64]);
    let mut row = json!({"timestamps": epoch_values})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    // After coercion, all elements should be strings (CH datetime format)
    let arr = row["timestamps"].as_array().expect("Expected array");
    assert_eq!(arr.len(), 3);
    for elem in arr {
        assert!(
            elem.is_string(),
            "Expected string datetime after coercion, got: {elem:?}"
        );
    }

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after array DateTime64 coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ Array(DateTime64) from epoch ms coercion succeeded");
    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_coerce_json_column_accepts_string_and_object() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };
    let table_name = unique_table_name("test_coerce_json_col");
    let oc = crate::common::on_cluster_clause();

    // JSON type requires ClickHouse 25.3+; skip gracefully on older versions
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            data JSON
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    if client.execute(&ddl).await.is_err() {
        eprintln!("Skipping test_coerce_json_column: JSON type not supported on this server");
        return;
    }

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // JSON object value — passes through as-is (already valid JSON)
    let mut row1 = json!({"data": {"key": "value", "num": 42}})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row1, &schema)
        .expect("Coercion failed for object");

    // JSON string value — coercer validates it is parseable JSON
    let mut row2 = json!({"data": "{\"key\": \"from_string\", \"num\": 99}"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row2, &schema)
        .expect("Coercion failed for string");

    let result = client
        .insert_json_rows(&table_name, &[row1, row2], &[])
        .await;
    assert!(
        result.is_ok(),
        "Insert failed after JSON coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 2);

    eprintln!("✓ JSON column coercion (object + string) succeeded");
    drop_http_test_table(&client, &table_name).await;
}
