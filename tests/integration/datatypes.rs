// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Data types integration tests
//!
//! Tests for various ClickHouse data types via JSONEachRow inserts

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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            i8_col Int8,
            i16_col Int16,
            i32_col Int32,
            i64_col Int64,
            u8_col UInt8,
            u16_col UInt16,
            u32_col UInt32,
            u64_col UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"i8_col": -128, "i16_col": -32768, "i32_col": -2147483648_i64, "i64_col": -9223372036854775808_i64, "u8_col": 0, "u16_col": 0, "u32_col": 0, "u64_col": 0}).as_object().unwrap().clone(),
        json!({"i8_col": 0, "i16_col": 0, "i32_col": 0, "i64_col": 0, "u8_col": 128, "u16_col": 32768, "u32_col": 2147483648_u64, "u64_col": 9223372036854775808_u64}).as_object().unwrap().clone(),
        json!({"i8_col": 127, "i16_col": 32767, "i32_col": 2147483647, "i64_col": 9223372036854775807_i64, "u8_col": 255, "u16_col": 65535, "u32_col": 4294967295_u64, "u64_col": 18446744073709551615_u64}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            f32_col Float32,
            f64_col Float64
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"f32_col": 0.0, "f64_col": 0.0}).as_object().unwrap().clone(),
        json!({"f32_col": 3.14159, "f64_col": 3.141592653589793}).as_object().unwrap().clone(),
        json!({"f32_col": -1.5e10, "f64_col": -1.5e100}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            str_col String,
            fixed_col FixedString(10)
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"str_col": "hello", "fixed_col": "0123456789"}).as_object().unwrap().clone(),
        json!({"str_col": "world", "fixed_col": "abc"}).as_object().unwrap().clone(),
        json!({"str_col": "test string with unicode: 日本語", "fixed_col": "short"}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            dt64_ms DateTime64(3),
            date_col Date
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let now = chrono::Utc::now();
    // DateTime64(3) accepts milliseconds integer or 'YYYY-MM-DD HH:MM:SS.mmm' string.
    // RFC3339 with timezone and sub-ms precision is not reliably supported.
    let now_ms = now.timestamp_millis();
    let now_str = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let today = now.format("%Y-%m-%d").to_string();

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"dt64_ms": now_ms, "date_col": &today}).as_object().unwrap().clone(),
        json!({"dt64_ms": &now_str, "date_col": &today}).as_object().unwrap().clone(),
        json!({"dt64_ms": now_ms, "date_col": &today}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            bool_col Bool
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            nullable_str Nullable(String),
            nullable_int Nullable(Int64),
            nullable_float Nullable(Float64)
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "nullable_str": "a", "nullable_int": 1, "nullable_float": null}).as_object().unwrap().clone(),
        json!({"id": 2, "nullable_str": null, "nullable_int": 2, "nullable_float": 2.0}).as_object().unwrap().clone(),
        json!({"id": 3, "nullable_str": "c", "nullable_int": null, "nullable_float": 3.0}).as_object().unwrap().clone(),
        json!({"id": 4, "nullable_str": null, "nullable_int": 4, "nullable_float": null}).as_object().unwrap().clone(),
        json!({"id": 5, "nullable_str": "e", "nullable_int": null, "nullable_float": 5.0}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            category LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let categories = ["auth", "api", "web", "auth", "api", "auth", "web", "api", "auth", "web"];
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = categories
        .iter()
        .enumerate()
        .map(|(i, cat)| json!({"id": i as u64, "category": cat}).as_object().unwrap().clone())
        .collect();

    let result = client.insert_json_rows(&table_name, &rows).await;
    assert!(result.is_ok(), "LowCardinality insert failed: {:?}", result.err());
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

    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            timestamp DateTime64(3),
            event_id UInt64,
            org_id String,
            event_type LowCardinality(String),
            user_id Nullable(UInt64),
            action String,
            value Float64,
            success Bool,
            metadata String
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
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
    let result = client.insert_json_rows(&table_name, &rows).await;
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
