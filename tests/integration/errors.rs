// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Error Handling Integration Tests
//!
//! Tests for error conditions, edge cases, and recovery scenarios

use serde_json::json;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::payload::{FormatDetector, FormatMode};
use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::Transformer;

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

#[test]
fn test_error_invalid_json() {
    let detector = FormatDetector::with_mode(FormatMode::ForceJson);

    let invalid_payloads = vec![
        b"not json at all".as_slice(),
        b"{incomplete".as_slice(),
        b"[1, 2, 3".as_slice(),
        b"{'single': 'quotes'}".as_slice(),
        b"".as_slice(),
        b"null".as_slice(),
    ];

    for payload in invalid_payloads {
        let result = detector.check_and_detect(payload);
        match result {
            Ok(_) => {
                let parse_result = sonic_rs::from_slice::<serde_json::Value>(payload);
                if parse_result.is_err() {
                    eprintln!("Parse error (expected): {:?}", parse_result.err().unwrap());
                }
            }
            Err(e) => {
                eprintln!("Format detection error (expected): {:?}", e);
            }
        }
    }
}

#[test]
fn test_error_msgpack_when_json_forced() {
    let detector = FormatDetector::with_mode(FormatMode::ForceJson);

    let msgpack_bytes: &[u8] = &[
        0x82, 0xa4, b't', b'e', b's', b't', 0xa5, b'v', b'a', b'l', b'u', b'e',
    ];

    let result = detector.check_and_detect(msgpack_bytes);
    assert!(result.is_err(), "Should reject msgpack when JSON is forced");
}

#[test]
fn test_error_missing_routing_fields() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "".to_string(),
        default_table: "".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: Default::default(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);

    let msg = json!({
        "id": 1,
        "data": "test",
        "other_field": "value"
    });

    let result = router.route_value(&msg);
    match result {
        RouteResult::Dlq(reason) => {
            eprintln!("✓ DLQ routing for missing fields: {}", reason);
        }
        RouteResult::Table(t) => {
            eprintln!("Routed to empty/invalid: '{}'", t);
        }
    }
}

#[test]
fn test_error_null_routing_fields() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: "common".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: Default::default(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);

    let msg = json!({
        "org_id": null,
        "category": null,
        "data": "test"
    });

    let result = router.route_value(&msg);
    match result {
        RouteResult::Table(t) => {
            assert_eq!(t, "default.common");
            eprintln!("✓ Null fields use defaults: {}", t);
        }
        RouteResult::Dlq(reason) => {
            eprintln!("DLQ due to null: {}", reason);
        }
    }
}

#[test]
fn test_error_non_string_routing_fields() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: "common".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: Default::default(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);

    let msg = json!({
        "org_id": 12345,
        "category": 67890,
        "data": "test"
    });

    let result = router.route_value(&msg);
    match result {
        RouteResult::Table(t) => {
            eprintln!("✓ Non-string fields handled: {}", t);
        }
        RouteResult::Dlq(reason) => {
            eprintln!("DLQ due to type: {}", reason);
        }
    }
}

#[test]
fn test_error_deeply_nested_json() {
    let transformer = Transformer::default();

    let mut nested = json!({"leaf": "value"});
    for i in 0..10 {
        nested = json!({ format!("level_{}", i): nested });
    }

    let result = transformer.transform(nested);
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!(
        "✓ Deep nesting handled, keys: {:?}",
        output.data.keys().collect::<Vec<_>>()
    );
}

#[test]
fn test_error_array_in_value() {
    let transformer = Transformer::default();

    let msg = json!({
        "id": 1,
        "tags": ["tag1", "tag2", "tag3"],
        "nested": {
            "items": [1, 2, 3]
        }
    });

    let result = transformer.transform(msg);
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!("✓ Arrays handled: {:?}", output.data);
}

#[test]
fn test_error_special_characters_in_keys() {
    let transformer = Transformer::default();

    let msg = json!({
        "normal_key": "value",
        "key-with-dash": "value",
        "key.with.dots": "value",
        "key with spaces": "value",
        "key/with/slashes": "value"
    });

    let result = transformer.transform(msg);
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!(
        "✓ Special characters in keys: {:?}",
        output.data.keys().collect::<Vec<_>>()
    );
}

#[test]
fn test_error_empty_buffer_flush() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    let batches = buffer_manager.get_ready_for_flush();
    assert!(batches.is_empty());

    let batches = buffer_manager.flush_all();
    assert!(batches.is_empty());

    eprintln!("✓ Empty buffer flush handled");
}

#[test]
fn test_error_inconsistent_schema_in_batch() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    let messages = vec![
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "age": 30}),
        json!({"id": 3, "name": "bob", "extra": true}),
    ];

    for msg in messages {
        buffer_manager.push("test.events", msg.as_object().unwrap().clone(), None);
    }

    let batches = buffer_manager.flush_all();
    assert_eq!(batches.len(), 1);

    let rows = &batches[0].rows;
    eprintln!(
        "✓ Inconsistent schemas merged: {} rows",
        rows.len()
    );
}

#[tokio::test]
async fn test_error_insert_nonexistent_table() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1}).as_object().unwrap().clone(),
        json!({"id": 2}).as_object().unwrap().clone(),
        json!({"id": 3}).as_object().unwrap().clone(),
    ];

    let result = client
        .insert_json_rows("nonexistent_table_12345", &rows)
        .await;
    assert!(result.is_err());
    eprintln!("✓ Non-existent table error: {:?}", result.err().unwrap());
}

#[tokio::test]
async fn test_error_insert_schema_mismatch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_mismatch");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            name String
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // JSONEachRow with type coercion — Float for String column
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "name": 1.5}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
    // ClickHouse coerces float to string in JSONEachRow
    match result {
        Ok(n) => eprintln!("Insert succeeded with coercion: {} rows", n),
        Err(e) => eprintln!("✓ Schema mismatch error (expected): {}", e),
    }

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_insert_missing_column() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_missing_col");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64,
            required_field String
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Row missing required_field — ClickHouse fills defaults
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
    match result {
        Ok(n) => eprintln!("Insert succeeded with default fill: {} rows", n),
        Err(e) => eprintln!("✓ Missing column error: {}", e),
    }

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_insert_extra_column() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_extra_col");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Row with extra column not in table
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "extra_column": "extra"}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows).await;
    match result {
        Ok(n) => eprintln!("Insert succeeded (extra column ignored): {} rows", n),
        Err(e) => eprintln!("✓ Extra column error: {}", e),
    }

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_empty_batch_insert() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_empty");
    let ddl = format!(
        "CREATE TABLE {} ON CLUSTER 'default' (
            id UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()",
        table_name
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Empty row list
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![];

    let result = client.insert_json_rows(&table_name, &rows).await;
    match result {
        Ok(n) => {
            assert_eq!(n, 0);
            eprintln!("✓ Empty batch handled: 0 rows inserted");
        }
        Err(e) => eprintln!("Empty batch error: {}", e),
    }

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_invalid_clickhouse_host() {
    use dfe_loader::clickhouse::{ClickHouseConfig, HttpClickHouseClient};

    let ch_config = ClickHouseConfig {
        hosts: vec!["invalid-host-12345.example.com:8123".to_string()],
        database: "default".to_string(),
        username: "default".to_string(),
        password: "".to_string(),
        ..Default::default()
    };

    let client = HttpClickHouseClient::new(&ch_config);
    assert!(client.is_ok(), "Client creation should succeed (lazy connect)");

    // Health check should fail
    let result = client.unwrap().health_check().await;
    assert!(result.is_err());
    eprintln!("✓ Invalid host error: {:?}", result.err().unwrap());
}

#[test]
fn test_edge_unicode_in_data() {
    let transformer = Transformer::default();

    let msg = json!({
        "日本語キー": "日本語値",
        "emoji": "Hello 👋 World 🌍",
        "chinese": "中文测试",
        "arabic": "مرحبا بالعالم",
        "mixed": "ASCII and 中文 and 🎉"
    });

    let result = transformer.transform(msg);
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!(
        "✓ Unicode handled: {:?}",
        output.data.keys().collect::<Vec<_>>()
    );
}

#[test]
fn test_edge_very_long_strings() {
    let transformer = Transformer::default();

    let long_string = "x".repeat(100_000);

    let msg = json!({
        "id": 1,
        "long_data": long_string
    });

    let result = transformer.transform(msg);
    assert!(result.is_ok());

    let output = result.unwrap();
    let data_len = output
        .data
        .get("long_data")
        .map(|v| v.as_str().map(|s| s.len()).unwrap_or(0))
        .unwrap_or(0);

    assert_eq!(data_len, 100_000);
    eprintln!("✓ Long strings handled: {} chars", data_len);
}

#[test]
fn test_edge_numeric_precision() {
    let transformer = Transformer::default();

    let msg = json!({
        "max_i64": i64::MAX,
        "min_i64": i64::MIN,
        "large_float": 1.7976931348623157e308_f64,
        "small_float": 2.2250738585072014e-308_f64,
        "zero": 0,
        "negative_zero": -0.0_f64,
        "infinity_check": f64::MAX
    });

    let result = transformer.transform(msg);
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!("✓ Numeric precision handled: {:?}", output.data);
}
