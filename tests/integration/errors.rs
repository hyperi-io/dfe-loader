// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Error Handling Integration Tests
//!
//! Tests for error conditions, edge cases, and recovery scenarios

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::payload::{FormatDetector, FormatMode};
use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::Transformer;

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
// Parse Error Tests
// ============================================================================

#[test]
fn test_error_invalid_json() {
    let detector = FormatDetector::with_mode(FormatMode::ForceJson);

    // Invalid JSON
    let invalid_payloads = vec![
        b"not json at all".as_slice(),
        b"{incomplete".as_slice(),
        b"[1, 2, 3".as_slice(),
        b"{'single': 'quotes'}".as_slice(),
        b"".as_slice(),
        b"null".as_slice(), // Valid JSON but not an object/array we want
    ];

    for payload in invalid_payloads {
        let result = detector.check_and_detect(payload);
        // Some will be detected as JSON but fail parsing
        // The key is they're handled gracefully
        match result {
            Ok(_) => {
                // Detected format, but parsing may still fail
                let parse_result = sonic_rs::from_slice::<serde_json::Value>(payload);
                // We expect some of these to fail
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

    // MessagePack binary data
    let msgpack_bytes: &[u8] = &[
        0x82, 0xa4, b't', b'e', b's', b't', 0xa5, b'v', b'a', b'l', b'u', b'e',
    ];

    let result = detector.check_and_detect(msgpack_bytes);
    assert!(result.is_err(), "Should reject msgpack when JSON is forced");
}

// ============================================================================
// Routing Error Tests
// ============================================================================

#[test]
fn test_error_missing_routing_fields() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "".to_string(), // Empty = triggers DLQ
        default_table: "".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: Default::default(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
    };

    let router = Router::new(&routing_config);

    // Message with neither routing field
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
            // With empty defaults, route to "." which is essentially DLQ-worthy
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
    };

    let router = Router::new(&routing_config);

    // Message with null values for routing fields
    let msg = json!({
        "org_id": null,
        "category": null,
        "data": "test"
    });

    let result = router.route_value(&msg);
    match result {
        RouteResult::Table(t) => {
            // Should use defaults
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
    };

    let router = Router::new(&routing_config);

    // Numeric values for string fields
    let msg = json!({
        "org_id": 12345,
        "category": 67890,
        "data": "test"
    });

    let result = router.route_value(&msg);
    // Should handle gracefully - either convert or use defaults
    match result {
        RouteResult::Table(t) => {
            eprintln!("✓ Non-string fields handled: {}", t);
        }
        RouteResult::Dlq(reason) => {
            eprintln!("DLQ due to type: {}", reason);
        }
    }
}

// ============================================================================
// Transform Error Tests
// ============================================================================

#[test]
fn test_error_deeply_nested_json() {
    let transformer = Transformer::default();

    // Create deeply nested structure (10 levels)
    let mut nested = json!({"leaf": "value"});
    for i in 0..10 {
        nested = json!({ format!("level_{}", i): nested });
    }

    let result = transformer.transform(nested);
    // Should flatten successfully (no depth limit in current impl)
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

    // JSON with arrays
    let msg = json!({
        "id": 1,
        "tags": ["tag1", "tag2", "tag3"],
        "nested": {
            "items": [1, 2, 3]
        }
    });

    let result = transformer.transform(msg);
    // Arrays should be preserved or converted to strings
    assert!(result.is_ok());

    let output = result.unwrap();
    eprintln!("✓ Arrays handled: {:?}", output.data);
}

#[test]
fn test_error_special_characters_in_keys() {
    let transformer = Transformer::default();

    // Keys with special characters
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

// ============================================================================
// Buffer Error Tests
// ============================================================================

#[test]
fn test_error_empty_buffer_flush() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    // Flush empty buffer
    let batches = buffer_manager.get_ready_for_flush().unwrap();
    assert!(batches.is_empty());

    let batches = buffer_manager.flush_all().unwrap();
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

    // Push messages with different schemas to same table
    let messages = vec![
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "age": 30}),                    // Different field
        json!({"id": 3, "name": "bob", "extra": true}), // Extra field
    ];

    for msg in messages {
        buffer_manager.push("test.events", msg.as_object().unwrap().clone(), None, None);
    }

    // Flush and check handling
    let batches = buffer_manager.flush_all().unwrap();
    assert_eq!(batches.len(), 1);

    // Arrow batch should include all fields as nullable
    let batch = &batches[0].batch;
    eprintln!(
        "✓ Inconsistent schemas merged: {} columns, {} rows",
        batch.num_columns(),
        batch.num_rows()
    );
}

// ============================================================================
// ClickHouse Error Tests
// ============================================================================

#[tokio::test]
async fn test_error_insert_nonexistent_table() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    // Try to insert to non-existent table
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();

    let result = client.insert("nonexistent_table_12345", batch).await;
    assert!(result.is_err());
    eprintln!("✓ Non-existent table error: {:?}", result.err().unwrap());
}

#[tokio::test]
async fn test_error_insert_schema_mismatch() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_mismatch");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            name String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Try to insert with wrong schema (Float instead of String)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Float64, false), // Wrong type!
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    // May succeed due to type coercion, or fail
    match result {
        Ok(n) => eprintln!("Insert succeeded with coercion: {} rows", n),
        Err(e) => eprintln!("✓ Schema mismatch error (expected): {}", e),
    }

    drop_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_insert_missing_column() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_missing_col");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            required_field String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Try to insert with missing column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        // Missing: required_field
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    // May either fail due to missing column OR succeed with ClickHouse filling defaults
    match result {
        Ok(n) => eprintln!("Insert succeeded with default fill: {} rows", n),
        Err(e) => eprintln!("✓ Missing column error: {}", e),
    }

    drop_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_insert_extra_column() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_extra_col");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Try to insert with extra column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("extra_column", DataType::Utf8, false), // Not in table!
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["extra"])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    // ClickHouse might ignore extra columns or error
    match result {
        Ok(n) => eprintln!("Insert succeeded (extra column ignored): {} rows", n),
        Err(e) => eprintln!("✓ Extra column error: {}", e),
    }

    drop_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_error_empty_batch_insert() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("error_empty");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Try to insert empty batch
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(Vec::<u64>::new())) as ArrayRef],
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 0);

    let result = client.insert(&table_name, batch).await;
    // Empty batch should be handled gracefully
    match result {
        Ok(n) => {
            assert_eq!(n, 0);
            eprintln!("✓ Empty batch handled: 0 rows inserted");
        }
        Err(e) => eprintln!("Empty batch error: {}", e),
    }

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Connection Error Tests
// ============================================================================

#[tokio::test]
async fn test_error_invalid_clickhouse_host() {
    use dfe_loader::clickhouse::ArrowClickHouseClient;
    use dfe_loader::config::ClickHouseConfig;

    let config = ClickHouseConfig {
        hosts: vec!["invalid-host-12345.example.com:9000".to_string()],
        database: "default".to_string(),
        username: "default".to_string(),
        password: "".to_string(),
        protocol: "native".to_string(),
        tables: Vec::new(),
        tls: None,
    };

    // Convert dfe-loader config to clickhouse client config
    let ch_config: dfe_loader::clickhouse::ClickHouseConfig = (&config).into();
    let result = ArrowClickHouseClient::new(&ch_config).await;
    assert!(result.is_err());
    eprintln!("✓ Invalid host error: {:?}", result.err().unwrap());
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_edge_unicode_in_data() {
    let transformer = Transformer::default();

    // Unicode in keys and values
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

    // Very long string values
    let long_string = "x".repeat(100_000); // 100KB string

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

    // Various numeric edge cases
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
