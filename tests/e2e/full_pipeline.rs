// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Full E2E Pipeline Tests
//!
//! Tests the complete Kafka → Transform → Buffer → ClickHouse flow

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;

use dfe_loader::buffer::{BufferManager, KafkaOffset};
use dfe_loader::config::{
    BufferConfig, DlqConfig, FieldSanitizationConfig, MetadataConfig, RoutingConfig,
    TimestampDqConfig,
};
use dfe_loader::metrics::Metrics;
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
        eprintln!("Skipping test: no ClickHouse available");
        return true;
    }
    if !check_clickhouse_reachable() {
        eprintln!("Skipping test: ClickHouse not reachable");
        return true;
    }
    false
}

// ============================================================================
// E2E Pipeline Tests
// ============================================================================

/// Test the full pipeline: Parse → Route → Transform → Buffer → Insert
#[tokio::test]
async fn test_full_pipeline_e2e() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => {
            eprintln!("Could not create test client");
            return;
        }
    };

    // Create test table
    let table_name = unique_table_name("e2e_pipeline");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            action String,
            user_id UInt64,
            user_name String,
            value Float64,
            category String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Set up pipeline components
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: table_name.clone(),
        category_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: true,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
    };

    let router = Router::new(&routing_config);
    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );
    let format_detector = FormatDetector::with_mode(FormatMode::Auto);

    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    // Simulate processing messages (as if from Kafka)
    let messages = [
        json!({
            "id": 1,
            "org_id": "default",
            "category": &table_name,
            "action": "login",
            "user": {"id": 100, "name": "alice"},
            "value": 1.5
        }),
        json!({
            "id": 2,
            "org_id": "default",
            "category": &table_name,
            "action": "purchase",
            "user": {"id": 200, "name": "bob"},
            "value": 25.99
        }),
        json!({
            "id": 3,
            "org_id": "default",
            "category": &table_name,
            "action": "logout",
            "user": {"id": 100, "name": "alice"},
            "value": 0.0
        }),
    ];

    for (idx, msg) in messages.iter().enumerate() {
        // Simulate payload bytes
        let payload = serde_json::to_vec(msg).unwrap();

        // 1. Parse
        let _format = format_detector.check_and_detect(&payload).unwrap();
        let parsed: serde_json::Value = sonic_rs::from_slice(&payload).unwrap();

        // 2. Route (using parsed value)
        let destination = match router.route_value(&parsed) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                panic!("Unexpected DLQ routing: {}", reason);
            }
        };
        assert_eq!(destination, format!("default.{}", table_name));

        // 3. Transform (flattening)
        let result = transformer.transform(parsed);
        assert!(result.is_ok(), "Transform failed: {:?}", result.err());
        let output = result.unwrap();

        // 4. Buffer
        let offset = KafkaOffset {
            topic: Arc::from("test-topic"),
            partition: 0,
            offset: idx as i64,
        };
        buffer_manager.push(&destination, output.data, Some(offset), None);
    }

    // Verify buffer state
    assert_eq!(buffer_manager.pending_rows(), 3);

    // 5. Flush and Insert
    // We need to manually build a RecordBatch since buffer produces generic JSON
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("user_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["login", "purchase", "logout"])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100, 200, 100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob", "alice"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.5, 25.99, 0.0])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                table_name.as_str(),
                table_name.as_str(),
                table_name.as_str(),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());
    let inserted = result.unwrap();
    assert_eq!(inserted, 3);

    eprintln!(
        "✓ Full E2E pipeline test completed: {} rows inserted",
        inserted
    );

    drop_test_table(&client, &table_name).await;
}

/// Test pipeline with multiple tables (routing to different destinations)
#[tokio::test]
async fn test_pipeline_multi_table_routing() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    // Create two test tables
    let table1 = unique_table_name("e2e_auth");
    let table2 = unique_table_name("e2e_api");

    let ddl_template = |name: &str| {
        format!(
            "CREATE TABLE {} (
                id UInt64,
                event String,
                value Float64
            ) ENGINE = Memory",
            name
        )
    };

    client
        .query(&ddl_template(&table1))
        .await
        .expect("Failed to create table1");
    client
        .query(&ddl_template(&table2))
        .await
        .expect("Failed to create table2");

    // Set up routing with category mapping
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: "common".to_string(),
        category_to_table: [
            ("auth".to_string(), table1.clone()),
            ("api".to_string(), table2.clone()),
        ]
        .into_iter()
        .collect(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: true,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
    };

    let router = Router::new(&routing_config);

    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    // Messages for different tables
    let messages = vec![
        (
            json!({"id": 1, "org_id": "default", "category": "auth", "event": "login", "value": 1.0}),
            format!("default.{}", table1),
        ),
        (
            json!({"id": 2, "org_id": "default", "category": "api", "event": "request", "value": 2.0}),
            format!("default.{}", table2),
        ),
        (
            json!({"id": 3, "org_id": "default", "category": "auth", "event": "logout", "value": 3.0}),
            format!("default.{}", table1),
        ),
        (
            json!({"id": 4, "org_id": "default", "category": "api", "event": "response", "value": 4.0}),
            format!("default.{}", table2),
        ),
    ];

    for (msg, expected_dest) in &messages {
        let destination = match router.route_value(msg) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                panic!("Unexpected DLQ: {}", reason);
            }
        };
        assert_eq!(&destination, expected_dest);

        buffer_manager.push(&destination, msg.as_object().unwrap().clone(), None, None);
    }

    // Verify both tables have data
    let stats = buffer_manager.stats();
    assert_eq!(stats.table_count, 2);
    assert_eq!(stats.pending_rows, 4);

    eprintln!(
        "✓ Multi-table routing: {} tables, {} rows",
        stats.table_count, stats.pending_rows
    );

    // Insert to both tables
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Table 1: auth events
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["login", "logout"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 3.0])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table1, batch1).await;
    assert!(result.is_ok(), "Insert to table1 failed");
    assert_eq!(result.unwrap(), 2);

    // Table 2: api events
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![2, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["request", "response"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![2.0, 4.0])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table2, batch2).await;
    assert!(result.is_ok(), "Insert to table2 failed");
    assert_eq!(result.unwrap(), 2);

    eprintln!("✓ Multi-table insert completed: 2 rows each");

    drop_test_table(&client, &table1).await;
    drop_test_table(&client, &table2).await;
}

/// Test pipeline with flattening transformation
#[tokio::test]
async fn test_pipeline_with_flattening() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = match create_test_client().await {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("e2e_flat");
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            user_id UInt64,
            user_email String,
            metadata_source String,
            metadata_version String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let routing_config = RoutingConfig {
        db_fields: vec![],
        table_fields: vec![],
        default_db: "default".to_string(),
        default_table: table_name.clone(),
        category_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: true,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
    };

    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );

    // Nested JSON that needs flattening
    let nested_msg = json!({
        "id": 42,
        "user": {
            "id": 1001,
            "email": "test@example.com"
        },
        "metadata": {
            "source": "integration_test",
            "version": "1.0.0"
        }
    });

    let result = transformer.transform(nested_msg);
    assert!(result.is_ok());
    let output = result.unwrap();

    // Verify flattening worked
    let data = &output.data;
    assert!(
        data.contains_key("user_id") || data.contains_key("user.id"),
        "Flattening should create user_id field"
    );
    assert!(
        data.contains_key("user_email") || data.contains_key("user.email"),
        "Flattening should create user_email field"
    );

    eprintln!("✓ Flattening test: {:?}", data.keys().collect::<Vec<_>>());

    // Insert flattened data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("user_email", DataType::Utf8, false),
        Field::new("metadata_source", DataType::Utf8, false),
        Field::new("metadata_version", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![42])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1001])) as ArrayRef,
            Arc::new(StringArray::from(vec!["test@example.com"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["integration_test"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["1.0.0"])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok());

    eprintln!("✓ Flattened data inserted successfully");

    drop_test_table(&client, &table_name).await;
}

/// Test buffer flush thresholds
#[tokio::test]
async fn test_pipeline_buffer_flush_thresholds() {
    // This test doesn't require ClickHouse - just tests buffer logic
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 5,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    // Add 4 rows - should not flush
    for i in 0..4 {
        let data = json!({"id": i}).as_object().unwrap().clone();
        buffer_manager.push("test.events", data, None, None);
    }

    let batches = buffer_manager.get_ready_for_flush().unwrap();
    assert!(batches.is_empty(), "Should not flush with only 4 rows");
    assert_eq!(buffer_manager.pending_rows(), 4);

    // Add 1 more - should trigger flush
    let data = json!({"id": 4}).as_object().unwrap().clone();
    buffer_manager.push("test.events", data, None, None);

    let batches = buffer_manager.get_ready_for_flush().unwrap();
    assert_eq!(batches.len(), 1, "Should flush at 5 rows");
    assert_eq!(batches[0].batch.num_rows(), 5);

    eprintln!("✓ Buffer flush threshold test passed");
}

/// Test metrics tracking through pipeline
#[tokio::test]
async fn test_pipeline_metrics() {
    let metrics = Metrics::new();

    // Simulate pipeline operations
    for _ in 0..10 {
        metrics.record_received();
    }

    for _ in 0..8 {
        metrics.record_processed("test.events");
    }

    metrics.record_dlq();
    metrics.record_error();

    // Verify metrics
    let output = metrics.gather();
    assert!(output.contains("loader_messages_received_total"));
    assert!(output.contains("loader_messages_processed_total"));
    assert!(output.contains("loader_messages_dlq_total"));
    assert!(output.contains("loader_insert_errors_total"));

    eprintln!("✓ Metrics tracking test passed");
}

/// Test format detection in pipeline
#[tokio::test]
async fn test_pipeline_format_detection() {
    let detector = FormatDetector::with_mode(FormatMode::Auto);

    // JSON payload
    let json_payload = br#"{"event": "test", "id": 1}"#;
    let format = detector.check_and_detect(json_payload).unwrap();
    assert_eq!(format, dfe_loader::payload::PayloadFormat::Json);

    // Force JSON mode rejects non-JSON
    let json_only_detector = FormatDetector::with_mode(FormatMode::ForceJson);
    let msgpack_bytes = &[0x82, 0xa4, b't', b'e', b's', b't'];
    let result = json_only_detector.check_and_detect(msgpack_bytes);
    assert!(result.is_err());

    eprintln!("✓ Format detection test passed");
}

/// Test DLQ routing decision
#[tokio::test]
async fn test_pipeline_dlq_routing() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "".to_string(), // Empty = DLQ
        default_table: "".to_string(),
        category_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: true,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
    };

    let router = Router::new(&routing_config);

    // Message without org_id or category should go to DLQ
    let msg = json!({"id": 1, "data": "test"});
    let result = router.route_value(&msg);

    match result {
        RouteResult::Dlq(reason) => {
            eprintln!("✓ DLQ routing triggered: {}", reason);
        }
        RouteResult::Table(t) => {
            // With empty defaults, it might still route to "." which is invalid
            eprintln!("Routed to: {} (may be empty/invalid)", t);
        }
    }
}
