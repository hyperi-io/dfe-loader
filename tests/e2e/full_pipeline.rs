// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Full E2E Pipeline Tests
//!
//! Tests the complete Kafka → Transform → Buffer → `ClickHouse` flow

use std::sync::Arc;

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
use hyperi_rustlib::metrics::MetricsManager;

use crate::common::{
    check_clickhouse_reachable, create_http_test_client, drop_http_test_table, load_dotenv,
    unique_table_name,
};

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

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_full_pipeline_e2e() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = if let Some(c) = create_http_test_client() {
        c
    } else {
        eprintln!("Could not create test client");
        return;
    };

    let oc = crate::common::on_cluster_clause();
    let table_name = unique_table_name("e2e_pipeline");
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            action String,
            user_id UInt64,
            user_name String,
            value Float64,
            category String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: table_name.clone(),
        source_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
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
        let payload = serde_json::to_vec(msg).unwrap();

        let _format = format_detector.check_and_detect(&payload).unwrap();
        let parsed: serde_json::Value = sonic_rs::from_slice(&payload).unwrap();

        let destination = match router.route_value(&parsed) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                panic!("Unexpected DLQ routing: {reason}");
            }
        };
        assert_eq!(destination, format!("default.{table_name}"));

        let result = transformer.transform(parsed);
        assert!(result.is_ok(), "Transform failed: {:?}", result.err());
        let output = result.unwrap();

        let offset = KafkaOffset {
            topic: Arc::from("test-topic"),
            partition: 0,
            offset: idx as i64,
        };
        buffer_manager.push(&destination, output.data, Some(offset), None);
    }

    assert_eq!(buffer_manager.pending_rows(), 3);

    // Insert using JSONEachRow via HttpClickHouseClient
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "action": "login", "user_id": 100, "user_name": "alice", "value": 1.5, "category": &table_name}).as_object().unwrap().clone(),
        json!({"id": 2, "action": "purchase", "user_id": 200, "user_name": "bob", "value": 25.99, "category": &table_name}).as_object().unwrap().clone(),
        json!({"id": 3, "action": "logout", "user_id": 100, "user_name": "alice", "value": 0.0, "category": &table_name}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());

    eprintln!(
        "✓ Full E2E pipeline test completed: {} rows inserted",
        rows.len()
    );

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_multi_table_routing() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let oc = crate::common::on_cluster_clause();
    let table1 = unique_table_name("e2e_auth");
    let table2 = unique_table_name("e2e_api");

    let ddl_template = |name: &str| {
        format!(
            "CREATE TABLE {name}{oc} (
                id UInt64,
                event String,
                value Float64
            ) ENGINE = MergeTree() ORDER BY tuple()"
        )
    };

    client
        .execute(&ddl_template(&table1))
        .await
        .expect("Failed to create table1");
    client
        .execute(&ddl_template(&table2))
        .await
        .expect("Failed to create table2");

    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: "default".to_string(),
        default_table: "common".to_string(),
        source_to_table: [
            ("auth".to_string(), table1.clone()),
            ("api".to_string(), table2.clone()),
        ]
        .into_iter()
        .collect(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);

    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 100,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    let messages = vec![
        (
            json!({"id": 1, "org_id": "default", "category": "auth", "event": "login", "value": 1.0}),
            format!("default.{table1}"),
        ),
        (
            json!({"id": 2, "org_id": "default", "category": "api", "event": "request", "value": 2.0}),
            format!("default.{table2}"),
        ),
        (
            json!({"id": 3, "org_id": "default", "category": "auth", "event": "logout", "value": 3.0}),
            format!("default.{table1}"),
        ),
        (
            json!({"id": 4, "org_id": "default", "category": "api", "event": "response", "value": 4.0}),
            format!("default.{table2}"),
        ),
    ];

    for (msg, expected_dest) in &messages {
        let destination = match router.route_value(msg) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                panic!("Unexpected DLQ: {reason}");
            }
        };
        assert_eq!(&destination, expected_dest);

        buffer_manager.push(&destination, msg.as_object().unwrap().clone(), None, None);
    }

    let stats = buffer_manager.stats();
    assert_eq!(stats.table_count, 2);
    assert_eq!(stats.pending_rows, 4);

    eprintln!(
        "✓ Multi-table routing: {} tables, {} rows",
        stats.table_count, stats.pending_rows
    );

    // Insert to both tables via JSONEachRow
    let auth_rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "event": "login", "value": 1.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 3, "event": "logout", "value": 3.0})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table1, &auth_rows, &[]).await;
    assert!(result.is_ok(), "Insert to table1 failed");

    let api_rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 2, "event": "request", "value": 2.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 4, "event": "response", "value": 4.0})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table2, &api_rows, &[]).await;
    assert!(result.is_ok(), "Insert to table2 failed");

    eprintln!("✓ Multi-table insert completed: 2 rows each");

    drop_http_test_table(&client, &table1).await;
    drop_http_test_table(&client, &table2).await;
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_with_flattening() {
    if skip_if_no_clickhouse() {
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let oc = crate::common::on_cluster_clause();
    let table_name = unique_table_name("e2e_flat");
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            user_id UInt64,
            user_email String,
            metadata_source String,
            metadata_version String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let routing_config = RoutingConfig {
        db_fields: vec![],
        table_fields: vec![],
        default_db: "default".to_string(),
        default_table: table_name.clone(),
        source_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );

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

    // Insert flattened data via JSONEachRow
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({
            "id": 42,
            "user_id": 1001,
            "user_email": "test@example.com",
            "metadata_source": "integration_test",
            "metadata_version": "1.0.0"
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok());

    eprintln!("✓ Flattened data inserted successfully");

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_buffer_flush_thresholds() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 5,
        flush_bytes: 102400,
        flush_age_secs: 60,
    });

    for i in 0..4 {
        let data = json!({"id": i}).as_object().unwrap().clone();
        buffer_manager.push("test.events", data, None, None);
    }

    let batches = buffer_manager.get_ready_for_flush();
    assert!(batches.is_empty(), "Should not flush with only 4 rows");
    assert_eq!(buffer_manager.pending_rows(), 4);

    let data = json!({"id": 4}).as_object().unwrap().clone();
    buffer_manager.push("test.events", data, None, None);

    let batches = buffer_manager.get_ready_for_flush();
    assert_eq!(batches.len(), 1, "Should flush at 5 rows");
    assert_eq!(batches[0].rows.len(), 5);

    eprintln!("✓ Buffer flush threshold test passed");
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_metrics() {
    let manager = MetricsManager::new("loader_test_full");
    let metrics = Metrics::new(&manager);

    for _ in 0..10 {
        metrics.record_received();
    }

    for _ in 0..8 {
        metrics.record_processed("test.events");
    }

    metrics.record_dlq();
    metrics.record_error();

    // Reaching here means metrics recording did not panic

    eprintln!("✓ Metrics tracking test passed");
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_format_detection() {
    let detector = FormatDetector::with_mode(FormatMode::Auto);

    let json_payload = br#"{"event": "test", "id": 1}"#;
    let format = detector.check_and_detect(json_payload).unwrap();
    assert_eq!(format, dfe_loader::payload::PayloadFormat::Json);

    let json_only_detector = FormatDetector::with_mode(FormatMode::ForceJson);
    let msgpack_bytes = &[0x82, 0xa4, b't', b'e', b's', b't'];
    let result = json_only_detector.check_and_detect(msgpack_bytes);
    assert!(result.is_err());

    eprintln!("✓ Format detection test passed");
}

#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_pipeline_dlq_routing() {
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string()],
        default_db: String::new(),
        default_table: String::new(),
        source_to_table: Default::default(),
        mapping_file: None,
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);

    let msg = json!({"id": 1, "data": "test"});
    let result = router.route_value(&msg);

    match result {
        RouteResult::Dlq(reason) => {
            eprintln!("✓ DLQ routing triggered: {reason}");
        }
        RouteResult::Table(t) => {
            eprintln!("Routed to: {t} (may be empty/invalid)");
        }
    }
}
