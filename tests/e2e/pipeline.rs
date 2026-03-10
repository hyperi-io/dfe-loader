// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Full pipeline E2E tests
//!
//! These tests require both Kafka and ClickHouse to be available

use std::env;

use serde_json::json;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::payload::{FormatDetector, FormatMode};
use dfe_loader::routing::Router;
use dfe_loader::transform::Transformer;

/// Skip test if no full test environment available
fn skip_if_no_env() -> bool {
    if env::var("CLICKHOUSE_HOST").is_ok() && env::var("KAFKA_BROKERS").is_ok() {
        return false; // Don't skip, env vars set
    }
    true
}

#[tokio::test]
async fn test_transform_pipeline_unit() {
    // Test the transform pipeline without external dependencies
    let transformer = Transformer::default();

    let input = json!({
        "category": "auth",
        "action": "login",
        "user": {
            "id": 12345,
            "email": "test@example.com"
        },
        "timestamp": "2024-01-15T10:30:00Z"
    });

    let result = transformer.transform(input);
    assert!(result.is_ok());

    let output = result.unwrap();
    // Check flattening worked
    assert!(output.data.contains_key("user_id") || output.data.contains_key("user.id"));
}

#[tokio::test]
async fn test_routing_and_buffer() {
    // Test routing and buffering without external dependencies
    // New db.table routing: db from org_id, table from category field
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["category".to_string(), "event_type".to_string()],
        default_db: "common".to_string(),
        default_table: "events_other".to_string(),
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![
            dfe_loader::config::OrgRoute {
                org_id: "acme".to_string(),
                database: None,
            },
            dfe_loader::config::OrgRoute {
                org_id: "tenant1".to_string(),
                database: None,
            },
        ],
        source_to_table: [
            ("auth".to_string(), "events_auth".to_string()),
            ("api".to_string(), "events_api".to_string()),
        ]
        .into_iter()
        .collect(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 10240,
        flush_age_secs: 60,
    });

    // Simulate processing messages with org_id for db routing
    // Route result is now "db.table" format
    let messages: Vec<(&[u8], &str)> = vec![
        (
            br#"{"org_id": "acme", "category": "auth", "action": "login"}"#.as_slice(),
            "acme.events_auth",
        ),
        (
            br#"{"org_id": "acme", "category": "api", "endpoint": "/users"}"#.as_slice(),
            "acme.events_api",
        ),
        (
            br#"{"org_id": "tenant1", "category": "unknown", "data": "test"}"#.as_slice(),
            "tenant1.unknown",
        ),
    ];

    for (payload, expected_table) in messages {
        let route = router.route(payload);
        match route {
            dfe_loader::routing::RouteResult::Table(table) => {
                assert_eq!(table, expected_table);
                let data = sonic_rs::from_slice::<serde_json::Value>(payload).unwrap();
                buffer_manager.push(&table, data.as_object().unwrap().clone(), None);
            }
            dfe_loader::routing::RouteResult::Dlq(_) => {
                panic!("Should not route to DLQ with default_table set");
            }
        }
    }

    assert_eq!(buffer_manager.pending_rows(), 3);
    assert_eq!(buffer_manager.stats().table_count, 3);
}

#[tokio::test]
async fn test_format_detection() {
    // Test payload format detection
    let detector = FormatDetector::with_mode(FormatMode::Auto);

    // JSON payload
    let json_payload = br#"{"event": "test"}"#;
    let result = detector.check_and_detect(json_payload);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), dfe_loader::payload::PayloadFormat::Json);

    // Force JSON mode
    let json_detector = FormatDetector::with_mode(FormatMode::ForceJson);
    let result = json_detector.check_and_detect(json_payload);
    assert!(result.is_ok());

    // Force JSON mode should reject msgpack-looking bytes
    let msgpack_like = &[0x82, 0xa4, b't', b'e', b's', b't']; // fixmap
    let result = json_detector.check_and_detect(msgpack_like);
    assert!(result.is_err()); // Should be Err because it's not JSON
}

#[tokio::test]
async fn test_buffer_flush_thresholds() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 5, // Flush at 5 rows
        flush_bytes: 10240,
        flush_age_secs: 60,
    });

    // Add 4 rows - should not flush
    for i in 0..4 {
        let data = json!({"id": i}).as_object().unwrap().clone();
        buffer_manager.push("events", data, None);
    }

    let batches = buffer_manager.get_ready_for_flush();
    assert!(batches.is_empty(), "Should not flush with only 4 rows");

    // Add 1 more - should trigger flush
    let data = json!({"id": 4}).as_object().unwrap().clone();
    buffer_manager.push("events", data, None);

    let batches = buffer_manager.get_ready_for_flush();
    assert_eq!(batches.len(), 1, "Should flush at 5 rows");
    assert_eq!(batches[0].rows.len(), 5);
}

#[tokio::test]
async fn test_full_pipeline_e2e() {
    if skip_if_no_env() {
        eprintln!("Skipping E2E test: no full test environment available");
        eprintln!("Set CLICKHOUSE_HOST and KAFKA_BROKERS env vars to run");
        return;
    }

    // This test requires actual Kafka and ClickHouse
    // TODO: Implement full E2E test with testcontainers or external services
    eprintln!("E2E test environment available - implement full test");
}

#[tokio::test]
async fn test_metrics_server_integration() {
    use dfe_loader::metrics::{Metrics, ServerState};
    use std::sync::Arc;

    let metrics = Metrics::new();
    metrics.record_received();
    metrics.record_received();
    metrics.record_processed("test_table");

    let scaling = Arc::new(hyperi_rustlib::ScalingPressure::new(
        hyperi_rustlib::scaling::ScalingPressureConfig::default(),
        vec![],
    ));
    let state = Arc::new(ServerState::new(metrics, scaling));
    state.set_ready(true);

    // We can't easily test the full server without binding to a port
    // Just verify the state works correctly
    let health = state.health.read().unwrap();
    assert!(health.ready);
    drop(health);

    // Verify metrics gathering works
    let metrics_output = state.metrics.gather();
    assert!(metrics_output.contains("loader_messages_received_total"));
    assert!(metrics_output.contains("loader_messages_processed_total"));
}
