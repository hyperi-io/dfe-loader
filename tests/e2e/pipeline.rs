// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Full pipeline E2E tests
//!
//! Transform, routing and buffer stages chained in process. No server needed.

use serde_json::json;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::routing::Router;
use dfe_loader::transform::Transformer;

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
    // db.table routing: db from the _org_id tenant, table from category field
    let routing_config = RoutingConfig {
        table_fields: vec!["category".to_string(), "event_type".to_string()],
        default_db: "common".to_string(),
        default_table: "events_other".to_string(),
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
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        dlq: DlqConfig::default(),
        ..RoutingConfig::default()
    };

    let router = Router::new(&routing_config);
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 10240,
        flush_age_secs: 60,
    });

    // Simulate processing messages with _org_id for db routing
    // Route result is "db.table" format
    let messages: Vec<(&[u8], &str)> = vec![
        (
            br#"{"_org_id": "acme", "category": "auth", "action": "login"}"#.as_slice(),
            "acme.events_auth",
        ),
        (
            br#"{"_org_id": "acme", "category": "api", "endpoint": "/users"}"#.as_slice(),
            "acme.events_api",
        ),
        (
            br#"{"_org_id": "tenant1", "category": "unknown", "data": "test"}"#.as_slice(),
            "tenant1.unknown",
        ),
    ];

    for (payload, expected_table) in messages {
        let route = router.route(payload);
        match route {
            dfe_loader::routing::RouteResult::Table(table) => {
                assert_eq!(table, expected_table);
                let data = sonic_rs::from_slice::<serde_json::Value>(payload).unwrap();
                buffer_manager.push(&table, data.as_object().unwrap().clone(), None, None);
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
async fn test_buffer_flush_thresholds() {
    let mut buffer_manager = BufferManager::new(&BufferConfig {
        flush_rows: 5, // Flush at 5 rows
        flush_bytes: 10240,
        flush_age_secs: 60,
    });

    // Add 4 rows - should not flush
    for i in 0..4 {
        let data = json!({"id": i}).as_object().unwrap().clone();
        buffer_manager.push("events", data, None, None);
    }

    let batches = buffer_manager.get_ready_for_flush();
    assert!(batches.is_empty(), "Should not flush with only 4 rows");

    // Add 1 more - should trigger flush
    let data = json!({"id": 4}).as_object().unwrap().clone();
    buffer_manager.push("events", data, None, None);

    let batches = buffer_manager.get_ready_for_flush();
    assert_eq!(batches.len(), 1, "Should flush at 5 rows");
    assert_eq!(batches[0].rows.len(), 5);
}

#[tokio::test]
async fn test_metrics_server_integration() {
    use dfe_loader::metrics::ServerState;
    use std::sync::Arc;

    let scaling = Arc::new(scalo::ScalingPressure::new(
        scalo::scaling::ScalingPressureConfig::default(),
        vec![],
    ));
    let manager = scalo::metrics::MetricsManager::new("loader_test_e2e");
    let metrics = dfe_loader::metrics::Metrics::new(&manager);
    let state = Arc::new(ServerState::new(metrics, scaling));

    assert!(!state.is_ready());
    state.set_ready(true);
    assert!(state.is_ready());
}
