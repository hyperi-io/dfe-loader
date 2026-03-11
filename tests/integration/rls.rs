// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Row-Level Security (RLS) integration tests
//
// Tests that _org_id field is correctly populated for RLS

use std::collections::HashMap;

use serde_json::json;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::routing::Router;
use dfe_loader::transform::Transformer;

use crate::common::{create_http_test_client, drop_http_test_table};

/// Test that _org_id field is populated from source data
#[tokio::test]
async fn test_org_id_field_population() {
    let routing_config = RoutingConfig {
        db_fields: vec![], // Shared schema - no per-org routing
        table_fields: vec!["category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        source_to_table: HashMap::new(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);
    let transformer = Transformer::default();

    // Test message with org_id
    let msg = json!({
        "org_id": "acme",
        "category": "auth",
        "action": "login",
        "user_id": 12345
    });

    let payload = serde_json::to_vec(&msg).unwrap();

    // Route the message
    let route = router.route(&payload);
    assert!(matches!(route, dfe_loader::routing::RouteResult::Table(_)));

    // Extract org_id before transform
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let org_id_owned = router
        .extract_org_id_from_value(&value)
        .map(|s| s.to_string());
    assert_eq!(org_id_owned.as_deref(), Some("acme"));

    // Transform with org_id
    let result = transformer
        .transform_with_raw(value, org_id_owned.as_deref(), None)
        .unwrap();

    // Verify _org_id field is present
    assert!(result.data.contains_key("_org_id"));
    assert_eq!(
        result.data.get("_org_id").unwrap().as_str().unwrap(),
        "acme"
    );

    // Verify original org_id is removed (routing field removal)
    assert!(!result.data.contains_key("org_id"));
}

/// Test that _org_id field works with different field names
#[tokio::test]
async fn test_org_id_custom_field_name() {
    let routing_config = RoutingConfig {
        db_fields: vec![],
        table_fields: vec!["event_type".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        org_id_field: Some("tenant_id".to_string()), // Custom field name
        org_routes: vec![],
        source_to_table: HashMap::new(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);
    let transformer = Transformer::default();

    let msg = json!({
        "tenant_id": "bigcorp",
        "event_type": "api_call",
        "endpoint": "/users"
    });

    let payload = serde_json::to_vec(&msg).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();

    let org_id_owned = router
        .extract_org_id_from_value(&value)
        .map(|s| s.to_string());
    assert_eq!(org_id_owned.as_deref(), Some("bigcorp"));

    let result = transformer
        .transform_with_raw(value, org_id_owned.as_deref(), None)
        .unwrap();

    // _org_id should be populated from tenant_id
    assert_eq!(
        result.data.get("_org_id").unwrap().as_str().unwrap(),
        "bigcorp"
    );
}

/// Test shared schema routing with multiple orgs
#[tokio::test]
async fn test_shared_schema_multiple_orgs() {
    let routing_config = RoutingConfig {
        db_fields: vec![], // Empty = shared schema
        table_fields: vec!["category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        org_id_field: Some("org_id".to_string()),
        org_routes: vec![],
        source_to_table: [("auth".to_string(), "events_auth".to_string())]
            .into_iter()
            .collect(),
        mapping_file: None,
        topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        compat_v2_source: false,
        dlq: DlqConfig::default(),
        rules: vec![],
    };

    let router = Router::new(&routing_config);
    let transformer = Transformer::default();
    let mut buffer = BufferManager::new(&BufferConfig {
        flush_rows: 10,
        flush_bytes: 10240,
        flush_age_secs: 60,
    });

    // Messages from different orgs, same category
    let messages = vec![
        json!({"org_id": "acme", "category": "auth", "action": "login"}),
        json!({"org_id": "bigcorp", "category": "auth", "action": "logout"}),
        json!({"org_id": "startup", "category": "auth", "action": "register"}),
    ];

    for msg in messages {
        let payload = serde_json::to_vec(&msg).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();

        let route = router.route(&payload);
        if let dfe_loader::routing::RouteResult::Table(table) = route {
            // All should route to same table (shared schema)
            assert_eq!(table, "common.events_auth");

            let org_id_owned = router
                .extract_org_id_from_value(&value)
                .map(|s| s.to_string());
            let result = transformer
                .transform_with_raw(value, org_id_owned.as_deref(), None)
                .unwrap();

            // Each should have its own org_id
            assert!(result.data.contains_key("_org_id"));

            buffer.push(&table, result.data, None, None);
        }
    }

    // All messages buffered to same table
    assert_eq!(buffer.pending_rows(), 3);
    assert_eq!(buffer.stats().table_count, 1); // Single table
}

/// Integration test: Insert data with _org_id and verify storage
///
/// Tests that _org_id field is correctly populated and stored in ClickHouse.
#[tokio::test]
async fn test_org_id_insert_to_clickhouse() {
    use crate::common::unique_table_name;
    use crate::skip_if_no_clickhouse;

    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => {
            eprintln!("Skipping RLS integration test: no ClickHouse available");
            return;
        }
    };

    // The cluster has two relevant databases:
    //   benchmark = Atomic engine (plain MergeTree, no replication, no auto-conversion)
    //   default   = Replicated engine (auto-converts MergeTree to ReplicatedMergeTree)
    //
    // For query-back verification we need the `default` database so that writes on any node
    // are replicated to all nodes. We omit ON CLUSTER — the Replicated DB propagates DDL itself.
    let table_name = unique_table_name("rls_test");
    let full_name = format!("default.{}", table_name);

    // Create table with _org_id field (Common Header v2 schema).
    // No ON CLUSTER — the Replicated `default` DB propagates DDL automatically.
    // ReplicatedMergeTree() with no args lets the Replicated DB auto-fill ZK paths.
    let create_ddl = format!(
        "CREATE TABLE {} (
            _timestamp DateTime64(3),
            _timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            action String,
            user_id UInt32
        )
        ENGINE = ReplicatedMergeTree()
        ORDER BY (_timestamp, _org_id, _uuid)
        PARTITION BY _org_id",
        full_name
    );

    client
        .execute(&create_ddl)
        .await
        .expect("Failed to create table");

    // Brief wait for Replicated DB to propagate DDL to all 3 nodes.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Insert rows via JSONEachRow
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"_timestamp": "2024-01-15 10:00:00.000", "_org_id": "acme", "action": "login", "user_id": 1001}).as_object().unwrap().clone(),
        json!({"_timestamp": "2024-01-15 10:05:00.000", "_org_id": "bigcorp", "action": "logout", "user_id": 2002}).as_object().unwrap().clone(),
        json!({"_timestamp": "2024-01-15 10:10:00.000", "_org_id": "acme", "action": "view", "user_id": 1003}).as_object().unwrap().clone(),
    ];

    let inserted = client
        .insert_json_rows(&full_name, &rows, &[])
        .await
        .expect("Failed to insert rows");

    assert_eq!(inserted, 3, "Should insert 3 rows");

    // Sync all replicas — the table is ReplicatedMergeTree (auto-converted by Replicated DB),
    // so SYSTEM SYNC REPLICA forces all nodes to catch up before we query back.
    client
        .execute(&format!(
            "SYSTEM SYNC REPLICA ON CLUSTER 'default' {}",
            full_name
        ))
        .await
        .expect("Failed to sync replicas");

    // Query back to verify data was inserted correctly
    let total_count = client
        .query_count(&full_name, None)
        .await
        .expect("Failed to query total count");
    assert_eq!(total_count, 3, "Total row count should be 3");

    // Verify org-specific counts
    let acme_count = client
        .query_count(&full_name, Some("_org_id = 'acme'"))
        .await
        .expect("Failed to query acme count");
    assert_eq!(acme_count, 2, "ACME should have 2 rows");

    let bigcorp_count = client
        .query_count(&full_name, Some("_org_id = 'bigcorp'"))
        .await
        .expect("Failed to query bigcorp count");
    assert_eq!(bigcorp_count, 1, "BigCorp should have 1 row");

    // Verify specific actions
    let acme_login_count = client
        .query_count(&full_name, Some("_org_id = 'acme' AND action = 'login'"))
        .await
        .expect("Failed to query acme login count");
    assert_eq!(acme_login_count, 1, "ACME should have 1 login");

    eprintln!("✓ Successfully inserted and verified 3 rows with _org_id field");
    eprintln!("✓ Query-back verification passed (acme: 2 rows, bigcorp: 1 row)");
    eprintln!("✓ Data is ready for row-level security policies");

    // Cleanup
    drop_http_test_table(&client, &full_name).await;
}
