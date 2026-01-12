// Row-Level Security (RLS) integration tests
//
// Tests that _org_id field is correctly populated for RLS

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde_json::json;

use dfe_loader_clickhouse::buffer::BufferManager;
use dfe_loader_clickhouse::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader_clickhouse::routing::Router;
use dfe_loader_clickhouse::transform::Transformer;

use crate::common::{create_test_client, drop_test_table};

/// Test that _org_id field is populated from source data
#[tokio::test]
async fn test_org_id_field_population() {
    let routing_config = RoutingConfig {
        db_fields: vec![],  // Shared schema - no per-org routing
        table_fields: vec!["category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: HashMap::new(),
        mapping_file: None,
        dlq: DlqConfig::default(),
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
    assert!(matches!(route, dfe_loader_clickhouse::routing::RouteResult::Table(_)));

    // Extract org_id before transform
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let org_id_owned = router.extract_org_id_from_value(&value).map(|s| s.to_string());
    assert_eq!(org_id_owned.as_deref(), Some("acme"));

    // Transform with org_id
    let result = transformer.transform_with_raw(value, &payload, org_id_owned.as_deref()).unwrap();

    // Verify _org_id field is present
    assert!(result.data.contains_key("_org_id"));
    assert_eq!(result.data.get("_org_id").unwrap().as_str().unwrap(), "acme");

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
        org_id_field: Some("tenant_id".to_string()),  // Custom field name
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: HashMap::new(),
        mapping_file: None,
        dlq: DlqConfig::default(),
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

    let org_id_owned = router.extract_org_id_from_value(&value).map(|s| s.to_string());
    assert_eq!(org_id_owned.as_deref(), Some("bigcorp"));

    let result = transformer.transform_with_raw(value, &payload, org_id_owned.as_deref()).unwrap();

    // _org_id should be populated from tenant_id
    assert_eq!(result.data.get("_org_id").unwrap().as_str().unwrap(), "bigcorp");
}

/// Test shared schema routing with multiple orgs
#[tokio::test]
async fn test_shared_schema_multiple_orgs() {
    let routing_config = RoutingConfig {
        db_fields: vec![],  // Empty = shared schema
        table_fields: vec!["category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        org_id_field: Some("org_id".to_string()),
        routed_orgs: vec![],
        route_all_by_org: false,
        category_to_table: [
            ("auth".to_string(), "events_auth".to_string()),
        ].into_iter().collect(),
        mapping_file: None,
        dlq: DlqConfig::default(),
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
        if let dfe_loader_clickhouse::routing::RouteResult::Table(table) = route {
            // All should route to same table (shared schema)
            assert_eq!(table, "common.events_auth");

            let org_id_owned = router.extract_org_id_from_value(&value).map(|s| s.to_string());
            let result = transformer.transform_with_raw(value, &payload, org_id_owned.as_deref()).unwrap();

            // Each should have its own org_id
            assert!(result.data.contains_key("_org_id"));

            buffer.push(&table, result.data, None);
        }
    }

    // All messages buffered to same table
    assert_eq!(buffer.pending_rows(), 3);
    assert_eq!(buffer.stats().table_count, 1);  // Single table
}

/// Integration test: Insert data with _org_id and verify storage
///
/// Tests that _org_id field is correctly populated and stored in ClickHouse.
/// Uses explicit Arrow schema to avoid timestamp conversion issues.
#[tokio::test]
async fn test_org_id_insert_to_clickhouse() {
    use arrow::array::{StringArray, TimestampMillisecondArray, UInt32Array};
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, TimeUnit};
    use crate::common::query_count;

    // Skip if no ClickHouse available
    let client = match create_test_client().await {
        Some(c) => c,
        None => {
            eprintln!("Skipping RLS integration test: no ClickHouse available");
            return;
        }
    };

    let table_name = "rls_test";

    // Ensure test database exists
    if let Err(e) = client.query("CREATE DATABASE IF NOT EXISTS test").await {
        eprintln!("Skipping RLS ClickHouse test: cannot create test database: {}", e);
        return;
    }

    // Cleanup from previous run
    drop_test_table(&client, &format!("test.{}", table_name)).await;

    // Create table with _org_id field (Common Header v2 schema)
    let create_ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS test.{table_name} (
            timestamp DateTime64(3),
            timestamp_load DateTime64(3) DEFAULT now64(3),
            _uuid UUID DEFAULT generateUUIDv7(),
            _org_id String,
            action String,
            user_id UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, _org_id, _uuid)
        PARTITION BY _org_id
        "#
    );

    client.query(&create_ddl).await.expect("Failed to create table");

    // Create explicit Arrow schema matching ClickHouse DDL
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("_org_id", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("user_id", DataType::UInt32, false),
    ]));

    // Create RecordBatch with explicit types
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                1705315200000_i64,  // 2024-01-15 10:00:00
                1705315500000_i64,  // 2024-01-15 10:05:00
                1705315800000_i64,  // 2024-01-15 10:10:00
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["acme", "bigcorp", "acme"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["login", "logout", "view"])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![1001, 2002, 1003])) as ArrayRef,
        ],
    )
    .expect("Failed to create RecordBatch");

    // Insert batch
    let inserted = client
        .insert(&format!("test.{}", table_name), batch)
        .await
        .expect("Failed to insert batch");

    assert_eq!(inserted, 3, "Should insert 3 rows");

    // Query back to verify data was inserted correctly
    let total_count = query_count(&client, &format!("test.{}", table_name), None)
        .await
        .expect("Failed to query total count");
    assert_eq!(total_count, 3, "Total row count should be 3");

    // Verify org-specific counts
    let acme_count = query_count(
        &client,
        &format!("test.{}", table_name),
        Some("_org_id = 'acme'"),
    )
    .await
    .expect("Failed to query acme count");
    assert_eq!(acme_count, 2, "ACME should have 2 rows");

    let bigcorp_count = query_count(
        &client,
        &format!("test.{}", table_name),
        Some("_org_id = 'bigcorp'"),
    )
    .await
    .expect("Failed to query bigcorp count");
    assert_eq!(bigcorp_count, 1, "BigCorp should have 1 row");

    // Verify specific actions
    let acme_login_count = query_count(
        &client,
        &format!("test.{}", table_name),
        Some("_org_id = 'acme' AND action = 'login'"),
    )
    .await
    .expect("Failed to query acme login count");
    assert_eq!(acme_login_count, 1, "ACME should have 1 login");

    eprintln!("✓ Successfully inserted and verified 3 rows with _org_id field");
    eprintln!("✓ Query-back verification passed (acme: 2 rows, bigcorp: 1 row)");
    eprintln!("✓ Data is ready for row-level security policies");
    eprintln!("  See reference/clickhouse_rls.md for policy setup");

    // Cleanup
    drop_test_table(&client, &format!("test.{}", table_name)).await;
}
