//! MemoryTransport-based unit tests for pipeline components.
//!
//! These tests verify the transport adapter, message processing, and buffer
//! management without requiring Kafka or ClickHouse infrastructure.

use serde_json::json;

use dfe_loader::buffer::{BufferManager, KafkaOffset};
use dfe_loader::config::{BufferConfig, FieldSanitizationConfig, MetadataConfig, RoutingConfig, TimestampDqConfig};
use dfe_loader::kafka::{KafkaMessage, MemoryTransportAdapter};
use dfe_loader::payload::{FormatDetector, FormatMode};
use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::Transformer;

// ============================================================================
// MemoryTransportAdapter Tests
// ============================================================================

#[tokio::test]
async fn test_memory_adapter_inject_and_recv() {
    let adapter = MemoryTransportAdapter::new("test-topic");

    // Inject messages
    adapter.inject(b"message 1".to_vec()).await.unwrap();
    adapter.inject(b"message 2".to_vec()).await.unwrap();
    adapter.inject(b"message 3".to_vec()).await.unwrap();

    // Receive them
    let messages = adapter.recv(10).await.unwrap();

    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].payload, b"message 1");
    assert_eq!(messages[1].payload, b"message 2");
    assert_eq!(messages[2].payload, b"message 3");

    // All messages should have the configured topic
    for msg in &messages {
        assert_eq!(msg.topic.as_ref(), "test-topic");
        assert_eq!(msg.partition, 0);
    }
}

#[tokio::test]
async fn test_memory_adapter_with_key() {
    let adapter = MemoryTransportAdapter::new("keyed-topic");

    adapter.inject_with_key("key-1", b"payload-1".to_vec()).await.unwrap();
    adapter.inject_with_key("key-2", b"payload-2".to_vec()).await.unwrap();

    let messages = adapter.recv(10).await.unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].key, Some(b"key-1".to_vec()));
    assert_eq!(messages[1].key, Some(b"key-2".to_vec()));
}

#[tokio::test]
async fn test_memory_adapter_empty_recv() {
    let adapter = MemoryTransportAdapter::new("empty-topic");

    // Should return empty vec, not error
    let messages = adapter.recv(10).await.unwrap();
    assert!(messages.is_empty());
}

#[tokio::test]
async fn test_memory_adapter_batch_recv() {
    let adapter = MemoryTransportAdapter::new("batch-topic");

    // Inject 100 messages
    for i in 0..100 {
        adapter.inject(format!("msg-{}", i).into_bytes()).await.unwrap();
    }

    // Receive in batches of 30
    let batch1 = adapter.recv(30).await.unwrap();
    let batch2 = adapter.recv(30).await.unwrap();
    let batch3 = adapter.recv(30).await.unwrap();
    let batch4 = adapter.recv(30).await.unwrap();

    assert_eq!(batch1.len(), 30);
    assert_eq!(batch2.len(), 30);
    assert_eq!(batch3.len(), 30);
    assert_eq!(batch4.len(), 10); // Remaining 10
}

#[tokio::test]
async fn test_memory_adapter_close() {
    let adapter = MemoryTransportAdapter::new("close-topic");

    assert!(adapter.is_healthy());
    assert_eq!(adapter.name(), "memory");

    adapter.close().await.unwrap();

    assert!(!adapter.is_healthy());
}

// ============================================================================
// Message Processing Tests (Format Detection + Routing + Transform)
// ============================================================================

#[tokio::test]
async fn test_json_message_processing() {
    let adapter = MemoryTransportAdapter::new("json-events");

    // Create a JSON event
    let event = json!({
        "org_id": "acme",
        "event_category": "auth",
        "user_id": 12345,
        "action": "login",
        "timestamp": "2025-01-01T00:00:00Z"
    });

    adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();

    // Receive and process
    let messages = adapter.recv(1).await.unwrap();
    assert_eq!(messages.len(), 1);

    let msg = &messages[0];

    // Verify format detection
    let format_detector = FormatDetector::with_mode(FormatMode::Auto);
    let format = format_detector.check_and_detect(&msg.payload).unwrap();
    assert!(matches!(format, dfe_loader::payload::PayloadFormat::Json));

    // Parse the payload
    let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();
    assert_eq!(value["org_id"], "acme");
    assert_eq!(value["event_category"], "auth");
}

#[tokio::test]
async fn test_routing_from_json() {
    let adapter = MemoryTransportAdapter::new("routing-events");

    // Inject events with different destinations
    let events = vec![
        json!({ "org_id": "org1", "event_category": "auth", "data": "a" }),
        json!({ "org_id": "org2", "event_category": "network", "data": "b" }),
        json!({ "org_id": "org1", "event_category": "file", "data": "c" }),
    ];

    for event in &events {
        adapter.inject(serde_json::to_vec(event).unwrap()).await.unwrap();
    }

    // Configure router
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["event_category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        ..Default::default()
    };
    let router = Router::new(&routing_config);

    // Receive and route
    let messages = adapter.recv(10).await.unwrap();
    assert_eq!(messages.len(), 3);

    let routes: Vec<_> = messages.iter().map(|msg| {
        let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();
        router.route_value(&value)
    }).collect();

    // Verify routing
    assert!(matches!(&routes[0], RouteResult::Table(t) if t == "org1.auth"));
    assert!(matches!(&routes[1], RouteResult::Table(t) if t == "org2.network"));
    assert!(matches!(&routes[2], RouteResult::Table(t) if t == "org1.file"));
}

#[tokio::test]
async fn test_routing_with_nested_fields() {
    let adapter = MemoryTransportAdapter::new("nested-routing");

    // Event with nested routing fields
    let event = json!({
        "tags": {
            "event": {
                "org_id": "nested_org",
                "category": "nested_cat"
            }
        },
        "payload": "test"
    });

    adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();

    // Configure router with nested field paths
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string(), "tags.event.org_id".to_string()],
        table_fields: vec!["event_category".to_string(), "tags.event.category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        ..Default::default()
    };
    let router = Router::new(&routing_config);

    let messages = adapter.recv(1).await.unwrap();
    let value: serde_json::Value = sonic_rs::from_slice(&messages[0].payload).unwrap();
    let route = router.route_value(&value);

    // Should find org_id and category in nested tags
    assert!(matches!(route, RouteResult::Table(t) if t == "nested_org.nested_cat"));
}

#[tokio::test]
async fn test_routing_fallback_to_defaults() {
    let adapter = MemoryTransportAdapter::new("default-routing");

    // Event without routing fields
    let event = json!({
        "some_field": "value",
        "other_field": 123
    });

    adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();

    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["event_category".to_string()],
        default_db: "fallback_db".to_string(),
        default_table: "fallback_table".to_string(),
        ..Default::default()
    };
    let router = Router::new(&routing_config);

    let messages = adapter.recv(1).await.unwrap();
    let value: serde_json::Value = sonic_rs::from_slice(&messages[0].payload).unwrap();
    let route = router.route_value(&value);

    // Should use defaults
    assert!(matches!(route, RouteResult::Table(t) if t == "fallback_db.fallback_table"));
}

// ============================================================================
// Transform Tests
// ============================================================================

#[tokio::test]
async fn test_transform_flattens_nested() {
    let adapter = MemoryTransportAdapter::new("transform-test");

    let event = json!({
        "org_id": "test",
        "event_category": "test",
        "user": {
            "id": 123,
            "name": "Alice",
            "profile": {
                "email": "alice@example.com"
            }
        },
        "timestamp": "2025-01-01T00:00:00Z"
    });

    adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();

    let transformer = Transformer::new(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
    );

    let messages = adapter.recv(1).await.unwrap();
    let value: serde_json::Value = sonic_rs::from_slice(&messages[0].payload).unwrap();

    let result = transformer.transform(value).unwrap();

    // Nested fields should be flattened with "." separator
    let data = result.data;
    assert_eq!(data.get("user.id").and_then(|v| v.as_i64()), Some(123));
    assert_eq!(data.get("user.name").and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(data.get("user.profile.email").and_then(|v| v.as_str()), Some("alice@example.com"));
}

#[tokio::test]
async fn test_transform_removes_routing_fields() {
    let adapter = MemoryTransportAdapter::new("routing-removal-test");

    let event = json!({
        "org_id": "should_be_removed",
        "event_category": "should_be_removed",
        "keep_this": "value",
        "timestamp": "2025-01-01T00:00:00Z"
    });

    adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();

    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["event_category".to_string()],
        ..Default::default()
    };

    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );

    let messages = adapter.recv(1).await.unwrap();
    let value: serde_json::Value = sonic_rs::from_slice(&messages[0].payload).unwrap();

    let result = transformer.transform(value).unwrap();
    let data = result.data;

    // Routing fields should be removed
    assert!(data.get("org_id").is_none());
    assert!(data.get("event_category").is_none());

    // Other fields should remain
    assert_eq!(data.get("keep_this").and_then(|v| v.as_str()), Some("value"));
}

// ============================================================================
// Buffer Manager Tests
// ============================================================================

#[tokio::test]
async fn test_buffer_accumulates_messages() {
    let adapter = MemoryTransportAdapter::new("buffer-test");

    // Inject multiple messages
    for i in 0..5 {
        let event = json!({
            "org_id": "test",
            "event_category": "events",
            "id": i,
            "timestamp": "2025-01-01T00:00:00Z"
        });
        adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();
    }

    let buffer_config = BufferConfig {
        flush_rows: 100, // High threshold so we don't flush
        flush_bytes: 1_000_000,
        flush_age_secs: 3600,
        ..Default::default()
    };
    let mut buffer_manager = BufferManager::new(&buffer_config);

    let routing_config = RoutingConfig::default();
    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );
    let router = Router::new(&routing_config);

    // Process messages through the pipeline
    let messages = adapter.recv(10).await.unwrap();
    for (i, msg) in messages.iter().enumerate() {
        let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();

        let route = match router.route_value(&value) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(_) => panic!("Unexpected DLQ"),
        };

        let result = transformer.transform(value).unwrap();

        let offset = KafkaOffset::with_shared_topic(
            msg.topic.clone(),
            msg.partition,
            msg.offset,
        );

        buffer_manager.push(&route, result.data, Some(offset));
    }

    // Check buffer stats
    let stats = buffer_manager.stats();
    assert_eq!(stats.pending_rows, 5);
    // pending_bytes is calculated separately via pending_bytes() method
    assert!(buffer_manager.pending_bytes() > 0);
}

#[tokio::test]
async fn test_buffer_flush_on_threshold() {
    let adapter = MemoryTransportAdapter::new("flush-test");

    // Inject 10 messages
    for i in 0..10 {
        let event = json!({
            "org_id": "test",
            "event_category": "events",
            "id": i,
            "timestamp": "2025-01-01T00:00:00Z"
        });
        adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();
    }

    let buffer_config = BufferConfig {
        flush_rows: 5, // Low threshold
        flush_bytes: 1_000_000,
        flush_age_secs: 3600,
        ..Default::default()
    };
    let mut buffer_manager = BufferManager::new(&buffer_config);

    let routing_config = RoutingConfig::default();
    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );
    let router = Router::new(&routing_config);

    // Process all messages
    let messages = adapter.recv(10).await.unwrap();
    for msg in &messages {
        let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();
        let route = match router.route_value(&value) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(_) => panic!("Unexpected DLQ"),
        };
        let result = transformer.transform(value).unwrap();
        let offset = KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);
        buffer_manager.push(&route, result.data, Some(offset));
    }

    // Should have triggered flush condition
    assert!(buffer_manager.should_flush());

    // Get ready batches
    let batches = buffer_manager.get_ready_for_flush().unwrap();

    // Should have at least one batch ready
    assert!(!batches.is_empty());

    // Total rows should be >= 5 (flush threshold)
    let total_rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();
    assert!(total_rows >= 5);
}

// ============================================================================
// End-to-End Message Flow Tests
// ============================================================================

#[tokio::test]
async fn test_full_message_flow_without_clickhouse() {
    // This test simulates the full pipeline flow without ClickHouse
    // It verifies: inject -> recv -> parse -> route -> transform -> buffer

    let adapter = MemoryTransportAdapter::new("full-flow-test");

    // Create realistic events
    let events = vec![
        json!({
            "org_id": "acme",
            "event_category": "auth",
            "action": "login",
            "user": { "id": 1, "name": "Alice" },
            "ip": "192.168.1.1",
            "timestamp": "2025-01-01T00:00:00Z"
        }),
        json!({
            "org_id": "acme",
            "event_category": "network",
            "action": "connection",
            "source": { "ip": "10.0.0.1", "port": 443 },
            "timestamp": "2025-01-01T00:00:01Z"
        }),
        json!({
            "org_id": "globex",
            "event_category": "file",
            "action": "create",
            "path": "/tmp/test.txt",
            "timestamp": "2025-01-01T00:00:02Z"
        }),
    ];

    for event in &events {
        adapter.inject(serde_json::to_vec(event).unwrap()).await.unwrap();
    }

    // Setup pipeline components
    let routing_config = RoutingConfig {
        db_fields: vec!["org_id".to_string()],
        table_fields: vec!["event_category".to_string()],
        default_db: "common".to_string(),
        default_table: "events".to_string(),
        ..Default::default()
    };
    let router = Router::new(&routing_config);

    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );

    let buffer_config = BufferConfig {
        flush_rows: 1000,
        flush_bytes: 1_000_000,
        flush_age_secs: 3600,
        ..Default::default()
    };
    let mut buffer_manager = BufferManager::new(&buffer_config);

    // Process all messages
    let messages = adapter.recv(10).await.unwrap();
    assert_eq!(messages.len(), 3);

    let mut routed_tables = Vec::new();

    for msg in &messages {
        // Parse
        let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();

        // Route
        let table = match router.route_value(&value) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => panic!("Unexpected DLQ: {}", reason),
        };
        routed_tables.push(table.clone());

        // Transform
        let result = transformer.transform(value).unwrap();

        // Buffer
        let offset = KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);
        buffer_manager.push(&table, result.data, Some(offset));
    }

    // Verify routing
    assert_eq!(routed_tables, vec!["acme.auth", "acme.network", "globex.file"]);

    // Verify buffer state
    let stats = buffer_manager.stats();
    assert_eq!(stats.pending_rows, 3);

    // Flush all and verify batches
    let batches = buffer_manager.flush_all().unwrap();

    // Should have 3 tables: acme.auth, acme.network, globex.file
    assert_eq!(batches.len(), 3);

    // Verify each batch has 1 row
    for batch in &batches {
        assert_eq!(batch.batch.num_rows(), 1);
        assert!(!batch.offsets.is_empty());
    }
}

#[tokio::test]
async fn test_high_volume_message_processing() {
    let adapter = MemoryTransportAdapter::new("high-volume-test");

    const MESSAGE_COUNT: usize = 1000;

    // Inject many messages
    for i in 0..MESSAGE_COUNT {
        let event = json!({
            "org_id": "test_org",
            "event_category": "events",
            "id": i,
            "value": i as f64 * 1.5,
            "timestamp": "2025-01-01T00:00:00Z"
        });
        adapter.inject(serde_json::to_vec(&event).unwrap()).await.unwrap();
    }

    // Process in batches
    let routing_config = RoutingConfig::default();
    let router = Router::new(&routing_config);
    let transformer = Transformer::with_routing(
        &TimestampDqConfig::default(),
        &MetadataConfig::default(),
        &FieldSanitizationConfig::default(),
        &routing_config,
    );

    let buffer_config = BufferConfig {
        flush_rows: 100,
        flush_bytes: 1_000_000,
        flush_age_secs: 3600,
        ..Default::default()
    };
    let mut buffer_manager = BufferManager::new(&buffer_config);

    let mut total_processed = 0;
    let mut flush_count = 0;

    // Process in batches of 100
    loop {
        let messages = adapter.recv(100).await.unwrap();
        if messages.is_empty() {
            break;
        }

        for msg in &messages {
            let value: serde_json::Value = sonic_rs::from_slice(&msg.payload).unwrap();
            let table = match router.route_value(&value) {
                RouteResult::Table(t) => t,
                RouteResult::Dlq(_) => continue,
            };
            let result = transformer.transform(value).unwrap();
            let offset = KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);
            buffer_manager.push(&table, result.data, Some(offset));
            total_processed += 1;
        }

        // Check for flush
        if buffer_manager.should_flush() {
            let batches = buffer_manager.get_ready_for_flush().unwrap();
            flush_count += batches.len();
        }
    }

    // Final flush
    let final_batches = buffer_manager.flush_all().unwrap();
    flush_count += final_batches.len();

    assert_eq!(total_processed, MESSAGE_COUNT);
    assert!(flush_count > 0); // Should have flushed multiple times
}
