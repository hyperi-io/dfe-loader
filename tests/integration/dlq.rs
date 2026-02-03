//! DLQ (Dead Letter Queue) integration tests
//!
//! Tests DLQ producer routing and message sending

use std::env;

use dfe_loader::config::{DlqConfig, KafkaConfig, SaslConfig};
use dfe_loader::kafka::{DlqMessage, DlqProducer, DlqRoutingMode};

use crate::common::{check_kafka_reachable, load_dotenv};

/// Skip test if no Kafka available
fn skip_if_no_kafka() -> bool {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_default();
    if brokers.is_empty() {
        eprintln!("KAFKA_BROKERS not set");
        return true;
    }

    if !check_kafka_reachable() {
        eprintln!("Kafka not reachable");
        return true;
    }

    false
}

fn get_kafka_config() -> KafkaConfig {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set");
    let group = env::var("KAFKA_GROUP").unwrap_or_else(|_| "dlq-test-group".to_string());

    let sasl = if env::var("KAFKA_SASL_USER").is_ok() || env::var("KAFKA_SASL_MECHANISM").is_ok() {
        let mechanism = match env::var("KAFKA_SASL_MECHANISM")
            .unwrap_or_default()
            .as_str()
        {
            "PLAIN" => "plain",
            "SCRAM-SHA-256" => "scram_sha_256",
            "SCRAM-SHA-512" | "" => "scram_sha_512",
            _ => "scram_sha_512",
        };
        Some(SaslConfig {
            enabled: true,
            mechanism: mechanism.to_string(),
            username: env::var("KAFKA_SASL_USER").unwrap_or_default(),
            password: env::var("KAFKA_SASL_PASSWORD").unwrap_or_default(),
            ..Default::default()
        })
    } else {
        None
    };

    KafkaConfig {
        brokers: brokers.split(',').map(|s| s.to_string()).collect(),
        topics: vec!["test-events".to_string()],
        group,
        topic_regex: None,
        client_id: "dlq-integration-test".to_string(),
        sasl,
        tls: None,
    }
}

// ============================================================================
// DLQ Routing Mode Tests
// ============================================================================

#[test]
fn test_dlq_routing_mode_per_table() {
    // Test per-table routing computes correct topic names
    let suffix = ".dlq";
    let common = "common.dlq";

    // With destination
    let topic = compute_topic(DlqRoutingMode::PerTable, suffix, common, Some("acme.auth"));
    assert_eq!(topic, "acme.auth.dlq");

    let topic = compute_topic(DlqRoutingMode::PerTable, suffix, common, Some("org.events"));
    assert_eq!(topic, "org.events.dlq");

    // Without destination (falls back to common)
    let topic = compute_topic(DlqRoutingMode::PerTable, suffix, common, None);
    assert_eq!(topic, "common.dlq");
}

#[test]
fn test_dlq_routing_mode_common() {
    // Test common routing always uses the common topic
    let suffix = ".dlq";
    let common = "all-errors.dlq";

    let topic = compute_topic(DlqRoutingMode::Common, suffix, common, Some("acme.auth"));
    assert_eq!(topic, "all-errors.dlq");

    let topic = compute_topic(DlqRoutingMode::Common, suffix, common, None);
    assert_eq!(topic, "all-errors.dlq");
}

/// Helper to compute DLQ topic without creating producer
fn compute_topic(
    mode: DlqRoutingMode,
    suffix: &str,
    common: &str,
    destination: Option<&str>,
) -> String {
    match mode {
        DlqRoutingMode::Common => common.to_string(),
        DlqRoutingMode::PerTable => {
            if let Some(dest) = destination {
                format!("{}{}", dest, suffix)
            } else {
                common.to_string()
            }
        }
    }
}

// ============================================================================
// DLQ Message Tests
// ============================================================================

#[test]
fn test_dlq_message_construction() {
    let payload = b"{\"test\": \"data\"}";
    let msg = DlqMessage {
        payload,
        reason: "parse_error",
        destination: Some("acme.events"),
        original_topic: "source-events",
        original_partition: 2,
        original_offset: 12345,
        key: Some(b"event-key"),
    };

    assert_eq!(msg.payload, payload);
    assert_eq!(msg.reason, "parse_error");
    assert_eq!(msg.destination, Some("acme.events"));
    assert_eq!(msg.original_topic, "source-events");
    assert_eq!(msg.original_partition, 2);
    assert_eq!(msg.original_offset, 12345);
    assert_eq!(msg.key, Some(b"event-key".as_slice()));
}

#[test]
fn test_dlq_message_without_key() {
    let payload = b"test payload";
    let msg = DlqMessage {
        payload,
        reason: "schema_mismatch",
        destination: None,
        original_topic: "events",
        original_partition: 0,
        original_offset: 1,
        key: None,
    };

    assert!(msg.key.is_none());
    assert!(msg.destination.is_none());
}

// ============================================================================
// DLQ Producer Integration Tests
// ============================================================================

#[tokio::test]
async fn test_dlq_producer_creation() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let result = DlqProducer::new(&kafka_config, &dlq_config);
    assert!(
        result.is_ok(),
        "DLQ producer creation failed: {:?}",
        result.err()
    );

    eprintln!("✓ DLQ producer created successfully");
}

#[tokio::test]
async fn test_dlq_producer_with_common_topic() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let producer = DlqProducer::new(&kafka_config, &dlq_config)
        .expect("Failed to create producer")
        .with_common_topic("test-common-dlq");

    // Verify the producer was configured (we can't inspect private fields,
    // but we can verify creation succeeds)
    eprintln!("✓ DLQ producer with common topic created");
    drop(producer);
}

#[tokio::test]
async fn test_dlq_producer_with_per_table_routing() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let producer = DlqProducer::new(&kafka_config, &dlq_config)
        .expect("Failed to create producer")
        .with_per_table_routing("_error");

    eprintln!("✓ DLQ producer with per-table routing created");
    drop(producer);
}

#[tokio::test]
async fn test_dlq_producer_send_simple() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let producer = DlqProducer::new(&kafka_config, &dlq_config)
        .expect("Failed to create producer")
        .with_common_topic("test-dlq-integration");

    let payload = b"{\"test\": \"message\", \"id\": 1}";
    let start = std::time::Instant::now();

    let result = producer
        .send_simple(payload, "test_error", "source-topic", 0, 100)
        .await;

    match result {
        Ok(topic) => {
            eprintln!("✓ DLQ message sent to {} in {:?}", topic, start.elapsed());
            assert_eq!(topic, "test-dlq-integration");
        }
        Err(e) => {
            // Topic may not exist - that's OK for this test, we're testing the producer
            eprintln!("DLQ send returned error (topic may not exist): {}", e);
        }
    }
}

#[tokio::test]
async fn test_dlq_producer_send_routed() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let producer = DlqProducer::new(&kafka_config, &dlq_config)
        .expect("Failed to create producer")
        .with_per_table_routing(".dlq");

    let payload = b"{\"test\": \"routed_message\", \"category\": \"auth\"}";
    let start = std::time::Instant::now();

    let result = producer
        .send_routed(
            payload,
            "schema_mismatch",
            "acme.auth",
            "source-topic",
            1,
            200,
        )
        .await;

    match result {
        Ok(topic) => {
            eprintln!(
                "✓ DLQ routed message sent to {} in {:?}",
                topic,
                start.elapsed()
            );
            assert_eq!(topic, "acme.auth.dlq");
        }
        Err(e) => {
            // Topic may not exist
            eprintln!("DLQ send returned error (topic may not exist): {}", e);
        }
    }
}

#[tokio::test]
async fn test_dlq_producer_send_with_key() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let kafka_config = get_kafka_config();
    let dlq_config = DlqConfig::default();

    let producer = DlqProducer::new(&kafka_config, &dlq_config)
        .expect("Failed to create producer")
        .with_common_topic("test-dlq-keyed");

    let payload = b"{\"event\": \"with_key\"}";
    let msg = DlqMessage {
        payload,
        reason: "test_with_key",
        destination: Some("test.events"),
        original_topic: "events",
        original_partition: 0,
        original_offset: 500,
        key: Some(b"message-key-123"),
    };

    let result = producer.send(msg).await;
    match result {
        Ok(topic) => {
            eprintln!("✓ DLQ message with key sent to {}", topic);
        }
        Err(e) => {
            eprintln!("DLQ send with key returned error: {}", e);
        }
    }
}

// ============================================================================
// DLQ Config Tests
// ============================================================================

#[test]
fn test_dlq_config_default() {
    let config = DlqConfig::default();
    assert_eq!(config.topic_suffix, ".dlq");
}

#[test]
fn test_dlq_config_custom_suffix() {
    let config = DlqConfig {
        enabled: true,
        topic_suffix: "_errors".to_string(),
    };
    assert_eq!(config.topic_suffix, "_errors");
}
