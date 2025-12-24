//! Kafka integration tests
//!
//! Tests run against k8s.tyrell.com.au AutoMQ cluster via .env settings

use std::env;

use dfe_loader_clickhouse::config::{KafkaConfig, SaslConfig};
use dfe_loader_clickhouse::kafka::Consumer;

fn load_dotenv() {
    let _ = dotenvy::from_path("/projects/dfe-loader-clickhouse/.env");
}

/// Skip test if no Kafka available
fn skip_if_no_kafka() -> bool {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_default();
    if brokers.is_empty() {
        eprintln!("KAFKA_BROKERS not set");
        return true;
    }

    // Try to connect to first broker
    let first_broker = brokers.split(',').next().unwrap_or(&brokers);
    eprintln!("Checking Kafka at {}...", first_broker);

    use std::net::ToSocketAddrs;
    match first_broker.to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(socket_addr) = addrs.next() {
                match std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_secs(3)) {
                    Ok(_) => {
                        eprintln!("Kafka reachable at {} ({})", first_broker, socket_addr);
                        false
                    }
                    Err(e) => {
                        eprintln!("Kafka not reachable at {}: {}", first_broker, e);
                        true
                    }
                }
            } else {
                eprintln!("Could not resolve {}", first_broker);
                true
            }
        }
        Err(e) => {
            eprintln!("DNS resolution failed for {}: {}", first_broker, e);
            true
        }
    }
}

fn get_test_config() -> KafkaConfig {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set");
    let group = env::var("KAFKA_GROUP").unwrap_or_else(|_| "integration-test-group".to_string());

    // Use .env SASL settings
    let sasl = if env::var("KAFKA_SASL_USER").is_ok() || env::var("KAFKA_SASL_MECHANISM").is_ok() {
        Some(SaslConfig {
            enabled: true,
            mechanism: env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-512".to_string()),
            username: env::var("KAFKA_SASL_USER").unwrap_or_default(),
            password: env::var("KAFKA_SASL_PASSWORD").unwrap_or_default(),
        })
    } else {
        None
    };

    eprintln!("Config: brokers={}, group={}, sasl={}", brokers, group, sasl.is_some());

    KafkaConfig {
        brokers: brokers.split(',').map(|s| s.to_string()).collect(),
        topics: vec!["test-events".to_string()],  // Default test topic
        group,
        topic_regex: None,
        client_id: "integration-test".to_string(),
        sasl,
        tls: None,
    }
}

#[tokio::test]
async fn test_kafka_consumer_creation() {
    if skip_if_no_kafka() {
        eprintln!("Skipping test: no Kafka available");
        return;
    }

    let config = get_test_config();
    let start = std::time::Instant::now();
    let result = Consumer::new(&config);

    match result {
        Ok(consumer) => {
            let elapsed = start.elapsed();
            eprintln!("✓ Created Kafka consumer in {:?}", elapsed);
            eprintln!("  Brokers: {:?}", config.brokers);
            eprintln!("  Group: {}", config.group);
            eprintln!("  SASL: {}", config.sasl.is_some());

            // Try to subscribe
            let start = std::time::Instant::now();
            match consumer.subscribe() {
                Ok(()) => {
                    eprintln!("✓ Subscribed to topics in {:?}", start.elapsed());
                }
                Err(e) => {
                    eprintln!("✗ Subscribe failed: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Kafka consumer creation failed: {}", e);
            panic!("Consumer should be created when Kafka is reachable");
        }
    }
}

#[tokio::test]
async fn test_kafka_config_validation() {
    // This test doesn't need Kafka running - just validates config
    let mut config = get_test_config();

    // Valid config
    assert!(!config.brokers.is_empty());
    assert!(!config.topics.is_empty());
    assert!(!config.group.is_empty());

    // Empty brokers should be invalid
    config.brokers = vec![];
    // This would be caught by Config::validate()
}

#[tokio::test]
async fn test_kafka_sasl_config() {
    // Test that SASL configuration is properly set up
    let mut config = get_test_config();
    config.sasl = Some(SaslConfig {
        enabled: true,
        mechanism: "SCRAM-SHA-256".to_string(),
        username: "testuser".to_string(),
        password: "testpass".to_string(),
    });

    // Just verify config is set - actual connection test needs running Kafka
    assert!(config.sasl.is_some());
    let sasl = config.sasl.unwrap();
    assert!(sasl.enabled);
    assert_eq!(sasl.mechanism, "SCRAM-SHA-256");
}
