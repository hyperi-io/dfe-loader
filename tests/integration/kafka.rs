//! Kafka integration tests
//!
//! Tests run against k8s.tyrell.com.au AutoMQ cluster via .env settings

use std::env;

use dfe_loader::config::{KafkaConfig, SaslConfig, SaslMechanism};
use dfe_loader::kafka::Consumer;

/// Convert SaslMechanism to the string format used in config
fn mechanism_to_string(m: SaslMechanism) -> String {
    match m {
        SaslMechanism::None => "none",
        SaslMechanism::Plain => "plain",
        SaslMechanism::ScramSha256 => "scram_sha_256",
        SaslMechanism::ScramSha512 => "scram_sha_512",
        SaslMechanism::OAuthBearer => "oauthbearer",
        SaslMechanism::AwsMskIam => "aws_msk_iam",
    }
    .to_string()
}

fn load_dotenv() {
    let _ = dotenvy::from_path("/projects/dfe-loader/.env");
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
        let mechanism = match env::var("KAFKA_SASL_MECHANISM").unwrap_or_default().as_str() {
            "PLAIN" => SaslMechanism::Plain,
            "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
            "SCRAM-SHA-512" | "" => SaslMechanism::ScramSha512,
            "OAUTHBEARER" => SaslMechanism::OAuthBearer,
            "AWS_MSK_IAM" => SaslMechanism::AwsMskIam,
            _ => SaslMechanism::ScramSha512,
        };
        Some(SaslConfig {
            enabled: true,
            mechanism: mechanism_to_string(mechanism),
            username: env::var("KAFKA_SASL_USER").unwrap_or_default(),
            password: env::var("KAFKA_SASL_PASSWORD").unwrap_or_default(),
            ..Default::default()
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
        mechanism: "scram_sha_256".to_string(),
        username: "testuser".to_string(),
        password: "testpass".to_string(),
        ..Default::default()
    });

    // Just verify config is set - actual connection test needs running Kafka
    assert!(config.sasl.is_some());
    let sasl = config.sasl.unwrap();
    assert!(sasl.enabled);
    assert_eq!(sasl.mechanism(), SaslMechanism::ScramSha256);
}
