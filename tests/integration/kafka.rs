// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka config parsing. The transport against a real broker is
//! `kafka_transport_e2e`, on the run's shared Kafka.

use dfe_loader::config::{KafkaConfig, SaslConfig, SaslMechanism};

#[tokio::test]
async fn test_kafka_sasl_config() {
    let config = KafkaConfig {
        brokers: vec!["localhost:9092".to_string()],
        topics: vec!["test-topic".to_string()],
        topic_regex: None,
        group: "test-group".to_string(),
        client_id: "test-client".to_string(),
        sasl: Some(SaslConfig {
            enabled: true,
            mechanism: "scram_sha_256".to_string(),
            username: "testuser".to_string(),
            password: scalo::config::sensitive::SensitiveString::from("testpass"),
            ..Default::default()
        }),
        tls: None,
        ..Default::default()
    };

    assert!(config.sasl.is_some());
    let sasl = config.sasl.unwrap();
    assert!(sasl.enabled);
    assert_eq!(sasl.mechanism(), SaslMechanism::ScramSha256);
}
