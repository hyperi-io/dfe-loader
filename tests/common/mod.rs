// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared test utilities and fixtures
#![allow(dead_code)]

pub mod containers;
pub mod metrics;

use std::env;
use std::time::Duration;

use serde_json::{Value, json};

use dfe_loader::config::{ClickHouseConfig, KafkaConfig, SaslConfig};

/// Load environment variables from .env file
pub fn load_dotenv() {
    let _ = dotenvy::from_path("/projects/dfe-loader/.env");
}

/// Check if external test environment is configured
pub fn has_external_env() -> bool {
    load_dotenv();
    env::var("CLICKHOUSE_HOST").is_ok() && env::var("KAFKA_BROKERS").is_ok()
}

/// Check if ClickHouse is available
pub fn has_clickhouse() -> bool {
    load_dotenv();
    env::var("CLICKHOUSE_HOST").is_ok()
}

/// Check if Kafka is available
pub fn has_kafka() -> bool {
    load_dotenv();
    env::var("KAFKA_BROKERS").is_ok()
}

/// Check if Docker is available for testcontainers
pub fn has_docker() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Skip test if no test environment is available
#[macro_export]
macro_rules! skip_if_no_env {
    () => {
        if !$crate::common::has_external_env() && !$crate::common::has_docker() {
            eprintln!("Skipping test: no test environment available");
            return;
        }
    };
}

/// Skip test if no ClickHouse available
#[macro_export]
macro_rules! skip_if_no_clickhouse {
    () => {
        if !$crate::common::has_clickhouse() {
            eprintln!("Skipping test: no ClickHouse available");
            return;
        }
        if !$crate::common::check_clickhouse_reachable() {
            eprintln!("Skipping test: ClickHouse not reachable");
            return;
        }
    };
}

/// Skip test if no Kafka available
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        if !$crate::common::has_kafka() {
            eprintln!("Skipping test: no Kafka available");
            return;
        }
        if !$crate::common::check_kafka_reachable() {
            eprintln!("Skipping test: Kafka not reachable");
            return;
        }
    };
}

/// Check if ClickHouse is reachable via TCP
pub fn check_clickhouse_reachable() -> bool {
    load_dotenv();

    let host = env::var("CLICKHOUSE_HOST").unwrap_or_default();
    let port: u16 = env::var("CLICKHOUSE_NATIVE_PORT")
        .unwrap_or_else(|_| "9000".to_string())
        .parse()
        .unwrap_or(9000);

    if host.is_empty() {
        return false;
    }

    let addr = format!("{}:{}", host, port);
    use std::net::ToSocketAddrs;
    match addr.to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(socket_addr) = addrs.next() {
                std::net::TcpStream::connect_timeout(&socket_addr, Duration::from_secs(3)).is_ok()
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Check if Kafka is reachable via TCP
pub fn check_kafka_reachable() -> bool {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_default();
    if brokers.is_empty() {
        return false;
    }

    let first_broker = brokers.split(',').next().unwrap_or(&brokers);
    use std::net::ToSocketAddrs;
    match first_broker.to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(socket_addr) = addrs.next() {
                std::net::TcpStream::connect_timeout(&socket_addr, Duration::from_secs(3)).is_ok()
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Get ClickHouse test configuration from environment
pub fn get_clickhouse_config() -> ClickHouseConfig {
    load_dotenv();

    let host = env::var("CLICKHOUSE_HOST").expect("CLICKHOUSE_HOST not set");
    let port = env::var("CLICKHOUSE_NATIVE_PORT").unwrap_or_else(|_| "9000".to_string());
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
    let username = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    ClickHouseConfig {
        hosts: vec![format!("{}:{}", host, port)],
        database,
        username,
        password,
        protocol: "native".to_string(),
        tables: Vec::new(),
        tls: None,
    }
}

/// Get Kafka test configuration from environment
pub fn get_kafka_config() -> KafkaConfig {
    load_dotenv();

    let brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set");
    let group = env::var("KAFKA_GROUP").unwrap_or_else(|_| "integration-test-group".to_string());

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
        client_id: "integration-test".to_string(),
        sasl,
        tls: None,
        ..Default::default()
    }
}

/// Create a unique test table name
pub fn unique_table_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    )
}

/// Generate sample event JSON for testing
pub fn generate_sample_event(id: u64, category: &str) -> Value {
    json!({
        "id": id,
        "org_id": "test_org",
        "event_category": category,
        "action": format!("action_{}", id % 10),
        "user": {
            "id": id * 100,
            "name": format!("user_{}", id),
            "email": format!("user{}@example.com", id)
        },
        "metadata": {
            "version": "1.0",
            "source": "integration_test"
        },
        "value": id as f64 * 1.5,
        "timestamp": chrono::Utc::now().to_rfc3339()
    })
}

/// Generate a batch of sample events
pub fn generate_sample_events(count: usize, category: &str) -> Vec<Value> {
    (0..count)
        .map(|i| generate_sample_event(i as u64, category))
        .collect()
}

/// Helper to create HTTP ClickHouse client for tests (JSONEachRow)
pub fn create_http_test_client() -> Option<dfe_loader::clickhouse::HttpClickHouseClient> {
    if !check_clickhouse_reachable() {
        return None;
    }

    let config = get_clickhouse_config();
    // Build an HTTP config from the test environment
    let host = std::env::var("CLICKHOUSE_HOST").unwrap_or_default();
    let http_port = std::env::var("CLICKHOUSE_HTTP_PORT").unwrap_or_else(|_| "8123".to_string());
    let ch_config = dfe_loader::clickhouse::ClickHouseConfig {
        hosts: vec![format!("{}:{}", host, http_port)],
        transport: dfe_loader::clickhouse::Transport::Http,
        database: config.database.clone(),
        username: config.username.clone(),
        password: config.password.clone(),
        tls: false,
        ..Default::default()
    };
    dfe_loader::clickhouse::HttpClickHouseClient::new(&ch_config).ok()
}

/// Drop a test table (HTTP client), using ON CLUSTER 'default' for replicated cluster.
pub async fn drop_http_test_table(
    client: &dfe_loader::clickhouse::HttpClickHouseClient,
    table_name: &str,
) {
    let _ = client
        .execute(&format!(
            "DROP TABLE IF EXISTS {} ON CLUSTER 'default'",
            table_name
        ))
        .await;
}
