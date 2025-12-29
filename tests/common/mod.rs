//! Shared test utilities and fixtures

use std::env;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde_json::{json, Value};

use dfe_loader_clickhouse::clickhouse::ArrowClickHouseClient;
use dfe_loader_clickhouse::config::{ClickHouseConfig, KafkaConfig, SaslConfig, SaslMechanism};

/// Load environment variables from .env file
pub fn load_dotenv() {
    let _ = dotenvy::from_path("/projects/dfe-loader-clickhouse/.env");
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
            "PLAIN" => SaslMechanism::Plain,
            "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
            "SCRAM-SHA-512" | "" => SaslMechanism::ScramSha512,
            _ => SaslMechanism::ScramSha512,
        };
        Some(SaslConfig {
            enabled: true,
            mechanism,
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

/// Create a simple Arrow RecordBatch for testing
pub fn create_simple_batch(row_count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<u64> = (0..row_count as u64).collect();
    let events: Vec<String> = (0..row_count).map(|i| format!("event_{}", i)).collect();
    let values: Vec<f64> = (0..row_count).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(events)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .expect("Failed to create RecordBatch")
}

/// Create a complex Arrow RecordBatch with multiple data types
pub fn create_complex_batch(row_count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
        Field::new("count", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));

    let now = chrono::Utc::now().timestamp_millis();
    let ids: Vec<u64> = (0..row_count as u64).collect();
    let names: Vec<String> = (0..row_count).map(|i| format!("item_{}", i)).collect();
    let scores: Vec<Option<f64>> = (0..row_count)
        .map(|i| if i % 3 == 0 { None } else { Some(i as f64 * 2.5) })
        .collect();
    let counts: Vec<Option<i64>> = (0..row_count)
        .map(|i| if i % 5 == 0 { None } else { Some(i as i64) })
        .collect();
    let timestamps: Vec<i64> = (0..row_count).map(|i| now + i as i64 * 1000).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Float64Array::from(scores)) as ArrayRef,
            Arc::new(Int64Array::from(counts)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef,
        ],
    )
    .expect("Failed to create complex RecordBatch")
}

/// Helper to create ClickHouse client for tests
pub async fn create_test_client() -> Option<ArrowClickHouseClient> {
    if !check_clickhouse_reachable() {
        return None;
    }

    let config = get_clickhouse_config();
    ArrowClickHouseClient::new(&config).await.ok()
}

/// Create a test table and return cleanup function
pub async fn create_test_table(
    client: &ArrowClickHouseClient,
    table_name: &str,
    ddl: &str,
) -> Result<(), String> {
    client
        .query(ddl)
        .await
        .map_err(|e| format!("Failed to create table: {}", e))?;
    Ok(())
}

/// Drop a test table
pub async fn drop_test_table(client: &ArrowClickHouseClient, table_name: &str) {
    let _ = client
        .query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .await;
}
