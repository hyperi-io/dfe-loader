// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared test utilities and fixtures.
//!
//! Supports dual-mode testing via `TEST_MODE` env var:
//! - `"remote"` (default): devex cluster endpoints from `.env`
//! - `"docker"`: dfe-docker infra profile (localhost, no auth, no TLS)
#![allow(dead_code)]

pub mod containers;
pub mod metrics;

use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

use serde_json::{Value, json};

use dfe_loader::config::{ClickHouseConfig, KafkaConfig, SaslConfig};

// ============================================================================
// Test Mode
// ============================================================================

/// Test backend mode — controls which endpoints tests connect to.
///
/// Set via `TEST_MODE` env var:
/// - `"remote"` (default): use Kafka/ClickHouse from `.env`
/// - `"docker"`: use dfe-docker infra profile (localhost, no auth, no TLS)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Remote,
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

impl std::fmt::Display for TestMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote => write!(f, "remote"),
            Self::Docker => write!(f, "docker"),
        }
    }
}

// ============================================================================
// ClickHouse Test Config
// ============================================================================

/// Mode-aware `ClickHouse` connection config for tests.
pub struct ClickHouseTestConfig {
    pub host: String,
    pub http_port: u16,
    pub native_port: u16,
    pub tls: bool,
    pub user: String,
    pub password: String,
    pub database: String,
    pub cluster: Option<String>,
}

impl ClickHouseTestConfig {
    /// Build from current test mode.
    pub fn from_env() -> Self {
        load_dotenv();
        match TestMode::detect() {
            TestMode::Docker => Self {
                host: "localhost".into(),
                http_port: 8123,
                native_port: 9000,
                tls: false,
                user: "default".into(),
                password: String::new(),
                database: "default".into(),
                cluster: None, // single node
            },
            TestMode::Remote => Self {
                host: env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".into()),
                http_port: env::var("CLICKHOUSE_HTTP_PORT")
                    .unwrap_or_else(|_| "8123".into())
                    .parse()
                    .unwrap_or(8123),
                native_port: env::var("CLICKHOUSE_NATIVE_PORT")
                    .unwrap_or_else(|_| "9000".into())
                    .parse()
                    .unwrap_or(9000),
                tls: env::var("CLICKHOUSE_TLS")
                    .unwrap_or_default()
                    .eq_ignore_ascii_case("true"),
                user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
                password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
                database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
                cluster: env::var("CLICKHOUSE_CLUSTER")
                    .ok()
                    .filter(|s| !s.is_empty()),
            },
        }
    }

    pub fn http_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.http_port)
    }

    pub fn native_addr(&self) -> String {
        format!("{}:{}", self.host, self.native_port)
    }

    /// Probe whether `ClickHouse` is actually responding (not just a TCP listener
    /// on port 9000 — devex hosts often have *something* bound there that
    /// false-positives a plain TCP probe).
    ///
    /// Tries the HTTP/HTTPS `/ping` endpoint which returns `Ok.\n` for live
    /// `ClickHouse` instances. Both plain HTTP and HTTPS variants supported.
    /// Sync (despite using reqwest::blocking) by isolating the blocking runtime
    /// inside a `std::thread::spawn` — necessary because tests run inside
    /// tokio runtimes that reject reqwest::blocking's internal runtime drop.
    pub fn is_reachable(&self) -> bool {
        clickhouse_http_ping_ok(&self.http_url())
    }

    pub fn http_addr(&self) -> String {
        format!("{}:{}", self.host, self.http_port)
    }

    /// Generate CREATE TABLE DDL — adds ON CLUSTER for remote cluster, omits for Docker single-node.
    pub fn create_table_ddl(&self, table: &str, columns: &str, order_by: &str) -> String {
        match self.cluster.as_deref() {
            Some(c) => format!(
                "CREATE TABLE IF NOT EXISTS {table} ON CLUSTER '{c}' ({columns}) ENGINE = MergeTree() ORDER BY {order_by}"
            ),
            None => format!(
                "CREATE TABLE IF NOT EXISTS {table} ({columns}) ENGINE = MergeTree() ORDER BY {order_by}"
            ),
        }
    }

    /// Generate DROP TABLE DDL — adds ON CLUSTER for remote cluster.
    pub fn drop_table_ddl(&self, table: &str) -> String {
        match self.cluster.as_deref() {
            Some(c) => format!("DROP TABLE IF EXISTS {table} ON CLUSTER '{c}'"),
            None => format!("DROP TABLE IF EXISTS {table}"),
        }
    }
}

/// Returns `" ON CLUSTER 'default'"` for remote mode, `""` for Docker single-node.
///
/// Use in DDL format strings: `format!("CREATE TABLE {}{} ...", table, on_cluster_clause())`
pub fn on_cluster_clause() -> &'static str {
    load_dotenv();
    let ch = ClickHouseTestConfig::from_env();
    if ch.cluster.is_some() {
        " ON CLUSTER 'default'"
    } else {
        ""
    }
}

// ============================================================================
// Kafka Test Config
// ============================================================================

/// Mode-aware Kafka connection config for tests.
pub struct KafkaTestConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_user: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaTestConfig {
    /// Build from current test mode.
    pub fn from_env() -> Self {
        load_dotenv();
        match TestMode::detect() {
            TestMode::Docker => Self {
                brokers: "localhost:19092".into(),
                security_protocol: "PLAINTEXT".into(),
                sasl_mechanism: None,
                sasl_user: None,
                sasl_password: None,
            },
            TestMode::Remote => Self {
                brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
                security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                    .unwrap_or_else(|_| "SASL_PLAINTEXT".into()),
                sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
                sasl_user: env::var("KAFKA_SASL_USER").ok(),
                sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
            },
        }
    }

    pub fn has_sasl(&self) -> bool {
        self.sasl_mechanism.is_some() && self.sasl_user.is_some()
    }

    pub fn is_reachable(&self) -> bool {
        let first = self.brokers.split(',').next().unwrap_or(&self.brokers);
        tcp_reachable(first)
    }
}

// ============================================================================
// Docker Lifecycle (convenience)
// ============================================================================

/// Start dfe-docker infra profile if `TEST_MODE=docker` and containers aren't running.
///
/// Looks for dfe-docker at:
///   1. `DFE_DOCKER_PATH` env var
///   2. `../dfe-docker` (sibling directory convention)
///
/// Returns `Ok(true)` if started, `Ok(false)` if already running or not Docker mode.
pub fn ensure_docker_infra() -> Result<bool, String> {
    if TestMode::detect() != TestMode::Docker {
        return Ok(false);
    }

    let output = std::process::Command::new("docker")
        .args([
            "ps",
            "--filter",
            "name=dfe-clickhouse",
            "--format",
            "{{.Names}}",
        ])
        .output()
        .map_err(|e| format!("docker not found: {e}"))?;

    if String::from_utf8_lossy(&output.stdout).contains("dfe-clickhouse") {
        return Ok(false);
    }

    let docker_path = env::var("DFE_DOCKER_PATH").unwrap_or_else(|_| "../dfe-docker".into());

    if !std::path::Path::new(&docker_path)
        .join("docker-compose.yml")
        .exists()
    {
        return Err(format!(
            "dfe-docker not found at {docker_path}. Set DFE_DOCKER_PATH or clone dfe-docker as a sibling."
        ));
    }

    let status = std::process::Command::new("docker")
        .args(["compose", "--profile", "infra", "up", "-d"])
        .current_dir(&docker_path)
        .status()
        .map_err(|e| format!("docker compose failed: {e}"))?;

    if !status.success() {
        return Err("docker compose --profile infra up -d failed".into());
    }

    for _ in 0..30 {
        std::thread::sleep(Duration::from_secs(1));
        if ClickHouseTestConfig::from_env().is_reachable() {
            return Ok(true);
        }
    }

    Err("ClickHouse did not become healthy within 30s".into())
}

// ============================================================================
// Skip Macros
// ============================================================================

/// Skip test if no test environment is available (either mode)
#[macro_export]
macro_rules! skip_if_no_env {
    () => {
        let ch = $crate::common::ClickHouseTestConfig::from_env();
        let kf = $crate::common::KafkaTestConfig::from_env();
        if !ch.is_reachable() && !kf.is_reachable() {
            eprintln!(
                "Skipping: no test environment reachable (TEST_MODE={})",
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

/// Skip test if `ClickHouse` is not reachable in current mode
#[macro_export]
macro_rules! skip_if_no_clickhouse {
    () => {
        let ch = $crate::common::ClickHouseTestConfig::from_env();
        if !ch.is_reachable() {
            eprintln!(
                "Skipping: ClickHouse not reachable at {} (TEST_MODE={})",
                ch.native_addr(),
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

/// Skip test if Kafka is not reachable in current mode
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        let kf = $crate::common::KafkaTestConfig::from_env();
        if !kf.is_reachable() {
            eprintln!(
                "Skipping: Kafka not reachable at {} (TEST_MODE={})",
                kf.brokers,
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

/// Skip test in Docker mode — use for tests requiring Replicated DB or multi-node cluster.
#[macro_export]
macro_rules! skip_if_docker {
    () => {
        if $crate::common::TestMode::detect() == $crate::common::TestMode::Docker {
            eprintln!(
                "Skipping: test requires replicated cluster (TEST_MODE=docker is single-node)"
            );
            return;
        }
    };
}

// ============================================================================
// Backward-Compatible Helpers
// ============================================================================

/// Load environment variables from .env file
pub fn load_dotenv() {
    let _ = dotenvy::from_path("/projects/dfe-loader/.env");
}

/// Check if external test environment is configured
pub fn has_external_env() -> bool {
    load_dotenv();
    env::var("CLICKHOUSE_HOST").is_ok() && env::var("KAFKA_BROKERS").is_ok()
}

/// Check if `ClickHouse` is available
pub fn has_clickhouse() -> bool {
    ClickHouseTestConfig::from_env().is_reachable()
}

/// Check if Kafka is available
pub fn has_kafka() -> bool {
    KafkaTestConfig::from_env().is_reachable()
}

/// Check if Docker is available for testcontainers
pub fn has_docker() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Check if `ClickHouse` is reachable via TCP (backward compat)
pub fn check_clickhouse_reachable() -> bool {
    ClickHouseTestConfig::from_env().is_reachable()
}

/// Check if Kafka is reachable via TCP (backward compat)
pub fn check_kafka_reachable() -> bool {
    KafkaTestConfig::from_env().is_reachable()
}

/// Get `ClickHouse` test configuration (backward compat — returns loader's `ClickHouseConfig`)
pub fn get_clickhouse_config() -> ClickHouseConfig {
    let ch = ClickHouseTestConfig::from_env();
    ClickHouseConfig {
        hosts: vec![ch.native_addr()],
        database: ch.database,
        username: ch.user,
        password: hyperi_rustlib::config::sensitive::SensitiveString::from(ch.password),
        protocol: "native".to_string(),
        tables: Vec::new(),
        tls: None,
    }
}

/// Get Kafka test configuration (backward compat — returns loader's `KafkaConfig`)
pub fn get_kafka_config() -> KafkaConfig {
    let kf = KafkaTestConfig::from_env();

    let sasl = if kf.has_sasl() {
        let mechanism = match kf.sasl_mechanism.as_deref().unwrap_or("") {
            "PLAIN" => "plain",
            "SCRAM-SHA-256" => "scram_sha_256",
            "SCRAM-SHA-512" | "" => "scram_sha_512",
            _ => "scram_sha_512",
        };
        Some(SaslConfig {
            enabled: true,
            mechanism: mechanism.to_string(),
            username: kf.sasl_user.unwrap_or_default(),
            password: hyperi_rustlib::config::sensitive::SensitiveString::from(
                kf.sasl_password.unwrap_or_default(),
            ),
            ..Default::default()
        })
    } else {
        None
    };

    KafkaConfig {
        brokers: kf
            .brokers
            .split(',')
            .map(std::string::ToString::to_string)
            .collect(),
        topics: vec!["test-events".to_string()],
        group: env::var("KAFKA_GROUP").unwrap_or_else(|_| "integration-test-group".to_string()),
        topic_regex: None,
        client_id: "integration-test".to_string(),
        sasl,
        tls: None,
        ..Default::default()
    }
}

/// Helper to create HTTP `ClickHouse` client for tests
pub fn create_http_test_client() -> Option<dfe_loader::clickhouse::ClickHouseQueryClient> {
    let ch = ClickHouseTestConfig::from_env();
    if !ch.is_reachable() {
        return None;
    }

    let ch_config = dfe_loader::clickhouse::ClickHouseConfig {
        hosts: vec![format!("{}:{}", ch.host, ch.http_port)],
        transport: dfe_loader::clickhouse::Transport::Http,
        database: ch.database,
        username: ch.user,
        password: ch.password,
        tls: ch.tls,
        ..Default::default()
    };
    dfe_loader::clickhouse::ClickHouseQueryClient::new(&ch_config).ok()
}

/// Create a clickhouse-rs fork `UnifiedClient` for integration tests.
///
/// Used by `Inserter` tests that need `DynamicInsert` or `InsertFormatted`.
/// Defaults to HTTP transport for test compatibility.
pub fn create_ch_test_client() -> Option<clickhouse::UnifiedClient> {
    let ch = ClickHouseTestConfig::from_env();
    if !ch.is_reachable() {
        return None;
    }

    let scheme = if ch.tls { "https" } else { "http" };
    let url = format!("{scheme}://{}:{}", ch.host, ch.http_port);

    Some(
        clickhouse::UnifiedClient::http()
            .with_url(&url)
            .with_user(&ch.user)
            .with_password(&ch.password)
            .with_database(&ch.database)
            .build(),
    )
}

/// Drop a test table — uses ON CLUSTER for remote cluster, plain for Docker.
pub async fn drop_http_test_table(
    client: &dfe_loader::clickhouse::ClickHouseQueryClient,
    table_name: &str,
) {
    let ch = ClickHouseTestConfig::from_env();
    let ddl = ch.drop_table_ddl(table_name);
    let _ = client.execute(&ddl).await;
}

// ============================================================================
// Test Data Generators
// ============================================================================

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

// ============================================================================
// Internal Helpers
// ============================================================================

fn tcp_reachable(addr: &str) -> bool {
    addr.to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .is_some_and(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok())
}

/// Probe `ClickHouse` HTTP/HTTPS `/ping` endpoint. Returns `true` only if a
/// live `ClickHouse` is actually responding with `Ok.` body — rejects bare
/// TCP listeners and other services that happen to be on the port.
///
/// Uses `reqwest::blocking` (a dev-only feature) inside a fresh
/// `std::thread::spawn` to isolate the blocking runtime from the outer tokio
/// runtime that `#[tokio::test]` provides. Without the thread isolation,
/// reqwest's internal runtime would panic on drop.
fn clickhouse_http_ping_ok(base_url: &str) -> bool {
    let url = format!("{}/ping", base_url.trim_end_matches('/'));
    std::thread::spawn(move || -> Option<bool> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .ok()?;
        let resp = client.get(&url).send().ok()?;
        if !resp.status().is_success() {
            return Some(false);
        }
        Some(resp.text().ok()?.trim() == "Ok.")
    })
    .join()
    .ok()
    .flatten()
    .unwrap_or(false)
}
