// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared test utilities and fixtures.
//!
//! Supports dual-mode testing via `TEST_MODE` env var:
//! - `"remote"` (default): remote cluster endpoints from `.env`
//! - `"docker"`: dfe-docker `clickhouse` and `kafka-redpanda` profiles (localhost, no auth, no TLS)
#![allow(dead_code)]

pub mod containers;
pub mod metrics;
pub mod shared_service;
pub mod test_logging;

use std::env;
use std::net::ToSocketAddrs;
use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{Value, json};

// ============================================================================
// Test Mode
// ============================================================================

/// Test backend mode — controls which endpoints tests connect to.
///
/// Set via `TEST_MODE` env var:
/// - `"remote"` (default): use Kafka/ClickHouse from `.env`
/// - `"docker"`: use dfe-docker `clickhouse` and `kafka-redpanda` profiles (localhost, no auth, no TLS)
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
    ///
    /// The run's shared container once `acquire_clickhouse` has published it,
    /// else dfe-docker under `TEST_MODE=docker`. Remote mode names no host, so
    /// `is_configured()` is false and a test never reaches for a ClickHouse it
    /// did not start, such as a dfe-docker stack on `localhost`.
    pub fn from_env() -> Self {
        load_dotenv();
        if let Some(endpoint) = container_endpoint() {
            return Self {
                host: endpoint.host,
                http_port: endpoint.http_port,
                native_port: endpoint.native_port,
                tls: false,
                user: "default".into(),
                password: endpoint.password,
                database: "default".into(),
                cluster: None,
            };
        }
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
            TestMode::Remote => Self::unconfigured(),
        }
    }

    /// No ClickHouse named: `is_configured()` is false and nothing is probed.
    pub fn unconfigured() -> Self {
        Self {
            host: String::new(),
            http_port: 8123,
            native_port: 9000,
            tls: false,
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            cluster: None,
        }
    }

    /// Whether a ClickHouse is named at all: `TEST_MODE=docker` or the run's
    /// shared container.
    pub fn is_configured(&self) -> bool {
        !self.host.is_empty()
    }

    pub fn http_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.http_port)
    }

    pub fn native_addr(&self) -> String {
        format!("{}:{}", self.host, self.native_port)
    }

    /// Probe whether `ClickHouse` is actually responding (not just a TCP listener
    /// on port 9000 — shared hosts often have *something* bound there that
    /// false-positives a plain TCP probe).
    ///
    /// Tries the HTTP/HTTPS `/ping` endpoint which returns `Ok.\n` for live
    /// `ClickHouse` instances. Both plain HTTP and HTTPS variants supported.
    /// Sync (despite using reqwest::blocking) by isolating the blocking runtime
    /// inside a `std::thread::spawn` — necessary because tests run inside
    /// tokio runtimes that reject reqwest::blocking's internal runtime drop.
    pub fn is_reachable(&self) -> bool {
        self.is_configured() && clickhouse_http_ping_ok(&self.http_url())
    }

    /// Probe whether the connected `ClickHouse`'s `default` database uses the
    /// `Replicated` engine.
    ///
    /// Tests that create `ReplicatedMergeTree()` tables with no explicit
    /// ZooKeeper path rely on the Replicated database engine to expand the
    /// `{uuid}` macro and auto-fill that path. Against a single-node Atomic
    /// `default` database the server rejects the DDL (error 36), so such tests
    /// must skip. This is capability-based (queries `system.databases`) rather
    /// than mode-based, so it also catches a single-node `ClickHouse` in `remote` mode.
    pub fn has_replicated_default_db(&self) -> bool {
        clickhouse_default_db_is_replicated(&self.http_url(), &self.user, &self.password)
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

/// Where the `ClickHouse` container a test started is listening.
#[derive(Clone)]
struct ContainerEndpoint {
    host: String,
    http_port: u16,
    native_port: u16,
    password: String,
}

/// Published by `acquire_clickhouse` so every `ClickHouseTestConfig::from_env()`
/// call in the test reads the run's shared container.
static CONTAINER_ENDPOINT: OnceLock<ContainerEndpoint> = OnceLock::new();

fn container_endpoint() -> Option<ContainerEndpoint> {
    CONTAINER_ENDPOINT.get().cloned()
}

/// A `ClickHouse` held for one test. The shared container outlives it: the
/// run's watcher removes that once the run ends.
pub struct ClickHouseLease;

/// The `ClickHouse` a test runs against: dfe-docker under `TEST_MODE=docker`,
/// else the run's shared container.
///
/// `None` means skip, and the reason is printed. A named but unreachable
/// ClickHouse fails in CI. Without the `testcontainers` feature there is no
/// container to start, so the test skips: the `testcontainers` feature set
/// runs these tests.
pub async fn acquire_clickhouse(_test: &str) -> Option<ClickHouseLease> {
    let ch = ClickHouseTestConfig::from_env();
    if ch.is_configured() {
        if ch.is_reachable() {
            return Some(ClickHouseLease);
        }
        assert!(
            env::var_os("CI").is_none(),
            "ClickHouse at {} is named but unreachable in CI",
            ch.http_addr()
        );
        eprintln!(
            "Skipping: ClickHouse not reachable at {} (TEST_MODE={})",
            ch.http_addr(),
            TestMode::detect()
        );
        return None;
    }
    start_clickhouse_container().await
}

#[cfg(feature = "testcontainers")]
async fn start_clickhouse_container() -> Option<ClickHouseLease> {
    let container = containers::shared_clickhouse().await;
    let _ = CONTAINER_ENDPOINT.set(ContainerEndpoint {
        host: container.host.clone(),
        http_port: container.port(containers::CLICKHOUSE_HTTP_PORT),
        native_port: container.port(containers::CLICKHOUSE_NATIVE_PORT),
        password: containers::clickhouse_password(),
    });
    Some(ClickHouseLease)
}

#[cfg(not(feature = "testcontainers"))]
async fn start_clickhouse_container() -> Option<ClickHouseLease> {
    eprintln!("Skipping: no ClickHouse named and containers need --features testcontainers");
    None
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

/// The dfe-docker profiles that give the Docker mode its ClickHouse
/// (`localhost:8123`) and Kafka (`localhost:19092`).
const DFE_DOCKER_COMPOSE_UP: [&str; 7] = [
    "compose",
    "--profile",
    "clickhouse",
    "--profile",
    "kafka-redpanda",
    "up",
    "-d",
];

/// Start the dfe-docker ClickHouse and Redpanda profiles if `TEST_MODE=docker` and containers aren't running.
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
        .args(DFE_DOCKER_COMPOSE_UP)
        .current_dir(&docker_path)
        .status()
        .map_err(|e| format!("docker compose failed: {e}"))?;

    if !status.success() {
        return Err(
            "docker compose up for the clickhouse and kafka-redpanda profiles failed".into(),
        );
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

/// Panic if the container runtime is missing while running in CI.
///
/// Scoped deliberately to Docker, NOT to the live-service probes below.
///
/// A testcontainers test that skips in CI passes VACUOUSLY -- CI provides the
/// daemon, so its absence means the suite reports green while exercising none of
/// the integration surface.
///
/// The live-service macros are a different case. They probe an EXTERNAL
/// ClickHouse or Kafka -- a real cluster, or a dfe-docker compose stack cloned
/// as a sibling -- which CI is not expected to provide. Failing on those would
/// assert an environment nobody promised.
pub fn require_docker_in_ci() {
    assert!(
        std::env::var_os("CI").is_none() || has_docker(),
        "Docker unreachable in CI -- container tests must RUN here, not skip. \
         Skipping would report green while testing nothing."
    );
}

// =============================================================================
// Container naming and cleanup
// =============================================================================
//
// Every container carries a name saying which repo, suite and service it is, and
// labels saying which run and process own it, so an operator looking at
// `docker ps` can tell what left it behind.
//
// `shared_service` starts them all. The run's shared containers are one per
// service per run, removed by a watcher once the run ends. A container one test
// owns alone is named here, `<prefix>-integration-<test>-<service>-p<pid>`, and
// removed when the test drops it or its process exits. Each create first sweeps
// this suite's containers whose owner has gone (`sweep_removes`).
//
// A sweep finds them all by label:
//   docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-loader-integration)

/// Label marking every container this suite starts, for bulk cleanup.
pub const TEST_SUITE_LABEL: (&str, &str) = ("io.hyperi.test.suite", "dfe-loader-integration");

/// Container name for a backing service in this suite.
///
/// Pass `Some(test)` -- the owning test -- for anything a test starts for itself,
/// which is everything here. `None` is for a container started once for a whole
/// test binary; nothing does that today, and using it from several tests would
/// make them collide on the name rather than share the container.
///
/// The name ends in the owning process id, so two runs of one test on one
/// daemon never ask for the same name. Without it the second start fails with
/// "name is already in use" and burns the whole startup timeout before the
/// error surfaces.
///
/// Names are lowercased and non-alphanumerics collapse to `-`, because Docker
/// only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and the test paths `test_name!`
/// produces have colons in them.
#[must_use]
pub fn container_name(test: Option<&str>, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    let pid = std::process::id();
    let prefix = containers_prefix();
    test.map_or_else(
        || format!("{prefix}-integration-{}-p{pid}", slug(service)),
        |t| format!("{prefix}-integration-{}-{}-p{pid}", slug(t), slug(service)),
    )
}

/// The name of the test this expands inside, for naming its containers.
///
/// Rust has no way to read the current test's name, and a hand-written literal
/// per call site would drift the moment a test is renamed. `type_name` of a
/// function declared
/// right here reports the path it is nested in, which is the calling test --
/// hence a macro: expanded in a helper it would report the helper.
///
/// An `async fn` body becomes a generated future, so the path picks up
/// `::{{closure}}`; the trailing generated segments are trimmed off.
#[macro_export]
macro_rules! test_name {
    () => {{
        fn probe() {}
        fn path_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let mut name = path_of(probe);
        name = name.strip_suffix("::probe").unwrap_or(name);
        name = name.strip_suffix("::{{closure}}").unwrap_or(name);
        name.rsplit("::").next().unwrap_or(name)
    }};
}

/// Container name prefix: `HYPERI_TEST_CONTAINER_PREFIX`, else `dfe-loader-test`.
pub fn containers_prefix() -> String {
    shared_service::Suite {
        repo: "dfe-loader",
        manifest_dir: env!("CARGO_MANIFEST_DIR"),
    }
    .prefix()
}

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

/// Hold a `ClickHouse` for the rest of the test, or return when there is none.
///
/// Use as the first line of an `async` test. The binding it leaves keeps the
/// container alive until the test ends.
#[macro_export]
macro_rules! require_clickhouse {
    () => {
        let Some(_clickhouse) = $crate::common::acquire_clickhouse($crate::test_name!()).await
        else {
            return;
        };
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

/// Skip test unless the connected `ClickHouse`'s `default` database is the
/// `Replicated` engine.
///
/// Capability-based replacement for `skip_if_docker!` on tests that create
/// `ReplicatedMergeTree()` tables with no ZooKeeper path: those need the
/// `Replicated` database engine so the `{uuid}` macro expands. Unlike the
/// mode-based `skip_if_docker!`, this also skips against a single-node
/// `ClickHouse` such as the test container, where the DDL would otherwise fail
/// with server error 36. Call after `require_clickhouse!`, which guarantees the
/// probe target is reachable.
#[macro_export]
macro_rules! skip_if_not_replicated {
    () => {
        let ch = $crate::common::ClickHouseTestConfig::from_env();
        if !ch.has_replicated_default_db() {
            eprintln!(
                "Skipping: test requires a Replicated-cluster ClickHouse at {} (TEST_MODE={}); \
                 the default database is not the Replicated engine",
                ch.http_addr(),
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

// ============================================================================
// Backward-Compatible Helpers
// ============================================================================

/// Load environment variables from the repo's `.env`, if present.
///
/// Resolved from `CARGO_MANIFEST_DIR` rather than an absolute
/// `/projects/...` path: a checkout elsewhere fails the load into `let _ =`,
/// and the live-service probes then report "not reachable" -- an absent config
/// file reading as an absent service.
pub fn load_dotenv() {
    let _ = dotenvy::from_path(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env"));
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

/// Create an HTTP `clickhouse::Client` for integration tests.
///
/// Used by `Inserter` tests that need `DynamicInsert` or `InsertFormatted`.
/// Defaults to HTTP transport for test compatibility.
pub fn create_ch_test_client() -> Option<clickhouse::Client> {
    let ch = ClickHouseTestConfig::from_env();
    if !ch.is_reachable() {
        return None;
    }

    let scheme = if ch.tls { "https" } else { "http" };
    let url = format!("{scheme}://{}:{}", ch.host, ch.http_port);

    Some(
        clickhouse::Client::default()
            .with_url(&url)
            .with_user(&ch.user)
            .with_password(&ch.password)
            .with_database(&ch.database),
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

/// Create a unique test table name (bare, no database).
pub fn unique_table_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    )
}

/// Unique test table name qualified with the configured database.
///
/// The `Inserter` resolves a bare table name to the `default` database
/// (`parse_db_table`), while DDL run through the query client lands in the
/// connection's configured database (`CLICKHOUSE_DATABASE`). Tests
/// that create a table AND drive the `Inserter` against it must agree on the
/// database, so they qualify the name with `ClickHouseTestConfig::database`.
pub fn unique_qualified_table_name(prefix: &str) -> String {
    let db = ClickHouseTestConfig::from_env().database;
    format!("{db}.{}", unique_table_name(prefix))
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

/// Query `system.databases` over HTTP and report whether the `default` database
/// uses the `Replicated` engine. Returns `false` on any error (unreachable,
/// auth failure, non-Replicated engine).
///
/// Same thread-isolated `reqwest::blocking` pattern as `clickhouse_http_ping_ok`
/// — the fresh `std::thread::spawn` keeps reqwest's internal runtime from
/// panicking on drop inside the outer `#[tokio::test]` runtime.
fn clickhouse_default_db_is_replicated(base_url: &str, user: &str, password: &str) -> bool {
    let url = base_url.trim_end_matches('/').to_string();
    let user = user.to_string();
    let password = password.to_string();
    std::thread::spawn(move || -> Option<bool> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .ok()?;
        let resp = client
            .get(&url)
            .header("X-ClickHouse-User", user)
            .header("X-ClickHouse-Key", password)
            .query(&[(
                "query",
                "SELECT engine FROM system.databases WHERE name = 'default'",
            )])
            .send()
            .ok()?;
        if !resp.status().is_success() {
            return Some(false);
        }
        Some(resp.text().ok()?.trim() == "Replicated")
    })
    .join()
    .ok()
    .flatten()
    .unwrap_or(false)
}
