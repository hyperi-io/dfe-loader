// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The loader reports ready, and takes records, only once `ClickHouse` accepts
//! its credentials.
//!
//! Each test creates a least-privilege user on a real `ClickHouse` and runs the
//! startup probe as that user: with its password, with a wrong one, and with a
//! wrong one the server is then changed to accept. The intake tests run the
//! whole loader on Kafka behind the same probe.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`. Needs process-per-test
//! (nextest): a test reading the probe's log installs the process's subscriber.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dfe_loader::clickhouse::{ClickHouseConfig, ClickHouseQueryClient, Transport};
use dfe_loader::config::Config;
use dfe_loader::metrics::{Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::FutureRecord;
use scalo::ScalingPressure;
use scalo::config::sensitive::SensitiveString;
use scalo::metrics::{MetricsConfig, MetricsManager};
use scalo::scaling::ScalingPressureConfig;
use tokio_util::sync::CancellationToken;

use super::grpc_outage::{clickhouse_address, free_port, query_client, wait_landed};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader};
use super::kafka_transport_e2e::{ensure_topic, make_producer};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// The user the probe logs in as.
const USER: &str = "dfe_loader_probe";

/// The line the probe logs for each refusal.
const REFUSED: &str = "ClickHouse refused the startup probe";

/// Longest a probe the server accepts may take to report ready.
const READY_WITHIN: Duration = Duration::from_secs(30);

/// How long a refused probe is watched for a ready it must never report.
const REFUSED_FOR: Duration = Duration::from_secs(5);

/// The first line the pipeline logs when it starts.
const PIPELINE_STARTED: &str = "Starting pipeline orchestrator";

/// Records on the topic before the loader starts.
const RECORDS: u64 = 20;

/// How long records have to land once the loader is running.
const LAND_WITHIN: Duration = Duration::from_secs(90);

/// The log lines this test process writes, kept for the test to read.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer")).into_owned()
    }

    /// Capture every log line in this process from here on.
    ///
    /// The one subscriber the process has: a span made under a scoped
    /// subscriber and dropped under another panics the registry.
    fn install() -> Self {
        let logs = Self::default();
        let writer = logs.clone();
        tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .try_init()
            .expect("no subscriber before this one: nextest gives each test its own process");
        logs
    }
}

/// A password made for one test run.
fn fresh_password() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Create the probe user with `password`, granted only the loader's reads and writes.
async fn create_user(admin: &ClickHouseQueryClient, password: &str) {
    admin
        .execute(&format!("CREATE USER {USER} IDENTIFIED BY '{password}'"))
        .await
        .expect("create the probe user");
    admin
        .execute(&format!("GRANT SELECT, INSERT ON default.* TO {USER}"))
        .await
        .expect("grant the probe user");
}

/// The loader's `ClickHouse` config, logging in as the probe user with `password`.
fn as_probe_user(clickhouse: &str, password: &str) -> ClickHouseConfig {
    ClickHouseConfig {
        hosts: vec![clickhouse.to_string()],
        transport: Transport::Http,
        database: "default".to_string(),
        username: USER.to_string(),
        password: password.to_string(),
        tls: false,
        ..Default::default()
    }
}

/// Loader state as `run_service` builds it, before anything has marked it ready.
fn not_ready_state() -> Arc<ServerState> {
    let manager = MetricsManager::with_config(MetricsConfig::offline(""));
    let scaling = Arc::new(ScalingPressure::new(
        ScalingPressureConfig::default(),
        vec![],
    ));
    Arc::new(ServerState::new(Metrics::new(&manager), scaling))
}

/// A Kafka loader for `topic` and `default.{table}` that logs in to
/// `ClickHouse` as the probe user with `password`.
fn probe_user_loader(
    bootstrap: &str,
    topic: &str,
    clickhouse: &str,
    table: &str,
    password: &str,
) -> Config {
    let mut config = kafka_loader(
        bootstrap,
        topic,
        &format!("dfe-{table}"),
        clickhouse.to_string(),
        table,
    );
    config.clickhouse.username = USER.to_string();
    config.clickhouse.password = SensitiveString::from(password.to_string());
    config
}

/// The real loader binary, killed if the test ends without stopping it.
struct LoaderProcess(std::process::Child);

impl LoaderProcess {
    /// Send SIGTERM, the signal Kubernetes stops a pod with, and wait up to
    /// `within` for the exit.
    async fn terminate(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(status)) = self.0.try_wait() {
                return Some(status);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

impl Drop for LoaderProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The status `/{path}` on the loader's metrics listener answers, if any.
async fn probe_status(port: u16, path: &str) -> Option<u16> {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/{path}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

/// Members of the consumer group `group` the broker reports right now.
async fn group_members(bootstrap: &str, group: &str) -> usize {
    let (bootstrap, group) = (bootstrap.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        // Its own group id: this client only asks, it never joins `group`.
        let observer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", format!("{group}-observer"))
            .create()
            .expect("group observer");
        let groups = observer
            .fetch_group_list(Some(&group), Duration::from_secs(10))
            .expect("describe the consumer group");
        groups.groups().iter().map(|g| g.members().len()).sum()
    })
    .await
    .expect("group observer task")
}

/// Whether `done` comes true within `within`, checked every 100 ms.
async fn comes_true(within: Duration, done: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    done()
}

/// The control: a user `ClickHouse` accepts goes ready.
#[tokio::test]
async fn a_user_clickhouse_accepts_goes_ready() {
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let password = fresh_password();
    create_user(&query_client(&clickhouse), &password).await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let probed = tokio::time::timeout(
        READY_WITHIN,
        state.ready_once_clickhouse_accepts(&as_probe_user(&clickhouse, &password), &shutdown),
    )
    .await;

    assert!(
        matches!(probed, Ok(Ok(true))),
        "the probe did not report acceptance within {READY_WITHIN:?}"
    );
    assert!(state.is_ready(), "a user ClickHouse accepts is not ready");
}

/// A wrong password never goes ready, and each retried refusal names the 516
/// `ClickHouse` answered with.
#[tokio::test]
async fn a_wrong_password_never_goes_ready_and_logs_the_516() {
    let logs = Captured::install();
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), false).await;
    let clickhouse = clickhouse_address(&infra).await;
    create_user(&query_client(&clickhouse), &fresh_password()).await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let wrong = as_probe_user(&clickhouse, &fresh_password());
    let probed = tokio::time::timeout(
        REFUSED_FOR,
        state.ready_once_clickhouse_accepts(&wrong, &shutdown),
    )
    .await;
    let logged = logs.text();
    let refusals: Vec<&str> = logged.lines().filter(|l| l.contains(REFUSED)).collect();
    eprintln!("{logged}");

    assert!(
        probed.is_err(),
        "the probe finished inside {REFUSED_FOR:?} on a wrong password"
    );
    assert!(!state.is_ready(), "a wrong password went ready");
    assert!(
        refusals.len() >= 2,
        "the probe did not retry inside {REFUSED_FOR:?}: {logged}"
    );
    assert!(
        refusals.iter().all(|l| l.contains("516")),
        "a refusal did not name the 516: {logged}"
    );
}

/// A probe refused for a wrong password goes ready once `ClickHouse` is
/// changed to accept it, with no restart.
#[tokio::test]
async fn a_refused_probe_goes_ready_once_clickhouse_accepts_the_password() {
    let logs = Captured::install();
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let admin = query_client(&clickhouse);
    create_user(&admin, &fresh_password()).await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let new_password = fresh_password();
    let probe = {
        let state = Arc::clone(&state);
        let config = as_probe_user(&clickhouse, &new_password);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            state
                .ready_once_clickhouse_accepts(&config, &shutdown)
                .await
        })
    };

    let refused = comes_true(REFUSED_FOR, || logs.text().contains(REFUSED)).await;
    let ready_while_refused = state.is_ready();
    admin
        .execute(&format!("ALTER USER {USER} IDENTIFIED BY '{new_password}'"))
        .await
        .expect("change the probe user's password");
    let recovered = tokio::time::timeout(READY_WITHIN, probe).await;
    shutdown.cancel();
    eprintln!("{}", logs.text());

    assert!(refused, "the old password was never refused");
    assert!(
        !ready_while_refused,
        "went ready while ClickHouse refused the password"
    );
    assert!(
        matches!(recovered, Ok(Ok(Ok(true)))),
        "the probe did not report acceptance within {READY_WITHIN:?} of the password change"
    );
    assert!(
        state.is_ready(),
        "not ready after ClickHouse accepted the password"
    );
}

/// A loader `ClickHouse` refuses never joins the consumer group, so it takes no
/// partition from a loader that can write. Once the password is accepted it
/// joins and the records already on the topic land.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_takes_no_partitions_until_clickhouse_accepts_it() {
    let logs = Captured::install();
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let admin = query_client(&clickhouse);
    create_user(&admin, &fresh_password()).await;

    let table = unique_table_name("readiness_intake");
    admin
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("readiness-{table}");
    let group = format!("dfe-{table}");
    ensure_topic(&bootstrap, &topic).await;
    let producer = make_producer(&bootstrap);
    for id in 0..RECORDS {
        let payload = format!(r#"{{"id":{id}}}"#);
        producer
            .send(
                FutureRecord::<(), str>::to(&topic).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("deliver record {id}: {e}"));
    }

    let password = fresh_password();
    let state = not_ready_state();
    let mut loader = Orchestrator::new(probe_user_loader(
        &bootstrap,
        &topic,
        &clickhouse,
        &table,
        &password,
    ));
    let stop = loader.shutdown_token();
    let running = {
        let state = Arc::clone(&state);
        tokio::spawn(async move { loader.run_once_clickhouse_accepts(&state).await })
    };

    let refused = comes_true(REFUSED_FOR, || logs.text().contains(REFUSED)).await;
    // Watch the group for a while, as long as a loader that subscribed would
    // need to join it.
    let mut most_members_while_refused = 0;
    let watch_until = tokio::time::Instant::now() + REFUSED_FOR * 2;
    while tokio::time::Instant::now() < watch_until {
        most_members_while_refused =
            most_members_while_refused.max(group_members(&bootstrap, &group).await);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let started_while_refused = logs.text().contains(PIPELINE_STARTED);
    let ready_while_refused = state.is_ready();

    admin
        .execute(&format!("ALTER USER {USER} IDENTIFIED BY '{password}'"))
        .await
        .expect("change the probe user's password");
    let want: BTreeSet<u64> = (0..RECORDS).collect();
    let landed = wait_landed(&clickhouse, &table, &want, LAND_WITHIN).await;
    let members_once_accepted = group_members(&bootstrap, &group).await;
    let ready_once_accepted = state.is_ready();
    stop.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(60), running).await;
    let _ = admin
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;
    eprintln!("{}", logs.text());

    assert!(refused, "the wrong password was never refused");
    assert_eq!(
        most_members_while_refused, 0,
        "the loader joined the consumer group while ClickHouse refused its password"
    );
    assert!(
        !started_while_refused,
        "the pipeline started while ClickHouse refused the password"
    );
    assert!(
        !ready_while_refused,
        "went ready while ClickHouse refused the password"
    );
    assert_eq!(
        landed, want,
        "the records on the topic did not land once ClickHouse accepted the password"
    );
    assert_eq!(
        members_once_accepted, 1,
        "the loader is not in the consumer group once ClickHouse accepted the password"
    );
    assert!(
        ready_once_accepted,
        "not ready after ClickHouse accepted the password"
    );
    assert!(
        matches!(stopped, Ok(Ok(Ok(())))),
        "the loader did not stop cleanly within 60s of the shutdown"
    );
}

/// A shutdown while `ClickHouse` refuses the password returns at once and never
/// starts the pipeline.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_while_clickhouse_refuses_never_starts_the_pipeline() {
    let logs = Captured::install();
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), false).await;
    let clickhouse = clickhouse_address(&infra).await;
    create_user(&query_client(&clickhouse), &fresh_password()).await;

    // No broker listens here: the pipeline has nothing to reach before shutdown.
    let no_broker = format!("127.0.0.1:{}", free_port());
    let table = unique_table_name("readiness_shutdown");
    let state = not_ready_state();
    let mut loader = Orchestrator::new(probe_user_loader(
        &no_broker,
        &format!("readiness-{table}"),
        &clickhouse,
        &table,
        &fresh_password(),
    ));
    let stop = loader.shutdown_token();
    let running = {
        let state = Arc::clone(&state);
        tokio::spawn(async move { loader.run_once_clickhouse_accepts(&state).await })
    };

    let refused = comes_true(REFUSED_FOR, || logs.text().contains(REFUSED)).await;
    stop.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(10), running).await;
    let logged = logs.text();
    eprintln!("{logged}");

    assert!(refused, "the wrong password was never refused");
    assert!(
        matches!(stopped, Ok(Ok(Ok(())))),
        "the loader did not return cleanly within 10s of the shutdown"
    );
    assert!(
        !logged.contains(PIPELINE_STARTED),
        "the pipeline started although ClickHouse never accepted the password"
    );
    assert!(!state.is_ready(), "went ready without ClickHouse accepting");
}

/// The shipped binary, against a `ClickHouse` that refuses its login, answers
/// `/livez`, reports `/readyz` 503, starts no pipeline, and exits cleanly on
/// SIGTERM.
#[tokio::test(flavor = "multi_thread")]
async fn the_binary_serves_its_probes_and_starts_no_pipeline_while_clickhouse_refuses() {
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!(), false).await;
    let clickhouse = clickhouse_address(&infra).await;
    // No user is created, so every login the binary makes is refused.
    let dir = tempfile::tempdir().expect("working directory");
    let log_path = dir.path().join("loader.log");
    let log = std::fs::File::create(&log_path).expect("loader log");
    let metrics_port = free_port();
    let mut loader = LoaderProcess(
        std::process::Command::new(env!("CARGO_BIN_EXE_dfe-loader"))
            .arg("--metrics-addr")
            .arg(format!("127.0.0.1:{metrics_port}"))
            // An empty directory, so no `.env` reaches the binary.
            .current_dir(dir.path())
            .env("DFE_LOADER_CLICKHOUSE_HOSTS", &clickhouse)
            .env("DFE_LOADER_CLICKHOUSE_DATABASE", "default")
            .env("DFE_LOADER_CLICKHOUSE_USERNAME", USER)
            .env("DFE_LOADER_CLICKHOUSE_PASSWORD", fresh_password())
            .env(
                "DFE_LOADER_KAFKA_BROKERS",
                format!("127.0.0.1:{}", free_port()),
            )
            .env("DFE_LOADER_VERSION_CHECK__ENABLED", "false")
            .stdout(log.try_clone().expect("loader log handle"))
            .stderr(log)
            .spawn()
            .expect("start the loader binary"),
    );
    let logged = || std::fs::read_to_string(&log_path).unwrap_or_default();

    let refused = comes_true(READY_WITHIN, || logged().contains(REFUSED)).await;
    let livez = probe_status(metrics_port, "livez").await;
    let readyz = probe_status(metrics_port, "readyz").await;
    tokio::time::sleep(REFUSED_FOR).await;
    let started_while_refused = logged().contains(PIPELINE_STARTED);
    let exit = loader.terminate(Duration::from_secs(30)).await;
    let log_text = logged();
    eprintln!("{log_text}");

    assert!(
        !started_while_refused,
        "the binary started the pipeline while ClickHouse refused its login"
    );
    assert!(refused, "the binary never logged a refused login");
    assert_eq!(
        livez,
        Some(200),
        "/livez did not answer while ClickHouse refused"
    );
    assert_eq!(readyz, Some(503), "/readyz did not report not ready");
    assert!(
        exit.is_some_and(|status| status.success()),
        "the binary did not exit cleanly within 30s of SIGTERM: {exit:?}"
    );
    assert!(
        log_text.contains("Shutdown before ClickHouse accepted the loader"),
        "the binary did not report stopping before the pipeline started"
    );
}
