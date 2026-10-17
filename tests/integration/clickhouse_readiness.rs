// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The loader reports ready only once `ClickHouse` accepts its credentials.
//!
//! Each test creates a least-privilege user on a real `ClickHouse` and runs the
//! startup probe as that user: with its password, with a wrong one, and with a
//! wrong one the server is then changed to accept.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dfe_loader::clickhouse::{ClickHouseConfig, ClickHouseQueryClient, Transport};
use dfe_loader::metrics::{Metrics, ServerState};
use scalo::ScalingPressure;
use scalo::metrics::{MetricsConfig, MetricsManager};
use scalo::scaling::ScalingPressureConfig;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

use super::grpc_outage::{clickhouse_address, query_client};
use crate::common::containers::TestInfrastructure;
use crate::test_name;

/// The user the probe logs in as.
const USER: &str = "dfe_loader_probe";

/// The line the probe logs for each refusal.
const REFUSED: &str = "ClickHouse refused the startup probe";

/// Longest a probe the server accepts may take to report ready.
const READY_WITHIN: Duration = Duration::from_secs(30);

/// How long a refused probe is watched for a ready it must never report.
const REFUSED_FOR: Duration = Duration::from_secs(5);

/// The log lines a future writes, kept for the test to read.
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

    /// A subscriber writing here, for one future to run under.
    fn dispatch(&self) -> tracing::Dispatch {
        let writer = self.clone();
        tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .finish(),
        )
    }
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
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!()).await;
    let clickhouse = clickhouse_address(&infra).await;
    create_user(&query_client(&clickhouse), "right").await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let probed = tokio::time::timeout(
        READY_WITHIN,
        state.ready_once_clickhouse_accepts(&as_probe_user(&clickhouse, "right"), &shutdown),
    )
    .await;

    assert!(
        probed.is_ok(),
        "the probe was still running after {READY_WITHIN:?}"
    );
    assert!(state.is_ready(), "a user ClickHouse accepts is not ready");
}

/// A wrong password never goes ready, and each retried refusal names the 516
/// `ClickHouse` answered with.
#[tokio::test]
async fn a_wrong_password_never_goes_ready_and_logs_the_516() {
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!()).await;
    let clickhouse = clickhouse_address(&infra).await;
    create_user(&query_client(&clickhouse), "right").await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let logs = Captured::default();
    let probed = tokio::time::timeout(
        REFUSED_FOR,
        state
            .ready_once_clickhouse_accepts(&as_probe_user(&clickhouse, "wrong"), &shutdown)
            .with_subscriber(logs.dispatch()),
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
    let infra = TestInfrastructure::clickhouse_managing_users(test_name!()).await;
    let clickhouse = clickhouse_address(&infra).await;
    let admin = query_client(&clickhouse);
    create_user(&admin, "old").await;

    let state = not_ready_state();
    let shutdown = CancellationToken::new();
    let logs = Captured::default();
    let probe = {
        let state = Arc::clone(&state);
        let config = as_probe_user(&clickhouse, "new");
        let shutdown = shutdown.clone();
        tokio::spawn(
            async move {
                state
                    .ready_once_clickhouse_accepts(&config, &shutdown)
                    .await;
            }
            .with_subscriber(logs.dispatch()),
        )
    };

    let refused = comes_true(REFUSED_FOR, || logs.text().contains(REFUSED)).await;
    let ready_while_refused = state.is_ready();
    admin
        .execute(&format!("ALTER USER {USER} IDENTIFIED BY 'new'"))
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
        matches!(recovered, Ok(Ok(()))),
        "the probe did not finish within {READY_WITHIN:?} of the password change: {recovered:?}"
    );
    assert!(
        state.is_ready(),
        "not ready after ClickHouse accepted the password"
    );
}
