// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Startup smoke tests — catch init panics before they reach production.
//!
//! These tests verify the full startup sequence without requiring external
//! services (Kafka, ClickHouse). They use `Config::load(None)` which falls
//! back to compiled defaults, and construct the Orchestrator just far enough
//! to prove it won't panic during init.
//!
//! The Prometheus recorder is global (one per process). Tests share a single
//! `MetricsManager` via `OnceLock` to avoid `SetRecorderError` panics.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dfe_loader::config::{Config, SharedConfig};
use dfe_loader::metrics::{Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;
use hyperi_rustlib::metrics::MetricsManager;

/// Shared MetricsManager for all smoke tests (recorder installed once).
fn shared_manager() -> &'static MetricsManager {
    static MANAGER: OnceLock<MetricsManager> = OnceLock::new();
    MANAGER.get_or_init(|| MetricsManager::new("smoke"))
}

/// Config loads from defaults without panicking.
#[test]
fn smoke_config_loads_defaults() {
    let config = Config::load(None).expect("default config should load");
    config.validate().expect("default config should validate");
}

/// Config loads from the example YAML file.
#[test]
fn smoke_config_loads_example_yaml() {
    let config = Config::load(Some("config.example.yaml")).expect("example config should load");
    config.validate().expect("example config should validate");
}

/// Metrics struct creates all counters/gauges/histograms without panicking.
#[test]
fn smoke_metrics_struct_creates() {
    let _metrics = Metrics::new(shared_manager());
}

/// Full init sequence: Config -> Metrics -> SharedConfig -> Orchestrator.
/// Verifies construction doesn't panic. Does NOT start the pipeline
/// (that would require Kafka).
#[tokio::test]
async fn smoke_orchestrator_constructs() {
    let config = Config::load(None).expect("config loads");
    config.validate().expect("config validates");

    let scaling = Arc::new(config.scaling.build_pressure());
    let metrics = Metrics::new(shared_manager());
    let server_state = Arc::new(ServerState::new(metrics.clone(), Arc::clone(&scaling)));

    assert!(!server_state.is_ready(), "should not be ready at init");

    let shared_config = SharedConfig::new(config.clone());

    let orchestrator = Orchestrator::with_metrics(config, metrics)
        .with_shared_config(shared_config)
        .with_scaling(Arc::clone(&scaling));

    // Verify shutdown token is usable
    let token = orchestrator.shutdown_token();
    assert!(!token.is_cancelled(), "should not be cancelled at init");
}

/// Orchestrator shuts down cleanly when token is cancelled immediately.
#[tokio::test]
async fn smoke_orchestrator_immediate_shutdown() {
    let config = Config::load(None).expect("config loads");
    let metrics = Metrics::new(shared_manager());

    let mut orchestrator = Orchestrator::with_metrics(config, metrics);
    let token = orchestrator.shutdown_token();

    // Cancel immediately before run()
    token.cancel();

    // run() should return promptly (transport fails to connect, but
    // shutdown is already requested so it exits cleanly)
    let result = tokio::time::timeout(Duration::from_secs(5), orchestrator.run()).await;

    assert!(
        result.is_ok(),
        "orchestrator should exit within 5s on immediate shutdown"
    );
}

/// ServerState readiness transitions work correctly.
#[test]
fn smoke_server_state_readiness() {
    let metrics = Metrics::new(shared_manager());
    let config = Config::load(None).expect("config loads");
    let scaling = Arc::new(config.scaling.build_pressure());
    let state = Arc::new(ServerState::new(metrics, scaling));

    assert!(!state.is_ready(), "should not be ready at init");

    state.set_ready(true);
    assert!(state.is_ready(), "should be ready after set_ready(true)");

    state.set_ready(false);
    assert!(
        !state.is_ready(),
        "should not be ready after set_ready(false)"
    );
}

/// Deployment contract generates without panicking.
#[test]
fn smoke_deployment_contract() {
    let contract = Config::deployment_contract();
    assert!(
        !contract.app_name.is_empty(),
        "contract should have an app_name"
    );
}

/// Helm chart generates from contract without panicking.
#[test]
fn smoke_helm_generation() {
    let contract = Config::deployment_contract();
    let tmp = tempfile::tempdir().expect("tempdir");
    let result = hyperi_rustlib::deployment::generate_chart(&contract, tmp.path());
    assert!(
        result.is_ok(),
        "chart generation should succeed: {result:?}"
    );
}

/// Dockerfile generates from contract without panicking.
#[test]
fn smoke_dockerfile_generation() {
    let contract = Config::deployment_contract();
    let content = hyperi_rustlib::deployment::generate_dockerfile(&contract);
    assert!(
        content.contains("FROM"),
        "Dockerfile should contain FROM directive"
    );
}
