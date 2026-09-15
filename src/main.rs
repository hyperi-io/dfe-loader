// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-loader

// =============================================================================
// Global Allocator Configuration
// =============================================================================
// jemalloc — DFE policy 2026-04-17: jemalloc at every channel, no mimalloc.
// hyperi-ci's release-track build adds `--features jemalloc` automatically on
// every channel (spike/alpha/beta/release). For local builds, opt in with:
//   cargo build --release --features jemalloc
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{debug, info, warn};

use dfe_loader::config::{Config, ConfigWatcher, SharedConfig, WatcherConfig};
use dfe_loader::metrics::{Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;
use scalo::cli::{
    CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo, run_app,
};

#[derive(Parser, Debug)]
#[command(name = "dfe-loader")]
#[command(about = "High-performance Kafka to ClickHouse data loader")]
#[command(version)]
struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,

    /// Generate Helm chart from deployment contract and exit (default output: ./chart)
    #[arg(long, value_name = "DIR", default_missing_value = "chart", num_args = 0..=1)]
    emit_helm: Option<PathBuf>,

    /// Generate Dockerfile from deployment contract and exit (default output: ./Dockerfile)
    #[arg(long, value_name = "FILE", default_missing_value = "Dockerfile", num_args = 0..=1)]
    emit_dockerfile: Option<PathBuf>,
}

impl ServiceApp for App {
    type Config = Config;

    fn name(&self) -> &'static str {
        "dfe-loader"
    }

    fn env_prefix(&self) -> &'static str {
        "DFE_LOADER"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-loader", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.command.as_ref()
    }

    fn load_config(&self, path: Option<&str>) -> Result<Self::Config, CliError> {
        // Seed scalo's cascade first: while it is unset every `from_cascade()`
        // reader -- scaling.*, worker_pool.*, batch_processing.*,
        // self_regulation.*, version_check.* -- resolves to its hard-coded
        // default and no env var moves it (#160).
        if let Err(e) = scalo::config::setup(self.common.to_config_options(self.env_prefix())) {
            // The cascade is a OnceLock; a second load keeps the first seed.
            debug!(error = %e, "scalo config cascade already seeded");
        }

        let config = Config::load(path).map_err(|e| CliError::Config(e.to_string()))?;
        config
            .validate()
            .map_err(|e| CliError::Config(e.to_string()))?;
        Ok(config)
    }

    fn run_service(
        &self,
        config: Self::Config,
        mut runtime: ServiceRuntime,
    ) -> impl std::future::Future<Output = Result<(), CliError>> + Send {
        // Same resolution Config::load() uses, so a config file named by
        // DFE_LOADER_CONFIG is hot-reloaded like one named by --config.
        let config_path = Config::resolve_config_path(self.common_args().config.as_deref());

        async move {
            let mut config = config;

            // Resolve env:/vault: credential specs on ClickHouse credentials before
            // anyone reads them. See src/config/credentials.rs for the spec syntax.
            dfe_loader::config::credentials::resolve_clickhouse_credentials(&mut config.clickhouse)
                .await
                .map_err(|e| CliError::Config(e.to_string()))?;

            info!(
                version = env!("CARGO_PKG_VERSION"),
                kafka_brokers = ?config.kafka.brokers,
                clickhouse_hosts = ?config.clickhouse.hosts,
                payload_format = %config.payload.format,
                "Starting dfe-loader"
            );

            debug!(
                brokers = ?config.kafka.brokers,
                group = %config.kafka.group,
                topics = ?config.kafka.topics,
                clickhouse = ?config.clickhouse.hosts,
                transport = %config.transport,
                flush_rows = config.buffer.flush_rows,
                flush_bytes = config.buffer.flush_bytes,
                flush_age_secs = config.buffer.flush_age_secs,
                "Resolved configuration"
            );

            // Share the runtime's single ScalingPressure engine (registered via
            // the `scaling_components` override below and served at
            // /scaling/pressure to KEDA). scalo 2.10 unified scaling onto this one
            // engine. If the scaling feature/section is off the runtime hands back
            // None; fall back to a standalone engine with the same components.
            let scaling = runtime
                .scaling
                .clone()
                .unwrap_or_else(|| Arc::new(config.scaling.build_pressure()));

            // The engine KEDA reads takes its gates from scalo's cascade, which
            // reads env but never the `--config` file, so a gate set only in
            // that file is reported here and ignored there (#160).
            if scaling.is_enabled() != config.scaling.enabled {
                warn!(
                    configured = config.scaling.enabled,
                    effective = scaling.is_enabled(),
                    "scaling.enabled in the config file does not reach the scaling engine, \
                     set DFE_LOADER_SCALING__ENABLED instead"
                );
            }

            // Register loader-specific metrics using runtime's MetricsManager
            let metrics = Metrics::new(&runtime.metrics);
            let server_state = Arc::new(ServerState::new(metrics.clone(), Arc::clone(&scaling)));

            let readiness_state = Arc::clone(&server_state);
            runtime.set_readiness_check(move || readiness_state.is_ready());

            // Create shared config for hot-reload
            let shared_config = SharedConfig::new(config.clone());

            // Take the runtime's self-regulation governor (default-on; None when
            // self_regulation.enabled = false). Moving it out of the runtime is
            // safe: the runtime already wired the byte-budget into the batch
            // engine at build time, and the loader attaches the Kafka
            // pause-partitions inbound gate itself via the orchestrator.
            let governor = runtime.governor.take();

            // Create orchestrator with hot-reload support and scaling pressure.
            // Inject the runtime's SHARED memory guard (the same one feeding the
            // governor and worker pool) so in-flight byte accounting drives the
            // inbound brake — never a stand-alone guard the pipeline ignores.
            // scalo 2.10 unified scaling onto ONE ScalingPressure engine (the
            // separate per-pod scaling-signal cell was removed): the orchestrator
            // pushes Kafka assigned-lag + ClickHouse sink circuit-open directly
            // into this `scaling` engine, which is served to KEDA at
            // /scaling/pressure.
            let mut orchestrator = Orchestrator::with_metrics(config.clone(), metrics)
                .with_shared_config(shared_config.clone())
                .with_scaling(Arc::clone(&scaling))
                .with_memory_guard(Arc::clone(&runtime.memory_guard))
                .with_governor(governor);

            // Use runtime worker pool if available
            if let Some(ref pool) = runtime.worker_pool {
                orchestrator = orchestrator.with_worker_pool(Arc::clone(pool));
            }

            // Use runtime batch engine if available (SIMD parse, pre-route, parallel transform)
            if let Some(ref engine) = runtime.batch_engine {
                orchestrator = orchestrator.with_batch_engine(Arc::clone(engine));
            }

            let shutdown_token = orchestrator.shutdown_token();

            // Connect runtime shutdown to orchestrator's shutdown token
            let runtime_shutdown = runtime.shutdown.clone();
            let orch_shutdown = shutdown_token.clone();
            tokio::spawn(async move {
                runtime_shutdown.cancelled().await;
                orch_shutdown.cancel();
            });

            // Wire worker pool to orchestrator's memory guard (single instance, shared state)
            if let Some(ref pool) = runtime.worker_pool {
                pool.set_memory_guard(Arc::clone(orchestrator.memory_guard()));
            }

            // Start config watcher if hot-reload is enabled
            if config.hot_reload.enabled {
                if let Some(config_path) = config_path.as_deref() {
                    let watcher_config = WatcherConfig {
                        config_path: PathBuf::from(config_path),
                        poll_interval: Duration::from_secs(config.hot_reload.poll_interval_secs),
                        debounce: Duration::from_millis(config.hot_reload.debounce_ms),
                        enabled: true,
                    };

                    match ConfigWatcher::new(watcher_config, shared_config.clone()) {
                        Ok(watcher) => {
                            let _handle = watcher.start();
                            info!(
                                path = config_path,
                                poll_secs = config.hot_reload.poll_interval_secs,
                                "Config hot-reload enabled"
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to start config watcher, hot-reload disabled");
                        }
                    }
                } else {
                    warn!(
                        "Hot-reload enabled but no config file path provided (--config), skipping"
                    );
                }
            }

            // Mark as ready
            server_state.set_ready(true);

            // Run the pipeline
            orchestrator
                .run()
                .await
                .map_err(|e| CliError::Service(e.to_string()))?;

            info!("Shutdown complete");
            Ok(())
        }
    }

    fn register_metrics(&self, manager: &scalo::metrics::MetricsManager) {
        // `metrics-manifest` and `generate-artefacts` read the registry without
        // starting the service, so the catalogue is empty until the loader's own
        // metrics are built against their manager (#158).
        let _ = Metrics::new(manager);
    }

    fn scaling_components(&self, config: &Self::Config) -> Vec<scalo::ScalingComponent> {
        // Register the loader's weighted KEDA components on the runtime's single
        // ScalingPressure -- the engine `/scaling/pressure` serves. The
        // orchestrator feeds it kafka_lag + circuit-open from its recv/flush loop.
        config.scaling.components()
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(Config::deployment_contract())
    }

    fn version_check_defaults(&self) -> scalo::version_check::VersionCheckConfig {
        // The runtime overlays the version_check cascade keys on this, so a
        // deployment's explicit enabled: false always wins.
        scalo::version_check::VersionCheckConfig {
            api_url: "https://releases.hyperi.io/api/v1/check".into(),
            ..Default::default()
        }
    }
}

/// Live heap bytes from jemalloc, for scalo's memory guard.
///
/// jemalloc caches its statistics, so the epoch advance is what refreshes them.
#[cfg(feature = "jemalloc")]
fn heap_allocated_bytes() -> usize {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0)
}

#[tokio::main]
async fn main() {
    // Until a heap source is registered the memory guard sees only the bytes
    // the batch engine reserved, so `memory_used_bytes` and the inbound brake
    // it feeds both read near zero (#162).
    #[cfg(feature = "jemalloc")]
    let _ = scalo::memory::set_heap_source(heap_allocated_bytes);

    let app = App::parse();

    if let Some(output) = &app.emit_helm {
        let contract = Config::deployment_contract();
        if let Err(e) = scalo::deployment::generate_chart(&contract, output, None) {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
        println!("Helm chart generated at {}", output.display());
        return;
    }

    if let Some(output) = &app.emit_dockerfile {
        let contract = Config::deployment_contract();
        let content = scalo::deployment::generate_dockerfile(&contract, None);
        if let Err(e) = std::fs::write(output, &content) {
            eprintln!("fatal: could not write Dockerfile: {e}");
            std::process::exit(1);
        }
        println!("Dockerfile generated at {}", output.display());
        return;
    }

    if let Err(e) = run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scalo cascade is a `OnceLock` and the env is process-global, so the
    /// seed is exercised once, in one test.
    #[test]
    fn the_scaling_gate_reaches_the_cascade_through_the_env() {
        // SAFETY: test-only env manipulation, set before the seed below and
        // removed after it.
        unsafe {
            std::env::set_var("DFE_LOADER_SCALING__ENABLED", "false");
            std::env::set_var("DFE_LOADER_SCALING__MEMORY_GATE_THRESHOLD", "0.55");
        }

        let app = App::parse_from(["dfe-loader"]);
        app.load_config(None).expect("default config loads");

        // Both values differ from the library defaults the reader falls back to.
        let gate = scalo::ScalingPressureConfig::from_cascade();
        assert!(
            !gate.enabled,
            "scaling.enabled must reach the engine KEDA reads"
        );
        assert!(
            (gate.memory_gate_threshold - 0.55).abs() < f64::EPSILON,
            "memory_gate_threshold reads {}",
            gate.memory_gate_threshold
        );

        unsafe {
            std::env::remove_var("DFE_LOADER_SCALING__ENABLED");
            std::env::remove_var("DFE_LOADER_SCALING__MEMORY_GATE_THRESHOLD");
        }
    }

    /// The registered heap source must move with the process heap, not with the
    /// batch engine's reservations.
    #[cfg(feature = "jemalloc")]
    #[test]
    fn the_heap_source_tracks_a_live_allocation() {
        let before = heap_allocated_bytes();
        let ballast: Vec<u8> = vec![7; 64 * 1024 * 1024];
        let after = heap_allocated_bytes();
        assert!(
            after >= before + 32 * 1024 * 1024,
            "heap source read {before} then {after} across a 64 MiB allocation"
        );
        drop(ballast);
    }
}
