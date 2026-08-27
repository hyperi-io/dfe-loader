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
        let config_path = self.common_args().config.as_deref().map(String::from);

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

            // Fire-and-forget startup version check; no-op unless the cascade
            // sets version_check.enabled + api_url.
            {
                use scalo::version_check::{VersionCheck, VersionCheckConfig};
                let checker = VersionCheck::new(VersionCheckConfig::from_cascade(
                    "dfe-loader",
                    env!("CARGO_PKG_VERSION"),
                ));
                checker.check_on_startup();
            }

            // Share the runtime's single ScalingPressure engine (registered via
            // the `scaling_components` override below and served at
            // /scaling/pressure to KEDA). scalo 2.10 unified scaling onto this one
            // engine. If the scaling feature/section is off the runtime hands back
            // None; fall back to a standalone engine with the same components.
            let scaling = runtime
                .scaling
                .clone()
                .unwrap_or_else(|| Arc::new(config.scaling.build_pressure()));

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

    fn scaling_components(&self, config: &Self::Config) -> Vec<scalo::ScalingComponent> {
        // Register the loader's weighted KEDA components on the runtime's single
        // ScalingPressure -- the engine `/scaling/pressure` serves. The
        // orchestrator feeds it kafka_lag + circuit-open from its recv/flush loop.
        config.scaling.components()
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(Config::deployment_contract())
    }
}

#[tokio::main]
async fn main() {
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
