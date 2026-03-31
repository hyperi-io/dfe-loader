// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-loader

// =============================================================================
// Global Allocator Configuration
// =============================================================================
// Use jemalloc or mimalloc for better performance than system allocator.
// Enable with: cargo build --release --features jemalloc
//          or: cargo build --release --features mimalloc

// When both are enabled (e.g. --all-features in CI), jemalloc wins
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::signal;
use tracing::{error, info, warn};

use dfe_loader::config::{Config, ConfigWatcher, SharedConfig, WatcherConfig};
use dfe_loader::metrics::{Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo, run_app};
use hyperi_rustlib::metrics::MetricsManager;

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

impl DfeApp for App {
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

    async fn run_service(&self, config: Self::Config) -> Result<(), CliError> {
        // Fire-and-forget version check
        hyperi_rustlib::VersionCheck::new(hyperi_rustlib::VersionCheckConfig {
            product: "dfe-loader".into(),
            current_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        })
        .check_on_startup();

        info!(
            version = env!("CARGO_PKG_VERSION"),
            kafka_brokers = ?config.kafka.brokers,
            clickhouse_hosts = ?config.clickhouse.hosts,
            payload_format = %config.payload.format,
            "Starting dfe-loader"
        );

        // Initialise metrics via rustlib MetricsManager
        let mut manager = MetricsManager::new("dfe_loader");

        // Wire readiness check — MetricsManager serves /readyz
        let scaling = Arc::new(config.scaling.build_pressure());
        let metrics = Metrics::new(&manager);
        let server_state = Arc::new(ServerState::new(metrics.clone(), Arc::clone(&scaling)));

        let readiness_state = Arc::clone(&server_state);
        manager.set_readiness_check(move || readiness_state.is_ready());

        // Start metrics server (serves /metrics, /healthz, /readyz)
        let metrics_addr = self.common_args().metrics_addr.as_str();
        if let Err(e) = manager.start_server(metrics_addr).await {
            error!(error = %e, addr = metrics_addr, "Failed to start metrics server");
        }

        // Create shared config for hot-reload
        let shared_config = SharedConfig::new(config.clone());

        // Create adaptive worker pool for parallel message processing
        let worker_pool = match hyperi_rustlib::worker::AdaptiveWorkerPool::from_cascade(
            "worker_pool",
        ) {
            Ok(pool) => {
                let pool = Arc::new(pool);
                pool.register_metrics(&manager);
                pool.set_memory_guard(Arc::clone(&Arc::new(
                    hyperi_rustlib::memory::MemoryGuard::new(
                        hyperi_rustlib::memory::MemoryGuardConfig::from_env("DFE_LOADER"),
                    ),
                )));
                pool.set_scaling_pressure(Arc::clone(&scaling));
                info!(
                    max_threads = pool.max_threads(),
                    "Adaptive worker pool enabled"
                );
                Some(pool)
            }
            Err(e) => {
                warn!(error = %e, "Worker pool not configured, falling back to sequential processing");
                None
            }
        };

        // Create orchestrator with hot-reload support and scaling pressure
        let mut orchestrator = Orchestrator::with_metrics(config.clone(), metrics)
            .with_shared_config(shared_config.clone())
            .with_scaling(Arc::clone(&scaling));

        if let Some(ref pool) = worker_pool {
            orchestrator = orchestrator.with_worker_pool(Arc::clone(pool));
        }

        let shutdown_token = orchestrator.shutdown_token();

        // Start worker pool scaling loop (if pool exists)
        if let Some(ref pool) = worker_pool {
            pool.start_scaling_loop(shutdown_token.clone());
        }

        // Start config watcher if hot-reload is enabled
        if config.hot_reload.enabled {
            if let Some(config_path) = self.common_args().config.as_deref() {
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
                warn!("Hot-reload enabled but no config file path provided (--config), skipping");
            }
        }

        // Spawn signal handler
        let signal_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received SIGINT, initiating shutdown");
                    signal_shutdown.cancel();
                }
                Err(e) => {
                    warn!(error = %e, "Failed to listen for SIGINT");
                }
            }
        });

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

#[tokio::main]
async fn main() {
    let app = App::parse();

    if let Some(output) = &app.emit_helm {
        let contract = Config::deployment_contract();
        if let Err(e) = hyperi_rustlib::deployment::generate_chart(&contract, output) {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
        println!("Helm chart generated at {}", output.display());
        return;
    }

    if let Some(output) = &app.emit_dockerfile {
        let contract = Config::deployment_contract();
        let content = hyperi_rustlib::deployment::generate_dockerfile(&contract);
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
