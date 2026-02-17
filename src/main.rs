// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-loader

// =============================================================================
// Global Allocator Configuration
// =============================================================================
// Use jemalloc or mimalloc for better performance than system allocator.
// Enable with: cargo build --release --features jemalloc
//          or: cargo build --release --features mimalloc

// Compile-time guard: jemalloc and mimalloc are mutually exclusive
#[cfg(all(feature = "jemalloc", feature = "mimalloc"))]
compile_error!("Features 'jemalloc' and 'mimalloc' are mutually exclusive. Enable only one.");

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tracing::{error, info, warn};

use dfe_loader::config::Config;
use dfe_loader::metrics::{run_server, Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;

#[derive(Parser, Debug)]
#[command(name = "dfe-loader")]
#[command(about = "High-performance Kafka to ClickHouse data loader")]
#[command(version)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, env = "LOADER_CONFIG")]
    config: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOADER_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Log format (json, text)
    #[arg(long, env = "LOADER_LOG_FORMAT", default_value = "json")]
    log_format: String,

    /// Validate config and exit
    #[arg(long)]
    validate: bool,

    /// Print effective config and exit
    #[arg(long)]
    print_config: bool,

    /// Metrics server bind address
    #[arg(long, env = "LOADER_METRICS_ADDR", default_value = "0.0.0.0:9090")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing based on format
    if args.log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(&args.log_level)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(&args.log_level)
            .init();
    }

    // Load configuration
    let config = match Config::load(args.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to load configuration");
            std::process::exit(1);
        }
    };

    // Validate configuration
    if let Err(e) = config.validate() {
        error!(error = %e, "Configuration validation failed");
        std::process::exit(1);
    }

    // Print config and exit if requested
    if args.print_config {
        println!("{:#?}", config);
        return Ok(());
    }

    // Validate only mode
    if args.validate {
        info!("Configuration is valid");
        return Ok(());
    }

    info!(
        kafka_brokers = ?config.kafka.brokers,
        clickhouse_hosts = ?config.clickhouse.hosts,
        payload_format = %config.payload.format,
        "Starting dfe-loader"
    );

    // Initialize metrics
    let metrics = Metrics::new();
    let server_state = Arc::new(ServerState::new(metrics.clone()));

    // Create orchestrator
    let mut orchestrator = Orchestrator::with_metrics(config, metrics);
    let shutdown_token = orchestrator.shutdown_token();

    // Start metrics server
    let metrics_addr: SocketAddr = args.metrics_addr.parse().unwrap_or_else(|_| {
        warn!(addr = %args.metrics_addr, "Invalid metrics address, using default");
        "0.0.0.0:9090".parse().unwrap()
    });

    let metrics_shutdown = shutdown_token.clone();
    let metrics_state = server_state.clone();
    tokio::spawn(async move {
        if let Err(e) = run_server(metrics_addr, metrics_state, metrics_shutdown).await {
            error!(error = %e, "Metrics server error");
        }
    });

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
    if let Err(e) = orchestrator.run().await {
        error!(error = %e, "Pipeline error");
        std::process::exit(1);
    }

    info!("Shutdown complete");
    Ok(())
}
