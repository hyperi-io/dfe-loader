//! CLI entry point for dfe-loader-clickhouse

use clap::Parser;
use tokio::signal;
use tracing::{error, info, warn};

use dfe_loader_clickhouse::config::Config;
use dfe_loader_clickhouse::pipeline::Orchestrator;

#[derive(Parser, Debug)]
#[command(name = "loader")]
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
        "Starting dfe-loader-clickhouse"
    );

    // Create orchestrator
    let mut orchestrator = Orchestrator::new(config);
    let shutdown_token = orchestrator.shutdown_token();

    // Spawn signal handler
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Received SIGINT, initiating shutdown");
                shutdown_token.cancel();
            }
            Err(e) => {
                warn!(error = %e, "Failed to listen for SIGINT");
            }
        }
    });

    // Run the pipeline
    if let Err(e) = orchestrator.run().await {
        error!(error = %e, "Pipeline error");
        std::process::exit(1);
    }

    info!("Shutdown complete");
    Ok(())
}
