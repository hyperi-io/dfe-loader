// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration hot-reload using scalo's `ConfigReloader<T>`
//!
//! Wraps the generic `ConfigReloader` from `scalo` with
//! dfe-loader-specific config loading and validation logic.
//!
//! ## Features
//!
//! - **File polling**: Detects config file changes via mtime (works on S3/NFS/FUSE)
//! - **SIGHUP support**: Standard daemon reload signal (Unix only)
//! - **Debouncing**: Avoids multiple reloads for rapid file changes
//! - **Validation**: Validates config before applying changes
//!
//! ## Usage
//!
//! ```ignore
//! use dfe_loader::config::{Config, SharedConfig, ConfigWatcher, WatcherConfig};
//!
//! let config = Config::load(Some("config.yaml"))?;
//! let shared = SharedConfig::new(config);
//!
//! let watcher_config = WatcherConfig {
//!     config_path: PathBuf::from("config.yaml"),
//!     poll_interval: Duration::from_secs(5),
//!     debounce: Duration::from_millis(500),
//!     enabled: true,
//! };
//!
//! let watcher = ConfigWatcher::new(watcher_config, shared)?;
//! let _handle = watcher.start();
//! ```

use std::path::PathBuf;
use std::time::Duration;

use scalo::config::reloader::{ConfigReloader, ReloaderConfig};
use tokio::task::JoinHandle;

use super::shared::SharedConfig;
use super::{Config, DlqLoadConfig};
use crate::Result;

/// Configuration for the watcher
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Path to config file to watch
    pub config_path: PathBuf,
    /// Polling interval for checking file changes (for S3/NFS/FUSE mounts)
    pub poll_interval: Duration,
    /// Debounce duration - minimum time between reloads
    pub debounce: Duration,
    /// Whether hot-reload is enabled
    pub enabled: bool,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            config_path: PathBuf::from("config.yaml"),
            poll_interval: Duration::from_secs(5),
            debounce: Duration::from_millis(500),
            enabled: false,
        }
    }
}

/// Configuration file watcher with polling and SIGHUP support.
///
/// Wraps scalo's `ConfigReloader<Config>` with dfe-loader-specific
/// loading and validation logic. Works on all filesystem types
/// including S3, NFS, and FUSE mounts.
pub struct ConfigWatcher {
    reloader: ConfigReloader<Config>,
}

impl ConfigWatcher {
    /// Create a new config watcher
    pub fn new(config: WatcherConfig, shared_config: SharedConfig) -> Result<Self> {
        // Validate path exists
        if !config.config_path.exists() {
            return Err(crate::Error::Config(format!(
                "Config file not found: {}",
                config.config_path.display()
            )));
        }

        let config_path_str = config.config_path.to_string_lossy().to_string();
        let live_dlq_load = shared_config.read().dlq_load.clone();

        let reloader_config = ReloaderConfig {
            config_path: Some(config.config_path),
            poll_interval: config.poll_interval,
            periodic_interval: Duration::ZERO, // disabled — file polling is sufficient
            debounce: config.debounce,
            enable_sighup: true, // bonus: also reload on SIGHUP
        };

        let reloader = ConfigReloader::new(
            reloader_config,
            shared_config,
            move || {
                Config::load(Some(&config_path_str))
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
            move |cfg| {
                validate_config(cfg, &live_dlq_load)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        );

        Ok(Self { reloader })
    }

    /// Create with default settings
    pub fn with_defaults(config_path: PathBuf, shared_config: SharedConfig) -> Result<Self> {
        Self::new(
            WatcherConfig {
                config_path,
                enabled: true,
                ..Default::default()
            },
            shared_config,
        )
    }

    /// Start watching the config file in the background
    ///
    /// Returns a `JoinHandle` that can be used to abort the watcher.
    pub fn start(self) -> JoinHandle<()> {
        self.reloader.start()
    }
}

/// Validate a config before applying.
///
/// Delegates to [`Config::validate`]: a reload judged by its own rule set
/// refuses a config startup accepted, and the loader then runs on the stale
/// one with only a log line to say so. `live` is the `dlq_load` the process
/// started with, which a reload does not change.
fn validate_config(config: &Config, live: &DlqLoadConfig) -> Result<()> {
    config.validate()?;
    config.refuse_dead_letter_default(live)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_config(dir: &TempDir) -> PathBuf {
        let config_path = dir.path().join("config.yaml");
        let config_content = r"
kafka:
  brokers:
    - localhost:9092
  group: test-group
  topics:
    - test-topic

clickhouse:
  hosts:
    - localhost:9000

buffer:
  flush_rows: 1000
";
        fs::write(&config_path, config_content).unwrap();
        config_path
    }

    #[test]
    fn test_watcher_creation() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(&dir);
        let shared = SharedConfig::new(Config::default());

        let watcher = ConfigWatcher::with_defaults(config_path, shared);
        assert!(watcher.is_ok());
    }

    #[test]
    fn test_watcher_nonexistent_path() {
        let shared = SharedConfig::new(Config::default());
        let result =
            ConfigWatcher::with_defaults(PathBuf::from("/nonexistent/config.yaml"), shared);
        assert!(result.is_err());
    }

    /// Validate `config` as a reload does, in a process started on the shipped
    /// `dlq_load`.
    fn reload(config: &Config) -> Result<()> {
        validate_config(config, &DlqLoadConfig::default())
    }

    #[test]
    fn a_reload_never_makes_the_dlq_table_the_default() {
        // (default table, dlq_load.table in the reloaded file, loading at
        //  startup, refused)
        let cases = [
            ("main", "dlq", true, false),
            ("dlq", "dlq", true, true),
            // dlq_load changes on restart, so the process still loads into dlq.
            ("dlq", "dead", true, true),
            ("main", "main", true, true),
            ("dlq", "dead", false, false),
        ];
        for (default_table, file_table, loading, refused) in cases {
            let mut config = Config::default();
            config.routing.default_table = default_table.into();
            config.dlq_load.table = file_table.into();
            let mut live = DlqLoadConfig::default();
            if !loading {
                live.topics.clear();
            }
            let label = format!("default {default_table}, file {file_table}, loading {loading}");
            match (validate_config(&config, &live), refused) {
                (Ok(()), false) => {}
                (Err(e), true) => assert!(
                    e.to_string().contains("routing.default_table"),
                    "{label}: {e}"
                ),
                (other, _) => panic!("{label}: unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn test_validate_config_valid() {
        let config = Config::default();
        assert!(reload(&config).is_ok());
    }

    #[test]
    fn test_validate_config_empty_brokers() {
        let mut config = Config::default();
        config.kafka.brokers = vec![];
        assert!(reload(&config).is_err());
    }

    #[test]
    fn test_validate_config_empty_clickhouse_hosts() {
        let mut config = Config::default();
        config.clickhouse.hosts = vec![];
        assert!(reload(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_flush_rows() {
        let mut config = Config::default();
        config.buffer.flush_rows = 0;
        assert!(reload(&config).is_err());
    }

    /// A brokerless profile gives the loader no broker address, and a reload
    /// must accept that as readily as startup does: rejected, the loader keeps
    /// serving the config it booted with.
    #[test]
    fn test_validate_config_grpc_transport_needs_no_broker() {
        let mut config = Config::default();
        config.transport = crate::config::loader::TRANSPORT_GRPC.to_string();
        config.kafka.brokers = vec![];
        config.grpc.listen = Some("0.0.0.0:6000".to_string());
        assert!(config.is_direct());
        reload(&config).expect("a reload on the direct transport reaches no broker");
    }
}
