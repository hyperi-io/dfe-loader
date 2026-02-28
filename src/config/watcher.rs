// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration hot-reload using rustlib's `ConfigReloader<T>`
//!
//! Wraps the generic `ConfigReloader` from `hyperi-rustlib` with
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

use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use tokio::task::JoinHandle;

use super::shared::SharedConfig;
use super::Config;
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
/// Wraps rustlib's `ConfigReloader<Config>` with dfe-loader-specific
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
            |cfg| {
                validate_config(cfg)
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
    /// Returns a JoinHandle that can be used to abort the watcher.
    pub fn start(self) -> JoinHandle<()> {
        self.reloader.start()
    }
}

/// Validate a config before applying
fn validate_config(config: &Config) -> Result<()> {
    if config.kafka.brokers.is_empty() {
        return Err(crate::Error::Config("Kafka brokers cannot be empty".into()));
    }

    if config.clickhouse.hosts.is_empty() {
        return Err(crate::Error::Config(
            "ClickHouse hosts cannot be empty".into(),
        ));
    }

    if config.buffer.flush_rows == 0 {
        return Err(crate::Error::Config("Buffer flush_rows must be > 0".into()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_config(dir: &TempDir) -> PathBuf {
        let config_path = dir.path().join("config.yaml");
        let config_content = r#"
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
"#;
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

    #[test]
    fn test_validate_config_valid() {
        let config = Config::default();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_empty_brokers() {
        let mut config = Config::default();
        config.kafka.brokers = vec![];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_empty_clickhouse_hosts() {
        let mut config = Config::default();
        config.clickhouse.hosts = vec![];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_flush_rows() {
        let mut config = Config::default();
        config.buffer.flush_rows = 0;
        assert!(validate_config(&config).is_err());
    }
}
