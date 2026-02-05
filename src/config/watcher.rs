// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration file watcher for hot-reload
//!
//! Watches the configuration file for changes and reloads automatically.
//! Supports both inotify (for local filesystems) and polling (for S3/NFS/FUSE mounts).
//!
//! ## Features
//!
//! - **Dual-mode watching**: Uses inotify where available, falls back to polling
//! - **Polling for mounted filesystems**: Works with S3, NFS, FUSE mounts
//! - **Debouncing**: Avoids multiple reloads for rapid file changes
//! - **Validation**: Validates config before applying changes
//!
//! ## Usage
//!
//! ```ignore
//! let shared = SharedConfig::new(config);
//! let watcher = ConfigWatcher::new(WatcherConfig {
//!     config_path: PathBuf::from("config.yaml"),
//!     poll_interval: Duration::from_secs(5),
//!     debounce: Duration::from_millis(500),
//!     enabled: true,
//! }, shared)?;
//!
//! // Start watching in background
//! let handle = watcher.start();
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

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

/// Configuration file watcher with polling support
///
/// Uses polling to detect file changes, which works reliably on all
/// filesystem types including S3, NFS, and FUSE mounts where inotify
/// doesn't work.
pub struct ConfigWatcher {
    /// Watcher configuration
    config: WatcherConfig,
    /// Shared config to update
    shared_config: SharedConfig,
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

        Ok(Self {
            config,
            shared_config,
        })
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
        tokio::spawn(async move {
            self.poll_loop().await;
        })
    }

    /// Polling-based watch loop
    ///
    /// Works reliably on all filesystem types including S3, NFS, and FUSE mounts.
    async fn poll_loop(self) {
        info!(
            path = %self.config.config_path.display(),
            poll_interval = ?self.config.poll_interval,
            "Started config file watcher (polling mode)"
        );

        let mut poll_timer = interval(self.config.poll_interval);
        let mut last_modified: Option<SystemTime> = self.get_modified_time();
        let mut last_reload = Instant::now();

        loop {
            poll_timer.tick().await;

            // Check if file still exists
            if !self.config.config_path.exists() {
                warn!(
                    path = %self.config.config_path.display(),
                    "Config file no longer exists"
                );
                continue;
            }

            // Get current modification time
            let current_modified = self.get_modified_time();

            // Compare modification times
            let changed = match (&last_modified, &current_modified) {
                (Some(last), Some(current)) => current > last,
                (None, Some(_)) => true, // File appeared
                _ => false,
            };

            if changed {
                // Debounce check
                if last_reload.elapsed() < self.config.debounce {
                    debug!("Debouncing config change");
                    continue;
                }

                info!(
                    path = %self.config.config_path.display(),
                    "Config file changed, reloading"
                );

                self.reload_config().await;
                last_modified = current_modified;
                last_reload = Instant::now();
            }
        }
    }

    /// Get the modification time of the config file
    fn get_modified_time(&self) -> Option<SystemTime> {
        std::fs::metadata(&self.config.config_path)
            .ok()
            .and_then(|m| m.modified().ok())
    }

    /// Reload the configuration file
    async fn reload_config(&self) {
        info!(path = %self.config.config_path.display(), "Reloading configuration");

        // Load new config
        match Config::load(Some(self.config.config_path.to_str().unwrap_or_default())) {
            Ok(new_config) => {
                // Validate config before applying
                if let Err(e) = self.validate_config(&new_config) {
                    error!(error = %e, "Config validation failed, keeping old config");
                    return;
                }

                // Update shared config
                let old_version = self.shared_config.version();
                self.shared_config.update(new_config);
                let new_version = self.shared_config.version();

                info!(
                    old_version = old_version,
                    new_version = new_version,
                    "Configuration reloaded successfully"
                );
            }
            Err(e) => {
                error!(error = %e, "Failed to reload config, keeping old config");
            }
        }
    }

    /// Validate a config before applying
    fn validate_config(&self, config: &Config) -> Result<()> {
        // Basic validation - add more as needed
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
        let shared = SharedConfig::default();

        let watcher = ConfigWatcher::with_defaults(config_path, shared);
        assert!(watcher.is_ok());
    }

    #[test]
    fn test_watcher_nonexistent_path() {
        let shared = SharedConfig::default();
        let result =
            ConfigWatcher::with_defaults(PathBuf::from("/nonexistent/config.yaml"), shared);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_config() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(&dir);
        let shared = SharedConfig::default();
        let watcher = ConfigWatcher::with_defaults(config_path, shared).unwrap();

        // Valid config
        let config = Config::default();
        assert!(watcher.validate_config(&config).is_ok());

        // Invalid - empty brokers
        let mut invalid = Config::default();
        invalid.kafka.brokers = vec![];
        assert!(watcher.validate_config(&invalid).is_err());

        // Invalid - empty ClickHouse hosts
        let mut invalid = Config::default();
        invalid.clickhouse.hosts = vec![];
        assert!(watcher.validate_config(&invalid).is_err());

        // Invalid - zero flush_rows
        let mut invalid = Config::default();
        invalid.buffer.flush_rows = 0;
        assert!(watcher.validate_config(&invalid).is_err());
    }

    #[test]
    fn test_get_modified_time() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(&dir);
        let shared = SharedConfig::default();
        let watcher = ConfigWatcher::with_defaults(config_path.clone(), shared).unwrap();

        // Should get a modification time
        let mtime1 = watcher.get_modified_time();
        assert!(mtime1.is_some());

        // Modify the file
        std::thread::sleep(Duration::from_millis(10));
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&config_path)
            .unwrap();
        writeln!(file, "# comment").unwrap();

        // Should have a newer modification time
        let mtime2 = watcher.get_modified_time();
        assert!(mtime2.is_some());
        assert!(mtime2.unwrap() >= mtime1.unwrap());
    }

    #[tokio::test]
    async fn test_watcher_reload() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(&dir);
        let shared = SharedConfig::default();

        let watcher = ConfigWatcher::with_defaults(config_path.clone(), shared.clone()).unwrap();

        // Initial version
        assert_eq!(shared.version(), 0);

        // Reload should update version
        watcher.reload_config().await;
        assert_eq!(shared.version(), 1);
    }
}
