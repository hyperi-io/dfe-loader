// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared configuration with hot-reload support
//!
//! Re-exports `hyperi_rustlib::config::shared::SharedConfig<Config>` as
//! `SharedConfig` for backward compatibility. All DFE components share
//! the same generic abstraction from rustlib.

use super::Config;

/// Thread-safe shared configuration with hot-reload support.
///
/// This is a type alias for the generic `SharedConfig<T>` from rustlib,
/// specialised to dfe-loader's `Config` struct.
///
/// ## Usage
///
/// ```ignore
/// use dfe_loader::config::{Config, SharedConfig};
///
/// let shared = SharedConfig::new(config);
///
/// // Read config (zero-copy via read guard)
/// let cfg = shared.read();
/// println!("Buffer size: {}", cfg.buffer.flush_rows);
///
/// // Subscribe to changes
/// let mut rx = shared.subscribe();
/// tokio::spawn(async move {
///     while rx.changed().await.is_ok() {
///         println!("Config reloaded! Version: {}", *rx.borrow());
///     }
/// });
///
/// // Update config (from reloader)
/// shared.update(new_config);
/// ```
pub type SharedConfig = hyperi_rustlib::config::shared::SharedConfig<Config>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_config_read() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        let cfg = shared.read();
        assert!(!cfg.kafka.brokers.is_empty());
    }

    #[test]
    fn test_shared_config_update() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        assert_eq!(shared.version(), 0);

        // Update config
        let mut new_config = Config::default();
        new_config.kafka.group = "new-group".to_string();
        shared.update(new_config);

        assert_eq!(shared.version(), 1);
        assert_eq!(shared.read().kafka.group, "new-group");
    }

    #[tokio::test]
    async fn test_shared_config_subscribe() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        let mut rx = shared.subscribe();

        // Initial value
        assert_eq!(*rx.borrow(), 0);

        // Update config
        let new_config = Config::default();
        shared.update(new_config);

        // Wait for notification
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 1);
    }

    #[test]
    fn test_shared_config_clone() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let shared2 = shared.clone();

        // Update through one, read from other
        let mut new_config = Config::default();
        new_config.kafka.group = "cloned-update".to_string();
        shared.update(new_config);

        assert_eq!(shared2.read().kafka.group, "cloned-update");
        assert_eq!(shared2.version(), 1);
    }

    #[test]
    fn test_shared_config_get() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // get() clones the config (available from rustlib generic)
        let cfg = shared.get();
        assert!(!cfg.kafka.brokers.is_empty());
    }

    #[test]
    fn test_shared_config_with() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // with() closure-based access (available from rustlib generic)
        let rows = shared.with(|c| c.buffer.flush_rows);
        assert!(rows > 0);
    }
}
