//! Shared configuration with hot-reload support
//!
//! Provides thread-safe access to configuration with support for runtime updates.
//! Components can subscribe to config changes via a watch channel.
//!
//! ## Usage
//!
//! ```ignore
//! let shared = SharedConfig::new(config);
//!
//! // Read config
//! let cfg = shared.read();
//! println!("Buffer size: {}", cfg.buffer.batch_size);
//!
//! // Subscribe to changes
//! let mut rx = shared.subscribe();
//! tokio::spawn(async move {
//!     while rx.changed().await.is_ok() {
//!         println!("Config reloaded! Version: {}", *rx.borrow());
//!     }
//! });
//!
//! // Update config (from watcher)
//! shared.update(new_config);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::watch;

use super::Config;

/// Thread-safe shared configuration with hot-reload support
pub struct SharedConfig {
    /// The current configuration
    inner: Arc<RwLock<Config>>,
    /// Monotonic version counter (incremented on each update)
    version: Arc<AtomicU64>,
    /// Watch channel for notifying subscribers of updates
    watch_tx: watch::Sender<u64>,
    /// Receiver for subscribers
    watch_rx: watch::Receiver<u64>,
}

impl SharedConfig {
    /// Create a new shared config from an initial configuration
    pub fn new(config: Config) -> Self {
        let (watch_tx, watch_rx) = watch::channel(0);

        Self {
            inner: Arc::new(RwLock::new(config)),
            version: Arc::new(AtomicU64::new(0)),
            watch_tx,
            watch_rx,
        }
    }

    /// Read the current configuration
    ///
    /// Returns a read guard that releases the lock when dropped.
    #[inline]
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Config> {
        self.inner.read()
    }

    /// Get the current config version
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Update the configuration atomically
    ///
    /// This will:
    /// 1. Acquire write lock
    /// 2. Replace the config
    /// 3. Increment version
    /// 4. Notify all subscribers
    pub fn update(&self, new_config: Config) {
        // Update under write lock
        {
            let mut guard = self.inner.write();
            *guard = new_config;
        }

        // Increment version and notify
        let new_version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = self.watch_tx.send(new_version);
    }

    /// Subscribe to configuration changes
    ///
    /// Returns a receiver that will be notified when config changes.
    /// The receiver yields the new version number on each change.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_rx.clone()
    }

    /// Clone the Arc for sharing across threads
    pub fn clone_inner(&self) -> Arc<RwLock<Config>> {
        self.inner.clone()
    }
}

impl Clone for SharedConfig {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            version: self.version.clone(),
            watch_tx: self.watch_tx.clone(),
            watch_rx: self.watch_rx.clone(),
        }
    }
}

impl Default for SharedConfig {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

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
}
