//! Configuration loading and hot-reload
//!
//! ## Hot-Reload Support
//!
//! The config module supports hot-reloading via polling, which works
//! reliably on all filesystem types including S3, NFS, and FUSE mounts.
//!
//! ```ignore
//! use dfe_loader::config::{Config, SharedConfig, ConfigWatcher, WatcherConfig};
//!
//! let config = Config::load(Some("config.yaml"))?;
//! let shared = SharedConfig::new(config);
//!
//! // Start watching for changes
//! let watcher = ConfigWatcher::new(WatcherConfig::default(), shared.clone())?;
//! let _handle = watcher.start();
//!
//! // Components can subscribe to changes
//! let mut rx = shared.subscribe();
//! ```

pub mod loader;
pub mod shared;
pub mod watcher;

pub use loader::{
    Config, KafkaConfig, SaslConfig, SaslMechanism, TlsConfig, ClickHouseConfig, PayloadConfig,
    RoutingConfig, DlqConfig, BufferConfig, MemoryConfig, MetricsConfig,
    LoggingConfig, TimestampDqConfig, FieldSanitizationConfig, MetadataConfig,
    CoercionConfig, NullHandling, SchemaConfig,
};
pub use shared::SharedConfig;
pub use watcher::{ConfigWatcher, WatcherConfig};
