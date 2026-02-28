// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration loading and hot-reload
//!
//! ## Hot-Reload Support
//!
//! The config module supports hot-reloading via file polling and SIGHUP,
//! using the generic `SharedConfig<T>` and `ConfigReloader<T>` from
//! `hyperi-rustlib`. File polling works reliably on all filesystem types
//! including S3, NFS, and FUSE mounts.
//!
//! ```ignore
//! use dfe_loader::config::{Config, SharedConfig, ConfigWatcher, WatcherConfig};
//!
//! let config = Config::load(Some("config.yaml"))?;
//! let shared = SharedConfig::new(config);
//!
//! // Start watching for changes (file polling + SIGHUP)
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
    AutoInitConfig, BufferConfig, ClickHouseConfig, CoercionConfig, Config, DlqConfig,
    FieldMappingConfig, FieldMappingOverride, FieldSanitizationConfig, HotReloadConfig,
    KafkaConfig, LoggingConfig, MemoryConfig, MetadataConfig, MetricsConfig, NullHandling,
    PayloadConfig, ProfilesConfig, RoutingConfig, SaslConfig, SaslMechanism, SchemaConfig,
    TableCaptureConfig, TimestampDqConfig, TlsConfig, ZenohConfig,
};
pub use shared::SharedConfig;
pub use watcher::{ConfigWatcher, WatcherConfig};
