// Project:   dfe-loader
// File:      schema.rs
// Purpose:   Schema caching for ClickHouse tables
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! Schema caching for ClickHouse tables
//!
//! Provides TTL-based caching of table schemas to avoid repeated
//! system.columns queries. Following the Go pattern:
//! - Fetch on first access or after TTL expiry
//! - Manual invalidation on insert errors
//! - Automatic invalidation on schema mismatch errors
//! - Optional background refresh task
//! - Thread-safe via parking_lot RwLock

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tracing::{debug, error, info, warn};

use crate::clickhouse::{ArrowClickHouseClient, TableSchema};

/// Cached schema with timestamp and metadata
struct CachedSchema {
    schema: TableSchema,
    cached_at: Instant,
    /// Number of times this schema has been refreshed
    refresh_count: u32,
}

/// Configuration for schema cache
#[derive(Debug, Clone)]
pub struct SchemaCacheConfig {
    /// TTL for cached schemas (default: 300 seconds)
    pub ttl_secs: u64,
    /// Enable automatic refresh of expiring schemas
    pub auto_refresh: bool,
    /// Refresh interval for background task (default: 60 seconds)
    pub refresh_interval_secs: u64,
    /// Refresh schemas before they expire (headroom in seconds)
    pub refresh_headroom_secs: u64,
}

impl Default for SchemaCacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 300,
            auto_refresh: false,
            refresh_interval_secs: 60,
            refresh_headroom_secs: 30,
        }
    }
}

/// Thread-safe schema cache with TTL-based expiry
///
/// Caches table schemas to avoid repeated queries to system.columns.
/// Schemas are automatically refreshed after TTL expiry.
pub struct SchemaCache {
    schemas: RwLock<FxHashMap<String, CachedSchema>>,
    config: SchemaCacheConfig,
    ttl: Duration,
    /// Metrics
    hits: AtomicU64,
    misses: AtomicU64,
    refreshes: AtomicU64,
    invalidations: AtomicU64,
    /// Shutdown flag for background task
    shutdown: AtomicBool,
}

impl SchemaCache {
    /// Create a new schema cache with default config
    ///
    /// # Arguments
    /// * `ttl_secs` - Time-to-live in seconds for cached schemas (default: 300 = 5 min)
    pub fn new(ttl_secs: u64) -> Self {
        let config = SchemaCacheConfig {
            ttl_secs,
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Create a schema cache with full configuration
    pub fn with_config(config: SchemaCacheConfig) -> Self {
        Self {
            schemas: RwLock::new(FxHashMap::default()),
            ttl: Duration::from_secs(config.ttl_secs),
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            refreshes: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Get cached schema if valid (not expired)
    pub fn get(&self, table: &str) -> Option<TableSchema> {
        let schemas = self.schemas.read();
        schemas.get(table).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(cached.schema.clone())
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        })
    }

    /// Check if schema needs refresh (expired or about to expire)
    pub fn needs_refresh(&self, table: &str) -> bool {
        let schemas = self.schemas.read();
        match schemas.get(table) {
            None => true,
            Some(cached) => {
                let elapsed = cached.cached_at.elapsed();
                let headroom = Duration::from_secs(self.config.refresh_headroom_secs);
                elapsed + headroom >= self.ttl
            }
        }
    }

    /// Insert or update a schema in the cache
    pub fn insert(&self, table: String, schema: TableSchema) {
        let mut schemas = self.schemas.write();
        let refresh_count = schemas
            .get(&table)
            .map(|c| c.refresh_count + 1)
            .unwrap_or(0);

        schemas.insert(
            table.clone(),
            CachedSchema {
                schema,
                cached_at: Instant::now(),
                refresh_count,
            },
        );

        if refresh_count > 0 {
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            debug!(table = %table, refresh_count = refresh_count, "Schema refreshed");
        }
    }

    /// Invalidate (remove) a schema from the cache
    ///
    /// Call this after insert errors to force a refresh on next access.
    pub fn invalidate(&self, table: &str) {
        let mut schemas = self.schemas.write();
        if schemas.remove(table).is_some() {
            self.invalidations.fetch_add(1, Ordering::Relaxed);
            debug!(table = %table, "Schema cache invalidated");
        }
    }

    /// Invalidate schema if error looks like a schema mismatch
    ///
    /// Returns true if the error was recognized as a schema mismatch.
    pub fn invalidate_on_schema_error(&self, table: &str, error: &str) -> bool {
        // Common ClickHouse schema mismatch error patterns
        let schema_error_patterns = [
            "Unknown column",
            "Missing columns",
            "Type mismatch",
            "Cannot insert",
            "Column types don't match",
            "expected column",
            "wrong number of columns",
        ];

        let is_schema_error = schema_error_patterns.iter().any(|p| error.contains(p));

        if is_schema_error {
            warn!(
                table = %table,
                error = %error,
                "Schema mismatch detected, invalidating cache"
            );
            self.invalidate(table);
            true
        } else {
            false
        }
    }

    /// Invalidate all cached schemas
    pub fn invalidate_all(&self) {
        let mut schemas = self.schemas.write();
        let count = schemas.len();
        schemas.clear();
        if count > 0 {
            self.invalidations
                .fetch_add(count as u64, Ordering::Relaxed);
            info!(count = count, "All schema caches invalidated");
        }
    }

    /// Get all cached table names
    pub fn cached_tables(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    /// Get tables that need refresh (expired or expiring soon)
    pub fn tables_needing_refresh(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        let headroom = Duration::from_secs(self.config.refresh_headroom_secs);

        schemas
            .iter()
            .filter(|(_, cached)| cached.cached_at.elapsed() + headroom >= self.ttl)
            .map(|(table, _)| table.clone())
            .collect()
    }

    /// Get cache statistics
    pub fn stats(&self) -> SchemaCacheStats {
        let schemas = self.schemas.read();
        let mut valid = 0;
        let mut expired = 0;

        for cached in schemas.values() {
            if cached.cached_at.elapsed() < self.ttl {
                valid += 1;
            } else {
                expired += 1;
            }
        }

        SchemaCacheStats {
            total: schemas.len(),
            valid,
            expired,
            ttl_secs: self.ttl.as_secs(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            refreshes: self.refreshes.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }

    /// Signal shutdown to background refresh task
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was signaled
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Start background refresh task
    ///
    /// Spawns a task that periodically refreshes expiring schemas.
    /// Returns a handle that can be used to cancel the task.
    pub fn start_background_refresh(
        self: &Arc<Self>,
        client: Arc<ArrowClickHouseClient>,
    ) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(self);
        let interval = Duration::from_secs(cache.config.refresh_interval_secs);

        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                "Starting schema cache background refresh"
            );

            loop {
                tokio::time::sleep(interval).await;

                if cache.is_shutdown() {
                    info!("Schema cache refresh task shutting down");
                    break;
                }

                let tables = cache.tables_needing_refresh();
                if tables.is_empty() {
                    continue;
                }

                debug!(count = tables.len(), "Refreshing expiring schemas");

                for table in tables {
                    if cache.is_shutdown() {
                        break;
                    }

                    match client.fetch_table_schema(&table).await {
                        Ok(schema) => {
                            cache.insert(table.clone(), schema);
                        }
                        Err(e) => {
                            error!(
                                table = %table,
                                error = %e,
                                "Failed to refresh schema"
                            );
                            // Don't invalidate - keep using stale schema
                        }
                    }
                }
            }
        })
    }
}

/// Statistics about the schema cache
#[derive(Debug, Clone)]
pub struct SchemaCacheStats {
    /// Total number of cached schemas
    pub total: usize,
    /// Number of valid (non-expired) schemas
    pub valid: usize,
    /// Number of expired schemas (will be refreshed on next access)
    pub expired: usize,
    /// TTL in seconds
    pub ttl_secs: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Number of schema refreshes
    pub refreshes: u64,
    /// Number of invalidations
    pub invalidations: u64,
}

/// Thread-safe shared schema cache
pub type SharedSchemaCache = Arc<SchemaCache>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::{ColumnInfo, ParsedType};

    fn make_test_schema(table: &str) -> TableSchema {
        TableSchema {
            database: "test".to_string(),
            table: table.to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                type_name: "UInt64".to_string(),
                parsed_type: ParsedType::parse("UInt64"),
                position: 1,
                default_kind: String::new(),
                default_expression: String::new(),
                comment: String::new(),
                is_in_primary_key: true,
                is_in_sorting_key: true,
            }],
            comment: String::new(),
        }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = SchemaCache::new(300);

        // Initially empty
        assert!(cache.get("events").is_none());

        // Insert
        cache.insert("events".to_string(), make_test_schema("events"));

        // Now available
        let schema = cache.get("events").unwrap();
        assert_eq!(schema.table, "events");
        assert_eq!(schema.columns.len(), 1);
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = SchemaCache::new(300);

        cache.insert("events".to_string(), make_test_schema("events"));
        assert!(cache.get("events").is_some());

        cache.invalidate("events");
        assert!(cache.get("events").is_none());
    }

    #[test]
    fn test_cache_invalidate_all() {
        let cache = SchemaCache::new(300);

        cache.insert("events".to_string(), make_test_schema("events"));
        cache.insert("logs".to_string(), make_test_schema("logs"));
        assert_eq!(cache.cached_tables().len(), 2);

        cache.invalidate_all();
        assert_eq!(cache.cached_tables().len(), 0);
    }

    #[test]
    fn test_cache_ttl_expiry() {
        // Use very short TTL for testing
        let cache = SchemaCache::new(0);

        cache.insert("events".to_string(), make_test_schema("events"));

        // Should be expired immediately
        assert!(cache.get("events").is_none());
    }

    #[test]
    fn test_cache_stats() {
        let cache = SchemaCache::new(300);

        cache.insert("events".to_string(), make_test_schema("events"));
        cache.insert("logs".to_string(), make_test_schema("logs"));

        let stats = cache.stats();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.valid, 2);
        assert_eq!(stats.expired, 0);
        assert_eq!(stats.ttl_secs, 300);
    }

    #[test]
    fn test_invalidate_on_schema_error() {
        let cache = SchemaCache::new(300);
        cache.insert("events".to_string(), make_test_schema("events"));

        // Non-schema error should not invalidate
        let invalidated = cache.invalidate_on_schema_error("events", "Connection timeout");
        assert!(!invalidated);
        assert!(cache.get("events").is_some());

        // Schema error should invalidate
        let invalidated = cache.invalidate_on_schema_error("events", "Unknown column 'foo'");
        assert!(invalidated);
        assert!(cache.get("events").is_none());
    }

    #[test]
    fn test_needs_refresh() {
        let config = SchemaCacheConfig {
            ttl_secs: 60,
            refresh_headroom_secs: 10,
            ..Default::default()
        };
        let cache = SchemaCache::with_config(config);

        // No schema = needs refresh
        assert!(cache.needs_refresh("events"));

        // Fresh schema = no refresh needed
        cache.insert("events".to_string(), make_test_schema("events"));
        assert!(!cache.needs_refresh("events"));
    }

    #[test]
    fn test_hit_miss_tracking() {
        let cache = SchemaCache::new(300);
        cache.insert("events".to_string(), make_test_schema("events"));

        // Hit
        let _ = cache.get("events");
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);

        // Miss (non-existent)
        let _ = cache.get("nonexistent");
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        // Note: misses only count expired, not non-existent
    }

    #[test]
    fn test_config_default() {
        let config = SchemaCacheConfig::default();
        assert_eq!(config.ttl_secs, 300);
        assert!(!config.auto_refresh);
        assert_eq!(config.refresh_interval_secs, 60);
        assert_eq!(config.refresh_headroom_secs, 30);
    }
}
