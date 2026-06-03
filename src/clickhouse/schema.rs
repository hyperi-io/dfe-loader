// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      schema.rs
// Purpose:   Schema caching for ClickHouse tables
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Schema caching for `ClickHouse` tables
//!
//! Provides TTL-based caching of table schemas to avoid repeated
//! system.columns queries. Following the Go pattern:
//! - Fetch on first access or after TTL expiry
//! - Manual invalidation on insert errors
//! - Automatic invalidation on schema mismatch errors
//! - Optional background refresh task
//! - Thread-safe via `parking_lot` `RwLock`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tracing::{debug, error, info, trace, warn};

use crate::clickhouse::{ClickHouseQueryClient, TableSchema};

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
            // Auto-refresh on by default. Without it, expired schemas cause the
            // pipeline to fall back from extractor to transformer path, dropping
            // @renamed directive mappings (issue #25).
            auto_refresh: true,
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
                debug!(table = %table, "Schema cache hit");
                Some(cached.schema.clone())
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                debug!(table = %table, "Schema cache miss (expired)");
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
        let columns = schema.columns.len();
        let mut schemas = self.schemas.write();
        let refresh_count = schemas.get(&table).map_or(0, |c| c.refresh_count + 1);

        if tracing::enabled!(tracing::Level::TRACE) {
            let column_names_and_types: Vec<(&str, &str)> = schema
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_name.as_str()))
                .collect();
            trace!(
                table = %table,
                columns = ?column_names_and_types,
                "Schema columns"
            );
        }

        if refresh_count == 0 {
            debug!(table = %table, columns = columns, "Schema fetched from ClickHouse");
        }

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
            debug!(table = %table, refresh_count = refresh_count, columns = columns, "Schema refreshed");
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
        client: Arc<ClickHouseQueryClient>,
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
        // auto_refresh on by default — prevents the issue #25 silent fallback
        // from extractor to transformer path on TTL expiry.
        assert!(config.auto_refresh);
        assert_eq!(config.refresh_interval_secs, 60);
        assert_eq!(config.refresh_headroom_secs, 30);
    }

    // ============================================================
    // SchemaCache::new / SchemaCache::with_config — construction
    // ============================================================

    #[test]
    fn test_new_sets_ttl_from_argument() {
        let cache = SchemaCache::new(42);
        let stats = cache.stats();
        assert_eq!(stats.ttl_secs, 42);
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn test_new_with_zero_ttl_makes_everything_expire_immediately() {
        let cache = SchemaCache::new(0);
        cache.insert("t".to_string(), make_test_schema("t"));
        // Empty interval = instant expiry
        assert!(cache.get("t").is_none());
        // But it's still in the backing map until accessed
        assert_eq!(cache.stats().total, 1);
        assert_eq!(cache.stats().expired, 1);
        assert_eq!(cache.stats().valid, 0);
    }

    #[test]
    fn test_with_config_preserves_all_fields() {
        let cfg = SchemaCacheConfig {
            ttl_secs: 99,
            auto_refresh: false,
            refresh_interval_secs: 11,
            refresh_headroom_secs: 3,
        };
        let cache = SchemaCache::with_config(cfg);
        assert_eq!(cache.stats().ttl_secs, 99);
    }

    // ============================================================
    // SchemaCacheConfig — edge cases and clone
    // ============================================================

    #[test]
    fn test_schema_cache_config_clone_preserves_values() {
        let cfg = SchemaCacheConfig {
            ttl_secs: 600,
            auto_refresh: false,
            refresh_interval_secs: 120,
            refresh_headroom_secs: 15,
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.ttl_secs, 600);
        assert!(!cloned.auto_refresh);
        assert_eq!(cloned.refresh_interval_secs, 120);
        assert_eq!(cloned.refresh_headroom_secs, 15);
    }

    #[test]
    fn test_schema_cache_config_debug_not_empty() {
        let cfg = SchemaCacheConfig::default();
        let debug = format!("{cfg:?}");
        assert!(debug.contains("SchemaCacheConfig"));
        assert!(debug.contains("300"));
    }

    // ============================================================
    // Hit/miss/invalidation counters — precise accounting
    // ============================================================

    #[test]
    fn test_multiple_hits_accumulate() {
        let cache = SchemaCache::new(300);
        cache.insert("t".to_string(), make_test_schema("t"));
        for _ in 0..10 {
            assert!(cache.get("t").is_some());
        }
        let stats = cache.stats();
        assert_eq!(stats.hits, 10);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_expired_increments_miss_counter() {
        // TTL = 0 means every get() on an existing entry is a miss.
        let cache = SchemaCache::new(0);
        cache.insert("t".to_string(), make_test_schema("t"));
        // First get — increments miss (expired).
        assert!(cache.get("t").is_none());
        // Second get — still miss.
        assert!(cache.get("t").is_none());
        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn test_invalidation_counter_only_increments_when_entry_existed() {
        let cache = SchemaCache::new(300);

        // No-op invalidate — entry absent
        cache.invalidate("nonexistent");
        assert_eq!(cache.stats().invalidations, 0);

        // Real invalidate — entry present
        cache.insert("t".to_string(), make_test_schema("t"));
        cache.invalidate("t");
        assert_eq!(cache.stats().invalidations, 1);

        // Repeat invalidate — entry already gone, no counter increment
        cache.invalidate("t");
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn test_invalidate_all_counter_sums_entries() {
        let cache = SchemaCache::new(300);
        for i in 0..5 {
            cache.insert(format!("t{i}"), make_test_schema(&format!("t{i}")));
        }
        cache.invalidate_all();
        assert_eq!(cache.stats().invalidations, 5);
        assert_eq!(cache.stats().total, 0);
    }

    #[test]
    fn test_invalidate_all_noop_on_empty_cache() {
        let cache = SchemaCache::new(300);
        cache.invalidate_all();
        assert_eq!(cache.stats().invalidations, 0);
    }

    #[test]
    fn test_refresh_counter_tracks_reinserts() {
        let cache = SchemaCache::new(300);

        // First insert — refresh_count = 0 (not counted as refresh)
        cache.insert("t".to_string(), make_test_schema("t"));
        assert_eq!(cache.stats().refreshes, 0);

        // Re-insert — counted as refresh
        cache.insert("t".to_string(), make_test_schema("t"));
        assert_eq!(cache.stats().refreshes, 1);

        // Third insert — refresh #2
        cache.insert("t".to_string(), make_test_schema("t"));
        assert_eq!(cache.stats().refreshes, 2);
    }

    // ============================================================
    // needs_refresh — headroom and TTL interplay
    // ============================================================

    #[test]
    fn test_needs_refresh_zero_headroom() {
        let cfg = SchemaCacheConfig {
            ttl_secs: 60,
            refresh_headroom_secs: 0,
            ..Default::default()
        };
        let cache = SchemaCache::with_config(cfg);
        cache.insert("t".to_string(), make_test_schema("t"));
        // Fresh + zero headroom = no refresh needed.
        assert!(!cache.needs_refresh("t"));
    }

    #[test]
    fn test_needs_refresh_large_headroom_triggers_immediately() {
        // Headroom larger than TTL → even a fresh cache entry "needs refresh"
        let cfg = SchemaCacheConfig {
            ttl_secs: 60,
            refresh_headroom_secs: 120, // 2× TTL
            ..Default::default()
        };
        let cache = SchemaCache::with_config(cfg);
        cache.insert("t".to_string(), make_test_schema("t"));
        assert!(cache.needs_refresh("t"));
    }

    #[test]
    fn test_needs_refresh_nonexistent_table() {
        let cache = SchemaCache::new(300);
        assert!(cache.needs_refresh("does_not_exist"));
    }

    // ============================================================
    // invalidate_on_schema_error — pattern recognition
    // ============================================================

    #[test]
    fn test_invalidate_on_schema_error_all_known_patterns() {
        let patterns = [
            "Unknown column 'foo'",
            "Missing columns: [a, b]",
            "Type mismatch for column x",
            "Cannot insert into table",
            "Column types don't match",
            "expected column of type Int64",
            "wrong number of columns: got 5, want 3",
        ];
        for pattern in patterns {
            let cache = SchemaCache::new(300);
            cache.insert("t".to_string(), make_test_schema("t"));
            assert!(
                cache.invalidate_on_schema_error("t", pattern),
                "pattern should trigger: {pattern}"
            );
            assert!(cache.get("t").is_none());
        }
    }

    #[test]
    fn test_invalidate_on_schema_error_unrecognised_patterns() {
        let benign = [
            "Connection timeout",
            "Network unreachable",
            "Too many requests",
            "503 Service Unavailable",
            "OOM killer",
            "",
        ];
        for err in benign {
            let cache = SchemaCache::new(300);
            cache.insert("t".to_string(), make_test_schema("t"));
            assert!(
                !cache.invalidate_on_schema_error("t", err),
                "pattern should NOT trigger: {err}"
            );
            assert!(cache.get("t").is_some(), "entry preserved for: {err}");
        }
    }

    #[test]
    fn test_invalidate_on_schema_error_substring_match() {
        // Patterns match anywhere in the string — not just prefix.
        let cache = SchemaCache::new(300);
        cache.insert("t".to_string(), make_test_schema("t"));
        let err =
            "DB::Exception: Unknown column 'bogus' in query SELECT x FROM events (OS code 42)";
        assert!(cache.invalidate_on_schema_error("t", err));
    }

    // ============================================================
    // cached_tables / tables_needing_refresh — listing behaviour
    // ============================================================

    #[test]
    fn test_cached_tables_returns_all_keys() {
        let cache = SchemaCache::new(300);
        cache.insert("a".to_string(), make_test_schema("a"));
        cache.insert("b".to_string(), make_test_schema("b"));
        cache.insert("c.nested".to_string(), make_test_schema("c.nested"));
        let mut tables = cache.cached_tables();
        tables.sort();
        assert_eq!(tables, vec!["a", "b", "c.nested"]);
    }

    #[test]
    fn test_cached_tables_empty() {
        let cache = SchemaCache::new(300);
        assert_eq!(cache.cached_tables().len(), 0);
    }

    #[test]
    fn test_tables_needing_refresh_returns_empty_when_fresh() {
        let cache = SchemaCache::new(300);
        cache.insert("fresh".to_string(), make_test_schema("fresh"));
        // TTL 300, headroom 30, just inserted — not due for refresh.
        assert_eq!(cache.tables_needing_refresh().len(), 0);
    }

    #[test]
    fn test_tables_needing_refresh_includes_expired() {
        // Zero TTL = everything is immediately "expired + past headroom".
        let cache = SchemaCache::new(0);
        cache.insert("x".to_string(), make_test_schema("x"));
        cache.insert("y".to_string(), make_test_schema("y"));
        let mut due = cache.tables_needing_refresh();
        due.sort();
        assert_eq!(due, vec!["x", "y"]);
    }

    // ============================================================
    // stats — aggregation correctness
    // ============================================================

    #[test]
    fn test_stats_mixed_valid_and_expired() {
        // We can't cleanly mix valid + expired without time manipulation,
        // but we can verify stats with all-valid and all-expired.
        let valid = SchemaCache::new(3600);
        for i in 0..3 {
            valid.insert(format!("t{i}"), make_test_schema(&format!("t{i}")));
        }
        let s = valid.stats();
        assert_eq!(s.total, 3);
        assert_eq!(s.valid, 3);
        assert_eq!(s.expired, 0);

        let expired = SchemaCache::new(0);
        for i in 0..3 {
            expired.insert(format!("t{i}"), make_test_schema(&format!("t{i}")));
        }
        let s = expired.stats();
        assert_eq!(s.total, 3);
        assert_eq!(s.valid, 0);
        assert_eq!(s.expired, 3);
    }

    #[test]
    fn test_stats_struct_clone() {
        let cache = SchemaCache::new(300);
        cache.insert("t".to_string(), make_test_schema("t"));
        let stats = cache.stats();
        let cloned = stats.clone();
        assert_eq!(cloned.total, stats.total);
        assert_eq!(cloned.ttl_secs, stats.ttl_secs);
    }

    #[test]
    fn test_stats_debug_output_includes_counters() {
        let cache = SchemaCache::new(300);
        cache.insert("t".to_string(), make_test_schema("t"));
        let _ = cache.get("t");
        let debug = format!("{:?}", cache.stats());
        assert!(debug.contains("SchemaCacheStats"));
        assert!(debug.contains("total"));
        assert!(debug.contains("hits"));
    }

    // ============================================================
    // shutdown — state transitions
    // ============================================================

    #[test]
    fn test_shutdown_flag_starts_false() {
        let cache = SchemaCache::new(300);
        assert!(!cache.is_shutdown());
    }

    #[test]
    fn test_shutdown_sets_flag() {
        let cache = SchemaCache::new(300);
        cache.shutdown();
        assert!(cache.is_shutdown());
    }

    #[test]
    fn test_shutdown_idempotent() {
        let cache = SchemaCache::new(300);
        cache.shutdown();
        cache.shutdown();
        cache.shutdown();
        assert!(cache.is_shutdown());
    }

    // ============================================================
    // Large-scale fuzz-style tests
    // ============================================================

    #[test]
    fn test_large_number_of_tables() {
        let cache = SchemaCache::new(300);
        // Insert 1000 distinct table schemas.
        for i in 0..1000 {
            let name = format!("db_{i}.table_{i}");
            cache.insert(name.clone(), make_test_schema(&name));
        }
        let stats = cache.stats();
        assert_eq!(stats.total, 1000);
        assert_eq!(stats.valid, 1000);

        // Every lookup must hit.
        for i in 0..1000 {
            let name = format!("db_{i}.table_{i}");
            assert!(cache.get(&name).is_some());
        }
        assert_eq!(cache.stats().hits, 1000);

        // Bulk invalidate.
        cache.invalidate_all();
        assert_eq!(cache.stats().invalidations, 1000);
        assert_eq!(cache.cached_tables().len(), 0);
    }

    #[test]
    fn test_unicode_table_names() {
        let cache = SchemaCache::new(300);
        let names = [
            "日本語.テーブル",
            "数据库.表",
            "база.таблица",
            "emoji_🔥.data",
        ];
        for name in names {
            cache.insert(name.to_string(), make_test_schema(name));
        }
        for name in names {
            assert!(cache.get(name).is_some(), "missing: {name}");
        }
    }

    #[test]
    fn test_empty_string_table_name() {
        // Edge case — empty key is legal for HashMap.
        let cache = SchemaCache::new(300);
        cache.insert(String::new(), make_test_schema(""));
        assert!(cache.get("").is_some());
        cache.invalidate("");
        assert!(cache.get("").is_none());
    }

    #[test]
    fn test_invalidate_independent_tables() {
        // Invalidating one table must not affect others.
        let cache = SchemaCache::new(300);
        cache.insert("a".to_string(), make_test_schema("a"));
        cache.insert("b".to_string(), make_test_schema("b"));
        cache.insert("c".to_string(), make_test_schema("c"));

        cache.invalidate("b");
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
        assert!(cache.get("c").is_some());
        assert_eq!(cache.stats().total, 2);
    }

    #[test]
    fn test_concurrent_hits_counter_thread_safe() {
        // Verify atomics — spawn threads that each read the same table.
        use std::thread;
        let cache = Arc::new(SchemaCache::new(300));
        cache.insert("t".to_string(), make_test_schema("t"));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = c.get("t");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 8 threads × 100 gets = 800 hits
        assert_eq!(cache.stats().hits, 800);
    }
}
