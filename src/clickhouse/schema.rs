// Project:   dfe-loader-clickhouse
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
//! - Thread-safe via parking_lot RwLock

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, info};

use crate::clickhouse::client::{ClickHouseClient, TableSchema};
use crate::Result;

/// Cached schema with timestamp
struct CachedSchema {
    schema: TableSchema,
    cached_at: Instant,
}

/// Thread-safe schema cache with TTL-based expiry
///
/// Caches table schemas to avoid repeated queries to system.columns.
/// Schemas are automatically refreshed after TTL expiry.
pub struct SchemaCache {
    schemas: RwLock<HashMap<String, CachedSchema>>,
    ttl: Duration,
}

impl SchemaCache {
    /// Create a new schema cache
    ///
    /// # Arguments
    /// * `ttl_secs` - Time-to-live in seconds for cached schemas (default: 300 = 5 min)
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Get cached schema if valid (not expired)
    pub fn get(&self, table: &str) -> Option<TableSchema> {
        let schemas = self.schemas.read();
        schemas.get(table).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                Some(cached.schema.clone())
            } else {
                None
            }
        })
    }

    /// Insert or update a schema in the cache
    pub fn insert(&self, table: String, schema: TableSchema) {
        let mut schemas = self.schemas.write();
        schemas.insert(
            table,
            CachedSchema {
                schema,
                cached_at: Instant::now(),
            },
        );
    }

    /// Invalidate (remove) a schema from the cache
    ///
    /// Call this after insert errors to force a refresh on next access.
    pub fn invalidate(&self, table: &str) {
        let mut schemas = self.schemas.write();
        if schemas.remove(table).is_some() {
            debug!(table = %table, "Schema cache invalidated");
        }
    }

    /// Invalidate all cached schemas
    pub fn invalidate_all(&self) {
        let mut schemas = self.schemas.write();
        let count = schemas.len();
        schemas.clear();
        if count > 0 {
            info!(count = count, "All schema caches invalidated");
        }
    }

    /// Get schema, fetching from ClickHouse if not cached or expired
    ///
    /// This is the primary method for schema access, following the Go pattern.
    pub async fn get_or_fetch(
        &self,
        table: &str,
        client: &ClickHouseClient,
    ) -> Result<TableSchema> {
        // Try cache first
        if let Some(schema) = self.get(table) {
            debug!(table = %table, "Schema cache hit");
            return Ok(schema);
        }

        // Fetch from ClickHouse
        debug!(table = %table, "Schema cache miss, fetching");
        let schema = client.describe_table(table).await?;

        // Cache the result
        self.insert(table.to_string(), schema.clone());

        Ok(schema)
    }

    /// Force refresh a schema, ignoring cache
    pub async fn refresh(&self, table: &str, client: &ClickHouseClient) -> Result<TableSchema> {
        debug!(table = %table, "Force refreshing schema");
        let schema = client.describe_table(table).await?;
        self.insert(table.to_string(), schema.clone());
        Ok(schema)
    }

    /// Get all cached table names
    pub fn cached_tables(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
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
        }
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
}

/// Thread-safe shared schema cache
pub type SharedSchemaCache = Arc<SchemaCache>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::ParsedType;
    use crate::clickhouse::ColumnInfo;

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
}
