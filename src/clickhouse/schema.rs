//! Schema caching for ClickHouse tables

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::clickhouse::client::ColumnInfo;

/// Cached schema information for tables
pub struct SchemaCache {
    schemas: HashMap<String, CachedSchema>,
    ttl: Duration,
}

struct CachedSchema {
    columns: Vec<ColumnInfo>,
    cached_at: Instant,
}

impl SchemaCache {
    /// Create a new schema cache
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            schemas: HashMap::new(),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Get cached schema if valid
    pub fn get(&self, table: &str) -> Option<&[ColumnInfo]> {
        self.schemas.get(table).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                Some(cached.columns.as_slice())
            } else {
                None
            }
        })
    }

    /// Insert or update schema
    pub fn insert(&mut self, table: String, columns: Vec<ColumnInfo>) {
        self.schemas.insert(
            table,
            CachedSchema {
                columns,
                cached_at: Instant::now(),
            },
        );
    }
}
