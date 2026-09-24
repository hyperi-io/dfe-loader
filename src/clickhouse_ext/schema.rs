// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/schema.rs
// Purpose:   system.columns schema fetch + TTL cache for dynamic inserts
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Schema reflection for dynamic inserts.
//!
//! Fetches column definitions from `system.columns` over the new clickhouse-rs
//! `Client` and caches them with a TTL. Each column's parsed type (carried on
//! [`ColumnDef`]) drives the runtime RowBinary encoding in `crate::encode`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use clickhouse::Client;

use super::encode::ColumnDef;
use super::error::DynamicError;

/// Resolved schema for a single table -- an ordered list of columns plus a
/// name index for O(1) lookup during encoding.
#[derive(Debug, Clone)]
pub struct DynamicSchema {
    /// Fully qualified table name (database.table).
    pub table: String,
    /// Columns in declaration (position) order.
    pub columns: Vec<ColumnDef>,
    column_index: HashMap<String, usize>,
}

impl DynamicSchema {
    /// Build a schema from an ordered column list.
    #[must_use]
    pub fn from_columns(table: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            table: table.into(),
            columns,
            column_index,
        }
    }

    /// Look up a column by name.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_index.get(name).map(|&i| &self.columns[i])
    }

    /// Columns that must appear in the INSERT (no server-side default).
    pub fn required_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.has_default)
    }

    /// Columns that may be omitted because the server supplies a default.
    pub fn optional_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| c.has_default)
    }

    /// Number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the schema has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Whether any column carries a JSON type at any depth -- bare, the
    /// parameterised `JSON(max_dynamic_paths=N)` the meta schemas declare,
    /// `Nullable(JSON)`, or nested inside an `Array`/`Map`.
    ///
    /// When true, RowBinary inserts must set
    /// `input_format_binary_read_json_as_string=1`, because the encoder writes
    /// JSON values as length-prefixed strings rather than the structured
    /// path-value binary ClickHouse expects by default.
    #[must_use]
    pub fn has_json_columns(&self) -> bool {
        self.columns.iter().any(|c| c.ty.contains_json())
    }
}

/// One row of the `system.columns` projection we query.
#[derive(clickhouse::Row, serde::Deserialize)]
struct SysColumn {
    name: String,
    col_type: String,
    default_kind: String,
}

/// Fetch a table's schema from `system.columns`.
///
/// Uses a parameterised query (no string interpolation) and works over
/// whichever transport the `Client` is configured for. Columns come back in
/// declaration order.
///
/// # Errors
///
/// Returns [`DynamicError::SchemaFetch`] if the query fails, or
/// [`DynamicError::EmptySchema`] if the table has no columns (or does not
/// exist).
pub async fn fetch_dynamic_schema(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<DynamicSchema, DynamicError> {
    let full_table = format!("{database}.{table}");

    let rows = client
        .query(
            "SELECT name, type AS col_type, default_kind \
             FROM system.columns \
             WHERE database = ? AND table = ? \
             ORDER BY position",
        )
        .bind(database)
        .bind(table)
        .fetch_all::<SysColumn>()
        .await
        .map_err(|source| DynamicError::SchemaFetch {
            table: full_table.clone(),
            source,
        })?;

    if rows.is_empty() {
        return Err(DynamicError::EmptySchema { table: full_table });
    }

    let columns = rows
        .into_iter()
        .map(|r| ColumnDef::with_default_kind(r.name, r.col_type, r.default_kind))
        .collect();

    Ok(DynamicSchema::from_columns(full_table, columns))
}

/// TTL-based schema cache, safe to share across insert tasks via `Arc`.
pub struct DynamicSchemaCache {
    inner: RwLock<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

struct CacheEntry {
    schema: DynamicSchema,
    fetched_at: Instant,
}

impl DynamicSchemaCache {
    /// Create a cache with the given freshness window, wrapped in `Arc`.
    #[must_use]
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
            ttl,
        })
    }

    /// How long a fetched schema is served before it is fetched again.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Return the cached schema if present and still fresh.
    #[must_use]
    pub fn get(&self, table: &str) -> Option<DynamicSchema> {
        let guard = self.inner.read().ok()?;
        guard.get(table).and_then(|e| {
            if e.fetched_at.elapsed() < self.ttl {
                Some(e.schema.clone())
            } else {
                None
            }
        })
    }

    /// Insert or refresh a table's schema.
    pub fn insert(&self, table: &str, schema: DynamicSchema) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(
                table.to_string(),
                CacheEntry {
                    schema,
                    fetched_at: Instant::now(),
                },
            );
        }
    }

    /// Drop a single table, forcing a re-fetch on next access (used by the
    /// schema-mismatch recovery path).
    pub fn invalidate(&self, table: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(table);
        }
    }

    /// Drop all cached schemas.
    pub fn invalidate_all(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
    }
}

impl std::fmt::Debug for DynamicSchemaCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.read().map_or(0, |g| g.len());
        f.debug_struct("DynamicSchemaCache")
            .field("ttl", &self.ttl)
            .field("entries", &count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, type_str: &str, default_kind: &str) -> ColumnDef {
        ColumnDef::with_default_kind(name, type_str, default_kind)
    }

    #[test]
    fn schema_basic_lookup_and_required() {
        let schema = DynamicSchema::from_columns(
            "db.test",
            vec![
                col("id", "UInt64", ""),
                col("name", "String", ""),
                col("created_at", "DateTime64(3)", "DEFAULT"),
            ],
        );
        assert_eq!(schema.len(), 3);
        assert!(!schema.is_empty());
        assert!(schema.column("id").is_some());
        assert!(schema.column("missing").is_none());
        assert_eq!(schema.required_columns().count(), 2);
        assert_eq!(schema.optional_columns().count(), 1);
    }

    #[test]
    fn has_default_tracks_default_kind() {
        assert!(col("ts", "DateTime64(3)", "DEFAULT").has_default);
        assert!(col("mv", "String", "MATERIALIZED").has_default);
        assert!(!col("id", "UInt64", "").has_default);
    }

    #[test]
    fn detects_json_columns() {
        let plain = DynamicSchema::from_columns(
            "db.t",
            vec![col("id", "UInt64", ""), col("name", "String", "")],
        );
        assert!(!plain.has_json_columns());

        let with_json = DynamicSchema::from_columns(
            "db.t",
            vec![col("id", "UInt64", ""), col("data", "JSON", "")],
        );
        assert!(with_json.has_json_columns());

        let nullable_json = DynamicSchema::from_columns(
            "db.t",
            vec![col("id", "UInt64", ""), col("tags", "Nullable(JSON)", "")],
        );
        assert!(nullable_json.has_json_columns());
    }

    #[test]
    fn detects_parameterised_json_columns() {
        // `system.columns` reports the meta schemas' `_json` with its
        // parameters. Missing it drops `input_format_binary_read_json_as_string`
        // from the INSERT, and the server rejects the batch with code 117.
        let meta = DynamicSchema::from_columns(
            "dfe.filebeat",
            vec![
                col("_timestamp", "DateTime64(3)", ""),
                col("_json", "JSON(max_dynamic_paths=2048)", ""),
            ],
        );
        assert!(meta.has_json_columns());
    }

    #[test]
    fn detects_json_nested_in_a_container_column() {
        // A JSON inside an Array or a Map needs the same RowBinary setting as
        // a top-level one; matching only the outer tag drops it.
        for ty in [
            "Array(JSON)",
            "Map(String, JSON)",
            "Nullable(Array(JSON))",
            "Array(JSON(max_dynamic_paths=64))",
            "Array(Array(JSON))",
            "Map(String, Array(JSON))",
        ] {
            let schema = DynamicSchema::from_columns(
                "db.t",
                vec![col("id", "UInt64", ""), col("c", ty, "")],
            );
            assert!(schema.has_json_columns(), "missed JSON in {ty}");
        }

        for ty in ["Array(String)", "Map(String, UInt64)", "Nullable(String)"] {
            let schema = DynamicSchema::from_columns(
                "db.t",
                vec![col("id", "UInt64", ""), col("c", ty, "")],
            );
            assert!(!schema.has_json_columns(), "false positive on {ty}");
        }
    }

    #[test]
    fn cache_insert_get_invalidate() {
        let cache = DynamicSchemaCache::new(Duration::from_secs(300));
        let schema = DynamicSchema::from_columns("db.t", vec![col("id", "UInt64", "")]);
        assert!(cache.get("db.t").is_none());
        cache.insert("db.t", schema);
        assert!(cache.get("db.t").is_some());
        cache.invalidate("db.t");
        assert!(cache.get("db.t").is_none());
    }

    #[test]
    fn cache_respects_ttl() {
        // Generous margins: a 1ms TTL flaked under concurrent test load because
        // the scheduler could deschedule this thread for >1ms between insert and
        // the freshness assertion, expiring the entry early. A 100ms TTL keeps
        // the "present immediately after insert" check reliable, and a 250ms
        // sleep is comfortably past expiry without racing scheduler jitter.
        let cache = DynamicSchemaCache::new(Duration::from_millis(100));
        let schema = DynamicSchema::from_columns("db.t", vec![col("id", "UInt64", "")]);
        cache.insert("db.t", schema);
        assert!(cache.get("db.t").is_some());
        std::thread::sleep(Duration::from_millis(250));
        assert!(cache.get("db.t").is_none());
    }

    #[test]
    fn cache_invalidate_all() {
        let cache = DynamicSchemaCache::new(Duration::from_secs(300));
        cache.insert(
            "db.t1",
            DynamicSchema::from_columns("db.t1", vec![col("id", "UInt64", "")]),
        );
        cache.insert(
            "db.t2",
            DynamicSchema::from_columns("db.t2", vec![col("id", "UInt64", "")]),
        );
        assert!(cache.get("db.t1").is_some());
        assert!(cache.get("db.t2").is_some());
        cache.invalidate_all();
        assert!(cache.get("db.t1").is_none());
        assert!(cache.get("db.t2").is_none());
    }
}
