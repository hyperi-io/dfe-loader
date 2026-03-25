// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/client_http.rs
// Purpose:   ClickHouse HTTP client for DDL, schema queries, and health checks
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` client for DDL, schema queries, and health checks.
//!
//! Uses `clickhouse::UnifiedClient` (from the `HyperI` fork) for runtime
//! transport dispatch -- HTTP or native TCP based on config. Data inserts
//! go through `DynamicInsert` (`RowBinary`) or `InsertFormatted` (`JSONEachRow`)
//! -- see `Inserter` for insert dispatch.
//!
//! This client handles:
//! - DDL execution (CREATE, ALTER, DROP)
//! - Schema fetching from `system.columns`
//! - Table existence checks
//! - Health checks (ping)

use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::Deserialize;

use super::config::ClickHouseConfig;
use super::error::ClickHouseError;
use super::types::{ColumnInfo, ParsedType, TableSchema};

/// Result type for `ClickHouse` operations.
pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// `ClickHouse` client for DDL, schema queries, and health checks.
///
/// Wraps `clickhouse::UnifiedClient` for runtime transport dispatch.
/// Data inserts are handled by `Inserter` via `DynamicInsert` or `InsertFormatted`.
pub struct HttpClickHouseClient {
    /// Unified client -- dispatches to HTTP or native TCP based on config.
    ch_client: clickhouse::UnifiedClient,
    /// Database name.
    database: String,
}

/// Row type for system.columns queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct SystemColumn {
    name: String,
    r#type: String,
    position: u64,
    default_kind: String,
    default_expression: String,
    comment: String,
    is_in_primary_key: u8,
    is_in_sorting_key: u8,
}

/// Row type for single-string result queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct SingleString {
    value: String,
}

/// Row type for table name queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct TableName {
    name: String,
}

/// Row type for scalar count queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct CountRow {
    count: u64,
}

impl HttpClickHouseClient {
    /// Create a new client from config, dispatching to HTTP or native TCP.
    ///
    /// # Errors
    ///
    /// Returns an error if the config has no hosts.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        use super::config::Transport;

        let endpoint = config
            .primary_endpoint()
            .ok_or_else(|| ClickHouseError::Connection("No ClickHouse hosts configured".into()))?;

        let ch_client = match config.transport {
            Transport::Http => {
                let scheme = if config.tls { "https" } else { "http" };
                clickhouse::UnifiedClient::http()
                    .with_url(format!("{scheme}://{endpoint}"))
                    .with_user(&config.username)
                    .with_password(&config.password)
                    .with_database(&config.database)
                    .build()
            }
            Transport::Native => {
                let mut builder = clickhouse::UnifiedClient::native()
                    .with_addr(&*endpoint)
                    .with_user(&config.username)
                    .with_password(&config.password)
                    .with_database(&config.database)
                    .with_lz4();
                if config.tls {
                    // Extract hostname for SNI (strip port if present).
                    let host = endpoint.split(':').next().unwrap_or(&endpoint);
                    builder = builder.with_tls(host);
                }
                builder.build()
            }
        };

        Ok(Self {
            ch_client,
            database: config.database.clone(),
        })
    }

    /// Execute a DDL or DML statement (CREATE, DROP, ALTER, TRUNCATE, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.ch_client
            .query(sql)
            .execute()
            .await
            .map_err(|e| ClickHouseError::Query(format!("{e}")))?;
        Ok(())
    }

    /// Fetch table schema from system.columns.
    ///
    /// Queries `ClickHouse` directly for column metadata, which gives us
    /// actual `ClickHouse` type strings (better than reverse-engineering from Arrow).
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist or the query fails.
    pub async fn fetch_table_schema(&self, table: &str) -> Result<TableSchema> {
        let (db, tbl) = parse_db_table(table, &self.database);

        let sql = format!(
            "SELECT name, type, position, default_kind, default_expression, comment, \
             is_in_primary_key, is_in_sorting_key \
             FROM system.columns \
             WHERE database = '{db}' AND table = '{tbl}' \
             ORDER BY position"
        );

        let rows: Vec<SystemColumn> =
            self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
                ClickHouseError::Schema(format!("Failed to fetch schema for {table}: {e}"))
            })?;

        if rows.is_empty() {
            return Err(ClickHouseError::Schema(format!(
                "Table {db}.{tbl} not found or has no columns"
            )));
        }

        let columns: Vec<ColumnInfo> = rows
            .into_iter()
            .map(|row| ColumnInfo {
                parsed_type: ParsedType::parse(&row.r#type),
                name: row.name,
                type_name: row.r#type,
                position: row.position,
                default_kind: row.default_kind,
                default_expression: row.default_expression,
                comment: row.comment,
                is_in_primary_key: row.is_in_primary_key != 0,
                is_in_sorting_key: row.is_in_sorting_key != 0,
            })
            .collect();

        let comment = self.fetch_table_comment(table).await.unwrap_or_default();

        Ok(TableSchema {
            database: db,
            table: tbl,
            columns,
            comment,
        })
    }

    /// Fetch a table's COMMENT string from system.tables.
    ///
    /// Returns empty string if no comment is set or fetch fails.
    pub async fn fetch_table_comment(&self, table: &str) -> Result<String> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT comment AS value FROM system.tables WHERE database = '{db}' AND name = '{tbl}'"
        );

        let rows: Vec<SingleString> =
            self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
                ClickHouseError::Schema(format!("Failed to fetch table comment: {e}"))
            })?;

        Ok(rows.into_iter().next().map(|r| r.value).unwrap_or_default())
    }

    /// Fetch column comments for a table.
    ///
    /// Returns a map of `column_name` -> comment for columns with non-empty comments.
    pub async fn fetch_column_comments(&self, table: &str) -> Result<FxHashMap<String, String>> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT name, comment FROM system.columns \
             WHERE database = '{db}' AND table = '{tbl}' AND comment != '' \
             ORDER BY position"
        );

        // The clickhouse crate requires a Row type for fetch_all.
        // For two-column results, use a simple struct.
        #[derive(Deserialize, clickhouse::Row)]
        struct NameComment {
            name: String,
            comment: String,
        }

        let rows: Vec<NameComment> = self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
            ClickHouseError::Schema(format!("Failed to fetch column comments: {e}"))
        })?;

        let mut comments = FxHashMap::default();
        for row in rows {
            comments.insert(row.name, row.comment);
        }
        Ok(comments)
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.fetch_table_schema(table).await {
            Ok(_) => Ok(true),
            Err(ClickHouseError::Schema(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List all tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name AS name FROM system.tables WHERE database = '{}'",
            self.database
        );

        let rows: Vec<TableName> = self
            .ch_client
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|e| ClickHouseError::Schema(format!("Failed to list tables: {e}")))?;

        Ok(rows.into_iter().map(|r| r.name).collect())
    }

    /// Execute a `SELECT COUNT(*)` query and return the count.
    ///
    /// # Arguments
    ///
    /// * `table` - Table name (may include "db.table" format)
    /// * `where_clause` - Optional WHERE condition (without "WHERE" keyword)
    pub async fn query_count(&self, table: &str, where_clause: Option<&str>) -> Result<usize> {
        let sql = match where_clause {
            Some(w) => format!("SELECT COUNT(*) AS count FROM {table} WHERE {w}"),
            None => format!("SELECT COUNT(*) AS count FROM {table}"),
        };

        let row: CountRow = self
            .ch_client
            .query(&sql)
            .fetch_one()
            .await
            .map_err(|e| ClickHouseError::Query(format!("{e}")))?;

        Ok(row.count as usize)
    }

    /// Health check — executes `SELECT 1`.
    pub async fn health_check(&self) -> Result<()> {
        self.ch_client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|e| ClickHouseError::Connection(format!("Health check failed: {e}")))?;
        Ok(())
    }

    /// Get the database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Insert rows as `JSONEachRow` via HTTP.
    ///
    /// Convenience method for tests and ad-hoc data injection. Production inserts
    /// go through `Inserter` which uses `DynamicInsert` (`RowBinary`) by default.
    ///
    /// The `_raw_payloads` parameter is ignored — kept for backward compatibility
    /// with test call sites.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails (HTTP-only — errors on native transport).
    pub async fn insert_json_rows(
        &self,
        table: &str,
        rows: &[serde_json::Map<String, serde_json::Value>],
        _raw_payloads: &[std::sync::Arc<[u8]>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let (db, tbl) = parse_db_table(table, &self.database);

        // Build NDJSON body
        let mut body = Vec::with_capacity(rows.len() * 256);
        for row in rows {
            serde_json::to_writer(&mut body, row)
                .map_err(|e| ClickHouseError::Query(format!("JSON serialise: {e}")))?;
            body.push(b'\n');
        }

        // Use InsertFormatted for JSONEachRow via HTTP
        let sql = format!("INSERT INTO {db}.{tbl} FORMAT JSONEachRow");
        let mut insert = self
            .ch_client
            .insert_formatted_with(sql)
            .map_err(|e| ClickHouseError::Insert(format!("{e}")))?
            .buffered();

        insert.write_buffered(&body);

        insert
            .end()
            .await
            .map_err(|e| ClickHouseError::Insert(format!("{e}")))?;

        Ok(rows.len())
    }
}

/// Parse "db.table" format, falling back to default database.
fn parse_db_table(table: &str, default_db: &str) -> (String, String) {
    if let Some((db, tbl)) = table.split_once('.') {
        (db.to_string(), tbl.to_string())
    } else {
        (default_db.to_string(), table.to_string())
    }
}

/// Thread-safe reference to HTTP `ClickHouse` client.
pub type SharedHttpClient = Arc<HttpClickHouseClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_db_table() {
        let (db, tbl) = parse_db_table("mydb.events", "default");
        assert_eq!(db, "mydb");
        assert_eq!(tbl, "events");

        let (db, tbl) = parse_db_table("events", "default");
        assert_eq!(db, "default");
        assert_eq!(tbl, "events");
    }

    #[test]
    fn test_parse_db_table_with_dots() {
        let (db, tbl) = parse_db_table("my.db.events", "default");
        assert_eq!(db, "my");
        assert_eq!(tbl, "db.events");
    }

    #[test]
    fn test_client_new_no_hosts() {
        let config = ClickHouseConfig {
            hosts: vec![],
            ..Default::default()
        };
        let result = HttpClickHouseClient::new(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_new_with_defaults() {
        let config = ClickHouseConfig::default();
        let client = HttpClickHouseClient::new(&config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.database(), "default");
    }
}
