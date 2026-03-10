// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/client_http.rs
// Purpose:   ClickHouse HTTP client with JSONEachRow inserts for dynamic schemas
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! ClickHouse HTTP client for dynamic schema inserts.
//!
//! Uses two underlying clients:
//! - `clickhouse::Client` for DDL, schema queries, and health checks (static Row types)
//! - `reqwest::Client` for data inserts via JSONEachRow (dynamic `Map<String, Value>`)
//!
//! JSONEachRow avoids the `clickhouse::Row` trait's compile-time schema requirement.
//! Performance is equivalent to RowBinary for inserts (network-dominated, 40-75ms).

use std::sync::Arc;

use reqwest::header::{CONTENT_TYPE, HeaderValue};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::debug;

use super::config::ClickHouseConfig;
use super::error::ClickHouseError;
use super::types::{ColumnInfo, ParsedType, TableSchema};

/// Result type for ClickHouse operations.
pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// ClickHouse HTTP client supporting dynamic schema inserts.
///
/// Uses `clickhouse::Client` for DDL/queries and `reqwest::Client` for
/// JSONEachRow data inserts. This avoids the `clickhouse::Row` trait
/// which requires compile-time schema knowledge.
pub struct HttpClickHouseClient {
    /// Official crate client for DDL and schema queries.
    ch_client: clickhouse::Client,
    /// HTTP client for JSONEachRow data inserts.
    http_client: reqwest::Client,
    /// Base URL for direct HTTP requests (e.g., "http://host:8123").
    base_url: String,
    /// Database name.
    database: String,
    /// Auth credentials for HTTP requests.
    username: String,
    password: String,
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
    /// Create a new HTTP client from config.
    ///
    /// # Errors
    ///
    /// Returns an error if the config has no hosts.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        let endpoint = config
            .primary_endpoint()
            .ok_or_else(|| ClickHouseError::Connection("No ClickHouse hosts configured".into()))?;

        let base_url = if config.tls {
            format!("https://{endpoint}")
        } else {
            format!("http://{endpoint}")
        };

        let ch_client = clickhouse::Client::default()
            .with_url(&base_url)
            .with_user(&config.username)
            .with_password(&config.password)
            .with_database(&config.database);

        let http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(config.connect_timeout_ms))
            .timeout(std::time::Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|e| ClickHouseError::Connection(format!("HTTP client build error: {e}")))?;

        Ok(Self {
            ch_client,
            http_client,
            base_url,
            database: config.database.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
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

    /// Insert rows into a table using JSONEachRow format.
    ///
    /// Each row is a `Map<String, Value>` serialised as a JSON line.
    /// ClickHouse handles type coercion from JSON values to column types.
    ///
    /// # Arguments
    ///
    /// * `table` - Table name (may include "db.table" format)
    /// * `rows` - Rows to insert (each is a JSON object)
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails.
    pub async fn insert_json_rows(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let (db, tbl) = parse_db_table(table, &self.database);

        // Serialise rows as newline-delimited JSON
        let estimated_size = rows.len() * 256;
        let mut body = Vec::with_capacity(estimated_size);
        for row in rows {
            serde_json::to_writer(&mut body, row)
                .map_err(|e| ClickHouseError::Insert(format!("JSON serialisation error: {e}")))?;
            body.push(b'\n');
        }

        let url = format!(
            "{}/?database={}&query=INSERT+INTO+{}.{}+FORMAT+JSONEachRow",
            self.base_url, db, db, tbl,
        );

        // Retry on UNKNOWN_TABLE to handle DDL propagation delay in clustered setups.
        // Code 60 = UNKNOWN_TABLE in ClickHouse error responses.
        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY_MS: u64 = 300;

        let mut last_error = String::new();
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                debug!(table = %table, attempt, "Retrying insert after DDL propagation delay");
                tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
            }

            let resp = self
                .http_client
                .post(&url)
                .basic_auth(&self.username, Some(&self.password))
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(body.clone())
                .send()
                .await
                .map_err(|e| ClickHouseError::Connection(format!("HTTP request error: {e}")))?;

            if resp.status().is_success() {
                debug!(table = %table, rows = rows.len(), "JSONEachRow insert successful");
                return Ok(rows.len());
            }

            let status = resp.status();
            let error_text = resp
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error body".to_string());

            // Retry only on UNKNOWN_TABLE (Code: 60) — DDL propagation delay in clusters.
            // All other errors are non-retriable.
            if error_text.contains("Code: 60") || error_text.contains("UNKNOWN_TABLE") {
                last_error = format!("HTTP {status}: {error_text}");
                continue;
            }

            return Err(ClickHouseError::Insert(format!(
                "HTTP {status}: {error_text}"
            )));
        }

        Err(ClickHouseError::Insert(last_error))
    }

    /// Fetch table schema from system.columns.
    ///
    /// Queries ClickHouse directly for column metadata, which gives us
    /// actual ClickHouse type strings (better than reverse-engineering from Arrow).
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
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position",
            db, tbl
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
            "SELECT comment AS value FROM system.tables WHERE database = '{}' AND name = '{}'",
            db, tbl
        );

        let rows: Vec<SingleString> =
            self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
                ClickHouseError::Schema(format!("Failed to fetch table comment: {e}"))
            })?;

        Ok(rows.into_iter().next().map(|r| r.value).unwrap_or_default())
    }

    /// Fetch column comments for a table.
    ///
    /// Returns a map of column_name -> comment for columns with non-empty comments.
    pub async fn fetch_column_comments(&self, table: &str) -> Result<FxHashMap<String, String>> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT name, comment FROM system.columns \
             WHERE database = '{}' AND table = '{}' AND comment != '' \
             ORDER BY position",
            db, tbl
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
}

/// Parse "db.table" format, falling back to default database.
fn parse_db_table(table: &str, default_db: &str) -> (String, String) {
    if let Some((db, tbl)) = table.split_once('.') {
        (db.to_string(), tbl.to_string())
    } else {
        (default_db.to_string(), table.to_string())
    }
}

/// Thread-safe reference to HTTP ClickHouse client.
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
