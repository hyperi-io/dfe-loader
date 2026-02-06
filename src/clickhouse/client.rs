// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/client.rs
// Purpose:   ClickHouse Arrow client wrapper with native and HTTP transport
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse Arrow client with support for native and HTTP transports.
//!
//! Wraps `clickhouse-arrow` with a simplified API supporting both connection modes.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
#[cfg(feature = "http")]
use clickhouse_arrow::http::{HttpClient, HttpOptions};
use clickhouse_arrow::{ArrowFormat, Client, ClientBuilder};
use futures_util::StreamExt;
use futures_util::TryStreamExt;

use super::config::{ClickHouseConfig, Transport};
use super::error::ClickHouseError;
use super::types::{ColumnInfo, ParsedType, TableSchema};

/// Result type for ClickHouse operations.
pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// Type alias for the underlying native Arrow client.
pub type NativeArrowClient = Client<ArrowFormat>;

/// ClickHouse client supporting both native and HTTP transports.
///
/// Provides a unified API for ClickHouse operations regardless of transport.
/// The transport is selected based on the `ClickHouseConfig.transport` field.
///
/// ## Example
///
/// ```rust,no_run
/// use dfe_loader::clickhouse::{ArrowClickHouseClient, ClickHouseConfig, Transport};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Native protocol (default)
/// let config = ClickHouseConfig::new("localhost:9000", "default");
/// let client = ArrowClickHouseClient::new(&config).await?;
///
/// // HTTP protocol
/// let http_config = ClickHouseConfig::http("localhost:8123", "default");
/// let http_client = ArrowClickHouseClient::new(&http_config).await?;
///
/// // Both support the same API
/// let batches = client.select("SELECT 1 as x").await?;
/// # Ok(())
/// # }
/// ```
pub struct ArrowClickHouseClient {
    inner: ClientInner,
    database: String,
}

/// Internal client implementation for different transports.
enum ClientInner {
    /// Native TCP protocol client.
    Native(NativeArrowClient),

    /// HTTP protocol client.
    #[cfg(feature = "http")]
    Http(HttpClient),
}

impl ArrowClickHouseClient {
    /// Create a new Arrow client from config.
    ///
    /// The transport is determined by `config.transport`:
    /// - `Transport::Native` - Uses native TCP protocol (port 9000)
    /// - `Transport::Http` - Uses HTTP protocol (port 8123)
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails.
    pub async fn new(config: &ClickHouseConfig) -> Result<Self> {
        let endpoint = config
            .primary_endpoint()
            .ok_or_else(|| ClickHouseError::Connection("No ClickHouse hosts configured".into()))?;

        let inner = match config.transport {
            Transport::Native => {
                let client = ClientBuilder::new()
                    .with_endpoint(&endpoint)
                    .with_username(&config.username)
                    .with_password(&config.password)
                    .with_database(&config.database)
                    .with_tls(config.tls)
                    .build_arrow()
                    .await
                    .map_err(|e| {
                        ClickHouseError::Connection(format!("Native client connect failed: {e}"))
                    })?;
                ClientInner::Native(client)
            }

            #[cfg(feature = "http")]
            Transport::Http => {
                // Build HTTP URL with scheme
                let scheme = if config.tls { "https" } else { "http" };
                let url = format!("{}://{}", scheme, endpoint);

                let mut options = HttpOptions::new(&url)
                    .map_err(|e| ClickHouseError::Connection(format!("Invalid HTTP URL: {e}")))?
                    .with_database(&config.database)
                    .with_compression(config.compression)
                    .with_timeout(Duration::from_millis(config.request_timeout_ms));

                if !config.username.is_empty() {
                    options = options.with_credentials(&config.username, &config.password);
                }

                let client = HttpClient::new(options).map_err(|e| {
                    ClickHouseError::Connection(format!("HTTP client build failed: {e}"))
                })?;
                ClientInner::Http(client)
            }

            #[cfg(not(feature = "http"))]
            Transport::Http => {
                return Err(ClickHouseError::Connection(
                    "HTTP transport requires the 'http' feature".into(),
                ));
            }
        };

        Ok(Self {
            inner,
            database: config.database.clone(),
        })
    }

    /// Get the database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the transport type being used.
    #[must_use]
    pub fn transport(&self) -> Transport {
        match &self.inner {
            ClientInner::Native(_) => Transport::Native,
            #[cfg(feature = "http")]
            ClientInner::Http(_) => Transport::Http,
        }
    }

    /// Insert an Arrow `RecordBatch` into a table.
    ///
    /// The table can be specified as "db.table" or just "table" (uses default database).
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails.
    pub async fn insert(&self, table: &str, batch: RecordBatch) -> Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let row_count = batch.num_rows();
        let (db, tbl) = parse_db_table(table, &self.database);

        match &self.inner {
            ClientInner::Native(client) => {
                let insert_query = format!("INSERT INTO {db}.{tbl} VALUES");
                let mut stream = client
                    .insert(&insert_query, batch, None)
                    .await
                    .map_err(ClickHouseError::InsertServer)?;

                while let Some(result) = stream.next().await {
                    result.map_err(ClickHouseError::InsertServer)?;
                }
            }

            #[cfg(feature = "http")]
            ClientInner::Http(client) => {
                let table_name = format!("{db}.{tbl}");
                client
                    .insert(&table_name, batch)
                    .await
                    .map_err(ClickHouseError::InsertServer)?;
            }
        }

        Ok(row_count)
    }

    /// Insert multiple Arrow `RecordBatch`es into a table.
    ///
    /// # Errors
    ///
    /// Returns an error if any insert fails.
    pub async fn insert_many(&self, table: &str, batches: Vec<RecordBatch>) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let (db, tbl) = parse_db_table(table, &self.database);

        match &self.inner {
            ClientInner::Native(client) => {
                let insert_query = format!("INSERT INTO {db}.{tbl} VALUES");
                let mut stream = client
                    .insert_many(&insert_query, batches, None)
                    .await
                    .map_err(ClickHouseError::InsertServer)?;

                while let Some(result) = stream.next().await {
                    result.map_err(ClickHouseError::InsertServer)?;
                }
            }

            #[cfg(feature = "http")]
            ClientInner::Http(client) => {
                // HTTP client doesn't have insert_many, iterate
                let table_name = format!("{db}.{tbl}");
                for batch in batches {
                    client
                        .insert(&table_name, batch)
                        .await
                        .map_err(ClickHouseError::InsertServer)?;
                }
            }
        }

        Ok(total_rows)
    }

    /// Fetch table schema as Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist or schema fetch fails.
    pub async fn fetch_schema(&self, table: &str) -> Result<SchemaRef> {
        let (db, tbl) = parse_db_table(table, &self.database);

        match &self.inner {
            ClientInner::Native(client) => {
                let schemas = client
                    .fetch_schema(Some(&db), &[tbl.as_str()], None)
                    .await
                    .map_err(|e| ClickHouseError::Schema(format!("Failed to fetch schema: {e}")))?;

                schemas
                    .get(&tbl)
                    .cloned()
                    .ok_or_else(|| ClickHouseError::Schema(format!("Table '{table}' not found")))
            }

            #[cfg(feature = "http")]
            ClientInner::Http(client) => {
                // HTTP: Query for schema via DESCRIBE
                let sql = format!("SELECT * FROM {db}.{tbl} LIMIT 0");
                let batches = client
                    .query(&sql)
                    .await
                    .map_err(|e| ClickHouseError::Schema(format!("Failed to fetch schema: {e}")))?;

                batches
                    .first()
                    .map(|b| b.schema())
                    .ok_or_else(|| ClickHouseError::Schema(format!("Table '{table}' not found")))
            }
        }
    }

    /// Execute a query (for DDL, schema queries, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query(&self, sql: &str) -> Result<()> {
        match &self.inner {
            ClientInner::Native(client) => client
                .execute(sql, None)
                .await
                .map_err(|e| ClickHouseError::Query(format!("Query failed: {e}"))),

            #[cfg(feature = "http")]
            ClientInner::Http(client) => client
                .execute(sql)
                .await
                .map_err(|e| ClickHouseError::Query(format!("Query failed: {e}"))),
        }
    }

    /// Execute a SELECT query and return Arrow `RecordBatch`es.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn select(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        match &self.inner {
            ClientInner::Native(client) => {
                let response = client
                    .query(sql, None)
                    .await
                    .map_err(|e| ClickHouseError::Query(format!("SELECT query failed: {e}")))?;

                let batches: Vec<RecordBatch> = response.try_collect().await.map_err(|e| {
                    ClickHouseError::Query(format!("Failed to collect query results: {e}"))
                })?;

                Ok(batches)
            }

            #[cfg(feature = "http")]
            ClientInner::Http(client) => client
                .query(sql)
                .await
                .map_err(|e| ClickHouseError::Query(format!("SELECT query failed: {e}"))),
        }
    }

    /// Check connection health.
    ///
    /// # Errors
    ///
    /// Returns an error if the health check fails.
    pub async fn health_check(&self) -> Result<()> {
        match &self.inner {
            ClientInner::Native(client) => client
                .health_check(true)
                .await
                .map_err(|e| ClickHouseError::Connection(format!("Health check failed: {e}"))),

            #[cfg(feature = "http")]
            ClientInner::Http(client) => {
                // HTTP: Use simple query as health check
                client
                    .query("SELECT 1")
                    .await
                    .map(|_| ())
                    .map_err(|e| ClickHouseError::Connection(format!("Health check failed: {e}")))
            }
        }
    }

    /// Check if a table exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails (other than table not found).
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.fetch_schema(table).await {
            Ok(_) => Ok(true),
            Err(ClickHouseError::Schema(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Get list of all table names in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match &self.inner {
            ClientInner::Native(client) => {
                let schemas = client
                    .fetch_schema(Some(&self.database), &[], None)
                    .await
                    .map_err(|e| ClickHouseError::Schema(format!("Failed to list tables: {e}")))?;

                Ok(schemas.keys().cloned().collect())
            }

            #[cfg(feature = "http")]
            ClientInner::Http(client) => {
                let sql = format!("SHOW TABLES FROM {}", self.database);
                let batches = client
                    .query(&sql)
                    .await
                    .map_err(|e| ClickHouseError::Schema(format!("Failed to list tables: {e}")))?;

                let mut tables = Vec::new();
                for batch in batches {
                    if let Some(col) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..col.len() {
                            if let Some(name) = col.value(i).into() {
                                tables.push(name.to_string());
                            }
                        }
                    }
                }
                Ok(tables)
            }
        }
    }

    /// Fetch a table's COMMENT string from `system.tables`.
    ///
    /// Returns empty string if no comment is set or fetch fails.
    /// Used for DDL tag resolution (e.g., `@no_capture_json: true`).
    pub async fn fetch_table_comment(&self, table: &str) -> Result<String> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT comment FROM system.tables WHERE database = '{}' AND name = '{}'",
            db, tbl
        );
        let batches = self.select(&sql).await?;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            // ClickHouse returns String as Binary via native Arrow protocol
            if let Some(col) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
            {
                if let Ok(s) = std::str::from_utf8(col.value(0)) {
                    return Ok(s.to_string());
                }
            }
            // HTTP transport returns StringArray
            if let Some(col) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                return Ok(col.value(0).to_string());
            }
        }

        Ok(String::new())
    }

    /// Fetch table schema as `TableSchema` (includes parsed types and comment).
    ///
    /// # Errors
    ///
    /// Returns an error if the schema fetch fails.
    pub async fn fetch_table_schema(&self, table: &str) -> Result<TableSchema> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let arrow_schema = self.fetch_schema(table).await?;

        let columns: Vec<ColumnInfo> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let type_name = arrow_type_to_ch_name(field.data_type());
                ColumnInfo {
                    name: field.name().clone(),
                    type_name: type_name.clone(),
                    parsed_type: ParsedType::parse(&type_name),
                    position: (i as u64) + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                }
            })
            .collect();

        // Fetch table comment for DDL tags (non-critical — default to empty on failure)
        let comment = self.fetch_table_comment(table).await.unwrap_or_default();

        Ok(TableSchema {
            database: db,
            table: tbl,
            columns,
            comment,
        })
    }

    /// Get the underlying native client for advanced operations.
    ///
    /// Returns `None` if using HTTP transport.
    #[must_use]
    pub fn native_client(&self) -> Option<&NativeArrowClient> {
        match &self.inner {
            ClientInner::Native(client) => Some(client),
            #[cfg(feature = "http")]
            ClientInner::Http(_) => None,
        }
    }

    /// Get the underlying HTTP client for advanced operations.
    ///
    /// Returns `None` if using native transport.
    #[cfg(feature = "http")]
    #[must_use]
    pub fn http_client(&self) -> Option<&HttpClient> {
        match &self.inner {
            ClientInner::Native(_) => None,
            ClientInner::Http(client) => Some(client),
        }
    }
}

/// Convert Arrow `DataType` to ClickHouse type name (best effort).
fn arrow_type_to_ch_name(dt: &DataType) -> String {
    match dt {
        DataType::Int8 => "Int8".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::UInt8 => "UInt8".to_string(),
        DataType::UInt16 => "UInt16".to_string(),
        DataType::UInt32 => "UInt32".to_string(),
        DataType::UInt64 => "UInt64".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Boolean => "Bool".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "String".to_string(),
        DataType::Binary | DataType::LargeBinary => "String".to_string(),
        DataType::Date32 | DataType::Date64 => "Date".to_string(),
        DataType::FixedSizeBinary(16) => "UUID".to_string(),
        DataType::FixedSizeBinary(4) => "IPv4".to_string(),
        DataType::FixedSizeBinary(n) => format!("FixedString({n})"),
        DataType::List(inner) => format!("Array({})", arrow_type_to_ch_name(inner.data_type())),
        DataType::Timestamp(_, _) => "DateTime64(3)".to_string(),
        DataType::Time32(_) => "DateTime".to_string(),
        DataType::Time64(_) => "DateTime64(6)".to_string(),
        _ => "String".to_string(), // Default fallback
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

/// Thread-safe reference to Arrow ClickHouse client.
pub type SharedArrowClient = Arc<ArrowClickHouseClient>;

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
    fn test_arrow_type_to_ch_name() {
        assert_eq!(arrow_type_to_ch_name(&DataType::Int64), "Int64");
        assert_eq!(arrow_type_to_ch_name(&DataType::Utf8), "String");
        assert_eq!(arrow_type_to_ch_name(&DataType::Boolean), "Bool");
        assert_eq!(
            arrow_type_to_ch_name(&DataType::FixedSizeBinary(16)),
            "UUID"
        );
        assert_eq!(arrow_type_to_ch_name(&DataType::FixedSizeBinary(4)), "IPv4");
    }
}
