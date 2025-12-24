// Project:   dfe-loader-clickhouse
// File:      client.rs
// Purpose:   ClickHouse client using klickhouse native protocol
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse client using klickhouse native protocol

use std::sync::Arc;

use klickhouse::{Client, ClientOptions, Row};
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::{debug, info};

use crate::clickhouse::types::ParsedType;
use crate::config::ClickHouseConfig;
use crate::Result;

/// Column information from system.columns
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Raw type string from ClickHouse
    pub type_name: String,
    /// Parsed type information
    pub parsed_type: ParsedType,
    /// Column position (1-based)
    pub position: u64,
    /// Default kind (empty, DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL)
    pub default_kind: String,
    /// Default expression
    pub default_expression: String,
    /// Column comment (may contain metadata directives)
    pub comment: String,
    /// Whether column is part of primary key
    pub is_in_primary_key: bool,
    /// Whether column is part of sorting key
    pub is_in_sorting_key: bool,
}

impl ColumnInfo {
    /// Check if this column is nullable
    pub fn is_nullable(&self) -> bool {
        self.parsed_type.nullable
    }

    /// Get the coercer category for this column
    pub fn coercer_category(&self) -> &str {
        self.parsed_type.coercer_category()
    }
}

/// Table schema with all column information
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Columns in order
    pub columns: Vec<ColumnInfo>,
    /// Table comment (may contain directives like logjson=force)
    pub comment: String,
}

impl TableSchema {
    /// Get column by name
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }
}

/// Row type for system.columns query
#[derive(Row, Deserialize, Debug)]
struct SystemColumnsRow {
    name: String,
    #[klickhouse(rename = "type")]
    type_name: String,
    position: u64,
    default_kind: String,
    default_expression: String,
    comment: String,
    is_in_primary_key: u8,
    is_in_sorting_key: u8,
}

/// Row type for system.tables query
#[derive(Row, Deserialize, Debug)]
struct SystemTablesRow {
    comment: String,
}

/// Row type for count query
#[derive(Row, Deserialize, Debug)]
struct CountRow {
    count: u64,
}

/// ClickHouse client wrapper using klickhouse
pub struct ClickHouseClient {
    client: Client,
    database: String,
}

impl ClickHouseClient {
    /// Create a new ClickHouse client from config
    pub async fn new(config: &ClickHouseConfig) -> Result<Self> {
        let host = config.hosts.first().ok_or_else(|| {
            crate::Error::Config("No ClickHouse hosts configured".into())
        })?;

        // Parse host:port
        let addr = if host.contains(':') {
            host.clone()
        } else {
            format!("{}:9000", host) // Default native port
        };

        info!(host = %addr, database = %config.database, "Connecting to ClickHouse");

        let mut options = ClientOptions::default();

        if !config.username.is_empty() {
            options.username = config.username.clone();
        }
        if !config.password.is_empty() {
            options.password = config.password.clone();
        }
        options.default_database = config.database.clone();

        let client = Client::connect(addr, options).await?;

        info!("Connected to ClickHouse");

        Ok(Self {
            client,
            database: config.database.clone(),
        })
    }

    /// Get the database name
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Execute a query (no results)
    pub async fn query(&self, sql: &str) -> Result<()> {
        debug!(sql = %sql, "Executing query");
        self.client.execute(sql).await?;
        Ok(())
    }

    /// Insert a batch of JSON rows into a table
    pub async fn insert_json(&self, table: &str, rows: Vec<Map<String, Value>>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let row_count = rows.len();
        debug!(table = %table, rows = row_count, "Inserting batch");

        // Convert to JSON block for insertion via JSONEachRow format
        let mut json_lines = String::with_capacity(rows.len() * 256);
        for row in &rows {
            let line = serde_json::to_string(row)
                .map_err(|e| crate::Error::Json(format!("Failed to serialise row: {}", e)))?;
            json_lines.push_str(&line);
            json_lines.push('\n');
        }

        let sql = format!(
            "INSERT INTO {} FORMAT JSONEachRow\n{}",
            table, json_lines
        );

        self.client.execute(&sql).await?;

        debug!(table = %table, rows = row_count, "Insert complete");
        Ok(row_count)
    }

    /// Get table schema from system.columns
    ///
    /// This is the primary schema introspection method, following the Go pattern
    /// of using ClickHouse as the Single Source of Truth (SSOT).
    pub async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        debug!(table = %table, database = %self.database, "Fetching table schema");

        // Query system.columns for column information
        let sql = format!(
            r#"
            SELECT
                name,
                type,
                position,
                default_kind,
                default_expression,
                comment,
                is_in_primary_key,
                is_in_sorting_key
            FROM system.columns
            WHERE database = '{}' AND table = '{}'
            ORDER BY position
            "#,
            self.database, table
        );

        let rows: Vec<SystemColumnsRow> = self.client.query_collect(&sql).await?;

        if rows.is_empty() {
            return Err(crate::Error::Schema(format!(
                "Table '{}.{}' not found or has no columns",
                self.database, table
            )));
        }

        // Convert to ColumnInfo
        let columns: Vec<ColumnInfo> = rows
            .into_iter()
            .map(|row| ColumnInfo {
                name: row.name,
                type_name: row.type_name.clone(),
                parsed_type: ParsedType::parse(&row.type_name),
                position: row.position,
                default_kind: row.default_kind,
                default_expression: row.default_expression,
                comment: row.comment,
                is_in_primary_key: row.is_in_primary_key != 0,
                is_in_sorting_key: row.is_in_sorting_key != 0,
            })
            .collect();

        // Get table comment
        let comment = self.get_table_comment(table).await.unwrap_or_default();

        let schema = TableSchema {
            database: self.database.clone(),
            table: table.to_string(),
            columns,
            comment,
        };

        debug!(
            table = %table,
            columns = schema.columns.len(),
            "Schema fetched"
        );

        Ok(schema)
    }

    /// Get table comment from system.tables
    async fn get_table_comment(&self, table: &str) -> Result<String> {
        let sql = format!(
            "SELECT comment FROM system.tables WHERE database = '{}' AND name = '{}'",
            self.database, table
        );

        let rows: Vec<SystemTablesRow> = self.client.query_collect(&sql).await?;
        Ok(rows.first().map(|r| r.comment.clone()).unwrap_or_default())
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = format!(
            "SELECT count() as count FROM system.tables WHERE database = '{}' AND name = '{}'",
            self.database, table
        );

        let rows: Vec<CountRow> = self.client.query_collect(&sql).await?;
        Ok(rows.first().map(|r| r.count > 0).unwrap_or(false))
    }

    /// Get list of all tables in the database
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}' ORDER BY name",
            self.database
        );

        #[derive(Row, Deserialize)]
        struct TableNameRow {
            name: String,
        }

        let rows: Vec<TableNameRow> = self.client.query_collect(&sql).await?;
        Ok(rows.into_iter().map(|r| r.name).collect())
    }

    /// Get inner client for advanced operations
    pub fn inner(&self) -> &Client {
        &self.client
    }
}

/// Thread-safe reference to ClickHouse client
pub type SharedClickHouseClient = Arc<ClickHouseClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info() {
        let col = ColumnInfo {
            name: "timestamp".to_string(),
            type_name: "DateTime64(3)".to_string(),
            parsed_type: ParsedType::parse("DateTime64(3)"),
            position: 1,
            default_kind: String::new(),
            default_expression: String::new(),
            comment: String::new(),
            is_in_primary_key: true,
            is_in_sorting_key: true,
        };
        assert_eq!(col.name, "timestamp");
        assert!(!col.is_nullable());
        assert_eq!(col.coercer_category(), "DateTime64");
        assert!(col.is_in_primary_key);
    }

    #[test]
    fn test_nullable_detection() {
        let col = ColumnInfo {
            name: "optional_field".to_string(),
            type_name: "Nullable(String)".to_string(),
            parsed_type: ParsedType::parse("Nullable(String)"),
            position: 2,
            default_kind: String::new(),
            default_expression: String::new(),
            comment: String::new(),
            is_in_primary_key: false,
            is_in_sorting_key: false,
        };
        assert!(col.is_nullable());
        assert_eq!(col.coercer_category(), "String");
    }

    #[test]
    fn test_table_schema() {
        let schema = TableSchema {
            database: "test".to_string(),
            table: "events".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    type_name: "UInt64".to_string(),
                    parsed_type: ParsedType::parse("UInt64"),
                    position: 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: true,
                    is_in_sorting_key: true,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: 2,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                },
            ],
            comment: String::new(),
        };

        assert!(schema.has_column("id"));
        assert!(schema.has_column("name"));
        assert!(!schema.has_column("missing"));

        let col = schema.column("id").unwrap();
        assert_eq!(col.coercer_category(), "UInt");

        assert_eq!(schema.column_names(), vec!["id", "name"]);
    }
}
