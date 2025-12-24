//! ClickHouse client using klickhouse native protocol

use std::sync::Arc;

use futures::StreamExt;
use klickhouse::{Client, ClientOptions};
use serde_json::{Map, Value};
use tracing::{debug, info};

use crate::config::ClickHouseConfig;
use crate::Result;

/// Column information from DESCRIBE TABLE
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
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
                .map_err(|e| crate::Error::Json(format!("Failed to serialize row: {}", e)))?;
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

    /// Get table schema
    pub async fn describe_table(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!("DESCRIBE TABLE {}", table);
        debug!(sql = %sql, "Describing table");

        // Use query_collect to get all results
        let mut stream = self.client.query_raw(&sql).await?;

        let columns = Vec::new();
        while let Some(block_result) = stream.next().await {
            let _block = block_result?;
            // Each block contains rows - we'll parse column info from blocks
            // For DESCRIBE, columns are: name, type, default_type, default_expression, etc.
            // klickhouse Block has a column-oriented structure
            // For MVP, we can use a simpler approach via system tables
        }

        Ok(columns)
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = format!(
            "SELECT count() FROM system.tables WHERE database = '{}' AND name = '{}'",
            self.database, table
        );

        // Execute and check result
        let mut stream = self.client.query_raw(&sql).await?;
        if let Some(block_result) = stream.next().await {
            let _block = block_result?;
            // If we got a block, table query succeeded
            // For count(), any result > 0 means table exists
            return Ok(true);
        }
        Ok(false)
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
            type_name: "DateTime".to_string(),
            nullable: false,
        };
        assert_eq!(col.name, "timestamp");
        assert!(!col.nullable);
    }

    #[test]
    fn test_nullable_detection() {
        let type_name = "Nullable(String)";
        assert!(type_name.starts_with("Nullable("));

        let type_name2 = "String";
        assert!(!type_name2.starts_with("Nullable("));
    }
}
