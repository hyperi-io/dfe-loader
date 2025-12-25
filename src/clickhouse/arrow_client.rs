//! ClickHouse Arrow client for native Arrow protocol inserts
//!
//! Uses clickhouse-arrow for efficient columnar data transfer.

use std::sync::Arc;

use arrow::array::RecordBatch;
use clickhouse_arrow::{Client, ArrowFormat, ClientBuilder};
use futures::StreamExt;
use tracing::{debug, info};

use crate::config::ClickHouseConfig;
use crate::Result;

/// Type alias for Arrow client
pub type ArrowClient = Client<ArrowFormat>;

/// ClickHouse Arrow client for native protocol inserts
pub struct ArrowClickHouseClient {
    client: ArrowClient,
    database: String,
}

impl ArrowClickHouseClient {
    /// Create a new Arrow client from config
    pub async fn new(config: &ClickHouseConfig) -> Result<Self> {
        let host = config.hosts.first().ok_or_else(|| {
            crate::Error::Config("No ClickHouse hosts configured".into())
        })?;

        // Parse host:port
        let addr = if host.contains(':') {
            host.clone()
        } else {
            format!("{}:9000", host)
        };

        info!(host = %addr, database = %config.database, "Connecting Arrow client to ClickHouse");

        let client = ClientBuilder::new()
            .with_endpoint(&addr)
            .with_username(&config.username)
            .with_password(&config.password)
            .with_database(&config.database)
            .build_arrow()
            .await
            .map_err(|e| crate::Error::ClickHouse(format!("Arrow client connect failed: {}", e)))?;

        info!("Arrow client connected to ClickHouse");

        Ok(Self {
            client,
            database: config.database.clone(),
        })
    }

    /// Get the database name
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Insert an Arrow RecordBatch into a table
    ///
    /// The table is specified as "db.table" format.
    pub async fn insert(&self, table: &str, batch: RecordBatch) -> Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let row_count = batch.num_rows();
        debug!(table = %table, rows = row_count, "Inserting Arrow batch");

        // Parse db.table format
        let (db, tbl) = parse_db_table(table, &self.database);

        // Build INSERT query - VALUES keyword required for Arrow format
        let insert_query = format!("INSERT INTO {}.{} VALUES", db, tbl);

        // Use clickhouse-arrow's insert method
        let mut stream = self.client
            .insert(&insert_query, batch, None)
            .await
            .map_err(|e| crate::Error::ClickHouse(format!("Arrow insert failed: {}", e)))?;

        // Consume the stream to complete the insert
        while let Some(result) = stream.next().await {
            result.map_err(|e| crate::Error::ClickHouse(format!("Arrow insert stream error: {}", e)))?;
        }

        debug!(table = %table, rows = row_count, "Arrow insert complete");
        Ok(row_count)
    }

    /// Insert multiple Arrow RecordBatches into a table
    pub async fn insert_many(&self, table: &str, batches: Vec<RecordBatch>) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(table = %table, batches = batches.len(), rows = total_rows, "Inserting Arrow batches");

        let (db, tbl) = parse_db_table(table, &self.database);
        let insert_query = format!("INSERT INTO {}.{} VALUES", db, tbl);

        let mut stream = self.client
            .insert_many(&insert_query, batches, None)
            .await
            .map_err(|e| crate::Error::ClickHouse(format!("Arrow insert_many failed: {}", e)))?;

        while let Some(result) = stream.next().await {
            result.map_err(|e| crate::Error::ClickHouse(format!("Arrow insert_many stream error: {}", e)))?;
        }

        debug!(table = %table, rows = total_rows, "Arrow insert_many complete");
        Ok(total_rows)
    }

    /// Fetch table schema as Arrow schema
    pub async fn fetch_schema(&self, table: &str) -> Result<arrow::datatypes::SchemaRef> {
        let (db, tbl) = parse_db_table(table, &self.database);

        let schemas = self.client
            .fetch_schema(Some(&db), &[tbl.as_str()], None)
            .await
            .map_err(|e| crate::Error::Schema(format!("Failed to fetch schema: {}", e)))?;

        schemas.get(&tbl)
            .cloned()
            .ok_or_else(|| crate::Error::Schema(format!("Table '{}' not found", table)))
    }

    /// Health check
    pub async fn health_check(&self) -> Result<()> {
        self.client
            .health_check(true)
            .await
            .map_err(|e| crate::Error::ClickHouse(format!("Health check failed: {}", e)))
    }

    /// Execute a query (for DDL, schema queries, etc.)
    pub async fn query(&self, sql: &str) -> Result<()> {
        debug!(sql = %sql, "Executing query via Arrow client");
        self.client
            .execute(sql, None)
            .await
            .map_err(|e| crate::Error::ClickHouse(format!("Query failed: {}", e)))
    }

    /// Check if a table exists by trying to fetch its schema
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.fetch_schema(table).await {
            Ok(_) => Ok(true),
            Err(crate::Error::Schema(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Get list of all table names in the database
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let schemas = self.client
            .fetch_schema(Some(&self.database), &[], None)
            .await
            .map_err(|e| crate::Error::Schema(format!("Failed to list tables: {}", e)))?;

        Ok(schemas.keys().cloned().collect())
    }

    /// Get inner client for advanced operations
    pub fn inner(&self) -> &ArrowClient {
        &self.client
    }
}

/// Parse "db.table" format, falling back to default database
fn parse_db_table(table: &str, default_db: &str) -> (String, String) {
    if let Some((db, tbl)) = table.split_once('.') {
        (db.to_string(), tbl.to_string())
    } else {
        (default_db.to_string(), table.to_string())
    }
}

/// Thread-safe reference to Arrow ClickHouse client
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
}
