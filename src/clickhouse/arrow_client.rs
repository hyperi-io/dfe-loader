//! ClickHouse Arrow client for native Arrow protocol inserts
//!
//! Uses clickhouse-arrow for efficient columnar data transfer.

use std::sync::Arc;

use arrow::array::RecordBatch;
use clickhouse_arrow::{Client, ArrowFormat, ClientBuilder};
use futures::StreamExt;
use tracing::{debug, info};

use crate::clickhouse::types::{ColumnInfo, ParsedType, TableSchema};
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

    /// Fetch table schema as TableSchema (for schema cache)
    ///
    /// This returns the full TableSchema used by the schema cache,
    /// including column info with parsed types.
    pub async fn fetch_table_schema(&self, table: &str) -> Result<TableSchema> {
        let (db, tbl) = parse_db_table(table, &self.database);

        // Fetch Arrow schema
        let arrow_schema = self.fetch_schema(table).await?;

        // Convert Arrow schema to ColumnInfo
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
                    position: i as u64 + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                }
            })
            .collect();

        Ok(TableSchema {
            database: db,
            table: tbl,
            columns,
            comment: String::new(),
        })
    }

    /// Get inner client for advanced operations
    pub fn inner(&self) -> &ArrowClient {
        &self.client
    }
}

/// Convert Arrow DataType to ClickHouse type name (best effort)
fn arrow_type_to_ch_name(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;

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
        DataType::FixedSizeBinary(n) => format!("FixedString({})", n),
        DataType::List(inner) => format!("Array({})", arrow_type_to_ch_name(inner.data_type())),
        DataType::Timestamp(_, _) => "DateTime64(3)".to_string(),
        DataType::Time32(_) => "DateTime".to_string(),
        DataType::Time64(_) => "DateTime64(6)".to_string(),
        _ => "String".to_string(), // Default fallback
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
