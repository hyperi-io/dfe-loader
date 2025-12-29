//! Per-table Arrow buffer manager with schema introspection
//!
//! Each destination table has its own ArrowBatchBuilder for schema uniformity.
//! Schema for each table is fetched from ClickHouse introspection on first use
//! and cached with TTL-based refresh.

use std::sync::Arc;
use std::time::{Duration, Instant};

use compact_str::CompactString;
use rustc_hash::FxHashMap;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value};
use tracing::debug;

use crate::buffer::arrow::KafkaOffset;
use crate::clickhouse::ArrowClickHouseClient;
use crate::config::BufferConfig;
use crate::transform::ArrowBatchBuilder;
use crate::Result;

/// Data ready to be flushed to ClickHouse (Arrow-native)
///
/// Uses CompactString for table names (stack-allocated for ≤24 bytes).
/// Typical "db.table" names fit in ~20 bytes, avoiding heap allocation.
pub struct FlushBatch {
    /// Destination table name (db.table) - stack-allocated for short names
    pub table: CompactString,
    /// Arrow RecordBatch ready for insert
    pub batch: RecordBatch,
    /// Kafka offsets for acknowledgment after successful insert
    pub offsets: Vec<KafkaOffset>,
}

/// Per-table schema metadata from ClickHouse introspection
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Fully qualified table name (database.table)
    pub table_name: String,
    /// Arrow schema for this table
    pub arrow_schema: SchemaRef,
    /// Column name to ClickHouse type mapping
    pub column_types: FxHashMap<String, String>,
    /// Last refresh time
    pub last_refresh: Instant,
}

impl TableSchema {
    /// Create a TableSchema from column definitions
    pub fn from_columns(table_name: String, columns: Vec<(String, String)>) -> Result<Self> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut column_types = FxHashMap::with_capacity_and_hasher(columns.len(), Default::default());

        for (name, ch_type) in columns {
            let arrow_type = ch_type_to_arrow(&ch_type)?;
            let nullable = ch_type.starts_with("Nullable(") || ch_type.contains("NULL");
            fields.push(Arc::new(Field::new(&name, arrow_type, nullable)));
            column_types.insert(name, ch_type);
        }

        Ok(Self {
            table_name,
            arrow_schema: Arc::new(Schema::new(fields)),
            column_types,
            last_refresh: Instant::now(),
        })
    }

    /// Check if schema needs refresh
    pub fn needs_refresh(&self, max_age: Duration) -> bool {
        self.last_refresh.elapsed() > max_age
    }
}

/// Convert Arrow DataType back to ClickHouse type hint (for logging/debugging)
fn arrow_type_to_ch_hint(arrow_type: &DataType) -> String {
    match arrow_type {
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
        DataType::Date32 => "Date".to_string(),
        DataType::Date64 => "Date".to_string(),
        DataType::FixedSizeBinary(16) => "UUID".to_string(),
        DataType::FixedSizeBinary(n) => format!("FixedString({})", n),
        DataType::List(_) => "Array".to_string(),
        _ => "String".to_string(), // Default fallback
    }
}

/// Convert ClickHouse type string to Arrow DataType
fn ch_type_to_arrow(ch_type: &str) -> Result<DataType> {
    // Strip Nullable wrapper
    let inner_type = if ch_type.starts_with("Nullable(") && ch_type.ends_with(')') {
        &ch_type[9..ch_type.len() - 1]
    } else {
        ch_type
    };

    Ok(match inner_type {
        // Integers
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Int128" | "UInt128" => DataType::FixedSizeBinary(16),
        "Int256" | "UInt256" => DataType::FixedSizeBinary(32),

        // Floats
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,

        // Strings
        "String" => DataType::Binary,
        "UUID" => DataType::FixedSizeBinary(16),

        // Date/Time
        "Date" | "Date32" => DataType::Date32,

        // IPv4/IPv6
        "IPv4" => DataType::FixedSizeBinary(4),
        "IPv6" => DataType::FixedSizeBinary(16),

        // Bool
        "Bool" => DataType::Boolean,

        // Complex types
        t if t.starts_with("DateTime64") => DataType::Int64,
        t if t.starts_with("DateTime") => DataType::Int64,
        t if t.starts_with("Decimal") => DataType::Binary,
        t if t.starts_with("FixedString") => {
            let n: i32 = t
                .trim_start_matches("FixedString(")
                .trim_end_matches(')')
                .parse()
                .unwrap_or(256);
            DataType::FixedSizeBinary(n)
        }
        t if t.starts_with("Array") => {
            let inner = &t[6..t.len() - 1];
            let inner_type = ch_type_to_arrow(inner)?;
            DataType::List(Arc::new(Field::new("item", inner_type, true)))
        }
        t if t.starts_with("Map") => DataType::Binary,
        t if t.starts_with("Tuple") => DataType::Binary,
        t if t.starts_with("LowCardinality") => {
            let inner = &t[15..t.len() - 1];
            ch_type_to_arrow(inner)?
        }

        // Dynamic types (from our fork)
        t if t.starts_with("Variant") => DataType::Binary,
        t if t.starts_with("Dynamic") => DataType::Binary,
        t if t.starts_with("Nested") => DataType::Binary,
        "JSON" => DataType::Binary,

        // Default fallback
        _ => DataType::Binary,
    })
}

/// Per-table buffer tracking pending messages and Kafka offsets
struct TableBuffer {
    /// Batch builder for this table
    builder: ArrowBatchBuilder,
    /// Kafka offsets for messages in this buffer
    offsets: Vec<KafkaOffset>,
    /// Created timestamp
    created_at: Instant,
}

impl TableBuffer {
    fn new(batch_size: usize) -> Self {
        Self {
            builder: ArrowBatchBuilder::new(batch_size),
            offsets: Vec::new(),
            created_at: Instant::now(),
        }
    }

    fn push(&mut self, data: Map<String, Value>, table: &str, offset: Option<KafkaOffset>) {
        self.builder.push(data, table);
        if let Some(off) = offset {
            self.offsets.push(off);
        }
    }

    fn is_ready(&self, flush_rows: usize, flush_age_secs: u64) -> bool {
        self.builder.len() >= flush_rows
            || self.created_at.elapsed().as_secs() >= flush_age_secs
    }

    fn build(&mut self) -> Result<Option<(RecordBatch, Vec<KafkaOffset>)>> {
        match self.builder.build()? {
            Some(batch) => {
                let offsets = std::mem::take(&mut self.offsets);
                self.created_at = Instant::now();
                Ok(Some((batch, offsets)))
            }
            None => Ok(None),
        }
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}

/// Buffer statistics
#[derive(Debug, Clone, Default)]
pub struct ArrowBufferStats {
    pub pending_rows: usize,
    pub pending_bytes: usize,
    pub pending_chunks: usize,
    pub table_count: usize,
}

/// Per-table Arrow buffer manager
///
/// Each destination table (db.table) has its own ArrowBatchBuilder.
/// This ensures schema uniformity within each Arrow RecordBatch.
pub struct BufferManager {
    /// Per-table buffers: key is "db.table"
    buffers: FxHashMap<String, TableBuffer>,
    /// Cached schemas per table
    schemas: FxHashMap<String, TableSchema>,
    /// Batch size per table buffer
    batch_size: usize,
    /// Flush trigger: row count
    flush_rows: usize,
    /// Flush trigger: age in seconds
    flush_age_secs: u64,
    /// Schema refresh interval
    schema_refresh_interval: Duration,
}

impl BufferManager {
    /// Create a new buffer manager with config
    pub fn new(config: &BufferConfig) -> Self {
        Self {
            buffers: FxHashMap::default(),
            schemas: FxHashMap::default(),
            batch_size: config.flush_rows.max(100), // At least 100 per batch
            flush_rows: config.flush_rows,
            flush_age_secs: config.flush_age_secs,
            schema_refresh_interval: Duration::from_secs(60),
        }
    }

    /// Set schema refresh interval
    pub fn with_schema_refresh(mut self, interval: Duration) -> Self {
        self.schema_refresh_interval = interval;
        self
    }

    /// Register a table schema (from ClickHouse introspection)
    pub fn register_schema(&mut self, schema: TableSchema) {
        self.schemas.insert(schema.table_name.clone(), schema);
    }

    /// Get the schema for a table (if cached)
    pub fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }

    /// Check if schema needs refresh
    pub fn schema_needs_refresh(&self, table: &str) -> bool {
        match self.schemas.get(table) {
            Some(schema) => schema.needs_refresh(self.schema_refresh_interval),
            None => true, // No schema = needs fetch
        }
    }

    /// Fetch schema from ClickHouse Arrow client and cache it
    ///
    /// This should be called before first insert to a table.
    pub async fn fetch_and_cache_schema(
        &mut self,
        table: &str,
        arrow_client: &ArrowClickHouseClient,
    ) -> Result<SchemaRef> {
        debug!(table = %table, "Fetching schema from ClickHouse");

        let arrow_schema = arrow_client.fetch_schema(table).await?;

        // Build column types map from Arrow schema
        let column_types: FxHashMap<String, String> = arrow_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), arrow_type_to_ch_hint(f.data_type())))
            .collect();

        let schema = TableSchema {
            table_name: table.to_string(),
            arrow_schema: arrow_schema.clone(),
            column_types,
            last_refresh: Instant::now(),
        };

        self.schemas.insert(table.to_string(), schema);
        debug!(table = %table, columns = arrow_schema.fields().len(), "Schema cached");

        Ok(arrow_schema)
    }

    /// Get or fetch schema for a table
    ///
    /// Uses cached schema if valid, otherwise fetches from ClickHouse.
    pub async fn get_or_fetch_schema(
        &mut self,
        table: &str,
        arrow_client: &ArrowClickHouseClient,
    ) -> Result<SchemaRef> {
        if let Some(schema) = self.schemas.get(table) {
            if !schema.needs_refresh(self.schema_refresh_interval) {
                debug!(table = %table, "Using cached schema");
                return Ok(schema.arrow_schema.clone());
            }
        }

        self.fetch_and_cache_schema(table, arrow_client).await
    }

    /// Push a JSON object to the appropriate table buffer
    ///
    /// The table is determined by the caller (from routing).
    /// Uses get_mut for existing tables (common case) to avoid key allocation.
    #[inline]
    pub fn push(
        &mut self,
        table: &str,
        data: Map<String, Value>,
        offset: Option<KafkaOffset>,
    ) {
        // Fast path: table already exists (common case after first message)
        // Avoids allocating String for HashMap key lookup
        if let Some(buffer) = self.buffers.get_mut(table) {
            buffer.push(data, table, offset);
            return;
        }

        // Slow path: new table - allocate key and create buffer
        let mut buffer = TableBuffer::new(self.batch_size);
        buffer.push(data, table, offset);
        self.buffers.insert(table.to_string(), buffer);
    }

    /// Check if any buffer needs flushing
    pub fn should_flush(&self) -> bool {
        self.buffers.values().any(|buf| {
            buf.is_ready(self.flush_rows, self.flush_age_secs)
        })
    }

    /// Get batches ready for flush
    ///
    /// Returns FlushBatch for each table that's ready.
    /// Uses in-place iteration to avoid intermediate Vec allocation.
    pub fn get_ready_for_flush(&mut self) -> Result<Vec<FlushBatch>> {
        let flush_rows = self.flush_rows;
        let flush_age_secs = self.flush_age_secs;

        // Count ready buffers for pre-allocation
        let ready_count = self.buffers.values()
            .filter(|buf| buf.is_ready(flush_rows, flush_age_secs))
            .count();

        let mut flush_batches = Vec::with_capacity(ready_count);

        // Build batches directly from mutable iterator
        for (table, buffer) in self.buffers.iter_mut() {
            if buffer.is_ready(flush_rows, flush_age_secs) {
                if let Some((batch, offsets)) = buffer.build()? {
                    flush_batches.push(FlushBatch {
                        table: CompactString::from(table.as_str()),
                        batch,
                        offsets,
                    });
                }
            }
        }

        Ok(flush_batches)
    }

    /// Flush all buffers (for shutdown)
    pub fn flush_all(&mut self) -> Result<Vec<FlushBatch>> {
        let mut flush_batches = Vec::with_capacity(self.buffers.len());

        for (table, buffer) in self.buffers.iter_mut() {
            if let Some((batch, offsets)) = buffer.build()? {
                flush_batches.push(FlushBatch {
                    table: CompactString::from(table.as_str()),
                    batch,
                    offsets,
                });
            }
        }

        Ok(flush_batches)
    }

    /// Get buffer statistics
    pub fn stats(&self) -> ArrowBufferStats {
        let mut stats = ArrowBufferStats::default();
        stats.table_count = self.buffers.len();

        for buffer in self.buffers.values() {
            stats.pending_rows += buffer.len();
            stats.pending_chunks += if buffer.is_empty() { 0 } else { 1 };
        }

        stats
    }

    /// Get total pending row count
    pub fn pending_rows(&self) -> usize {
        self.buffers.values().map(|b| b.len()).sum()
    }

    /// Get pending bytes (estimate)
    pub fn pending_bytes(&self) -> usize {
        // Rough estimate: 200 bytes per row average
        self.pending_rows() * 200
    }

    /// Clear all buffers
    pub fn clear(&mut self) {
        self.buffers.clear();
    }

    /// Get list of tables with pending data
    pub fn tables_with_pending(&self) -> Vec<&str> {
        self.buffers
            .iter()
            .filter(|(_, buf)| !buf.is_empty())
            .map(|(table, _)| table.as_str())
            .collect()
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self {
            buffers: FxHashMap::default(),
            schemas: FxHashMap::default(),
            batch_size: 1000,
            flush_rows: 10000,
            flush_age_secs: 5,
            schema_refresh_interval: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::config::BufferConfig;

    fn test_config() -> BufferConfig {
        BufferConfig {
            flush_bytes: 1024 * 1024,
            flush_rows: 5,
            flush_age_secs: 10,
        }
    }

    #[test]
    fn test_buffer_manager_push() {
        let mut manager = BufferManager::new(&test_config());

        let data1 = json!({"id": 1, "name": "foo"}).as_object().unwrap().clone();
        let data2 = json!({"id": 2, "name": "bar"}).as_object().unwrap().clone();

        manager.push("db.table_a", data1, None);
        manager.push("db.table_b", data2, None);

        assert_eq!(manager.pending_rows(), 2);
        assert_eq!(manager.stats().table_count, 2);
    }

    #[test]
    fn test_buffer_manager_flush_threshold() {
        let mut manager = BufferManager::new(&test_config());

        // Push enough rows to trigger flush (5 rows)
        for i in 0..6 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.events", data, None);
        }

        assert!(manager.should_flush());

        let batches = manager.get_ready_for_flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "db.events");
        assert_eq!(batches[0].batch.num_rows(), 6);
    }

    #[test]
    fn test_buffer_manager_per_table_isolation() {
        let mut manager = BufferManager::new(&test_config());

        // Push 3 rows to table_a, 2 rows to table_b
        for i in 0..3 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None);
        }
        for i in 0..2 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_b", data, None);
        }

        // Neither should trigger flush (threshold is 5)
        assert!(!manager.should_flush());
        assert_eq!(manager.pending_rows(), 5);

        // Add more to table_a to trigger
        for i in 3..6 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None);
        }

        assert!(manager.should_flush());

        let batches = manager.get_ready_for_flush().unwrap();
        // Only table_a should flush
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "db.table_a");
        assert_eq!(batches[0].batch.num_rows(), 6);

        // table_b still has 2 pending
        assert_eq!(manager.pending_rows(), 2);
    }

    #[test]
    fn test_buffer_manager_flush_all() {
        let mut manager = BufferManager::new(&test_config());

        // Push to multiple tables
        for i in 0..3 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None);
        }
        for i in 0..2 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_b", data, None);
        }

        // Flush all (shutdown scenario)
        let batches = manager.flush_all().unwrap();
        assert_eq!(batches.len(), 2);

        // All buffers should be empty
        assert_eq!(manager.pending_rows(), 0);
    }

    #[test]
    fn test_ch_type_to_arrow() {
        assert!(matches!(ch_type_to_arrow("Int64").unwrap(), DataType::Int64));
        assert!(matches!(ch_type_to_arrow("String").unwrap(), DataType::Binary));
        assert!(matches!(ch_type_to_arrow("Nullable(Int64)").unwrap(), DataType::Int64));
        assert!(matches!(ch_type_to_arrow("UUID").unwrap(), DataType::FixedSizeBinary(16)));
        assert!(matches!(ch_type_to_arrow("IPv4").unwrap(), DataType::FixedSizeBinary(4)));
        assert!(matches!(ch_type_to_arrow("Bool").unwrap(), DataType::Boolean));
    }

    #[test]
    fn test_table_schema() {
        let columns = vec![
            ("id".to_string(), "Int64".to_string()),
            ("name".to_string(), "String".to_string()),
            ("created_at".to_string(), "DateTime".to_string()),
        ];

        let schema = TableSchema::from_columns("test.events".to_string(), columns).unwrap();

        assert_eq!(schema.table_name, "test.events");
        assert_eq!(schema.arrow_schema.fields().len(), 3);
        assert_eq!(schema.column_types.get("id").unwrap(), "Int64");
    }

    #[test]
    fn test_kafka_offset_tracking() {
        use std::sync::Arc;
        let mut manager = BufferManager::new(&test_config());

        // Use shared topic Arc to avoid redundant allocations
        let topic: Arc<str> = Arc::from("test");
        let offset1 = KafkaOffset::with_shared_topic(topic.clone(), 0, 100);
        let offset2 = KafkaOffset::with_shared_topic(topic, 0, 101);

        // Push with offsets
        let data1 = json!({"id": 1}).as_object().unwrap().clone();
        let data2 = json!({"id": 2}).as_object().unwrap().clone();

        manager.push("db.events", data1, Some(offset1));
        manager.push("db.events", data2, Some(offset2));

        // Add more to trigger flush
        for i in 3..7 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.events", data, None);
        }

        let batches = manager.get_ready_for_flush().unwrap();
        assert_eq!(batches.len(), 1);

        // Should have 2 offsets tracked
        assert_eq!(batches[0].offsets.len(), 2);
        assert_eq!(batches[0].offsets[0].offset, 100);
        assert_eq!(batches[0].offsets[1].offset, 101);
    }
}
