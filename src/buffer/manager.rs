//! Arrow-based buffer manager with schema introspection
//!
//! Manages a single partitioned Arrow buffer where destination table is a column.
//! Schema for each table is fetched from ClickHouse introspection.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::buffer::arrow::{ArrowBuffer, KafkaOffset, PartitionedBatch};
use crate::config::BufferConfig;
use crate::Result;

/// Data ready to be flushed to ClickHouse (Arrow-native)
pub struct FlushBatch {
    /// Destination table name
    pub table: String,
    /// Arrow RecordBatch ready for insert
    pub batch: RecordBatch,
    /// Chunk IDs included in this batch (for acknowledgment)
    pub chunk_ids: Vec<u64>,
}

/// Per-table schema metadata from ClickHouse introspection
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Fully qualified table name (database.table)
    pub table_name: String,
    /// Arrow schema for this table
    pub arrow_schema: SchemaRef,
    /// Column name to ClickHouse type mapping
    pub column_types: HashMap<String, String>,
}

impl TableSchema {
    /// Create a TableSchema from column definitions
    pub fn from_columns(table_name: String, columns: Vec<(String, String)>) -> Result<Self> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut column_types = HashMap::with_capacity(columns.len());

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
        })
    }
}

/// Convert ClickHouse type string to Arrow DataType
///
/// This is used during schema introspection to build Arrow schemas
/// that match the ClickHouse table structure.
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
        "Bool" | "UInt8" => DataType::UInt8,

        // Complex types - handle generically
        t if t.starts_with("DateTime64") => DataType::Int64,
        t if t.starts_with("DateTime") => DataType::Int64,
        t if t.starts_with("Decimal") => DataType::Binary, // Store as bytes
        t if t.starts_with("FixedString") => {
            // Parse FixedString(N)
            let n: i32 = t
                .trim_start_matches("FixedString(")
                .trim_end_matches(')')
                .parse()
                .unwrap_or(256);
            DataType::FixedSizeBinary(n)
        }
        t if t.starts_with("Array") => {
            // Array(T) -> List(T)
            let inner = &t[6..t.len() - 1];
            let inner_type = ch_type_to_arrow(inner)?;
            DataType::List(Arc::new(Field::new("item", inner_type, true)))
        }
        t if t.starts_with("Map") => {
            // Map(K, V) -> Map(K, V)
            DataType::Binary // Simplified for now
        }
        t if t.starts_with("Tuple") => {
            // Tuple(T1, T2, ...) -> Struct
            DataType::Binary // Simplified for now
        }
        t if t.starts_with("LowCardinality") => {
            // LowCardinality(T) - unwrap the inner type
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

/// Arrow-based buffer manager
///
/// Uses a single ArrowBuffer with schema introspection from ClickHouse.
/// Each table's schema is fetched on first use and cached.
pub struct BufferManager {
    /// The underlying Arrow buffer
    buffer: ArrowBuffer,
    /// Cached schemas per table
    schemas: HashMap<String, TableSchema>,
    /// Combined schema including _destination column
    destination_schema: Option<SchemaRef>,
}

impl BufferManager {
    /// Create a new buffer manager with config
    pub fn new(config: &BufferConfig) -> Self {
        Self {
            buffer: ArrowBuffer::new(config.flush_rows, config.flush_bytes, config.flush_age_secs),
            schemas: HashMap::new(),
            destination_schema: None,
        }
    }

    /// Register a table schema (from ClickHouse introspection)
    pub fn register_schema(&mut self, schema: TableSchema) {
        self.schemas.insert(schema.table_name.clone(), schema);
    }

    /// Get the Arrow schema for a table
    pub fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }

    /// Push an Arrow RecordBatch to the buffer
    ///
    /// The batch must include a _destination column for routing.
    pub fn push_batch(&mut self, batch: RecordBatch, offset: Option<KafkaOffset>) -> u64 {
        self.buffer.push(batch, offset)
    }

    /// Check if buffer should flush
    pub fn should_flush(&self) -> bool {
        self.buffer.should_flush()
    }

    /// Get batches ready for flush, partitioned by destination
    ///
    /// Returns FlushBatch structs with Arrow RecordBatches ready for ClickHouse insert.
    pub fn get_ready_for_flush(&mut self) -> Result<Vec<FlushBatch>> {
        if !self.buffer.should_flush() {
            return Ok(Vec::new());
        }

        let (partitioned, chunk_ids) = self.buffer.partition_pending()?;

        // Mark chunks as in-flight
        self.buffer.mark_in_flight(&chunk_ids);

        // Convert to FlushBatch
        let batches = partitioned
            .into_iter()
            .map(|pb| FlushBatch {
                table: pb.table,
                batch: pb.batch,
                chunk_ids: chunk_ids.clone(),
            })
            .collect();

        Ok(batches)
    }

    /// Acknowledge successful insert of chunks
    ///
    /// Returns Kafka offsets for acknowledgment.
    pub fn ack_chunks(&mut self, chunk_ids: &[u64]) -> Vec<KafkaOffset> {
        chunk_ids
            .iter()
            .filter_map(|id| self.buffer.ack(*id))
            .collect()
    }

    /// Mark chunks as failed (for retry)
    pub fn fail_chunks(&mut self, chunk_ids: &[u64]) {
        for id in chunk_ids {
            self.buffer.fail(*id);
        }
    }

    /// Reset failed chunks for retry
    pub fn retry_failed(&mut self) {
        self.buffer.retry_failed();
    }

    /// Get all pending batches (for shutdown flush)
    pub fn flush_all(&mut self) -> Result<Vec<FlushBatch>> {
        let (partitioned, chunk_ids) = self.buffer.partition_pending()?;

        if partitioned.is_empty() {
            return Ok(Vec::new());
        }

        self.buffer.mark_in_flight(&chunk_ids);

        let batches = partitioned
            .into_iter()
            .map(|pb| FlushBatch {
                table: pb.table,
                batch: pb.batch,
                chunk_ids: chunk_ids.clone(),
            })
            .collect();

        Ok(batches)
    }

    /// Get buffer statistics
    pub fn stats(&self) -> crate::buffer::ArrowBufferStats {
        self.buffer.stats()
    }

    /// Get total pending row count
    pub fn pending_rows(&self) -> usize {
        self.buffer.pending_rows()
    }

    /// Get total pending bytes
    pub fn pending_bytes(&self) -> usize {
        self.buffer.pending_bytes()
    }

    /// Clear the buffer (for shutdown)
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self {
            buffer: ArrowBuffer::default(),
            schemas: HashMap::new(),
            destination_schema: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use crate::config::BufferConfig;

    fn create_test_batch(destinations: &[&str], values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("_destination", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int64, false)),
        ]));

        let dest_array = StringArray::from(destinations.to_vec());
        let value_array = Int64Array::from(values.to_vec());

        RecordBatch::try_new(
            schema,
            vec![Arc::new(dest_array), Arc::new(value_array)],
        )
        .unwrap()
    }

    fn test_config() -> BufferConfig {
        BufferConfig {
            flush_bytes: 1024 * 1024,
            flush_rows: 5,
            flush_age_secs: 10,
        }
    }

    #[test]
    fn test_buffer_manager_basic() {
        let mut manager = BufferManager::new(&test_config());

        let batch = create_test_batch(&["table_a", "table_b"], &[1, 2]);
        manager.push_batch(batch, None);

        assert_eq!(manager.pending_rows(), 2);
    }

    #[test]
    fn test_buffer_manager_flush() {
        let mut manager = BufferManager::new(&test_config());

        // Add enough rows to trigger flush
        let batch = create_test_batch(
            &["t", "t", "t", "t", "t", "t"],
            &[1, 2, 3, 4, 5, 6],
        );
        manager.push_batch(batch, None);

        assert!(manager.should_flush());

        let batches = manager.get_ready_for_flush().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "t");
        assert_eq!(batches[0].batch.num_rows(), 6);
    }

    #[test]
    fn test_buffer_manager_partition() {
        let mut manager = BufferManager::new(&test_config());

        // Add rows for multiple tables
        let batch = create_test_batch(
            &["table_a", "table_b", "table_a", "table_b", "table_a", "table_b"],
            &[1, 2, 3, 4, 5, 6],
        );
        manager.push_batch(batch, None);

        let batches = manager.flush_all().unwrap();
        assert_eq!(batches.len(), 2);

        // Each table should have 3 rows
        for fb in &batches {
            assert_eq!(fb.batch.num_rows(), 3);
        }
    }

    #[test]
    fn test_ch_type_to_arrow() {
        assert!(matches!(ch_type_to_arrow("Int64").unwrap(), DataType::Int64));
        assert!(matches!(ch_type_to_arrow("String").unwrap(), DataType::Binary));
        assert!(matches!(ch_type_to_arrow("Nullable(Int64)").unwrap(), DataType::Int64));
        assert!(matches!(ch_type_to_arrow("UUID").unwrap(), DataType::FixedSizeBinary(16)));
        assert!(matches!(ch_type_to_arrow("IPv4").unwrap(), DataType::FixedSizeBinary(4)));
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
}
