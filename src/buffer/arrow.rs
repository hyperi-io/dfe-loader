//! Arrow-based chunked buffer for ClickHouse inserts
//!
//! Uses Arrow RecordBatch as the fundamental unit of data, with chunked
//! lifecycle management for efficient at-least-once delivery.
//!
//! ## Design Philosophy
//!
//! Instead of per-table buffers that accumulate rows, we use a single
//! partitioned Arrow buffer where:
//!
//! 1. Each Kafka batch becomes one immutable Arrow chunk
//! 2. Destination table is encoded as a column (_destination)
//! 3. Kafka offset metadata is tracked per-chunk for acknowledgment
//! 4. At flush time, chunks are partitioned by _destination
//! 5. On successful insert, the chunk reference is dropped (no row-level cleanup)
//!
//! ## Performance Benefits
//!
//! - **No row-level removal**: Drop whole chunks on ack, O(1) memory free
//! - **Efficient partitioning**: Arrow's columnar format enables fast group-by
//! - **Zero-copy to ClickHouse**: Arrow → ClickHouse native format via clickhouse-arrow
//! - **Unified format**: JSON and MessagePack both deserialize to Arrow
//! - **Memory locality**: All data from one Kafka batch stays together

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema, SchemaRef};

use crate::Result;

/// Kafka offset metadata for a chunk
#[derive(Debug, Clone)]
pub struct KafkaOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// State of a chunk in the buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// Chunk is buffered, waiting for flush
    Pending,
    /// Chunk is being written to ClickHouse
    InFlight,
    /// Chunk has been successfully inserted and acked
    Acked,
    /// Chunk failed to insert
    Failed,
}

/// An immutable Arrow chunk with metadata
///
/// This is the fundamental unit of data in the buffer. Each chunk is:
/// - Created from a Kafka batch
/// - Contains multiple rows with a _destination column for routing
/// - Tracked by Kafka offset for acknowledgment
/// - Dropped entirely on successful insert (no row-level cleanup)
#[derive(Debug)]
pub struct ArrowChunk {
    /// Unique ID for this chunk
    id: u64,
    /// The Arrow data
    batch: RecordBatch,
    /// Kafka offset for this chunk (for at-least-once acknowledgment)
    offset: Option<KafkaOffset>,
    /// Current state of the chunk
    state: ChunkState,
    /// Time this chunk was created
    created_at: Instant,
    /// Row count (cached for efficiency)
    row_count: usize,
}

impl ArrowChunk {
    /// Create a new chunk from a RecordBatch
    pub fn new(id: u64, batch: RecordBatch, offset: Option<KafkaOffset>) -> Self {
        let row_count = batch.num_rows();
        Self {
            id,
            batch,
            offset,
            state: ChunkState::Pending,
            created_at: Instant::now(),
            row_count,
        }
    }

    /// Get the chunk ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the Arrow RecordBatch
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Get the Kafka offset if set
    pub fn offset(&self) -> Option<&KafkaOffset> {
        self.offset.as_ref()
    }

    /// Get the current state
    pub fn state(&self) -> ChunkState {
        self.state
    }

    /// Set the state
    pub fn set_state(&mut self, state: ChunkState) {
        self.state = state;
    }

    /// Get age since creation in seconds
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Estimate memory size in bytes
    pub fn memory_size(&self) -> usize {
        // Rough estimate based on Arrow internals
        self.batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum()
    }
}

/// Result of partitioning a chunk by destination table
pub struct PartitionedBatch {
    /// Destination table name
    pub table: String,
    /// The Arrow RecordBatch for this table (without _destination column)
    pub batch: RecordBatch,
}

/// Arrow-based chunked buffer
///
/// Manages a collection of Arrow chunks with:
/// - Efficient partition-by-destination at flush time
/// - Chunk-level lifecycle (pending → in_flight → acked/failed)
/// - Memory tracking and flush thresholds
pub struct ArrowBuffer {
    /// All chunks, keyed by chunk ID
    chunks: HashMap<u64, ArrowChunk>,
    /// Next chunk ID to assign
    next_id: u64,
    /// Name of the destination column
    destination_column: String,
    /// Flush thresholds
    flush_rows: usize,
    flush_bytes: usize,
    flush_age_secs: u64,
}

impl ArrowBuffer {
    /// Create a new Arrow buffer
    pub fn new(flush_rows: usize, flush_bytes: usize, flush_age_secs: u64) -> Self {
        Self {
            chunks: HashMap::new(),
            next_id: 0,
            destination_column: "_destination".to_string(),
            flush_rows,
            flush_bytes,
            flush_age_secs,
        }
    }

    /// Add a chunk to the buffer
    ///
    /// Returns the assigned chunk ID for later acknowledgment.
    pub fn push(&mut self, batch: RecordBatch, offset: Option<KafkaOffset>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        let chunk = ArrowChunk::new(id, batch, offset);
        self.chunks.insert(id, chunk);

        id
    }

    /// Get total pending row count
    pub fn pending_rows(&self) -> usize {
        self.chunks
            .values()
            .filter(|c| c.state == ChunkState::Pending)
            .map(|c| c.row_count())
            .sum()
    }

    /// Get total pending memory size in bytes
    pub fn pending_bytes(&self) -> usize {
        self.chunks
            .values()
            .filter(|c| c.state == ChunkState::Pending)
            .map(|c| c.memory_size())
            .sum()
    }

    /// Get oldest pending chunk age in seconds
    pub fn oldest_pending_age(&self) -> u64 {
        self.chunks
            .values()
            .filter(|c| c.state == ChunkState::Pending)
            .map(|c| c.age_secs())
            .max()
            .unwrap_or(0)
    }

    /// Check if buffer should flush based on thresholds
    pub fn should_flush(&self) -> bool {
        let pending_rows = self.pending_rows();
        if pending_rows == 0 {
            return false;
        }

        pending_rows >= self.flush_rows
            || self.pending_bytes() >= self.flush_bytes
            || self.oldest_pending_age() >= self.flush_age_secs
    }

    /// Get pending chunk IDs ready for flush
    pub fn get_pending_chunk_ids(&self) -> Vec<u64> {
        self.chunks
            .iter()
            .filter(|(_, c)| c.state == ChunkState::Pending)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Mark chunks as in-flight (being written to ClickHouse)
    pub fn mark_in_flight(&mut self, chunk_ids: &[u64]) {
        for id in chunk_ids {
            if let Some(chunk) = self.chunks.get_mut(id) {
                chunk.set_state(ChunkState::InFlight);
            }
        }
    }

    /// Mark a chunk as successfully acked
    ///
    /// This removes the chunk from the buffer, freeing memory immediately.
    /// Returns the Kafka offset for acknowledgment if present.
    pub fn ack(&mut self, chunk_id: u64) -> Option<KafkaOffset> {
        self.chunks.remove(&chunk_id).and_then(|c| c.offset)
    }

    /// Mark a chunk as failed
    ///
    /// The chunk stays in the buffer for retry. Returns false if chunk not found.
    pub fn fail(&mut self, chunk_id: u64) -> bool {
        if let Some(chunk) = self.chunks.get_mut(&chunk_id) {
            chunk.set_state(ChunkState::Failed);
            true
        } else {
            false
        }
    }

    /// Reset failed chunks back to pending for retry
    pub fn retry_failed(&mut self) {
        for chunk in self.chunks.values_mut() {
            if chunk.state == ChunkState::Failed {
                chunk.set_state(ChunkState::Pending);
            }
        }
    }

    /// Partition pending chunks by destination table
    ///
    /// This is the core operation at flush time. It:
    /// 1. Concatenates all pending chunks
    /// 2. Groups rows by _destination column
    /// 3. Returns separate RecordBatches per table (without _destination column)
    ///
    /// Returns the partitioned batches and the chunk IDs that were included.
    pub fn partition_pending(&self) -> Result<(Vec<PartitionedBatch>, Vec<u64>)> {
        let pending_chunks: Vec<_> = self
            .chunks
            .iter()
            .filter(|(_, c)| c.state == ChunkState::Pending)
            .collect();

        if pending_chunks.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let chunk_ids: Vec<u64> = pending_chunks.iter().map(|(id, _)| **id).collect();

        // Collect all batches
        let batches: Vec<&RecordBatch> = pending_chunks.iter().map(|(_, c)| c.batch()).collect();

        // Partition by destination
        let partitioned = self.partition_by_destination(&batches)?;

        Ok((partitioned, chunk_ids))
    }

    /// Partition batches by destination column
    fn partition_by_destination(&self, batches: &[&RecordBatch]) -> Result<Vec<PartitionedBatch>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Group rows by destination
        let mut table_rows: HashMap<String, Vec<(&RecordBatch, usize)>> = HashMap::new();

        for batch in batches {
            // Find destination column
            let dest_idx = batch
                .schema()
                .index_of(&self.destination_column)
                .map_err(|_| {
                    crate::Error::Buffer(format!(
                        "Missing {} column in batch",
                        self.destination_column
                    ))
                })?;

            let dest_array = batch
                .column(dest_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::Error::Buffer(format!(
                        "{} column must be String type",
                        self.destination_column
                    ))
                })?;

            // Group row indices by destination
            for row_idx in 0..batch.num_rows() {
                if let Some(dest) = dest_array.value(row_idx).into() {
                    table_rows
                        .entry(dest.to_string())
                        .or_default()
                        .push((*batch, row_idx));
                }
            }
        }

        // Build output batches per table
        let mut result = Vec::with_capacity(table_rows.len());

        for (table, rows) in table_rows {
            if rows.is_empty() {
                continue;
            }

            // Get schema without destination column
            let first_batch = rows[0].0;
            let output_schema = self.schema_without_destination(first_batch.schema())?;

            // Build arrays by filtering and copying rows
            let batch = self.build_filtered_batch(first_batch.schema(), &output_schema, &rows)?;

            result.push(PartitionedBatch { table, batch });
        }

        Ok(result)
    }

    /// Create schema without the destination column
    fn schema_without_destination(&self, schema: SchemaRef) -> Result<SchemaRef> {
        let fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .filter(|f| f.name() != &self.destination_column)
            .cloned()
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Build a filtered RecordBatch from selected rows
    fn build_filtered_batch(
        &self,
        _input_schema: SchemaRef,
        output_schema: &SchemaRef,
        rows: &[(&RecordBatch, usize)],
    ) -> Result<RecordBatch> {
        use arrow::compute::take;

        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }

        // For simplicity, we assume all rows come from the same batch structure
        // In production, we'd handle heterogeneous batches
        let first_batch = rows[0].0;

        // Build indices array
        let indices: Vec<u64> = rows.iter().map(|(_, idx)| *idx as u64).collect();
        let indices_array = arrow::array::UInt64Array::from(indices);

        // Take rows from each column (excluding destination)
        let columns: Result<Vec<ArrayRef>> = first_batch
            .columns()
            .iter()
            .zip(first_batch.schema().fields())
            .filter(|(_, field)| field.name() != &self.destination_column)
            .map(|(col, _)| {
                take(col, &indices_array, None)
                    .map_err(|e| crate::Error::Buffer(format!("Arrow take error: {}", e)))
            })
            .collect();

        let columns = columns?;

        RecordBatch::try_new(output_schema.clone(), columns)
            .map_err(|e| crate::Error::Buffer(format!("RecordBatch creation error: {}", e)))
    }

    /// Get buffer statistics
    pub fn stats(&self) -> ArrowBufferStats {
        let pending = self
            .chunks
            .values()
            .filter(|c| c.state == ChunkState::Pending)
            .count();
        let in_flight = self
            .chunks
            .values()
            .filter(|c| c.state == ChunkState::InFlight)
            .count();
        let failed = self
            .chunks
            .values()
            .filter(|c| c.state == ChunkState::Failed)
            .count();

        ArrowBufferStats {
            total_chunks: self.chunks.len(),
            pending_chunks: pending,
            in_flight_chunks: in_flight,
            failed_chunks: failed,
            pending_rows: self.pending_rows(),
            pending_bytes: self.pending_bytes(),
        }
    }

    /// Clear all chunks (for shutdown)
    pub fn clear(&mut self) {
        self.chunks.clear();
    }
}

/// Statistics about the Arrow buffer
#[derive(Debug, Clone)]
pub struct ArrowBufferStats {
    pub total_chunks: usize,
    pub pending_chunks: usize,
    pub in_flight_chunks: usize,
    pub failed_chunks: usize,
    pub pending_rows: usize,
    pub pending_bytes: usize,
}

impl Default for ArrowBuffer {
    fn default() -> Self {
        Self::new(10_000, 10 * 1024 * 1024, 5) // 10K rows, 10MB, 5 seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(destinations: &[&str], values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_destination", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let dest_array = StringArray::from(destinations.to_vec());
        let value_array = Int64Array::from(values.to_vec());

        RecordBatch::try_new(
            schema,
            vec![Arc::new(dest_array), Arc::new(value_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_arrow_buffer_basic() {
        let mut buffer = ArrowBuffer::new(100, 1024 * 1024, 5);

        let batch = create_test_batch(&["table_a", "table_b", "table_a"], &[1, 2, 3]);
        let chunk_id = buffer.push(batch, None);

        assert_eq!(chunk_id, 0);
        assert_eq!(buffer.pending_rows(), 3);
    }

    #[test]
    fn test_arrow_buffer_partition() {
        let mut buffer = ArrowBuffer::new(100, 1024 * 1024, 5);

        let batch = create_test_batch(
            &["table_a", "table_b", "table_a", "table_b"],
            &[1, 2, 3, 4],
        );
        buffer.push(batch, None);

        let (partitioned, chunk_ids) = buffer.partition_pending().unwrap();

        assert_eq!(chunk_ids.len(), 1);
        assert_eq!(partitioned.len(), 2);

        // Check that each table got its rows
        for pb in &partitioned {
            match pb.table.as_str() {
                "table_a" => assert_eq!(pb.batch.num_rows(), 2),
                "table_b" => assert_eq!(pb.batch.num_rows(), 2),
                _ => panic!("Unexpected table"),
            }
        }
    }

    #[test]
    fn test_arrow_buffer_ack() {
        let mut buffer = ArrowBuffer::new(100, 1024 * 1024, 5);

        let batch = create_test_batch(&["table_a"], &[1]);
        let offset = KafkaOffset {
            topic: "test".to_string(),
            partition: 0,
            offset: 42,
        };
        let chunk_id = buffer.push(batch, Some(offset));

        // Ack the chunk
        let returned_offset = buffer.ack(chunk_id);
        assert!(returned_offset.is_some());
        assert_eq!(returned_offset.unwrap().offset, 42);

        // Buffer should be empty
        assert_eq!(buffer.pending_rows(), 0);
    }

    #[test]
    fn test_arrow_buffer_fail_retry() {
        let mut buffer = ArrowBuffer::new(100, 1024 * 1024, 5);

        let batch = create_test_batch(&["table_a"], &[1]);
        let chunk_id = buffer.push(batch, None);

        // Mark as failed
        buffer.fail(chunk_id);
        assert_eq!(buffer.chunks.get(&chunk_id).unwrap().state(), ChunkState::Failed);

        // Retry should reset to pending
        buffer.retry_failed();
        assert_eq!(buffer.chunks.get(&chunk_id).unwrap().state(), ChunkState::Pending);
    }

    #[test]
    fn test_arrow_buffer_should_flush() {
        let mut buffer = ArrowBuffer::new(5, 1024 * 1024, 60); // 5 rows threshold

        // Under threshold
        let batch = create_test_batch(&["t", "t", "t"], &[1, 2, 3]);
        buffer.push(batch, None);
        assert!(!buffer.should_flush());

        // Over threshold
        let batch2 = create_test_batch(&["t", "t", "t"], &[4, 5, 6]);
        buffer.push(batch2, None);
        assert!(buffer.should_flush());
    }
}
