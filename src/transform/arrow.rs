//! JSON/MessagePack to Arrow conversion for the pipeline
//!
//! Provides high-performance batch conversion using arrow-json's SIMD-accelerated
//! parsing. Batches multiple messages before conversion for efficiency.
//!
//! ## SIMD Optimization
//!
//! The arrow-json crate uses SIMD instructions for fast JSON parsing. We leverage this through:
//!
//! 1. `RawReaderBuilder` for direct bytes-to-Arrow conversion when schema is known
//! 2. Batch processing to amortize parsing overhead
//! 3. Pre-computed schema to avoid repeated inference
//!
//! ## Performance Hierarchy (fastest to slowest)
//!
//! 1. `json_bytes_to_arrow_simd()` - Direct SIMD bytes→Arrow with known schema
//! 2. `json_batch_to_arrow()` - Batch conversion with schema inference
//! 3. `json_to_arrow_batch()` - Single-row conversion (avoid in hot path)

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use serde_json::{Map, Value};
use tracing::debug;

use crate::Result;

/// Batch builder that accumulates messages before Arrow conversion
///
/// This is the proper Arrow approach - batch multiple messages then
/// convert to a single RecordBatch for columnar efficiency.
///
/// ## Per-Table Design
///
/// Each ArrowBatchBuilder is used for a single destination table.
/// The destination is stored once, not per-row, avoiding redundant allocations.
pub struct ArrowBatchBuilder {
    /// Accumulated JSON objects (destination is common for all rows)
    pending: Vec<Map<String, Value>>,
    /// Common destination for all rows (stored once, not per-row)
    destination: Option<Arc<str>>,
    /// Target batch size before auto-flush
    batch_size: usize,
}

impl ArrowBatchBuilder {
    /// Create a new batch builder
    pub fn new(batch_size: usize) -> Self {
        Self {
            pending: Vec::with_capacity(batch_size),
            destination: None,
            batch_size,
        }
    }

    /// Add a JSON object to the batch
    ///
    /// The destination is stored once on first push, not per-row.
    /// Subsequent pushes should use the same destination (per-table buffer design).
    #[inline]
    pub fn push(&mut self, data: Map<String, Value>, destination: &str) {
        // Store destination once on first push
        if self.destination.is_none() {
            self.destination = Some(Arc::from(destination));
        }
        self.pending.push(data);
    }

    /// Check if batch is ready for conversion (reached target size)
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.pending.len() >= self.batch_size
    }

    /// Get current pending count
    #[inline]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Build Arrow RecordBatch from accumulated messages
    ///
    /// Clears the pending buffer after conversion.
    pub fn build(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let dest = self.destination.as_ref()
            .map(|d| d.as_ref())
            .unwrap_or("unknown");

        // Pass pending directly - avoid intermediate Vec allocation
        let batch = json_batch_to_arrow_direct(&self.pending, dest)?;
        self.pending.clear();
        // Keep destination for next batch (same table)

        Ok(Some(batch))
    }

    /// Force build even if not at target size
    #[inline]
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        self.build()
    }
}

/// Convert a single JSON object to an Arrow RecordBatch with a _destination column
///
/// This creates a single-row batch. For efficiency, prefer using ArrowBatchBuilder
/// to batch multiple messages before conversion.
pub fn json_to_arrow_batch(data: &Map<String, Value>, destination: &str) -> Result<RecordBatch> {
    // Build schema from JSON keys + _destination
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(data.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data.len() + 1);

    // Add _destination column first
    fields.push(Arc::new(Field::new("_destination", DataType::Utf8, false)));
    let mut dest_builder = StringBuilder::new();
    dest_builder.append_value(destination);
    columns.push(Arc::new(dest_builder.finish()));

    // Add data columns
    for (key, value) in data {
        let (field, array) = json_value_to_arrow(key, value)?;
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::Error::Transform(format!("Failed to create RecordBatch: {}", e)))
}

/// Convert multiple JSON objects to a single Arrow RecordBatch
///
/// This is the efficient batch conversion - all objects converted at once
/// into columnar format. All objects should have the same structure (schema).
pub fn json_batch_to_arrow(
    rows: &[(&Map<String, Value>, &str)], // (data, destination)
) -> Result<RecordBatch> {
    if rows.is_empty() {
        // Return empty batch with just _destination column
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "_destination",
            DataType::Utf8,
            false,
        ))]));
        return Ok(RecordBatch::new_empty(schema));
    }

    // Determine schema from first row
    let (first_data, _) = rows[0];
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(first_data.len() + 1);

    // _destination column
    fields.push(Arc::new(Field::new("_destination", DataType::Utf8, false)));

    // Data columns - infer type from first row and cache it
    let mut column_types: Vec<(&str, DataType)> = Vec::with_capacity(first_data.len());
    for (key, value) in first_data {
        let data_type = infer_arrow_type(value);
        fields.push(Arc::new(Field::new(key, data_type.clone(), true)));
        column_types.push((key.as_str(), data_type));
    }

    let schema = Arc::new(Schema::new(fields));

    // Build columns
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(column_types.len() + 1);

    // Build _destination column
    let mut dest_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    for (_, dest) in rows {
        dest_builder.append_value(*dest);
    }
    columns.push(Arc::new(dest_builder.finish()));

    // Build data columns using cached types - iterate directly without intermediate Vec
    for (key, data_type) in &column_types {
        let array = build_column_from_json_iter(
            rows.iter().map(|(data, _)| data.get(*key)),
            rows.len(),
            data_type,
        )?;
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::Error::Transform(format!("Failed to create RecordBatch: {}", e)))
}

/// Convert multiple JSON objects to Arrow RecordBatch with shared destination
///
/// Optimized version for per-table buffers where all rows share the same destination.
/// Avoids intermediate Vec allocation by taking direct slice reference.
#[inline]
fn json_batch_to_arrow_direct(
    pending: &[Map<String, Value>],
    destination: &str,
) -> Result<RecordBatch> {
    if pending.is_empty() {
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "_destination",
            DataType::Utf8,
            false,
        ))]));
        return Ok(RecordBatch::new_empty(schema));
    }

    // Determine schema from first row
    let first_data = &pending[0];
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(first_data.len() + 1);

    // _destination column
    fields.push(Arc::new(Field::new("_destination", DataType::Utf8, false)));

    // Data columns - infer type from first row and cache it
    let mut column_types: Vec<(&str, DataType)> = Vec::with_capacity(first_data.len());
    for (key, value) in first_data {
        let data_type = infer_arrow_type(value);
        fields.push(Arc::new(Field::new(key, data_type.clone(), true)));
        column_types.push((key.as_str(), data_type));
    }

    let schema = Arc::new(Schema::new(fields));

    // Build columns
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(column_types.len() + 1);

    // Build _destination column - all rows share same destination
    let mut dest_builder = StringBuilder::with_capacity(pending.len(), pending.len() * destination.len());
    for _ in pending {
        dest_builder.append_value(destination);
    }
    columns.push(Arc::new(dest_builder.finish()));

    // Build data columns using cached types - iterate directly without intermediate Vec
    for (key, data_type) in &column_types {
        let array = build_column_from_json_iter(
            pending.iter().map(|data| data.get(*key)),
            pending.len(),
            data_type,
        )?;
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::Error::Transform(format!("Failed to create RecordBatch: {}", e)))
}

/// Try to use arrow-json for high-performance conversion
///
/// Falls back to manual conversion if schema inference fails.
pub fn json_to_arrow_with_schema(
    json_strings: &[&str],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    // Use arrow-json ReaderBuilder for SIMD-accelerated parsing
    let json_concat = json_strings.join("\n");
    let cursor = std::io::Cursor::new(json_concat.as_bytes());

    let reader = ReaderBuilder::new(schema)
        .build(cursor)
        .map_err(|e| crate::Error::Transform(format!("Failed to create JSON reader: {}", e)))?;

    // Read all batches (should be just one with our input)
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| crate::Error::Transform(format!("Failed to read JSON: {}", e)))?;

    if batches.is_empty() {
        return Err(crate::Error::Transform("No batches produced".into()));
    }

    // Concatenate if multiple batches (shouldn't happen normally)
    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| crate::Error::Transform(format!("Failed to concat batches: {}", e)))
    }
}

/// SIMD-accelerated JSON bytes to Arrow conversion
///
/// This is the fastest path when you have:
/// 1. Raw JSON bytes (not already parsed to serde_json::Value)
/// 2. A known Arrow schema
///
/// Uses arrow-json's internal SIMD parsing for maximum throughput.
///
/// # Arguments
/// * `json_bytes` - Newline-delimited JSON records as bytes
/// * `schema` - Pre-computed Arrow schema (should match JSON structure)
///
/// # Example
/// ```ignore
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]));
/// let json = b"{\"id\": 1, \"name\": \"test\"}\n{\"id\": 2, \"name\": \"test2\"}";
/// let batch = json_bytes_to_arrow_simd(json, schema)?;
/// ```
pub fn json_bytes_to_arrow_simd(json_bytes: &[u8], schema: SchemaRef) -> Result<RecordBatch> {
    if json_bytes.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let cursor = std::io::Cursor::new(json_bytes);

    // ReaderBuilder uses SIMD internally via lexical-core
    let reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(65536) // Large batch size for efficiency
        .build(cursor)
        .map_err(|e| crate::Error::Transform(format!("SIMD JSON reader error: {}", e)))?;

    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| crate::Error::Transform(format!("SIMD JSON parse error: {}", e)))?;

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| crate::Error::Transform(format!("Failed to concat batches: {}", e)))
    }
}

/// Infer Arrow schema from JSON bytes
///
/// Uses arrow-json's schema inference on a sample of records.
/// Cache the result for repeated use with `json_bytes_to_arrow_simd()`.
pub fn infer_schema_from_json_bytes(json_bytes: &[u8], max_records: usize) -> Result<SchemaRef> {
    if json_bytes.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }

    // Parse JSON lines for schema inference
    let values: Vec<serde_json::Value> = json_bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .take(max_records)
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect();

    if values.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }

    // Use arrow-json's schema inference
    let schema = infer_json_schema_from_iterator(values.iter().map(Ok))
        .map_err(|e| crate::Error::Transform(format!("Schema inference error: {}", e)))?;

    debug!(fields = schema.fields().len(), "Inferred schema from JSON");
    Ok(Arc::new(schema))
}

/// High-performance batch builder using SIMD JSON parsing
///
/// Unlike `ArrowBatchBuilder` which stores parsed `serde_json::Value` objects,
/// this builder accumulates raw JSON bytes and parses them all at once using
/// SIMD-accelerated arrow-json.
///
/// ## When to use
///
/// - When you receive raw JSON bytes from Kafka (no MessagePack)
/// - When you have a known or cached schema
/// - When maximum throughput is required
pub struct SimdBatchBuilder {
    /// Raw JSON bytes (newline-separated records)
    buffer: Vec<u8>,
    /// Cached schema (inferred or provided)
    schema: Option<SchemaRef>,
    /// Number of records in buffer
    record_count: usize,
    /// Target batch size
    batch_size: usize,
    /// Common destination for all rows (stored once, not per-row)
    destination: Option<Arc<str>>,
}

impl SimdBatchBuilder {
    /// Create a new SIMD batch builder
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(batch_size * 256), // Estimate 256 bytes per record
            schema: None,
            record_count: 0,
            batch_size,
            destination: None,
        }
    }

    /// Create with a pre-computed schema (faster - no inference needed)
    pub fn with_schema(batch_size: usize, schema: SchemaRef) -> Self {
        Self {
            buffer: Vec::with_capacity(batch_size * 256),
            schema: Some(schema),
            record_count: 0,
            batch_size,
            destination: None,
        }
    }

    /// Add raw JSON bytes to the batch
    ///
    /// The JSON should be a single complete object (no newline at end).
    /// Destination is stored once on first push (per-table buffer design).
    #[inline]
    pub fn push_bytes(&mut self, json_bytes: &[u8], destination: &str) {
        // Store destination once on first push
        if self.destination.is_none() {
            self.destination = Some(Arc::from(destination));
        }
        if !self.buffer.is_empty() {
            self.buffer.push(b'\n');
        }
        self.buffer.extend_from_slice(json_bytes);
        self.record_count += 1;
    }

    /// Check if batch is ready
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.record_count >= self.batch_size
    }

    /// Get current record count
    #[inline]
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Build Arrow RecordBatch using SIMD parsing
    ///
    /// Infers schema if not provided, then uses SIMD parsing for the data.
    pub fn build(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // Infer schema if not cached
        let schema = match &self.schema {
            Some(s) => s.clone(),
            None => {
                let inferred = infer_schema_from_json_bytes(&self.buffer, 100)?;
                self.schema = Some(inferred.clone());
                inferred
            }
        };

        // Parse data using SIMD
        let data_batch = json_bytes_to_arrow_simd(&self.buffer, schema.clone())?;

        // Add _destination column - use shared destination
        let dest = self.destination.as_ref()
            .map(|d| d.as_ref())
            .unwrap_or("unknown");
        let batch = add_destination_column_shared(data_batch, dest, self.record_count)?;

        // Clear buffer but keep destination for next batch (same table)
        self.buffer.clear();
        self.record_count = 0;

        Ok(Some(batch))
    }
}

/// Add a _destination column with shared value (all rows have same destination)
///
/// Optimized for per-table buffers where all rows share the same destination.
#[inline]
fn add_destination_column_shared(batch: RecordBatch, destination: &str, row_count: usize) -> Result<RecordBatch> {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("_destination", DataType::Utf8, false))];
    fields.extend(batch.schema().fields().iter().cloned());

    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);

    // Build _destination column - same value for all rows
    let mut dest_builder = StringBuilder::with_capacity(row_count, row_count * destination.len());
    for _ in 0..row_count {
        dest_builder.append_value(destination);
    }
    columns.push(Arc::new(dest_builder.finish()));

    // Add existing columns
    columns.extend(batch.columns().iter().cloned());

    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::Error::Transform(format!("Failed to add destination column: {}", e)))
}

/// Convert a single JSON value to an Arrow field and array
fn json_value_to_arrow(key: &str, value: &Value) -> Result<(Arc<Field>, ArrayRef)> {
    match value {
        Value::Null => {
            // Default to string for null values
            let field = Arc::new(Field::new(key, DataType::Utf8, true));
            let mut builder = StringBuilder::new();
            builder.append_null();
            Ok((field, Arc::new(builder.finish())))
        }
        Value::Bool(b) => {
            let field = Arc::new(Field::new(key, DataType::Boolean, true));
            let mut builder = BooleanBuilder::new();
            builder.append_value(*b);
            Ok((field, Arc::new(builder.finish())))
        }
        Value::Number(n) => {
            if n.is_i64() {
                let field = Arc::new(Field::new(key, DataType::Int64, true));
                let mut builder = Int64Builder::new();
                builder.append_value(n.as_i64().unwrap());
                Ok((field, Arc::new(builder.finish())))
            } else {
                let field = Arc::new(Field::new(key, DataType::Float64, true));
                let mut builder = Float64Builder::new();
                builder.append_value(n.as_f64().unwrap());
                Ok((field, Arc::new(builder.finish())))
            }
        }
        Value::String(s) => {
            let field = Arc::new(Field::new(key, DataType::Utf8, true));
            let mut builder = StringBuilder::new();
            builder.append_value(s);
            Ok((field, Arc::new(builder.finish())))
        }
        Value::Array(_) | Value::Object(_) => {
            // Serialize complex types as JSON binary
            let field = Arc::new(Field::new(key, DataType::Binary, true));
            let mut builder = BinaryBuilder::new();
            let json_bytes = serde_json::to_vec(value)
                .map_err(|e| crate::Error::Transform(format!("Failed to serialize JSON: {}", e)))?;
            builder.append_value(&json_bytes);
            Ok((field, Arc::new(builder.finish())))
        }
    }
}

/// Infer Arrow DataType from JSON value
fn infer_arrow_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Utf8,
        Value::Bool(_) => DataType::Boolean,
        Value::Number(n) if n.is_i64() => DataType::Int64,
        Value::Number(_) => DataType::Float64,
        Value::String(_) => DataType::Utf8,
        Value::Array(_) | Value::Object(_) => DataType::Binary,
    }
}

/// Build an Arrow column from an iterator of JSON values
///
/// Avoids intermediate Vec allocation by iterating directly.
#[inline]
fn build_column_from_json_iter<'a>(
    values: impl Iterator<Item = Option<&'a Value>>,
    len: usize,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(len);
            for v in values {
                match v {
                    Some(Value::Bool(b)) => builder.append_value(*b),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(len);
            for v in values {
                match v {
                    Some(Value::Number(n)) if n.is_i64() => {
                        builder.append_value(n.as_i64().unwrap())
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(len);
            for v in values {
                match v {
                    Some(Value::Number(n)) => builder.append_value(n.as_f64().unwrap_or(0.0)),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(len, len * 64);
            for v in values {
                match v {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        // Convert non-strings to string representation
                        builder.append_value(other.to_string())
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(len, len * 128);
            for v in values {
                match v {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(val) => {
                        let json_bytes = serde_json::to_vec(val).unwrap_or_default();
                        builder.append_value(&json_bytes);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Default to binary for unknown types
            let mut builder = BinaryBuilder::with_capacity(len, len * 128);
            for v in values {
                match v {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(val) => {
                        let json_bytes = serde_json::to_vec(val).unwrap_or_default();
                        builder.append_value(&json_bytes);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_to_arrow_batch() {
        let data = json!({
            "id": 123,
            "name": "test",
            "active": true,
            "score": 98.5
        });

        let batch = json_to_arrow_batch(data.as_object().unwrap(), "events.auth").unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5); // 4 data + 1 _destination

        // Check _destination column
        let dest_col = batch.column(0);
        assert_eq!(dest_col.len(), 1);
    }

    #[test]
    fn test_json_batch_to_arrow() {
        let data1 = json!({"id": 1, "name": "foo"});
        let data2 = json!({"id": 2, "name": "bar"});
        let data3 = json!({"id": 3, "name": "baz"});

        let rows = vec![
            (data1.as_object().unwrap(), "table_a"),
            (data2.as_object().unwrap(), "table_b"),
            (data3.as_object().unwrap(), "table_a"),
        ];

        let batch = json_batch_to_arrow(&rows).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3); // 2 data + 1 _destination
    }

    #[test]
    fn test_arrow_batch_builder() {
        let mut builder = ArrowBatchBuilder::new(3);

        builder.push(json!({"id": 1}).as_object().unwrap().clone(), "t1".into());
        builder.push(json!({"id": 2}).as_object().unwrap().clone(), "t2".into());

        assert!(!builder.is_ready());
        assert_eq!(builder.len(), 2);

        builder.push(json!({"id": 3}).as_object().unwrap().clone(), "t1".into());

        assert!(builder.is_ready());

        let batch = builder.build().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_empty_batch_builder() {
        let mut builder = ArrowBatchBuilder::new(10);
        let batch = builder.build().unwrap();
        assert!(batch.is_none());
    }

    #[test]
    fn test_simd_json_bytes_to_arrow() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]));

        let json_bytes = b"{\"id\": 1, \"name\": \"foo\"}\n{\"id\": 2, \"name\": \"bar\"}\n{\"id\": 3, \"name\": \"baz\"}";

        let batch = json_bytes_to_arrow_simd(json_bytes, schema).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_simd_batch_builder() {
        let mut builder = SimdBatchBuilder::new(3);

        // Per-table buffer design: all rows share same destination
        builder.push_bytes(b"{\"id\": 1, \"name\": \"foo\"}", "table_a");
        builder.push_bytes(b"{\"id\": 2, \"name\": \"bar\"}", "table_a");

        assert!(!builder.is_ready());
        assert_eq!(builder.len(), 2);

        builder.push_bytes(b"{\"id\": 3, \"name\": \"baz\"}", "table_a");

        assert!(builder.is_ready());

        let batch = builder.build().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        // 2 data columns + 1 _destination
        assert_eq!(batch.num_columns(), 3);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_infer_schema_from_json_bytes() {
        let json_bytes = b"{\"id\": 1, \"name\": \"foo\", \"active\": true}\n{\"id\": 2, \"name\": \"bar\", \"active\": false}";

        let schema = infer_schema_from_json_bytes(json_bytes, 10).unwrap();

        assert_eq!(schema.fields().len(), 3);
        // Check field names exist (order may vary)
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("active").is_ok());
    }

    #[test]
    fn test_simd_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int64, true)),
        ]));

        let batch = json_bytes_to_arrow_simd(b"", schema.clone()).unwrap();
        assert_eq!(batch.num_rows(), 0);

        let mut builder = SimdBatchBuilder::new(10);
        let batch = builder.build().unwrap();
        assert!(batch.is_none());
    }
}
