//! JSON/MessagePack to Arrow conversion for the pipeline
//!
//! Provides high-performance batch conversion using arrow-json.
//! Batches multiple messages before conversion for efficiency.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_json::ReaderBuilder;
use serde_json::{Map, Value};

use crate::Result;

/// Batch builder that accumulates messages before Arrow conversion
///
/// This is the proper Arrow approach - batch multiple messages then
/// convert to a single RecordBatch for columnar efficiency.
pub struct ArrowBatchBuilder {
    /// Accumulated JSON objects with their destinations
    pending: Vec<(Map<String, Value>, String)>,
    /// Target batch size before auto-flush
    batch_size: usize,
}

impl ArrowBatchBuilder {
    /// Create a new batch builder
    pub fn new(batch_size: usize) -> Self {
        Self {
            pending: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    /// Add a JSON object to the batch
    pub fn push(&mut self, data: Map<String, Value>, destination: String) {
        self.pending.push((data, destination));
    }

    /// Check if batch is ready for conversion (reached target size)
    pub fn is_ready(&self) -> bool {
        self.pending.len() >= self.batch_size
    }

    /// Get current pending count
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if empty
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

        let rows: Vec<(&Map<String, Value>, &str)> = self
            .pending
            .iter()
            .map(|(data, dest)| (data, dest.as_str()))
            .collect();

        let batch = json_batch_to_arrow(&rows)?;
        self.pending.clear();

        Ok(Some(batch))
    }

    /// Force build even if not at target size
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

    // Data columns - infer type from first row
    for (key, value) in first_data {
        let data_type = infer_arrow_type(value);
        fields.push(Arc::new(Field::new(key, data_type, true)));
    }

    let schema = Arc::new(Schema::new(fields.clone()));

    // Build columns
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());

    // Build _destination column
    let mut dest_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    for (_, dest) in rows {
        dest_builder.append_value(*dest);
    }
    columns.push(Arc::new(dest_builder.finish()));

    // Build data columns
    for (key, _) in first_data {
        let values: Vec<Option<&Value>> = rows.iter().map(|(data, _)| data.get(key)).collect();

        let array = build_column_from_json_values(
            &values,
            &infer_arrow_type(first_data.get(key).unwrap()),
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

/// Build an Arrow column from a slice of JSON values
fn build_column_from_json_values(
    values: &[Option<&Value>],
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
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
            let mut builder = Int64Builder::with_capacity(values.len());
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
            let mut builder = Float64Builder::with_capacity(values.len());
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
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 64);
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
            let mut builder = BinaryBuilder::with_capacity(values.len(), values.len() * 128);
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
            let mut builder = BinaryBuilder::with_capacity(values.len(), values.len() * 128);
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
}
