//! Direct-to-Arrow Column Builders
//!
//! Builds Arrow RecordBatches directly from Mison-extracted values without
//! intermediate serde_json::Value allocations. Schema-guided extraction means
//! we only extract and convert fields that exist in the destination schema.
//!
//! ## Schema-Guided Approach
//!
//! Traditional pipeline extracts ALL fields, flattens, then discards unused:
//! ```text
//! JSON → DOM (all fields) → Flatten → Filter to schema → Arrow
//! ```
//!
//! Mison schema-guided pipeline extracts ONLY needed fields:
//! ```text
//! JSON bytes → StructuralIndex → Extract schema fields → Arrow
//! ```
//!
//! This eliminates:
//! - DOM allocation for unused fields
//! - Flattening of nested structures we don't need
//! - Field filtering step
//!
//! ## Usage with ClickHouse Schema Introspection
//!
//! ```ignore
//! // Get schema from ClickHouse (cached per table)
//! let schema = schema_cache.get("db.events").await?;
//!
//! // Create Mison extractor for this schema
//! let extractor = SchemaExtractor::from_columns(&schema.columns);
//!
//! // Create Arrow builder
//! let mut builder = MisonArrowBuilder::new(&arrow_schema);
//!
//! // Process batch of JSON messages
//! for json_bytes in messages {
//!     let index = StructuralIndex::build(json_bytes);
//!     builder.append_from_index(&index, json_bytes, &mut extractor)?;
//! }
//!
//! // Get RecordBatch for ClickHouse insert
//! let batch = builder.finish()?;
//! ```

use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

use super::extract::{ExtractError, ExtractedValue, SchemaExtractor};
use super::index::StructuralIndex;

/// Error during Arrow building
#[derive(Debug)]
pub enum BuildError {
    /// Arrow error
    Arrow(ArrowError),
    /// Extraction error
    Extract(ExtractError),
    /// Type mismatch
    TypeMismatch {
        field: String,
        expected: DataType,
        got: String,
    },
    /// Schema mismatch
    SchemaMismatch(String),
}

impl From<ArrowError> for BuildError {
    fn from(e: ArrowError) -> Self {
        BuildError::Arrow(e)
    }
}

impl From<ExtractError> for BuildError {
    fn from(e: ExtractError) -> Self {
        BuildError::Extract(e)
    }
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Arrow(e) => write!(f, "Arrow error: {}", e),
            BuildError::Extract(e) => write!(f, "Extract error: {:?}", e),
            BuildError::TypeMismatch {
                field,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Type mismatch for field '{}': expected {:?}, got {}",
                    field, expected, got
                )
            }
            BuildError::SchemaMismatch(msg) => write!(f, "Schema mismatch: {}", msg),
        }
    }
}

impl std::error::Error for BuildError {}

/// Per-column builder enum to handle different Arrow types
enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    /// For JSON/Object columns - stored as string
    Json(StringBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => ColumnBuilder::Bool(BooleanBuilder::new()),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => ColumnBuilder::Int64(Int64Builder::new()),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                ColumnBuilder::Float64(Float64Builder::new())
            }
            DataType::Utf8 | DataType::LargeUtf8 => ColumnBuilder::String(StringBuilder::new()),
            // For complex types, store as JSON string
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                ColumnBuilder::Json(StringBuilder::new())
            }
            _ => ColumnBuilder::String(StringBuilder::new()),
        }
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Bool(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::String(b) => b.append_null(),
            ColumnBuilder::Json(b) => b.append_null(),
        }
    }

    fn append_value(&mut self, value: &ExtractedValue<'_>) -> Result<(), BuildError> {
        match self {
            ColumnBuilder::Bool(b) => match value {
                ExtractedValue::Bool(v) => b.append_value(*v),
                ExtractedValue::Null => b.append_null(),
                _ => b.append_null(),
            },
            ColumnBuilder::Int64(b) => match value {
                ExtractedValue::Int(v) => b.append_value(*v),
                ExtractedValue::Float(v) => b.append_value(*v as i64),
                ExtractedValue::Null => b.append_null(),
                _ => b.append_null(),
            },
            ColumnBuilder::Float64(b) => match value {
                ExtractedValue::Float(v) => b.append_value(*v),
                ExtractedValue::Int(v) => b.append_value(*v as f64),
                ExtractedValue::Null => b.append_null(),
                _ => b.append_null(),
            },
            ColumnBuilder::String(b) => match value {
                ExtractedValue::String(v) => {
                    if let Ok(s) = std::str::from_utf8(v) {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                }
                ExtractedValue::Int(v) => b.append_value(v.to_string()),
                ExtractedValue::Float(v) => b.append_value(v.to_string()),
                ExtractedValue::Bool(v) => b.append_value(if *v { "true" } else { "false" }),
                ExtractedValue::Null => b.append_null(),
                _ => b.append_null(),
            },
            ColumnBuilder::Json(b) => match value {
                ExtractedValue::Object(v) | ExtractedValue::Array(v) => {
                    if let Ok(s) = std::str::from_utf8(v) {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                }
                ExtractedValue::String(v) => {
                    // String can also be stored as JSON
                    if let Ok(s) = std::str::from_utf8(v) {
                        b.append_value(format!("\"{}\"", s));
                    } else {
                        b.append_null();
                    }
                }
                ExtractedValue::Int(v) => b.append_value(v.to_string()),
                ExtractedValue::Float(v) => b.append_value(v.to_string()),
                ExtractedValue::Bool(v) => b.append_value(if *v { "true" } else { "false" }),
                ExtractedValue::Null => b.append_null(),
            },
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::String(b) => Arc::new(b.finish()),
            ColumnBuilder::Json(b) => Arc::new(b.finish()),
        }
    }

    fn len(&self) -> usize {
        match self {
            ColumnBuilder::Bool(b) => b.len(),
            ColumnBuilder::Int64(b) => b.len(),
            ColumnBuilder::Float64(b) => b.len(),
            ColumnBuilder::String(b) => b.len(),
            ColumnBuilder::Json(b) => b.len(),
        }
    }
}

/// Mison-based Arrow builder
///
/// Builds Arrow RecordBatches directly from Mison structural index extraction.
/// Only extracts fields that exist in the target schema.
pub struct MisonArrowBuilder {
    /// Arrow schema for the output
    schema: Arc<Schema>,
    /// Per-column builders
    builders: Vec<ColumnBuilder>,
    /// Number of rows added
    row_count: usize,
}

impl MisonArrowBuilder {
    /// Create a new builder for the given schema
    pub fn new(schema: Arc<Schema>) -> Self {
        let builders: Vec<ColumnBuilder> = schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new(f.data_type()))
            .collect();

        Self {
            schema,
            builders,
            row_count: 0,
        }
    }

    /// Create from ClickHouse column definitions
    pub fn from_clickhouse_columns(columns: &[(String, String)]) -> Self {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, type_str)| {
                let data_type = Self::clickhouse_type_to_arrow(type_str);
                Field::new(name, data_type, true)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        Self::new(schema)
    }

    /// Map ClickHouse type to Arrow DataType
    fn clickhouse_type_to_arrow(type_str: &str) -> DataType {
        let type_lower = type_str.to_lowercase();

        if type_lower.starts_with("bool") {
            DataType::Boolean
        } else if type_lower.starts_with("int8") || type_lower == "tinyint" {
            DataType::Int64
        } else if type_lower.starts_with("int16") || type_lower == "smallint" {
            DataType::Int64
        } else if type_lower.starts_with("int32") || type_lower == "int" {
            DataType::Int64
        } else if type_lower.starts_with("int64") || type_lower == "bigint" {
            DataType::Int64
        } else if type_lower.starts_with("uint") {
            DataType::Int64
        } else if type_lower.starts_with("float32") || type_lower == "float" {
            DataType::Float64
        } else if type_lower.starts_with("float64") || type_lower == "double" {
            DataType::Float64
        } else if type_lower.starts_with("decimal") {
            DataType::Float64
        } else if type_lower.starts_with("string") || type_lower.starts_with("fixedstring") {
            DataType::Utf8
        } else if type_lower.starts_with("uuid") {
            DataType::Utf8
        } else if type_lower.starts_with("date") || type_lower.starts_with("datetime") {
            DataType::Utf8 // Will be parsed by ClickHouse
        } else if type_lower.starts_with("json") {
            DataType::Utf8 // Store as JSON string
        } else if type_lower.starts_with("array") {
            DataType::Utf8 // Store as JSON string
        } else if type_lower.starts_with("map") || type_lower.starts_with("tuple") {
            DataType::Utf8 // Store as JSON string
        } else {
            DataType::Utf8 // Default to string
        }
    }

    /// Append a row from extracted values
    ///
    /// Values must be in the same order as schema fields.
    pub fn append_row(
        &mut self,
        values: &[Result<ExtractedValue<'_>, ExtractError>],
    ) -> Result<(), BuildError> {
        if values.len() != self.builders.len() {
            return Err(BuildError::SchemaMismatch(format!(
                "Expected {} values, got {}",
                self.builders.len(),
                values.len()
            )));
        }

        for (builder, value) in self.builders.iter_mut().zip(values.iter()) {
            match value {
                Ok(v) => builder.append_value(v)?,
                Err(_) => builder.append_null(),
            }
        }

        self.row_count += 1;
        Ok(())
    }

    /// Append a row directly from JSON bytes using structural index
    ///
    /// This is the optimal path - goes directly from bytes to Arrow.
    pub fn append_from_index(
        &mut self,
        index: &StructuralIndex,
        data: &[u8],
        extractor: &mut SchemaExtractor,
    ) -> Result<(), BuildError> {
        let values = extractor.extract_all(index, data);
        self.append_row(&values)
    }

    /// Append a row using batch extraction (single-pass through colons)
    ///
    /// OPTIMIZED: Uses extract_all_batch which iterates colons once for all fields.
    /// Best for many-field schemas (10+ fields).
    pub fn append_from_index_batch(
        &mut self,
        index: &StructuralIndex,
        data: &[u8],
        extractor: &SchemaExtractor,
    ) -> Result<(), BuildError> {
        let values = extractor.extract_all_batch(index, data);
        self.append_row(&values)
    }

    /// Append multiple rows from a batch of JSON bytes
    ///
    /// More efficient than calling append_from_index for each message.
    pub fn append_batch(
        &mut self,
        messages: &[&[u8]],
        extractor: &mut SchemaExtractor,
    ) -> Result<(), BuildError> {
        for data in messages {
            let index = StructuralIndex::build(data);
            self.append_from_index(&index, data, extractor)?;
        }
        Ok(())
    }

    /// Append multiple rows using batch extraction (optimized for many fields)
    ///
    /// OPTIMIZED: Uses extract_all_batch for single-pass field extraction.
    pub fn append_batch_optimized(
        &mut self,
        messages: &[&[u8]],
        extractor: &SchemaExtractor,
    ) -> Result<(), BuildError> {
        for data in messages {
            let index = StructuralIndex::build(data);
            self.append_from_index_batch(&index, data, extractor)?;
        }
        Ok(())
    }

    /// Get current row count
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Check if builder is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Finish building and return RecordBatch
    pub fn finish(&mut self) -> Result<RecordBatch, BuildError> {
        let columns: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();

        self.row_count = 0;

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Get the schema
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Clear and reset for reuse
    pub fn clear(&mut self) {
        // Re-create builders
        self.builders = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::new(f.data_type()))
            .collect();
        self.row_count = 0;
    }
}

/// Batch processor that combines structural indexing with Arrow building
///
/// This is the main entry point for schema-guided JSON to Arrow conversion.
pub struct MisonBatchProcessor {
    /// Arrow builder
    builder: MisonArrowBuilder,
    /// Field extractor
    extractor: SchemaExtractor,
}

impl MisonBatchProcessor {
    /// Create a new processor from ClickHouse column schema
    pub fn new(columns: &[(String, String)]) -> Self {
        let builder = MisonArrowBuilder::from_clickhouse_columns(columns);

        // Create extractor for the same columns
        let extractor = SchemaExtractor::from_columns(columns);

        Self { builder, extractor }
    }

    /// Process a batch of JSON messages
    pub fn process_batch(&mut self, messages: &[&[u8]]) -> Result<RecordBatch, BuildError> {
        self.builder.append_batch(messages, &mut self.extractor)?;
        self.builder.finish()
    }

    /// Process a batch of JSON messages using optimized single-pass extraction
    ///
    /// OPTIMIZED: Uses extract_all_batch which iterates colons once for all fields.
    /// Best for many-field schemas (10+ fields).
    pub fn process_batch_optimized(
        &mut self,
        messages: &[&[u8]],
    ) -> Result<RecordBatch, BuildError> {
        self.builder
            .append_batch_optimized(messages, &self.extractor)?;
        self.builder.finish()
    }

    /// Process a single message (for routing/preview)
    pub fn process_single(&mut self, data: &[u8]) -> Result<RecordBatch, BuildError> {
        let index = StructuralIndex::build(data);
        self.builder
            .append_from_index(&index, data, &mut self.extractor)?;
        self.builder.finish()
    }

    /// Get current row count
    pub fn pending_rows(&self) -> usize {
        self.builder.len()
    }

    /// Clear for reuse
    pub fn clear(&mut self) {
        self.builder.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn test_builder_basic() {
        let schema = test_schema();
        let mut builder = MisonArrowBuilder::new(schema);

        // Append values directly
        let values = vec![
            Ok(ExtractedValue::String(b"test123")),
            Ok(ExtractedValue::Int(42)),
            Ok(ExtractedValue::Bool(true)),
        ];
        builder.append_row(&values).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_builder_with_nulls() {
        let schema = test_schema();
        let mut builder = MisonArrowBuilder::new(schema);

        let values = vec![
            Ok(ExtractedValue::String(b"test")),
            Err(ExtractError::FieldNotFound("value".to_string())),
            Ok(ExtractedValue::Null),
        ];
        builder.append_row(&values).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_batch_processor() {
        let columns = vec![
            ("id".to_string(), "String".to_string()),
            ("value".to_string(), "Int64".to_string()),
        ];

        let mut processor = MisonBatchProcessor::new(&columns);

        let messages: Vec<&[u8]> = vec![
            br#"{"id":"a","value":1}"#,
            br#"{"id":"b","value":2}"#,
            br#"{"id":"c","value":3}"#,
        ];

        let batch = processor.process_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_from_clickhouse_columns() {
        let columns = vec![
            ("timestamp".to_string(), "DateTime64(3)".to_string()),
            ("level".to_string(), "String".to_string()),
            ("count".to_string(), "UInt64".to_string()),
            ("tags".to_string(), "JSON".to_string()),
        ];

        let builder = MisonArrowBuilder::from_clickhouse_columns(&columns);
        assert_eq!(builder.schema().fields().len(), 4);
    }

    #[test]
    fn test_type_coercion() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let mut builder = MisonArrowBuilder::new(schema);

        // Int coerced to string
        let values = vec![Ok(ExtractedValue::Int(42))];
        builder.append_row(&values).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_json_column() {
        let columns = vec![("data".to_string(), "JSON".to_string())];

        let mut processor = MisonBatchProcessor::new(&columns);

        let messages: Vec<&[u8]> = vec![br#"{"data":{"nested":"value"}}"#];

        let batch = processor.process_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
