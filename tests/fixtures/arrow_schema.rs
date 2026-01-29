//! Arrow schema fixture builders

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Builder for common Arrow schemas used in tests
#[derive(Debug, Clone)]
pub struct ArrowSchemaBuilder {
    fields: Vec<Field>,
}

impl ArrowSchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Add a timestamp field (DateTime64(3) in ClickHouse)
    pub fn with_timestamp(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(
            name,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            nullable,
        ));
        self
    }

    /// Add a string field
    pub fn with_string(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Utf8, nullable));
        self
    }

    /// Add an integer field (UInt32)
    pub fn with_uint32(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::UInt32, nullable));
        self
    }

    /// Add an integer field (UInt64)
    pub fn with_uint64(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::UInt64, nullable));
        self
    }

    /// Add an integer field (Int32)
    pub fn with_int32(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Int32, nullable));
        self
    }

    /// Add an integer field (Int64)
    pub fn with_int64(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Int64, nullable));
        self
    }

    /// Add a float field (Float32)
    pub fn with_float32(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Float32, nullable));
        self
    }

    /// Add a float field (Float64)
    pub fn with_float64(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Float64, nullable));
        self
    }

    /// Add a boolean field
    pub fn with_bool(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(name, DataType::Boolean, nullable));
        self
    }

    /// Add a UUID field
    pub fn with_uuid(mut self, name: &str, nullable: bool) -> Self {
        self.fields.push(Field::new(
            name,
            DataType::FixedSizeBinary(16),
            nullable,
        ));
        self
    }

    /// Add a custom field
    pub fn with_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Build the schema
    pub fn build(self) -> Arc<Schema> {
        Arc::new(Schema::new(self.fields))
    }
}

impl Default for ArrowSchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Common schemas for testing

/// Standard event schema (_timestamp + _org_id + common fields)
pub fn event_schema() -> Arc<Schema> {
    ArrowSchemaBuilder::new()
        .with_timestamp("_timestamp", false)
        .with_timestamp("_timestamp_load", false)
        .with_timestamp("_timestamp_received", true)
        .with_uuid("_uuid", false)
        .with_string("_org_id", false)
        .with_string("event_category", false)
        .with_string("action", false)
        .build()
}

/// RLS test schema (_timestamp + _org_id + action + user_id)
pub fn rls_schema() -> Arc<Schema> {
    ArrowSchemaBuilder::new()
        .with_timestamp("_timestamp", false)
        .with_string("_org_id", false)
        .with_string("action", false)
        .with_uint32("user_id", false)
        .build()
}

/// Authentication event schema
pub fn auth_schema() -> Arc<Schema> {
    ArrowSchemaBuilder::new()
        .with_timestamp("_timestamp", false)
        .with_string("_org_id", false)
        .with_string("event_category", false)
        .with_string("action", false)
        .with_uint64("user_id", false)
        .with_string("ip_address", true)
        .with_string("user_agent", true)
        .with_bool("success", false)
        .build()
}

/// API event schema
pub fn api_schema() -> Arc<Schema> {
    ArrowSchemaBuilder::new()
        .with_timestamp("_timestamp", false)
        .with_string("_org_id", false)
        .with_string("event_category", false)
        .with_string("endpoint", false)
        .with_string("method", false)
        .with_uint32("status_code", false)
        .with_float64("duration_ms", false)
        .with_uint64("bytes_sent", false)
        .with_uint64("bytes_received", false)
        .build()
}

/// Minimal schema (just _timestamp and _org_id)
pub fn minimal_schema() -> Arc<Schema> {
    ArrowSchemaBuilder::new()
        .with_timestamp("_timestamp", false)
        .with_string("_org_id", false)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = ArrowSchemaBuilder::new()
            .with_timestamp("timestamp", false)
            .with_string("org_id", false)
            .with_uint32("count", true)
            .build();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "org_id");
        assert_eq!(schema.field(2).name(), "count");

        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        ));
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt32);

        assert!(!schema.field(0).is_nullable());
        assert!(!schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn test_event_schema() {
        let schema = event_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "_timestamp");
        assert_eq!(schema.field(2).name(), "_timestamp_received");
        assert_eq!(schema.field(3).name(), "_uuid");
        assert_eq!(schema.field(4).name(), "_org_id");
    }

    #[test]
    fn test_rls_schema() {
        let schema = rls_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "_timestamp");
        assert_eq!(schema.field(1).name(), "_org_id");
        assert_eq!(schema.field(2).name(), "action");
        assert_eq!(schema.field(3).name(), "user_id");
    }

    #[test]
    fn test_auth_schema() {
        let schema = auth_schema();
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(5).name(), "ip_address");
        assert!(schema.field(5).is_nullable());
        assert_eq!(schema.field(7).name(), "success");
        assert!(!schema.field(7).is_nullable());
    }

    #[test]
    fn test_api_schema() {
        let schema = api_schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(3).name(), "endpoint");
        assert_eq!(schema.field(6).name(), "duration_ms");
        assert_eq!(schema.field(6).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_minimal_schema() {
        let schema = minimal_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "_timestamp");
        assert_eq!(schema.field(1).name(), "_org_id");
    }
}
