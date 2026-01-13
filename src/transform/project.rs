// Project:   dfe-loader
// File:      project.rs
// Purpose:   Project JSON fields to match ClickHouse schema
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Schema projection - filter JSON fields to match ClickHouse schema
//!
//! Projection ensures only columns present in the destination table schema
//! are included in the insert. Fields not in the schema are dropped.
//!
//! ## Use Cases
//!
//! 1. **Schema enforcement**: Reject fields not in ClickHouse schema
//! 2. **Bandwidth reduction**: Don't send unnecessary fields
//! 3. **Error prevention**: Avoid schema mismatch errors on insert
//!
//! ## Example
//!
//! ```ignore
//! let schema = TableSchema { columns: vec![col("id"), col("name")] };
//! let projector = Projector::new(&schema);
//!
//! let mut data = json!({"id": 1, "name": "test", "extra": "dropped"});
//! let result = projector.project(data.as_object_mut().unwrap());
//!
//! assert!(result.data.contains_key("id"));
//! assert!(result.data.contains_key("name"));
//! assert!(!result.data.contains_key("extra"));
//! assert_eq!(result.dropped_fields, vec!["extra"]);
//! ```

use rustc_hash::FxHashSet;
use serde_json::{Map, Value};

use crate::clickhouse::TableSchema;

/// Result of projecting a JSON object against a schema
#[derive(Debug, Clone)]
pub struct ProjectedData {
    /// The projected data with only schema-matching fields
    pub data: Map<String, Value>,
    /// Fields that were dropped (not in schema)
    pub dropped_fields: Vec<String>,
    /// Fields in schema that are missing from data
    pub missing_fields: Vec<String>,
}

impl ProjectedData {
    /// Check if any fields were dropped
    pub fn has_dropped(&self) -> bool {
        !self.dropped_fields.is_empty()
    }

    /// Check if any schema fields are missing
    pub fn has_missing(&self) -> bool {
        !self.missing_fields.is_empty()
    }

    /// Get count of fields in projected data
    pub fn field_count(&self) -> usize {
        self.data.len()
    }
}

/// Projects flattened JSON to match a ClickHouse schema
///
/// Only fields present in the schema are retained.
/// Uses FxHashSet for O(1) column name lookups.
pub struct Projector {
    /// Set of column names for fast lookup
    column_set: FxHashSet<String>,
    /// Ordered column names (for missing field detection)
    column_names: Vec<String>,
}

impl Projector {
    /// Create a new projector for the given schema
    pub fn new(schema: &TableSchema) -> Self {
        let column_set: FxHashSet<String> = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let column_names: Vec<String> = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        Self {
            column_set,
            column_names,
        }
    }

    /// Create a projector from a list of column names
    ///
    /// Useful when you don't have a full TableSchema available.
    pub fn from_columns(columns: &[&str]) -> Self {
        let column_set: FxHashSet<String> = columns.iter().map(|s| (*s).to_string()).collect();
        let column_names: Vec<String> = columns.iter().map(|s| (*s).to_string()).collect();

        Self {
            column_set,
            column_names,
        }
    }

    /// Project a JSON object, keeping only fields in the schema
    ///
    /// This method takes ownership of the data to avoid cloning values.
    pub fn project(&self, mut data: Map<String, Value>) -> ProjectedData {
        let mut dropped_fields = Vec::new();

        // Remove fields not in schema
        // Collect keys to remove first to avoid borrowing issues
        let keys_to_remove: Vec<String> = data
            .keys()
            .filter(|k| !self.column_set.contains(*k))
            .cloned()
            .collect();

        for key in keys_to_remove {
            data.remove(&key);
            dropped_fields.push(key);
        }

        // Find missing schema fields
        let missing_fields: Vec<String> = self
            .column_names
            .iter()
            .filter(|name| !data.contains_key(*name))
            .cloned()
            .collect();

        ProjectedData {
            data,
            dropped_fields,
            missing_fields,
        }
    }

    /// Project a JSON object by reference, returning only matching field names
    ///
    /// More efficient when you just need to filter without modifying.
    pub fn project_ref<'a>(&self, data: &'a Map<String, Value>) -> Vec<&'a str> {
        data.keys()
            .filter(|k| self.column_set.contains(*k))
            .map(|k| k.as_str())
            .collect()
    }

    /// Check if a field name is in the schema
    #[inline]
    pub fn has_column(&self, name: &str) -> bool {
        self.column_set.contains(name)
    }

    /// Get the number of columns in the schema
    pub fn column_count(&self) -> usize {
        self.column_set.len()
    }

    /// Get column names as a slice
    pub fn columns(&self) -> &[String] {
        &self.column_names
    }
}

/// Project JSON bytes to only include known schema columns
///
/// Legacy function for backwards compatibility.
/// Parses JSON, projects, and re-serialises.
///
/// For hot paths, prefer using `Projector::project()` directly
/// with already-parsed JSON to avoid double parsing.
pub fn project(json: &[u8], schema: &TableSchema) -> Vec<u8> {
    let value: Value = match sonic_rs::from_slice(json) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let obj = match value {
        Value::Object(map) => map,
        _ => return Vec::new(),
    };

    let projector = Projector::new(schema);
    let result = projector.project(obj);

    sonic_rs::to_vec(&Value::Object(result.data)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::{ColumnInfo, ParsedType};
    use serde_json::json;

    fn make_schema(columns: &[&str]) -> TableSchema {
        TableSchema {
            database: "test".to_string(),
            table: "events".to_string(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnInfo {
                    name: (*name).to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: i as u64 + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                })
                .collect(),
            comment: String::new(),
        }
    }

    #[test]
    fn test_project_keeps_schema_fields() {
        let schema = make_schema(&["id", "name", "timestamp"]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "name": "test", "timestamp": 12345});
        let result = projector.project(data.as_object().unwrap().clone());

        assert_eq!(result.data.len(), 3);
        assert!(result.data.contains_key("id"));
        assert!(result.data.contains_key("name"));
        assert!(result.data.contains_key("timestamp"));
        assert!(result.dropped_fields.is_empty());
    }

    #[test]
    fn test_project_drops_extra_fields() {
        let schema = make_schema(&["id", "name"]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "name": "test", "extra": "dropped", "another": 42});
        let result = projector.project(data.as_object().unwrap().clone());

        assert_eq!(result.data.len(), 2);
        assert!(result.data.contains_key("id"));
        assert!(result.data.contains_key("name"));
        assert!(!result.data.contains_key("extra"));
        assert!(!result.data.contains_key("another"));
        assert_eq!(result.dropped_fields.len(), 2);
        assert!(result.dropped_fields.contains(&"extra".to_string()));
        assert!(result.dropped_fields.contains(&"another".to_string()));
    }

    #[test]
    fn test_project_tracks_missing_fields() {
        let schema = make_schema(&["id", "name", "required_field"]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "name": "test"});
        let result = projector.project(data.as_object().unwrap().clone());

        assert_eq!(result.data.len(), 2);
        assert!(result.has_missing());
        assert_eq!(result.missing_fields, vec!["required_field"]);
    }

    #[test]
    fn test_project_empty_data() {
        let schema = make_schema(&["id", "name"]);
        let projector = Projector::new(&schema);

        let data = json!({});
        let result = projector.project(data.as_object().unwrap().clone());

        assert_eq!(result.data.len(), 0);
        assert!(result.dropped_fields.is_empty());
        assert_eq!(result.missing_fields.len(), 2);
    }

    #[test]
    fn test_project_no_schema_columns() {
        let schema = make_schema(&[]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "name": "test"});
        let result = projector.project(data.as_object().unwrap().clone());

        assert_eq!(result.data.len(), 0);
        assert_eq!(result.dropped_fields.len(), 2);
    }

    #[test]
    fn test_projector_has_column() {
        let schema = make_schema(&["id", "name"]);
        let projector = Projector::new(&schema);

        assert!(projector.has_column("id"));
        assert!(projector.has_column("name"));
        assert!(!projector.has_column("other"));
    }

    #[test]
    fn test_projector_from_columns() {
        let projector = Projector::from_columns(&["id", "name", "value"]);

        assert_eq!(projector.column_count(), 3);
        assert!(projector.has_column("id"));
        assert!(projector.has_column("name"));
        assert!(projector.has_column("value"));
        assert!(!projector.has_column("other"));
    }

    #[test]
    fn test_project_ref() {
        let schema = make_schema(&["id", "name"]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "name": "test", "extra": "dropped"});
        let matching = projector.project_ref(data.as_object().unwrap());

        assert_eq!(matching.len(), 2);
        assert!(matching.contains(&"id"));
        assert!(matching.contains(&"name"));
        assert!(!matching.contains(&"extra"));
    }

    #[test]
    fn test_project_bytes_function() {
        let schema = make_schema(&["id", "name"]);
        let json_bytes = br#"{"id": 1, "name": "test", "extra": "dropped"}"#;

        let result = project(json_bytes, &schema);
        let parsed: Value = sonic_rs::from_slice(&result).unwrap();

        assert!(parsed.get("id").is_some());
        assert!(parsed.get("name").is_some());
        assert!(parsed.get("extra").is_none());
    }

    #[test]
    fn test_project_bytes_invalid_json() {
        let schema = make_schema(&["id"]);
        let result = project(b"not json", &schema);
        assert!(result.is_empty());
    }

    #[test]
    fn test_project_bytes_non_object() {
        let schema = make_schema(&["id"]);
        let result = project(b"[1, 2, 3]", &schema);
        assert!(result.is_empty());
    }

    #[test]
    fn test_projected_data_helpers() {
        let schema = make_schema(&["id", "name", "extra"]);
        let projector = Projector::new(&schema);

        let data = json!({"id": 1, "dropped": "value"});
        let result = projector.project(data.as_object().unwrap().clone());

        assert!(result.has_dropped());
        assert!(result.has_missing());
        assert_eq!(result.field_count(), 1);
    }
}
