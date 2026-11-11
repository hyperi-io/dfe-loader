// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/types.rs
// Purpose:   ClickHouse type parsing (runtime, not compiled)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` type system - runtime parsed, not compiled.
//!
//! `ParsedType` is `dfe_clickhouse`'s runtime type parser, re-exported here
//! beside the DFE-specific `ColumnInfo` and `TableSchema` types.

pub use dfe_clickhouse::dynamic::ParsedType;

/// Column information from `ClickHouse` system.columns.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Raw type string from `ClickHouse`.
    pub type_name: String,
    /// Parsed type information.
    pub parsed_type: ParsedType,
    /// Column position (1-based).
    pub position: u64,
    /// Default kind (empty, DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL).
    pub default_kind: String,
    /// Default expression.
    pub default_expression: String,
    /// Column comment (may contain metadata directives).
    pub comment: String,
    /// Whether column is part of primary key.
    pub is_in_primary_key: bool,
    /// Whether column is part of sorting key.
    pub is_in_sorting_key: bool,
}

impl ColumnInfo {
    /// Check if this column is nullable.
    #[must_use]
    pub fn is_nullable(&self) -> bool {
        self.parsed_type.nullable
    }
}

/// Table schema with all column information.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Columns in order.
    pub columns: Vec<ColumnInfo>,
    /// Table comment (may contain directives like logjson=force).
    pub comment: String,
}

impl TableSchema {
    /// Get column by name.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column names.
    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Check if a column exists.
    #[must_use]
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Helper to build a ColumnInfo for testing ----

    fn make_column(name: &str, type_str: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_str.to_string(),
            parsed_type: ParsedType::parse(type_str),
            position: 1,
            default_kind: String::new(),
            default_expression: String::new(),
            comment: String::new(),
            is_in_primary_key: false,
            is_in_sorting_key: false,
        }
    }

    fn make_schema(columns: Vec<ColumnInfo>) -> TableSchema {
        TableSchema {
            database: "dfe".to_string(),
            table: "events".to_string(),
            columns,
            comment: String::new(),
        }
    }

    // ---- NEW: TableSchema tests ----

    #[test]
    fn table_schema_column_by_name_found() {
        let schema = make_schema(vec![
            make_column("_timestamp", "DateTime64(3)"),
            make_column("_org_id", "LowCardinality(String)"),
            make_column("_raw", "Nullable(String)"),
        ]);

        let col = schema.column("_org_id");
        assert!(col.is_some());
        assert_eq!(col.unwrap().name, "_org_id");
        assert_eq!(col.unwrap().type_name, "LowCardinality(String)");
    }

    #[test]
    fn table_schema_column_by_name_not_found() {
        let schema = make_schema(vec![make_column("_timestamp", "DateTime64(3)")]);
        assert!(schema.column("nonexistent").is_none());
    }

    #[test]
    fn table_schema_column_empty_schema() {
        let schema = make_schema(vec![]);
        assert!(schema.column("anything").is_none());
    }

    #[test]
    fn table_schema_column_names_preserves_order() {
        let schema = make_schema(vec![
            make_column("_timestamp", "DateTime64(3)"),
            make_column("_org_id", "LowCardinality(String)"),
            make_column("_raw", "Nullable(String)"),
            make_column("_json", "JSON"),
        ]);

        let names = schema.column_names();
        assert_eq!(names, vec!["_timestamp", "_org_id", "_raw", "_json"]);
    }

    #[test]
    fn table_schema_column_names_empty() {
        let schema = make_schema(vec![]);
        let names = schema.column_names();
        assert_eq!(names, [] as [&str; 0]);
    }

    #[test]
    fn table_schema_has_column_true() {
        let schema = make_schema(vec![
            make_column("_timestamp", "DateTime64(3)"),
            make_column("user_id", "String"),
        ]);
        assert!(schema.has_column("_timestamp"));
        assert!(schema.has_column("user_id"));
    }

    #[test]
    fn table_schema_has_column_false() {
        let schema = make_schema(vec![make_column("_timestamp", "DateTime64(3)")]);
        assert!(!schema.has_column("missing"));
        assert!(!schema.has_column("")); // empty name
        assert!(!schema.has_column("_Timestamp")); // case sensitive
    }

    #[test]
    fn table_schema_has_column_empty_schema() {
        let schema = make_schema(vec![]);
        assert!(!schema.has_column("anything"));
    }

    // ---- NEW: ColumnInfo tests ----

    #[test]
    fn column_info_is_nullable_true() {
        let col = make_column("_raw", "Nullable(String)");
        assert!(col.is_nullable());
    }

    #[test]
    fn column_info_is_nullable_false() {
        let col = make_column("_timestamp", "DateTime64(3)");
        assert!(!col.is_nullable());
    }

    #[test]
    fn column_info_is_nullable_lc_nullable() {
        let col = make_column("status", "LowCardinality(Nullable(String))");
        assert!(col.is_nullable());
    }

    #[test]
    fn column_info_with_default_kind() {
        let mut col = make_column("_uuid", "UUID");
        col.default_kind = "DEFAULT".to_string();
        col.default_expression = "generateUUIDv7()".to_string();
        assert_eq!(col.default_kind, "DEFAULT");
        assert_eq!(col.default_expression, "generateUUIDv7()");
    }

    #[test]
    fn column_info_primary_and_sorting_key() {
        let mut col = make_column("_timestamp", "DateTime64(3)");
        col.is_in_primary_key = true;
        col.is_in_sorting_key = true;
        assert!(col.is_in_primary_key);
        assert!(col.is_in_sorting_key);
    }
}
