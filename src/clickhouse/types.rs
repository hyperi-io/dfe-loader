// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/types.rs
// Purpose:   ClickHouse type parsing (runtime, not compiled)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` type system - runtime parsed, not compiled.
//!
//! `ParsedType` is re-exported from the clickhouse-rs fork's `dynamic` module.
//! The fork owns the type parser — it's generic and useful to any clickhouse-rs user.
//!
//! This module adds `coercer_category()` as a DFE-specific alias for the fork's
//! `category()` method, plus DFE-specific types (`ColumnInfo`, `TableSchema`).

/// Re-export `ParsedType` from clickhouse-rs fork.
///
/// The fork's `dynamic::ParsedType` is the canonical type parser.
/// Use `ParsedTypeExt::coercer_category()` for the DFE coercion category.
pub use clickhouse::dynamic::ParsedType;

/// DFE extension trait for `ParsedType` — adds `coercer_category()` alias.
///
/// The fork's `category()` and this `coercer_category()` return the same values.
/// This alias exists for backward compatibility with existing DFE coercer code.
pub trait ParsedTypeExt {
    /// Get the coercer category for this type (alias for `category()`).
    fn coercer_category(&self) -> &str;
}

impl ParsedTypeExt for ParsedType {
    fn coercer_category(&self) -> &str {
        self.category()
    }
}

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

    /// Get the coercer category for this column.
    #[must_use]
    pub fn coercer_category(&self) -> &str {
        self.parsed_type.coercer_category()
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

/// Default values for `ClickHouse` types (used when handling nulls).
///
/// These are safe zero values that avoid NULL in `ClickHouse`
/// (following `ClickHouse` best practices to avoid Nullable overhead).
#[must_use]
pub fn default_value_for_category(category: &str) -> &'static str {
    match category {
        "String" => "",
        "Int" | "UInt" => "0",
        "Float" | "Decimal" => "0.0",
        "Bool" => "false",
        "Date" => "1970-01-01",
        "DateTime" | "DateTime64" => "1970-01-01T00:00:00Z",
        "UUID" => "00000000-0000-0000-0000-000000000000",
        "IPv4" => "0.0.0.0",
        "IPv6" => "::",
        "Array" => "[]",
        "Map" => "{}",
        "JSON" => "{}",
        "Enum" => "",
        "Geo" => "(0, 0)",
        _ => "",
    }
}

/// Common null string representations to recognise.
pub const NULL_STRINGS: &[&str] = &[
    "null",
    "NULL",
    "Null",
    "None",
    "nil",
    "undefined",
    "\\N",
    "<null>",
    "NA",
    "N/A",
    "n/a",
    "NaN",
    "",
];

/// Check if a string value represents null.
#[must_use]
pub fn is_null_string(value: &str) -> bool {
    NULL_STRINGS.contains(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_types() {
        let t = ParsedType::parse("String");
        assert_eq!(t.base, "String");
        assert!(!t.nullable);
        assert!(!t.low_cardinality);

        let t = ParsedType::parse("Int64");
        assert_eq!(t.base, "Int64");
        assert_eq!(t.coercer_category(), "Int");

        let t = ParsedType::parse("Float64");
        assert_eq!(t.base, "Float64");
        assert_eq!(t.coercer_category(), "Float");
    }

    #[test]
    fn test_parse_nullable() {
        let t = ParsedType::parse("Nullable(String)");
        assert_eq!(t.base, "String");
        assert!(t.nullable);
        assert!(!t.low_cardinality);
    }

    #[test]
    fn test_parse_low_cardinality() {
        let t = ParsedType::parse("LowCardinality(String)");
        assert_eq!(t.base, "String");
        assert!(!t.nullable);
        assert!(t.low_cardinality);
    }

    #[test]
    fn test_parse_nullable_low_cardinality() {
        let t = ParsedType::parse("LowCardinality(Nullable(String))");
        assert_eq!(t.base, "String");
        assert!(t.nullable);
        assert!(t.low_cardinality);
    }

    #[test]
    fn test_parse_array() {
        let t = ParsedType::parse("Array(Int64)");
        assert_eq!(t.base, "Array");
        assert!(t.array_element.is_some());
        let elem = t.array_element.as_ref().unwrap();
        assert_eq!(elem.base, "Int64");
    }

    #[test]
    fn test_parse_map() {
        let t = ParsedType::parse("Map(String, Int64)");
        assert_eq!(t.base, "Map");
        assert!(t.map_types.is_some());
        let (key, value) = t.map_types.as_ref().unwrap();
        assert_eq!(key.base, "String");
        assert_eq!(value.base, "Int64");
    }

    #[test]
    fn test_parse_datetime64() {
        let t = ParsedType::parse("DateTime64(3)");
        assert_eq!(t.base, "DateTime64");
        assert_eq!(t.precision, Some(3));
        assert!(t.timezone.is_none());

        let t = ParsedType::parse("DateTime64(6, 'UTC')");
        assert_eq!(t.base, "DateTime64");
        assert_eq!(t.precision, Some(6));
        assert_eq!(t.timezone, Some("UTC".to_string()));
    }

    #[test]
    fn test_coercer_categories() {
        assert_eq!(ParsedType::parse("String").coercer_category(), "String");
        assert_eq!(ParsedType::parse("Int64").coercer_category(), "Int");
        assert_eq!(ParsedType::parse("UInt32").coercer_category(), "UInt");
        assert_eq!(ParsedType::parse("Float64").coercer_category(), "Float");
        assert_eq!(ParsedType::parse("Bool").coercer_category(), "Bool");
        assert_eq!(ParsedType::parse("DateTime").coercer_category(), "DateTime");
        assert_eq!(ParsedType::parse("UUID").coercer_category(), "UUID");
        assert_eq!(ParsedType::parse("IPv4").coercer_category(), "IPv4");
        assert_eq!(ParsedType::parse("JSON").coercer_category(), "JSON");

        // Unknown type falls back to String
        assert_eq!(
            ParsedType::parse("SomeNewType").coercer_category(),
            "String"
        );
    }

    #[test]
    fn test_is_helpers() {
        assert!(ParsedType::parse("Int64").is_numeric());
        assert!(ParsedType::parse("Float64").is_numeric());
        assert!(!ParsedType::parse("String").is_numeric());

        assert!(ParsedType::parse("String").is_string());
        assert!(ParsedType::parse("FixedString(10)").is_string());

        assert!(ParsedType::parse("DateTime").is_datetime());
        assert!(ParsedType::parse("DateTime64(3)").is_datetime());
        assert!(ParsedType::parse("Date").is_datetime());

        assert!(ParsedType::parse("IPv4").is_ip());
        assert!(ParsedType::parse("IPv6").is_ip());
    }

    #[test]
    fn test_null_strings() {
        assert!(is_null_string("null"));
        assert!(is_null_string("NULL"));
        assert!(is_null_string("None"));
        assert!(is_null_string(""));
        assert!(!is_null_string("hello"));
        assert!(!is_null_string("0"));
    }
}
