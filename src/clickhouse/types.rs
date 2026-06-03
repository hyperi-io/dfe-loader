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
//! `ParsedType` and `ParsedTypeExt` are re-exported from the loader's
//! `clickhouse_ext` module (the HyperI dynamic-insert layer over the
//! clickhouse-rs hyperi-port fork). `clickhouse_ext` owns the runtime type
//! parser; this module re-exports it plus the DFE-specific `ColumnInfo` /
//! `TableSchema` types.

/// Re-export `ParsedType` + `ParsedTypeExt` from `clickhouse_ext`.
///
/// `clickhouse_ext::ParsedType` is the canonical runtime type parser.
/// `ParsedTypeExt::coercer_category()` returns the DFE coercion category.
pub use crate::clickhouse_ext::{ParsedType, ParsedTypeExt};

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

    // ---- ParsedType tests (existing, kept for regression) ----

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
        assert!(names.is_empty());
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
    fn column_info_coercer_category_delegates() {
        let col = make_column("count", "UInt64");
        assert_eq!(col.coercer_category(), "UInt");

        let col = make_column("ts", "DateTime64(3)");
        assert_eq!(col.coercer_category(), "DateTime64");

        let col = make_column("data", "JSON");
        assert_eq!(col.coercer_category(), "JSON");
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

    // ---- NEW: default_value_for_category tests ----

    #[test]
    fn default_values_all_known_categories() {
        assert_eq!(default_value_for_category("String"), "");
        assert_eq!(default_value_for_category("Int"), "0");
        assert_eq!(default_value_for_category("UInt"), "0");
        assert_eq!(default_value_for_category("Float"), "0.0");
        assert_eq!(default_value_for_category("Decimal"), "0.0");
        assert_eq!(default_value_for_category("Bool"), "false");
        assert_eq!(default_value_for_category("Date"), "1970-01-01");
        assert_eq!(
            default_value_for_category("DateTime"),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            default_value_for_category("DateTime64"),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            default_value_for_category("UUID"),
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(default_value_for_category("IPv4"), "0.0.0.0");
        assert_eq!(default_value_for_category("IPv6"), "::");
        assert_eq!(default_value_for_category("Array"), "[]");
        assert_eq!(default_value_for_category("Map"), "{}");
        assert_eq!(default_value_for_category("JSON"), "{}");
        assert_eq!(default_value_for_category("Enum"), "");
        assert_eq!(default_value_for_category("Geo"), "(0, 0)");
    }

    #[test]
    fn default_value_unknown_category_returns_empty() {
        assert_eq!(default_value_for_category(""), "");
        assert_eq!(default_value_for_category("SomeFutureType"), "");
        assert_eq!(default_value_for_category("Tuple"), "");
    }

    // ---- NEW: is_null_string edge cases ----

    #[test]
    fn null_string_all_known_representations() {
        for &s in NULL_STRINGS {
            assert!(is_null_string(s), "Expected '{s}' to be recognised as null");
        }
    }

    #[test]
    fn null_string_case_sensitivity() {
        // "null", "NULL", "Null" are in the list, but not "nULL" or "nUll"
        assert!(is_null_string("null"));
        assert!(is_null_string("NULL"));
        assert!(is_null_string("Null"));
        assert!(!is_null_string("nULL"));
        assert!(!is_null_string("nUll"));
    }

    #[test]
    fn null_string_whitespace_not_recognised() {
        assert!(!is_null_string(" "));
        assert!(!is_null_string("  null  "));
        assert!(!is_null_string("\tnull"));
        assert!(!is_null_string("null\n"));
    }

    #[test]
    fn null_string_similar_but_not_null() {
        assert!(!is_null_string("0"));
        assert!(!is_null_string("false"));
        assert!(!is_null_string("no"));
        assert!(!is_null_string("nulls"));
        assert!(!is_null_string("NONE")); // "None" is in list, "NONE" is not
        assert!(!is_null_string("Nil")); // "nil" is in list, "Nil" is not
    }

    #[test]
    fn null_string_backslash_n() {
        // \\N is a ClickHouse-specific null representation
        assert!(is_null_string("\\N"));
        assert!(!is_null_string("\\n")); // lowercase — not in list
    }

    #[test]
    fn null_string_na_variants() {
        assert!(is_null_string("NA"));
        assert!(is_null_string("N/A"));
        assert!(is_null_string("n/a"));
        assert!(is_null_string("NaN"));
        assert!(!is_null_string("na")); // not in list
        assert!(!is_null_string("nan")); // not in list
    }

    // ---- NEW: coercer_category via ParsedTypeExt for additional types ----

    #[test]
    fn coercer_category_date_types() {
        assert_eq!(ParsedType::parse("Date").coercer_category(), "Date");
        assert_eq!(ParsedType::parse("Date32").coercer_category(), "Date");
    }

    #[test]
    fn coercer_category_decimal_types() {
        assert_eq!(
            ParsedType::parse("Decimal(18, 4)").coercer_category(),
            "Decimal"
        );
        assert_eq!(
            ParsedType::parse("Decimal32(2)").coercer_category(),
            "Decimal"
        );
        assert_eq!(
            ParsedType::parse("Decimal64(4)").coercer_category(),
            "Decimal"
        );
        assert_eq!(
            ParsedType::parse("Decimal128(8)").coercer_category(),
            "Decimal"
        );
    }

    #[test]
    fn coercer_category_int_widths() {
        for t in ["Int8", "Int16", "Int32", "Int64", "Int128", "Int256"] {
            assert_eq!(
                ParsedType::parse(t).coercer_category(),
                "Int",
                "Failed for {t}"
            );
        }
    }

    #[test]
    fn coercer_category_uint_widths() {
        for t in ["UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256"] {
            assert_eq!(
                ParsedType::parse(t).coercer_category(),
                "UInt",
                "Failed for {t}"
            );
        }
    }

    #[test]
    fn coercer_category_nullable_preserves_inner() {
        // Nullable wrapper should not change the category
        assert_eq!(
            ParsedType::parse("Nullable(Int64)").coercer_category(),
            "Int"
        );
        assert_eq!(
            ParsedType::parse("Nullable(UUID)").coercer_category(),
            "UUID"
        );
    }

    #[test]
    fn coercer_category_lc_preserves_inner() {
        assert_eq!(
            ParsedType::parse("LowCardinality(String)").coercer_category(),
            "String"
        );
    }

    // ---- NEW: ParsedType edge cases ----

    #[test]
    fn parse_empty_string() {
        let t = ParsedType::parse("");
        assert_eq!(t.base, "");
    }

    #[test]
    fn parse_enum_type() {
        let t = ParsedType::parse("Enum8('a' = 1, 'b' = 2)");
        assert_eq!(t.coercer_category(), "Enum");
    }

    #[test]
    fn parse_nested_array() {
        let t = ParsedType::parse("Array(Array(String))");
        assert_eq!(t.base, "Array");
        let inner = t.array_element.as_ref().unwrap();
        assert_eq!(inner.base, "Array");
        let innermost = inner.array_element.as_ref().unwrap();
        assert_eq!(innermost.base, "String");
    }

    #[test]
    fn parse_fixedstring_with_length() {
        let t = ParsedType::parse("FixedString(16)");
        assert_eq!(t.base, "FixedString");
        assert!(t.is_string());
        assert_eq!(t.coercer_category(), "String");
    }
}
