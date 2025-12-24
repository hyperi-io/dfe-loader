// Project:   dfe-loader-clickhouse
// File:      types.rs
// Purpose:   ClickHouse type parsing (runtime, not compiled)
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse type system - runtime parsed, not compiled
//!
//! Following the Go clickhouse-loader pattern:
//! - ClickHouse is the Single Source of Truth (SSOT)
//! - Types are queried from system.columns at runtime
//! - Type mappings are config-driven for extensibility
//! - New ClickHouse types require config changes, not code changes

use std::fmt;

/// Parsed ClickHouse type information
///
/// This is a runtime representation, not an exhaustive enum.
/// Unknown types are preserved as-is for forward compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedType {
    /// Original type string from ClickHouse
    pub raw: String,
    /// Base type name (e.g., "String", "Int64", "DateTime64")
    pub base: String,
    /// Whether wrapped in Nullable()
    pub nullable: bool,
    /// Whether wrapped in LowCardinality()
    pub low_cardinality: bool,
    /// For Array types, the element type
    pub array_element: Option<Box<ParsedType>>,
    /// For Map types, (key_type, value_type)
    pub map_types: Option<(Box<ParsedType>, Box<ParsedType>)>,
    /// Extended info: precision for DateTime64, Decimal, etc.
    pub precision: Option<u8>,
    /// Extended info: scale for Decimal types
    pub scale: Option<u8>,
    /// Extended info: timezone for DateTime64
    pub timezone: Option<String>,
    /// Extended info: size for FixedString
    pub fixed_size: Option<usize>,
}

impl ParsedType {
    /// Parse a ClickHouse type string
    pub fn parse(type_str: &str) -> Self {
        let type_str = type_str.trim();
        Self::parse_inner(type_str, type_str.to_string())
    }

    fn parse_inner(type_str: &str, raw: String) -> Self {
        let mut result = Self {
            raw,
            base: String::new(),
            nullable: false,
            low_cardinality: false,
            array_element: None,
            map_types: None,
            precision: None,
            scale: None,
            timezone: None,
            fixed_size: None,
        };

        let mut type_str = type_str.trim().to_string();

        // Unwrap wrappers in a loop (handles LowCardinality(Nullable(...)) etc.)
        loop {
            let (unwrapped, is_nullable) = Self::unwrap_wrapper(&type_str, "Nullable");
            if is_nullable {
                result.nullable = true;
                type_str = unwrapped;
                continue;
            }

            let (unwrapped, is_lc) = Self::unwrap_wrapper(&type_str, "LowCardinality");
            if is_lc {
                result.low_cardinality = true;
                type_str = unwrapped;
                continue;
            }

            break;
        }

        // Check for Array
        if let Some(inner) = Self::extract_wrapper(&type_str, "Array") {
            result.base = "Array".to_string();
            result.array_element = Some(Box::new(Self::parse(&inner)));
            return result;
        }

        // Check for Map
        if let Some(inner) = Self::extract_wrapper(&type_str, "Map") {
            if let Some((key, value)) = Self::split_type_args(&inner) {
                result.base = "Map".to_string();
                result.map_types = Some((Box::new(Self::parse(&key)), Box::new(Self::parse(&value))));
                return result;
            }
        }

        // Check for DateTime64(precision, 'timezone')
        if type_str.starts_with("DateTime64") {
            result.base = "DateTime64".to_string();
            if let Some(inner) = Self::extract_wrapper(&type_str, "DateTime64") {
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                result.precision = parts.first().and_then(|p| p.trim().parse().ok());
                result.timezone = parts.get(1).map(|tz| {
                    tz.trim().trim_matches('\'').trim_matches('"').to_string()
                });
            }
            return result;
        }

        // Check for FixedString(N)
        if let Some(inner) = Self::extract_wrapper(&type_str, "FixedString") {
            result.base = "FixedString".to_string();
            result.fixed_size = inner.trim().parse().ok();
            return result;
        }

        // Check for Decimal(P, S) or Decimal32/64/128/256(S)
        if type_str.starts_with("Decimal") {
            result.base = Self::parse_decimal_base(&type_str);
            if let Some(inner) = Self::extract_parens(&type_str) {
                let parts: Vec<&str> = inner.split(',').collect();
                if parts.len() == 2 {
                    // Decimal(P, S)
                    result.precision = parts[0].trim().parse().ok();
                    result.scale = parts[1].trim().parse().ok();
                } else if parts.len() == 1 {
                    // Decimal32(S), etc.
                    result.scale = parts[0].trim().parse().ok();
                }
            }
            return result;
        }

        // Check for Enum8/Enum16 - just capture base, values aren't needed for coercion
        if type_str.starts_with("Enum8") || type_str.starts_with("Enum16") {
            result.base = if type_str.starts_with("Enum8") {
                "Enum8".to_string()
            } else {
                "Enum16".to_string()
            };
            return result;
        }

        // Simple type - just the base name
        result.base = type_str.to_string();
        result
    }

    /// Unwrap a wrapper type like Nullable(...) or LowCardinality(...)
    fn unwrap_wrapper(type_str: &str, wrapper: &str) -> (String, bool) {
        let prefix = format!("{}(", wrapper);
        if let Some(rest) = type_str.strip_prefix(&prefix) {
            if let Some(inner) = rest.strip_suffix(')') {
                return (inner.to_string(), true);
            }
        }
        (type_str.to_string(), false)
    }

    /// Extract inner content from Wrapper(...)
    fn extract_wrapper(type_str: &str, wrapper: &str) -> Option<String> {
        let prefix = format!("{}(", wrapper);
        type_str
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix(')'))
            .map(|s| s.to_string())
    }

    /// Extract content within parentheses
    fn extract_parens(type_str: &str) -> Option<String> {
        let start = type_str.find('(')?;
        let end = type_str.rfind(')')?;
        if start < end {
            Some(type_str[start + 1..end].to_string())
        } else {
            None
        }
    }

    /// Parse Decimal base name
    fn parse_decimal_base(type_str: &str) -> String {
        if type_str.starts_with("Decimal256") {
            "Decimal256".to_string()
        } else if type_str.starts_with("Decimal128") {
            "Decimal128".to_string()
        } else if type_str.starts_with("Decimal64") {
            "Decimal64".to_string()
        } else if type_str.starts_with("Decimal32") {
            "Decimal32".to_string()
        } else {
            "Decimal".to_string()
        }
    }

    /// Split Map(K, V) or similar two-arg types, handling nested parens
    fn split_type_args(inner: &str) -> Option<(String, String)> {
        let mut depth = 0;
        for (i, c) in inner.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    return Some((inner[..i].trim().to_string(), inner[i + 1..].trim().to_string()));
                }
                _ => {}
            }
        }
        None
    }

    /// Get the coercer category for this type
    ///
    /// Maps ClickHouse types to coercer categories for type conversion.
    /// Unknown types map to "String" (safe fallback).
    pub fn coercer_category(&self) -> &str {
        match self.base.as_str() {
            // String types
            "String" | "FixedString" => "String",

            // Integer types
            "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256" => "Int",
            "UInt8" | "UInt16" | "UInt32" | "UInt64" | "UInt128" | "UInt256" => "UInt",

            // Float types
            "Float32" | "Float64" => "Float",

            // Decimal types
            "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => "Decimal",

            // Boolean
            "Bool" => "Bool",

            // Date/time types
            "Date" | "Date32" => "Date",
            "DateTime" => "DateTime",
            "DateTime64" => "DateTime64",

            // UUID
            "UUID" => "UUID",

            // IP address types
            "IPv4" => "IPv4",
            "IPv6" => "IPv6",

            // Complex types
            "Array" => "Array",
            "Map" => "Map",
            "Tuple" => "Tuple",

            // JSON types (ClickHouse 23.1+)
            "JSON" | "Object" => "JSON",

            // Variant/Dynamic (ClickHouse 24.1+/25.3+)
            "Variant" => "Variant",
            "Dynamic" => "Dynamic",

            // Enum types
            "Enum8" | "Enum16" => "Enum",

            // Geo types
            "Point" | "Ring" | "Polygon" | "MultiPolygon" | "LineString" | "MultiLineString" => "Geo",

            // Unknown - fallback to String (safe coercion)
            _ => "String",
        }
    }

    /// Check if this is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(
            self.coercer_category(),
            "Int" | "UInt" | "Float" | "Decimal"
        )
    }

    /// Check if this is a string type
    pub fn is_string(&self) -> bool {
        self.coercer_category() == "String"
    }

    /// Check if this is a date/time type
    pub fn is_datetime(&self) -> bool {
        matches!(
            self.coercer_category(),
            "Date" | "DateTime" | "DateTime64"
        )
    }

    /// Check if this is an IP address type
    pub fn is_ip(&self) -> bool {
        matches!(self.coercer_category(), "IPv4" | "IPv6")
    }
}

impl fmt::Display for ParsedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

/// Default values for ClickHouse types (used when handling nulls)
///
/// These are safe zero values that avoid NULL in ClickHouse
/// (following ClickHouse best practices to avoid Nullable overhead)
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

/// Common null string representations to recognise
pub const NULL_STRINGS: &[&str] = &[
    "null", "NULL", "Null", "None", "nil", "undefined", "\\N", "<null>", "NA", "N/A", "n/a", "NaN", "",
];

/// Check if a string value represents null
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
    fn test_parse_nested_array() {
        let t = ParsedType::parse("Array(Nullable(String))");
        assert_eq!(t.base, "Array");
        let elem = t.array_element.as_ref().unwrap();
        assert_eq!(elem.base, "String");
        assert!(elem.nullable);
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
    fn test_parse_fixed_string() {
        let t = ParsedType::parse("FixedString(16)");
        assert_eq!(t.base, "FixedString");
        assert_eq!(t.fixed_size, Some(16));
    }

    #[test]
    fn test_parse_decimal() {
        let t = ParsedType::parse("Decimal(18, 4)");
        assert_eq!(t.base, "Decimal");
        assert_eq!(t.precision, Some(18));
        assert_eq!(t.scale, Some(4));

        let t = ParsedType::parse("Decimal64(2)");
        assert_eq!(t.base, "Decimal64");
        assert_eq!(t.scale, Some(2));
    }

    #[test]
    fn test_coercer_categories() {
        assert_eq!(ParsedType::parse("String").coercer_category(), "String");
        assert_eq!(ParsedType::parse("Int64").coercer_category(), "Int");
        assert_eq!(ParsedType::parse("UInt32").coercer_category(), "UInt");
        assert_eq!(ParsedType::parse("Float64").coercer_category(), "Float");
        assert_eq!(ParsedType::parse("Bool").coercer_category(), "Bool");
        assert_eq!(ParsedType::parse("DateTime").coercer_category(), "DateTime");
        assert_eq!(ParsedType::parse("DateTime64(3)").coercer_category(), "DateTime64");
        assert_eq!(ParsedType::parse("UUID").coercer_category(), "UUID");
        assert_eq!(ParsedType::parse("IPv4").coercer_category(), "IPv4");
        assert_eq!(ParsedType::parse("IPv6").coercer_category(), "IPv6");
        assert_eq!(ParsedType::parse("Array(String)").coercer_category(), "Array");
        assert_eq!(ParsedType::parse("Map(String, Int64)").coercer_category(), "Map");
        assert_eq!(ParsedType::parse("JSON").coercer_category(), "JSON");

        // Unknown type falls back to String
        assert_eq!(ParsedType::parse("SomeNewType").coercer_category(), "String");
    }

    #[test]
    fn test_is_helpers() {
        assert!(ParsedType::parse("Int64").is_numeric());
        assert!(ParsedType::parse("Float64").is_numeric());
        assert!(ParsedType::parse("Decimal(18, 4)").is_numeric());
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
        assert!(is_null_string("\\N"));
        assert!(!is_null_string("hello"));
        assert!(!is_null_string("0"));
    }

    #[test]
    fn test_default_values() {
        assert_eq!(default_value_for_category("String"), "");
        assert_eq!(default_value_for_category("Int"), "0");
        assert_eq!(default_value_for_category("Float"), "0.0");
        assert_eq!(default_value_for_category("Bool"), "false");
        assert_eq!(default_value_for_category("UUID"), "00000000-0000-0000-0000-000000000000");
        assert_eq!(default_value_for_category("IPv4"), "0.0.0.0");
    }
}
