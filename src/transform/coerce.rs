// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Type coercion to match ClickHouse schema
//!
//! Following the Go clickhouse-loader pattern:
//! - ClickHouse is the Single Source of Truth (SSOT)
//! - Type mappings are config-driven for extensibility
//! - Registry-based coercer lookup by type category

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use serde_json::Value;
use tracing::warn;

use crate::Result;
use crate::clickhouse::types::ParsedTypeExt;
use crate::clickhouse::{ParsedType, TableSchema};
use crate::config::{CoercionConfig, NullHandling};

/// Coercion mode — controls which type coercions are applied.
///
/// `Full` applies all coercions (for non-JSONEachRow insert paths).
/// `Delta` applies only the 4 coercions that ClickHouse JSONEachRow cannot
/// perform server-side — suitable for the schema-guided hot path.
///
/// Delta coercions:
/// - Epoch ms/μs/ns → DateTime64 ISO string (magnitude detection)
/// - ISO 8601 T separator → space (default `basic` parser rejects T)
/// - UUID without hyphens → RFC 4122 format
/// - IPv4 integer → dotted-decimal string
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CoercionMode {
    /// Apply all coercions (default — safe for any insert path)
    #[default]
    Full,
    /// Apply only coercions not handled by ClickHouse JSONEachRow server-side
    Delta,
}

/// Coercer registry for type-aware value conversion
pub struct Coercer {
    config: CoercionConfig,
    mode: CoercionMode,
}

impl Coercer {
    /// Create a new coercer with the given configuration (Full mode by default)
    pub fn new(config: CoercionConfig) -> Self {
        Self {
            config,
            mode: CoercionMode::Full,
        }
    }

    /// Set the coercion mode.
    pub fn with_mode(mut self, mode: CoercionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Coerce a JSON row map to match the table schema.
    ///
    /// Fields not in the schema are passed through unchanged.
    /// In `Delta` mode, only columns whose types require client-side coercion are
    /// touched — others are skipped entirely (no clone, no hashmap lookup).
    pub fn coerce_row(
        &self,
        row: &mut serde_json::Map<String, Value>,
        schema: &TableSchema,
    ) -> Result<()> {
        for col in &schema.columns {
            let field_name = col.name.as_str();

            // Delta mode: skip columns that JSONEachRow handles server-side.
            // Only coerce if no custom type_mapping overrides the category.
            if self.mode == CoercionMode::Delta
                && !self
                    .config
                    .type_mappings
                    .contains_key(&col.parsed_type.base)
            {
                let category = col.parsed_type.coercer_category();
                if !matches!(
                    category,
                    "DateTime64" | "DateTime" | "UUID" | "IPv4" | "Bool" | "Array"
                ) {
                    continue;
                }
            }

            if let Some(value) = row.get_mut(field_name) {
                match self.coerce_value(value, &col.parsed_type) {
                    Ok(coerced) => {
                        *value = coerced;
                    }
                    Err(e) => {
                        if self.config.strict {
                            return Err(e);
                        }
                        // Non-strict: log warning and use default
                        warn!(
                            field = field_name,
                            error = %e,
                            "Coercion failed, using default"
                        );
                        *value = self.default_value(&col.parsed_type);
                    }
                }
            }
        }

        Ok(())
    }

    /// Coerce a single value to match the target type.
    ///
    /// In `Delta` mode only the 4 coercions JSONEachRow cannot do server-side are applied;
    /// all other types pass through unchanged. This is O(1) per type check.
    pub fn coerce_value(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        // Handle null values first (both modes — null handling is always needed)
        if value.is_null() || self.is_null_value(value) {
            return self.handle_null(target);
        }

        // Get coercer category (may be overridden by config)
        let category = self
            .config
            .type_mappings
            .get(&target.base)
            .map(|s| s.as_str())
            .unwrap_or_else(|| target.coercer_category());

        // Delta mode: pass through all types that JSONEachRow handles server-side.
        // Only dispatch to specific coercers for the 4 delta cases + Bool + Array(DateTime64).
        if self.mode == CoercionMode::Delta {
            return match category {
                "DateTime64" => self.coerce_datetime64(value, target),
                "DateTime" => self.coerce_datetime(value),
                "UUID" => self.coerce_uuid(value),
                "IPv4" => self.coerce_ipv4(value),
                // Bool: CH handles 1/0/true/false; delta also handles non-standard strings
                "Bool" => self.coerce_bool(value),
                // Array: needed for Array(DateTime64) inner-element coercion
                "Array" => self.coerce_array(value, target),
                // Everything else: CH JSONEachRow handles server-side — pass through
                _ => Ok(value.clone()),
            };
        }

        // Full mode: dispatch to type-specific coercer
        match category {
            "String" => self.coerce_string(value, target),
            "Int" => self.coerce_int(value),
            "UInt" => self.coerce_uint(value),
            "Float" => self.coerce_float(value),
            "Decimal" => self.coerce_decimal(value, target),
            "Bool" => self.coerce_bool(value),
            "Date" => self.coerce_date(value),
            "DateTime" => self.coerce_datetime(value),
            "DateTime64" => self.coerce_datetime64(value, target),
            "UUID" => self.coerce_uuid(value),
            "IPv4" => self.coerce_ipv4(value),
            "IPv6" => self.coerce_ipv6(value),
            "Array" => self.coerce_array(value, target),
            "Map" => self.coerce_map(value, target),
            "JSON" => self.coerce_json(value),
            "Enum" => self.coerce_enum(value),
            _ => {
                // Unknown type - treat as string (safe fallback)
                self.coerce_string(value, target)
            }
        }
    }

    /// Check if a value represents null
    fn is_null_value(&self, value: &Value) -> bool {
        if let Some(s) = value.as_str() {
            self.config.is_null_string(s)
        } else {
            false
        }
    }

    /// Handle null values based on configuration
    fn handle_null(&self, target: &ParsedType) -> Result<Value> {
        if target.nullable {
            // Nullable column - pass null through
            return Ok(Value::Null);
        }

        match self.config.null_handling {
            NullHandling::Passthrough => Ok(Value::Null),
            NullHandling::Error => Err(crate::Error::Coercion(format!(
                "NULL value for non-nullable column type {}",
                target.raw
            ))),
            NullHandling::Default => Ok(self.default_value(target)),
        }
    }

    /// Get default value for a type
    fn default_value(&self, target: &ParsedType) -> Value {
        let category = target.coercer_category();
        match category {
            "String" => Value::String(String::new()),
            "Int" | "UInt" => Value::Number(0.into()),
            "Float" | "Decimal" => serde_json::json!(0.0),
            "Bool" => Value::Bool(false),
            "Date" => Value::String("1970-01-01".to_string()),
            "DateTime" | "DateTime64" => Value::String("1970-01-01 00:00:00.000".to_string()),
            "UUID" => Value::String("00000000-0000-0000-0000-000000000000".to_string()),
            "IPv4" => Value::String("0.0.0.0".to_string()),
            "IPv6" => Value::String("::".to_string()),
            "Array" => Value::Array(vec![]),
            "Map" | "JSON" => Value::Object(serde_json::Map::new()),
            _ => Value::String(String::new()),
        }
    }

    // ========================================================================
    // Type-specific coercers
    // ========================================================================

    /// Coerce to String type
    fn coerce_string(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let s = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i.to_string()
                } else if let Some(u) = n.as_u64() {
                    u.to_string()
                } else if let Some(f) = n.as_f64() {
                    if f.fract() == 0.0 && f.abs() < i64::MAX as f64 {
                        format!("{:.0}", f)
                    } else {
                        f.to_string()
                    }
                } else {
                    n.to_string()
                }
            }
            Value::Bool(b) => b.to_string(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
            Value::Null => String::new(),
        };

        // Handle FixedString length
        if let Some(size) = target.fixed_size
            && s.len() > size
        {
            return Ok(Value::String(s[..size].to_string()));
        }

        Ok(Value::String(s))
    }

    /// Coerce to signed integer
    fn coerce_int(&self, value: &Value) -> Result<Value> {
        let n = self.to_i64(value)?;
        Ok(Value::Number(n.into()))
    }

    /// Coerce to unsigned integer
    fn coerce_uint(&self, value: &Value) -> Result<Value> {
        let n = self.to_u64(value)?;
        Ok(Value::Number(n.into()))
    }

    /// Coerce to float
    fn coerce_float(&self, value: &Value) -> Result<Value> {
        let f = self.to_f64(value)?;
        Ok(serde_json::json!(f))
    }

    /// Coerce to decimal (stored as string for precision)
    fn coerce_decimal(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let f = self.to_f64(value)?;
        let scale = target.scale.unwrap_or(0) as usize;
        let s = format!("{:.prec$}", f, prec = scale);
        Ok(Value::String(s))
    }

    /// Coerce to boolean
    fn coerce_bool(&self, value: &Value) -> Result<Value> {
        let b = match value {
            Value::Bool(b) => *b,
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i != 0
                } else if let Some(f) = n.as_f64() {
                    f != 0.0
                } else {
                    false
                }
            }
            Value::String(s) => {
                let s = s.to_lowercase();
                matches!(s.as_str(), "true" | "1" | "yes" | "on" | "t" | "y")
            }
            _ => false,
        };
        Ok(Value::Bool(b))
    }

    /// Coerce to Date (YYYY-MM-DD)
    fn coerce_date(&self, value: &Value) -> Result<Value> {
        match value {
            Value::String(s) => {
                if s.len() >= 10 {
                    let date_part = &s[..10];
                    if self.is_valid_date(date_part) {
                        return Ok(Value::String(date_part.to_string()));
                    }
                }
                if let Ok(ts) = s.parse::<i64>() {
                    return Ok(Value::String(self.epoch_to_date(ts)));
                }
                Err(crate::Error::Coercion(format!("Invalid date: {}", s)))
            }
            Value::Number(n) => {
                let ts = n.as_i64().ok_or_else(|| {
                    crate::Error::Coercion("Cannot convert number to date".to_string())
                })?;
                Ok(Value::String(self.epoch_to_date(ts)))
            }
            _ => Err(crate::Error::Coercion("Cannot convert to date".to_string())),
        }
    }

    /// Coerce to DateTime (YYYY-MM-DD HH:MM:SS)
    fn coerce_datetime(&self, value: &Value) -> Result<Value> {
        match value {
            Value::String(s) => {
                if s.contains('T') || s.contains(' ') {
                    let normalized = s.replace('T', " ");
                    let dt = normalized.split('+').next().unwrap_or(&normalized);
                    let dt = dt.split('Z').next().unwrap_or(dt);
                    if dt.len() >= 19 {
                        return Ok(Value::String(dt[..19].to_string()));
                    }
                }
                if let Ok(ts) = s.parse::<i64>() {
                    return Ok(Value::String(self.epoch_to_datetime(ts)));
                }
                Err(crate::Error::Coercion(format!("Invalid datetime: {}", s)))
            }
            Value::Number(n) => {
                let ts = n.as_i64().ok_or_else(|| {
                    crate::Error::Coercion("Cannot convert number to datetime".to_string())
                })?;
                Ok(Value::String(self.epoch_to_datetime(ts)))
            }
            _ => Err(crate::Error::Coercion(
                "Cannot convert to datetime".to_string(),
            )),
        }
    }

    /// Coerce to DateTime64 (with sub-second precision)
    fn coerce_datetime64(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let precision = target.precision.unwrap_or(3) as usize;

        match value {
            Value::String(s) => {
                // Normalise ISO 8601 format to ClickHouse-accepted "YYYY-MM-DD HH:MM:SS.mmm".
                // Must run before the `.` early-return — "2024-12-25T10:30:00.123Z" contains
                // a dot, so without this check it would pass through unchanged and be rejected.
                if s.contains('T') {
                    let normalized = s.replace('T', " ");
                    let dt = normalized.split('+').next().unwrap_or(&normalized);
                    let dt = dt.split('Z').next().unwrap_or(dt);
                    return Ok(Value::String(dt.to_string()));
                }
                if s.contains('.') {
                    return Ok(value.clone());
                }
                if let Ok(ts) = s.parse::<f64>() {
                    return Ok(Value::String(self.epoch_to_datetime64(ts, precision)));
                }
                if let Ok(ts) = s.parse::<i64>() {
                    return Ok(Value::String(
                        self.epoch_to_datetime64(ts as f64, precision),
                    ));
                }
                Ok(Value::String(format!("{}.{}", s, "0".repeat(precision))))
            }
            Value::Number(n) => {
                let ts = n.as_f64().ok_or_else(|| {
                    crate::Error::Coercion("Cannot convert number to datetime64".to_string())
                })?;
                Ok(Value::String(self.epoch_to_datetime64(ts, precision)))
            }
            _ => Err(crate::Error::Coercion(
                "Cannot convert to datetime64".to_string(),
            )),
        }
    }

    /// Coerce to UUID
    fn coerce_uuid(&self, value: &Value) -> Result<Value> {
        let s = value
            .as_str()
            .ok_or_else(|| crate::Error::Coercion("UUID must be a string".to_string()))?;
        let normalized = self.normalize_uuid(s)?;
        Ok(Value::String(normalized))
    }

    /// Coerce to IPv4
    fn coerce_ipv4(&self, value: &Value) -> Result<Value> {
        match value {
            Value::String(s) => Ipv4Addr::from_str(s)
                .map(|_| value.clone())
                .map_err(|e| crate::Error::Coercion(format!("Invalid IPv4: {}", e))),
            Value::Number(n) => {
                let num = n.as_u64().ok_or_else(|| {
                    crate::Error::Coercion("Cannot convert number to IPv4".to_string())
                })? as u32;
                let ip = Ipv4Addr::from(num);
                Ok(Value::String(ip.to_string()))
            }
            _ => Err(crate::Error::Coercion("Cannot convert to IPv4".to_string())),
        }
    }

    /// Coerce to IPv6
    fn coerce_ipv6(&self, value: &Value) -> Result<Value> {
        match value {
            Value::String(s) => Ipv6Addr::from_str(s)
                .map(|_| value.clone())
                .map_err(|e| crate::Error::Coercion(format!("Invalid IPv6: {}", e))),
            _ => Err(crate::Error::Coercion("Cannot convert to IPv6".to_string())),
        }
    }

    /// Coerce to Array
    fn coerce_array(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let arr = match value {
            Value::Array(arr) => arr,
            Value::String(s) if s.starts_with('[') => {
                let parsed: Value = serde_json::from_str(s)
                    .map_err(|e| crate::Error::Coercion(format!("Invalid array JSON: {}", e)))?;
                return self.coerce_array(&parsed, target);
            }
            _ => {
                return Ok(Value::Array(vec![value.clone()]));
            }
        };

        if let Some(ref elem_type) = target.array_element {
            let mut coerced = Vec::with_capacity(arr.len());
            for elem in arr {
                coerced.push(self.coerce_value(elem, elem_type)?);
            }
            Ok(Value::Array(coerced))
        } else {
            Ok(value.clone())
        }
    }

    /// Coerce to Map
    fn coerce_map(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let obj = match value {
            Value::Object(obj) => obj,
            Value::String(s) if s.starts_with('{') => {
                let parsed: Value = serde_json::from_str(s)
                    .map_err(|e| crate::Error::Coercion(format!("Invalid map JSON: {}", e)))?;
                return self.coerce_map(&parsed, target);
            }
            _ => return Err(crate::Error::Coercion("Cannot convert to map".to_string())),
        };

        if let Some((_, ref value_type)) = target.map_types {
            let mut coerced = serde_json::Map::new();
            for (k, v) in obj {
                let coerced_val = self.coerce_value(v, value_type)?;
                coerced.insert(k.clone(), coerced_val);
            }
            Ok(Value::Object(coerced))
        } else {
            Ok(value.clone())
        }
    }

    /// Coerce to JSON
    ///
    /// Strings are parsed into their actual JSON value — ClickHouse JSON type
    /// requires a real object, not a string containing JSON.
    fn coerce_json(&self, value: &Value) -> Result<Value> {
        if let Value::String(s) = value {
            let parsed: Value = serde_json::from_str(s)
                .map_err(|e| crate::Error::Coercion(format!("Invalid JSON: {}", e)))?;
            return Ok(parsed);
        }
        Ok(value.clone())
    }

    /// Coerce to Enum (validate string value)
    fn coerce_enum(&self, value: &Value) -> Result<Value> {
        match value {
            Value::String(_) => Ok(value.clone()),
            Value::Number(n) => {
                let s = n.to_string();
                Ok(Value::String(s))
            }
            _ => Err(crate::Error::Coercion(
                "Enum must be string or integer".to_string(),
            )),
        }
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn to_i64(&self, value: &Value) -> Result<i64> {
        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(i)
                } else if let Some(u) = n.as_u64() {
                    if u > i64::MAX as u64 {
                        Err(crate::Error::Coercion("Integer overflow".to_string()))
                    } else {
                        Ok(u as i64)
                    }
                } else if let Some(f) = n.as_f64() {
                    if f > i64::MAX as f64 || f < i64::MIN as f64 {
                        Err(crate::Error::Coercion(
                            "Float overflow for integer".to_string(),
                        ))
                    } else {
                        Ok(f as i64)
                    }
                } else {
                    Err(crate::Error::Coercion("Invalid number".to_string()))
                }
            }
            Value::String(s) => {
                let s = s.trim();
                if let Ok(n) = s.parse::<i64>() {
                    return Ok(n);
                }
                if let Ok(f) = s.parse::<f64>()
                    && f.fract() == 0.0
                {
                    return Ok(f as i64);
                }
                Err(crate::Error::Coercion(format!(
                    "Cannot parse '{}' as integer",
                    s
                )))
            }
            Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
            _ => Err(crate::Error::Coercion(
                "Cannot convert to integer".to_string(),
            )),
        }
    }

    fn to_u64(&self, value: &Value) -> Result<u64> {
        match value {
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Ok(u)
                } else if let Some(i) = n.as_i64() {
                    if i < 0 {
                        Err(crate::Error::Coercion(
                            "Negative value for unsigned integer".to_string(),
                        ))
                    } else {
                        Ok(i as u64)
                    }
                } else if let Some(f) = n.as_f64() {
                    if f < 0.0 || f > u64::MAX as f64 {
                        Err(crate::Error::Coercion(
                            "Float out of range for unsigned integer".to_string(),
                        ))
                    } else {
                        Ok(f as u64)
                    }
                } else {
                    Err(crate::Error::Coercion("Invalid number".to_string()))
                }
            }
            Value::String(s) => {
                let s = s.trim();
                if let Ok(n) = s.parse::<u64>() {
                    return Ok(n);
                }
                if let Ok(f) = s.parse::<f64>()
                    && f >= 0.0
                    && f.fract() == 0.0
                {
                    return Ok(f as u64);
                }
                Err(crate::Error::Coercion(format!(
                    "Cannot parse '{}' as unsigned integer",
                    s
                )))
            }
            Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
            _ => Err(crate::Error::Coercion(
                "Cannot convert to unsigned integer".to_string(),
            )),
        }
    }

    fn to_f64(&self, value: &Value) -> Result<f64> {
        match value {
            Value::Number(n) => n.as_f64().ok_or_else(|| {
                crate::Error::Coercion("Cannot convert number to float".to_string())
            }),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map_err(|_| crate::Error::Coercion(format!("Cannot parse '{}' as float", s))),
            Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            _ => Err(crate::Error::Coercion(
                "Cannot convert to float".to_string(),
            )),
        }
    }

    fn is_valid_date(&self, s: &str) -> bool {
        if s.len() != 10 {
            return false;
        }
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return false;
        }
        parts[0].len() == 4
            && parts[1].len() == 2
            && parts[2].len() == 2
            && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()))
    }

    fn epoch_to_date(&self, ts: i64) -> String {
        let secs = if ts > 1_000_000_000_000_000_000 {
            ts / 1_000_000_000
        } else if ts > 1_000_000_000_000_000 {
            ts / 1_000_000
        } else if ts > 1_000_000_000_000 {
            ts / 1_000
        } else {
            ts
        };
        let days = secs / 86400;
        self.days_to_date(days as i32)
    }

    fn epoch_to_datetime(&self, ts: i64) -> String {
        let secs = if ts > 1_000_000_000_000_000_000 {
            ts / 1_000_000_000
        } else if ts > 1_000_000_000_000_000 {
            ts / 1_000_000
        } else if ts > 1_000_000_000_000 {
            ts / 1_000
        } else {
            ts
        };

        let date = self.epoch_to_date(secs);
        let time_of_day = secs % 86400;
        let hours = time_of_day / 3600;
        let minutes = (time_of_day % 3600) / 60;
        let seconds = time_of_day % 60;

        format!("{} {:02}:{:02}:{:02}", date, hours, minutes, seconds)
    }

    fn epoch_to_datetime64(&self, ts: f64, precision: usize) -> String {
        let (secs, frac) = if ts > 1e18 {
            let s = (ts / 1e9) as i64;
            let f = (ts % 1e9) / 1e9;
            (s, f)
        } else if ts > 1e15 {
            let s = (ts / 1e6) as i64;
            let f = (ts % 1e6) / 1e6;
            (s, f)
        } else if ts > 1e12 {
            let s = (ts / 1e3) as i64;
            let f = (ts % 1e3) / 1e3;
            (s, f)
        } else {
            (ts as i64, ts.fract())
        };

        let base = self.epoch_to_datetime(secs);
        let frac_str = format!("{:.prec$}", frac, prec = precision);
        let frac_digits = &frac_str[2..];
        format!("{}.{}", base, frac_digits)
    }

    fn days_to_date(&self, days: i32) -> String {
        let z = days + 719468;
        let era = if z >= 0 { z } else { z - 146096 } / 146097;
        let doe = (z - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };

        format!("{:04}-{:02}-{:02}", y, m, d)
    }

    fn normalize_uuid(&self, s: &str) -> Result<String> {
        let s = s
            .trim()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim_start_matches("urn:uuid:");

        let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();

        if hex.len() != 32 {
            return Err(crate::Error::Coercion(format!(
                "Invalid UUID: expected 32 hex chars, got {}",
                hex.len()
            )));
        }

        Ok(format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        ))
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    fn default_coercer() -> Coercer {
        Coercer::new(CoercionConfig::default())
    }

    // ========================================================================
    // Integer coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_int_from_int() {
        let c = default_coercer();
        let v = serde_json::json!(42);
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_coerce_int_from_float() {
        let c = default_coercer();
        let v = serde_json::json!(42.0);
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_coerce_int_from_string() {
        let c = default_coercer();
        let v = serde_json::json!("42");
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_coerce_int_from_bool() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_int(&serde_json::json!(true)).unwrap().as_i64(),
            Some(1)
        );
        assert_eq!(
            c.coerce_int(&serde_json::json!(false)).unwrap().as_i64(),
            Some(0)
        );
    }

    // ========================================================================
    // Float coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_float_from_float() {
        let c = default_coercer();
        let v = serde_json::json!(3.14);
        let result = c.coerce_float(&v).unwrap();
        assert!((result.as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_coerce_float_from_int() {
        let c = default_coercer();
        let v = serde_json::json!(42);
        let result = c.coerce_float(&v).unwrap();
        assert_eq!(result.as_f64(), Some(42.0));
    }

    #[test]
    fn test_coerce_float_from_string() {
        let c = default_coercer();
        let v = serde_json::json!("3.14159");
        let result = c.coerce_float(&v).unwrap();
        assert!((result.as_f64().unwrap() - 3.14159).abs() < 0.00001);
    }

    // ========================================================================
    // String coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_string_from_string() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!("hello");
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("hello"));
    }

    #[test]
    fn test_coerce_string_from_int() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!(42);
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("42"));
    }

    #[test]
    fn test_coerce_string_from_float_whole() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!(42.0);
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("42"));
    }

    #[test]
    fn test_coerce_string_from_array() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!([1, 2, 3]);
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("[1,2,3]"));
    }

    // ========================================================================
    // Boolean coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_bool_from_bool() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_bool(&serde_json::json!(true)).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!(false)).unwrap().as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_coerce_bool_from_int() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_bool(&serde_json::json!(1)).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!(0)).unwrap().as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_coerce_bool_from_string() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_bool(&serde_json::json!("true")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("yes")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("1")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("false"))
                .unwrap()
                .as_bool(),
            Some(false)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("no")).unwrap().as_bool(),
            Some(false)
        );
    }

    // ========================================================================
    // UUID coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_uuid_standard() {
        let c = default_coercer();
        let v = serde_json::json!("550e8400-e29b-41d4-a716-446655440000");
        let result = c.coerce_uuid(&v).unwrap();
        assert_eq!(
            result.as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_coerce_uuid_no_hyphens() {
        let c = default_coercer();
        let v = serde_json::json!("550e8400e29b41d4a716446655440000");
        let result = c.coerce_uuid(&v).unwrap();
        assert_eq!(
            result.as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_coerce_uuid_with_braces() {
        let c = default_coercer();
        let v = serde_json::json!("{550e8400-e29b-41d4-a716-446655440000}");
        let result = c.coerce_uuid(&v).unwrap();
        assert_eq!(
            result.as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    // ========================================================================
    // IP address coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_ipv4_string() {
        let c = default_coercer();
        let v = serde_json::json!("192.168.1.1");
        let result = c.coerce_ipv4(&v).unwrap();
        assert_eq!(result.as_str(), Some("192.168.1.1"));
    }

    #[test]
    fn test_coerce_ipv4_integer() {
        let c = default_coercer();
        let v = serde_json::json!(3232235777u64);
        let result = c.coerce_ipv4(&v).unwrap();
        assert_eq!(result.as_str(), Some("192.168.1.1"));
    }

    #[test]
    fn test_coerce_ipv6_string() {
        let c = default_coercer();
        let v = serde_json::json!("::1");
        let result = c.coerce_ipv6(&v).unwrap();
        assert_eq!(result.as_str(), Some("::1"));
    }

    // ========================================================================
    // Date/time coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_date_string() {
        let c = default_coercer();
        let v = serde_json::json!("2024-12-24");
        let result = c.coerce_date(&v).unwrap();
        assert_eq!(result.as_str(), Some("2024-12-24"));
    }

    #[test]
    fn test_coerce_date_from_datetime() {
        let c = default_coercer();
        let v = serde_json::json!("2024-12-24T10:30:00Z");
        let result = c.coerce_date(&v).unwrap();
        assert_eq!(result.as_str(), Some("2024-12-24"));
    }

    #[test]
    fn test_coerce_datetime_string() {
        let c = default_coercer();
        let v = serde_json::json!("2024-12-24T10:30:00Z");
        let result = c.coerce_datetime(&v).unwrap();
        assert_eq!(result.as_str(), Some("2024-12-24 10:30:00"));
    }

    // ========================================================================
    // Null handling tests
    // ========================================================================

    #[test]
    fn test_null_value_nullable_column() {
        let c = default_coercer();
        let target = ParsedType::parse("Nullable(String)");
        let result = c.handle_null(&target).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_null_value_non_nullable_default() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let result = c.handle_null(&target).unwrap();
        assert_eq!(result.as_str(), Some(""));
    }

    #[test]
    fn test_null_value_non_nullable_error() {
        let config = CoercionConfig {
            null_handling: NullHandling::Error,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let target = ParsedType::parse("String");
        let result = c.handle_null(&target);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_string_detection() {
        let c = default_coercer();
        assert!(c.is_null_value(&serde_json::json!("null")));
        assert!(c.is_null_value(&serde_json::json!("NULL")));
        assert!(c.is_null_value(&serde_json::json!("None")));
        assert!(c.is_null_value(&serde_json::json!("")));
        assert!(!c.is_null_value(&serde_json::json!("hello")));
        assert!(!c.is_null_value(&serde_json::json!("0")));
    }

    // ========================================================================
    // Array coercion tests
    // ========================================================================

    #[test]
    fn test_coerce_array_passthrough() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(Int64)");
        let v = serde_json::json!([1, 2, 3]);
        let result = c.coerce_array(&v, &target).unwrap();
        assert!(result.is_array());
    }

    #[test]
    fn test_coerce_array_from_json_string() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(Int64)");
        let v = serde_json::json!("[1, 2, 3]");
        let result = c.coerce_array(&v, &target).unwrap();
        assert!(result.is_array());
    }

    // ========================================================================
    // Helper tests
    // ========================================================================

    #[test]
    fn test_days_to_date() {
        let c = default_coercer();
        assert_eq!(c.days_to_date(0), "1970-01-01");
        assert_eq!(c.days_to_date(1), "1970-01-02");
        assert_eq!(c.days_to_date(365), "1971-01-01");
    }

    #[test]
    fn test_epoch_to_date() {
        let c = default_coercer();
        assert_eq!(c.epoch_to_date(0), "1970-01-01");
        assert_eq!(c.epoch_to_date(1735084800), "2024-12-25");
    }

    #[test]
    fn test_normalize_uuid() {
        let c = default_coercer();

        assert_eq!(
            c.normalize_uuid("550e8400-e29b-41d4-a716-446655440000")
                .unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );

        assert_eq!(
            c.normalize_uuid("550e8400e29b41d4a716446655440000")
                .unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );

        assert!(c.normalize_uuid("550e8400").is_err());
    }
}
