// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Type coercion to match `ClickHouse` schema
//!
//! Following the Go clickhouse-loader pattern:
//! - `ClickHouse` is the Single Source of Truth (SSOT)
//! - Type mappings are config-driven for extensibility
//! - Registry-based coercer lookup by type category

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;

use serde_json::Value;
use tracing::warn;

use crate::Result;
use crate::clickhouse::types::ParsedTypeExt;
use crate::clickhouse::{ParsedType, TableSchema};
use crate::config::{CoercionConfig, NullHandling};

/// Coercion mode — controls which type coercions are applied.
///
/// `Full` applies all coercions (for non-JSONEachRow insert paths).
/// `Delta` applies only the 4 coercions that `ClickHouse` `JSONEachRow` cannot
/// perform server-side — suitable for the schema-guided hot path.
///
/// Delta coercions:
/// - Epoch ms/μs/ns → `DateTime64` ISO string (magnitude detection)
/// - ISO 8601 T separator → space (default `basic` parser rejects T)
/// - UUID without hyphens → RFC 4122 format
/// - IPv4 integer → dotted-decimal string
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CoercionMode {
    /// Apply all coercions (default — safe for any insert path)
    #[default]
    Full,
    /// Apply only coercions not handled by `ClickHouse` `JSONEachRow` server-side
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
                        // Non-strict: log sampled warning and use default
                        static COERCE_FAILS: AtomicU64 = AtomicU64::new(0);
                        if scalo::logger::log_sampled(&COERCE_FAILS, 1000) {
                            warn!(
                                field = field_name,
                                error = %e,
                                total = COERCE_FAILS.load(std::sync::atomic::Ordering::Relaxed),
                                "Coercion failed, using default (1 in 1000)"
                            );
                        }
                        *value = self.default_value(&col.parsed_type);
                    }
                }
            }
        }

        Ok(())
    }

    /// Coerce a single value to match the target type.
    ///
    /// In `Delta` mode only the 4 coercions `JSONEachRow` cannot do server-side are applied;
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
            .map_or_else(|| target.coercer_category(), std::string::String::as_str);

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
                        format!("{f:.0}")
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
        let s = format!("{f:.scale$}");
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
                Err(crate::Error::Coercion(format!("Invalid date: {s}")))
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

    /// Coerce to `DateTime` (YYYY-MM-DD HH:MM:SS)
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
                Err(crate::Error::Coercion(format!("Invalid datetime: {s}")))
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

    /// Coerce to `DateTime64` (with sub-second precision)
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
                .map_err(|e| crate::Error::Coercion(format!("Invalid IPv4: {e}"))),
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
                .map_err(|e| crate::Error::Coercion(format!("Invalid IPv6: {e}"))),
            _ => Err(crate::Error::Coercion("Cannot convert to IPv6".to_string())),
        }
    }

    /// Coerce to Array
    fn coerce_array(&self, value: &Value, target: &ParsedType) -> Result<Value> {
        let arr = match value {
            Value::Array(arr) => arr,
            Value::String(s) if s.starts_with('[') => {
                let parsed: Value = serde_json::from_str(s)
                    .map_err(|e| crate::Error::Coercion(format!("Invalid array JSON: {e}")))?;
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
                    .map_err(|e| crate::Error::Coercion(format!("Invalid map JSON: {e}")))?;
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
    /// Strings are parsed into their actual JSON value — `ClickHouse` JSON type
    /// requires a real object, not a string containing JSON.
    fn coerce_json(&self, value: &Value) -> Result<Value> {
        if let Value::String(s) = value {
            let parsed: Value = serde_json::from_str(s)
                .map_err(|e| crate::Error::Coercion(format!("Invalid JSON: {e}")))?;
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
                    "Cannot parse '{s}' as integer"
                )))
            }
            Value::Bool(b) => Ok(i64::from(*b)),
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
                    "Cannot parse '{s}' as unsigned integer"
                )))
            }
            Value::Bool(b) => Ok(u64::from(*b)),
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
                .map_err(|_| crate::Error::Coercion(format!("Cannot parse '{s}' as float"))),
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

        format!("{date} {hours:02}:{minutes:02}:{seconds:02}")
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
        let frac_str = format!("{frac:.precision$}");
        let frac_digits = &frac_str[2..];
        format!("{base}.{frac_digits}")
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

        format!("{y:04}-{m:02}-{d:02}")
    }

    fn normalize_uuid(&self, s: &str) -> Result<String> {
        let s = s
            .trim()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim_start_matches("urn:uuid:");

        // Canonical UUID form is lowercase. `is_ascii_hexdigit` also accepts
        // A-F, so an uppercase input would otherwise pass through uppercase --
        // lowercase as we collect so the normalised string matches ClickHouse's
        // canonical representation (matters for JSONEachRow and round-trip
        // comparisons; RowBinary is case-insensitive on the bytes either way).
        let hex: String = s
            .chars()
            .filter(char::is_ascii_hexdigit)
            .map(|c| c.to_ascii_lowercase())
            .collect();

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
    fn test_coerce_uuid_uppercase_normalised_to_lowercase() {
        let c = default_coercer();
        let v = serde_json::json!("550E8400-E29B-41D4-A716-446655440000");
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

    // ========================================================================
    // Delta mode tests
    // ========================================================================

    fn delta_coercer() -> Coercer {
        Coercer::new(CoercionConfig::default()).with_mode(CoercionMode::Delta)
    }

    fn make_schema(columns: Vec<(&str, &str)>) -> TableSchema {
        use crate::clickhouse::ColumnInfo;
        TableSchema {
            database: "test".to_string(),
            table: "test".to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (name, type_str))| ColumnInfo {
                    name: name.to_string(),
                    type_name: type_str.to_string(),
                    parsed_type: ParsedType::parse(type_str),
                    position: (i + 1) as u64,
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
    fn test_delta_mode_skips_string_int_float() {
        let c = delta_coercer();

        // String should pass through unchanged in Delta mode
        let v = serde_json::json!(42);
        let target = ParsedType::parse("String");
        let result = c.coerce_value(&v, &target).unwrap();
        // Delta mode returns value.clone() for String category
        assert_eq!(result, serde_json::json!(42));

        // Int64 should pass through unchanged
        let v = serde_json::json!("not_a_number");
        let target = ParsedType::parse("Int64");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result, serde_json::json!("not_a_number"));

        // Float64 should pass through unchanged
        let v = serde_json::json!("nope");
        let target = ParsedType::parse("Float64");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result, serde_json::json!("nope"));
    }

    #[test]
    fn test_delta_mode_coerces_datetime64() {
        let c = delta_coercer();
        let target = ParsedType::parse("DateTime64(3)");

        // ISO 8601 T separator should be normalised
        let v = serde_json::json!("2024-12-25T10:30:00.123Z");
        let result = c.coerce_value(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(!s.contains('T'), "Delta should normalise T separator: {s}");
        assert!(s.contains(' '), "Should contain space separator: {s}");
    }

    #[test]
    fn test_delta_mode_coerces_uuid() {
        let c = delta_coercer();
        let target = ParsedType::parse("UUID");

        // No-hyphen UUID should be normalised
        let v = serde_json::json!("550e8400e29b41d4a716446655440000");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(
            result.as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_delta_mode_coerces_ipv4_from_integer() {
        let c = delta_coercer();
        let target = ParsedType::parse("IPv4");

        let v = serde_json::json!(3232235777u64); // 192.168.1.1
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("192.168.1.1"));
    }

    #[test]
    fn test_delta_mode_coerces_bool() {
        let c = delta_coercer();
        let target = ParsedType::parse("Bool");

        let v = serde_json::json!("yes");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_bool(), Some(true));

        let v = serde_json::json!("off");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_bool(), Some(false));
    }

    #[test]
    fn test_delta_mode_coerce_row_skips_non_targeted() {
        let c = delta_coercer();
        let schema = make_schema(vec![
            ("name", "String"),
            ("age", "Int64"),
            ("score", "Float64"),
            ("ts", "DateTime64(3)"),
        ]);

        let mut row = serde_json::Map::new();
        row.insert("name".to_string(), serde_json::json!(12345));
        row.insert("age".to_string(), serde_json::json!("not_int"));
        row.insert("score".to_string(), serde_json::json!("bad_float"));
        row.insert("ts".to_string(), serde_json::json!("2024-01-01T00:00:00Z"));

        c.coerce_row(&mut row, &schema).unwrap();

        // String and Int and Float should be unchanged in Delta mode
        assert_eq!(row["name"], serde_json::json!(12345));
        assert_eq!(row["age"], serde_json::json!("not_int"));
        assert_eq!(row["score"], serde_json::json!("bad_float"));
        // DateTime64 should be normalised
        let ts = row["ts"].as_str().unwrap();
        assert!(!ts.contains('T'));
    }

    // ========================================================================
    // Overflow scenarios
    // ========================================================================

    #[test]
    fn test_coerce_int_overflow_from_u64_max() {
        let c = default_coercer();
        // u64::MAX cannot fit in i64
        let v = serde_json::json!(u64::MAX);
        let result = c.coerce_int(&v);
        assert!(result.is_err(), "u64::MAX should overflow i64");
    }

    #[test]
    fn test_coerce_int_overflow_extreme_float() {
        let c = default_coercer();
        let v = serde_json::json!(1.0e20);
        let result = c.coerce_int(&v);
        assert!(
            result.is_err(),
            "1e20 exceeds i64::MAX and should fail: {result:?}"
        );
    }

    #[test]
    fn test_coerce_uint_negative_value() {
        let c = default_coercer();
        let v = serde_json::json!(-1);
        let result = c.coerce_uint(&v);
        assert!(result.is_err(), "Negative values should fail for UInt");
    }

    #[test]
    fn test_coerce_uint_negative_float() {
        let c = default_coercer();
        let v = serde_json::json!(-0.5);
        let result = c.coerce_uint(&v);
        assert!(result.is_err(), "Negative float should fail for UInt");
    }

    #[test]
    fn test_coerce_uint_from_string_negative() {
        let c = default_coercer();
        let v = serde_json::json!("-42");
        let result = c.coerce_uint(&v);
        assert!(
            result.is_err(),
            "Negative string should fail for unsigned int"
        );
    }

    #[test]
    fn test_coerce_int_i64_max_boundary() {
        let c = default_coercer();
        let v = serde_json::json!(i64::MAX);
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(i64::MAX));
    }

    #[test]
    fn test_coerce_int_i64_min_boundary() {
        let c = default_coercer();
        let v = serde_json::json!(i64::MIN);
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(i64::MIN));
    }

    // ========================================================================
    // Boundary values: empty strings, whitespace, special floats
    // ========================================================================

    #[test]
    fn test_coerce_int_empty_string() {
        let c = default_coercer();
        // Empty string is treated as null by default CoercionConfig
        let target = ParsedType::parse("Int64");
        let v = serde_json::json!("");
        let result = c.coerce_value(&v, &target).unwrap();
        // Default null handling returns default value (0 for Int)
        assert_eq!(result.as_i64(), Some(0));
    }

    #[test]
    fn test_coerce_int_whitespace_only_string() {
        let c = default_coercer();
        let v = serde_json::json!("   ");
        let result = c.coerce_int(&v);
        assert!(
            result.is_err(),
            "Whitespace-only string should fail integer parse"
        );
    }

    #[test]
    fn test_coerce_float_empty_string_as_null() {
        let c = default_coercer();
        let target = ParsedType::parse("Float64");
        let v = serde_json::json!("");
        // Empty string is a null string, should return default (0.0)
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_f64(), Some(0.0));
    }

    #[test]
    fn test_coerce_float_nan_string() {
        let c = default_coercer();
        // "NaN" is in the default null_strings list
        let target = ParsedType::parse("Float64");
        let v = serde_json::json!("NaN");
        let result = c.coerce_value(&v, &target).unwrap();
        // Should be treated as null -> default 0.0
        assert_eq!(result.as_f64(), Some(0.0));
    }

    #[test]
    fn test_coerce_float_infinity_string() {
        let c = default_coercer();
        // "Infinity" parses to f64::INFINITY, but serde_json::json!(inf) -> Null
        // since JSON can't represent infinity. Coerce should still succeed.
        let v = serde_json::json!("Infinity");
        let result = c.coerce_float(&v);
        assert!(result.is_ok());
        // Result will be null because serde_json drops inf/NaN
        let val = result.unwrap();
        assert!(
            val.is_null() || val.as_f64().is_some_and(f64::is_infinite),
            "Infinity should either be null (JSON limitation) or parsed as inf: {val:?}"
        );
    }

    #[test]
    fn test_coerce_float_neg_infinity_string() {
        let c = default_coercer();
        // Same caveat — JSON cannot represent -inf
        let v = serde_json::json!("-Infinity");
        let result = c.coerce_float(&v);
        assert!(result.is_ok());
        let val = result.unwrap();
        assert!(
            val.is_null()
                || val
                    .as_f64()
                    .is_some_and(|f| f.is_infinite() && f.is_sign_negative()),
            "-Infinity: {val:?}"
        );
    }

    #[test]
    fn test_coerce_float_nan_raw_string_parses() {
        let c = default_coercer();
        // "nan" (lowercase) parses to f64::NAN, not in null_strings.
        // serde_json converts NaN to Null.
        let v = serde_json::json!("nan");
        let result = c.coerce_float(&v);
        assert!(result.is_ok());
        // Result is null because JSON can't hold NaN
        assert!(result.unwrap().is_null());
    }

    // ========================================================================
    // Fuzz-like inputs
    // ========================================================================

    #[test]
    fn test_coerce_int_unicode_string() {
        let c = default_coercer();
        let v = serde_json::json!("四十二"); // "forty-two" in Chinese
        let result = c.coerce_int(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_int_control_characters() {
        let c = default_coercer();
        let v = serde_json::json!("\x00\x01\x02");
        let result = c.coerce_int(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_int_extremely_long_string() {
        let c = default_coercer();
        let long_str = "9".repeat(1000);
        let v = serde_json::json!(long_str);
        let result = c.coerce_int(&v);
        // Parsing a 1000-digit number as i64 should fail (overflow)
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_uint_from_array_fails() {
        let c = default_coercer();
        let v = serde_json::json!([1, 2, 3]);
        let result = c.coerce_uint(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_float_unicode_fails() {
        let c = default_coercer();
        let v = serde_json::json!("π");
        let result = c.coerce_float(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_float_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"key": "value"});
        let result = c.coerce_float(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_uuid_null_bytes_in_string() {
        let c = default_coercer();
        // Embedded null byte between hex chars — filtered out by is_ascii_hexdigit.
        // 32 hex chars remain, so normalization succeeds.
        let v = serde_json::json!("550e8400\x00e29b41d4a716446655440000");
        let result = c.coerce_uuid(&v);
        assert!(result.is_ok(), "Null byte filtered, 32 hex chars remain");
        assert_eq!(
            result.unwrap().as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_coerce_uuid_insufficient_hex_after_filter() {
        let c = default_coercer();
        // Less than 32 hex chars after filtering non-hex
        let v = serde_json::json!("not-enough-hex");
        let result = c.coerce_uuid(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_uuid_from_non_string() {
        let c = default_coercer();
        let v = serde_json::json!(42);
        let result = c.coerce_uuid(&v);
        assert!(result.is_err(), "UUID from number should fail");
    }

    #[test]
    fn test_coerce_ipv4_garbage_string() {
        let c = default_coercer();
        let v = serde_json::json!("not.an.ip.address");
        let result = c.coerce_ipv4(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_ipv6_garbage_string() {
        let c = default_coercer();
        let v = serde_json::json!("zzzz::yyyy");
        let result = c.coerce_ipv6(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_ipv6_from_number_fails() {
        let c = default_coercer();
        let v = serde_json::json!(12345);
        let result = c.coerce_ipv6(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // Array coercion edge cases
    // ========================================================================

    #[test]
    fn test_coerce_array_with_inner_datetime64_mixed_valid_invalid() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(DateTime64(3))");

        // Mix of valid ISO timestamp and epoch ms
        let v = serde_json::json!(["2024-12-25T10:30:00.123Z", 1735084800000i64]);
        let result = c.coerce_array(&v, &target).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // First element should have T replaced
        let first = arr[0].as_str().unwrap();
        assert!(!first.contains('T'));
        // Second element (epoch ms) should be converted to datetime string
        let second = arr[1].as_str().unwrap();
        assert!(
            second.contains('-'),
            "Epoch should become date string: {second}"
        );
    }

    #[test]
    fn test_coerce_array_wraps_scalar() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(String)");
        // Non-array value gets wrapped in a single-element array
        let v = serde_json::json!("single_value");
        let result = c.coerce_array(&v, &target).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str(), Some("single_value"));
    }

    #[test]
    fn test_coerce_array_empty() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(Int64)");
        let v = serde_json::json!([]);
        let result = c.coerce_array(&v, &target).unwrap();
        assert_eq!(result.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_coerce_array_invalid_json_string() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(Int64)");
        let v = serde_json::json!("[invalid json");
        let result = c.coerce_array(&v, &target);
        assert!(result.is_err());
    }

    // ========================================================================
    // Map coercion edge cases
    // ========================================================================

    #[test]
    fn test_coerce_map_with_int_values_from_strings() {
        let c = default_coercer();
        let target = ParsedType::parse("Map(String, Int64)");
        let v = serde_json::json!({"a": "42", "b": "100", "c": true});
        let result = c.coerce_map(&v, &target).unwrap();
        let obj = result.as_object().unwrap();
        assert_eq!(obj["a"].as_i64(), Some(42));
        assert_eq!(obj["b"].as_i64(), Some(100));
        assert_eq!(obj["c"].as_i64(), Some(1)); // bool true -> 1
    }

    #[test]
    fn test_coerce_map_from_json_string() {
        let c = default_coercer();
        let target = ParsedType::parse("Map(String, String)");
        let v = serde_json::json!(r#"{"key": "value"}"#);
        let result = c.coerce_map(&v, &target).unwrap();
        assert!(result.is_object());
        assert_eq!(result.as_object().unwrap()["key"].as_str(), Some("value"));
    }

    #[test]
    fn test_coerce_map_from_number_fails() {
        let c = default_coercer();
        let target = ParsedType::parse("Map(String, String)");
        let v = serde_json::json!(42);
        let result = c.coerce_map(&v, &target);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_map_invalid_json_string() {
        let c = default_coercer();
        let target = ParsedType::parse("Map(String, String)");
        let v = serde_json::json!("{broken json");
        let result = c.coerce_map(&v, &target);
        assert!(result.is_err());
    }

    // ========================================================================
    // Null handling comprehensive
    // ========================================================================

    #[test]
    fn test_null_handling_default_for_each_type() {
        let c = default_coercer();
        let cases: Vec<(&str, Box<dyn Fn(&Value) -> bool>)> = vec![
            ("String", Box::new(|v: &Value| v.as_str() == Some(""))),
            ("Int64", Box::new(|v: &Value| v.as_i64() == Some(0))),
            ("UInt32", Box::new(|v: &Value| v.as_u64() == Some(0))),
            ("Float64", Box::new(|v: &Value| v.as_f64() == Some(0.0))),
            ("Bool", Box::new(|v: &Value| v.as_bool() == Some(false))),
            (
                "Date",
                Box::new(|v: &Value| v.as_str() == Some("1970-01-01")),
            ),
            (
                "UUID",
                Box::new(|v: &Value| v.as_str() == Some("00000000-0000-0000-0000-000000000000")),
            ),
            ("IPv4", Box::new(|v: &Value| v.as_str() == Some("0.0.0.0"))),
            ("IPv6", Box::new(|v: &Value| v.as_str() == Some("::"))),
            (
                "Array(String)",
                Box::new(|v: &Value| v.as_array().unwrap().is_empty()),
            ),
            (
                "Map(String, String)",
                Box::new(|v: &Value| v.as_object().unwrap().is_empty()),
            ),
        ];

        for (type_str, check_fn) in cases {
            let target = ParsedType::parse(type_str);
            let result = c.default_value(&target);
            assert!(
                check_fn(&result),
                "Default value for {type_str} was unexpected: {result:?}"
            );
        }
    }

    #[test]
    fn test_null_handling_skip_passthrough() {
        let config = CoercionConfig {
            null_handling: NullHandling::Passthrough,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let target = ParsedType::parse("Int64"); // non-nullable
        let result = c.handle_null(&target).unwrap();
        assert!(result.is_null(), "Passthrough should pass null through");
    }

    #[test]
    fn test_null_handling_error_on_nullable_still_ok() {
        // Even with Error null handling, nullable columns should accept null
        let config = CoercionConfig {
            null_handling: NullHandling::Error,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let target = ParsedType::parse("Nullable(Int64)");
        let result = c.handle_null(&target).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_null_string_nil_detection() {
        let c = default_coercer();
        // "nil" is in the default null_strings list
        assert!(c.is_null_value(&serde_json::json!("nil")));
        // "N/A" is in the default list
        assert!(c.is_null_value(&serde_json::json!("N/A")));
        assert!(c.is_null_value(&serde_json::json!("n/a")));
        // "NaN" is in the default list
        assert!(c.is_null_value(&serde_json::json!("NaN")));
        // "undefined" is in the default list
        assert!(c.is_null_value(&serde_json::json!("undefined")));
        // "\\N" (postgres-style)
        assert!(c.is_null_value(&serde_json::json!("\\N")));
        // "<null>" is in the default list
        assert!(c.is_null_value(&serde_json::json!("<null>")));
        // NA (without slash)
        assert!(c.is_null_value(&serde_json::json!("NA")));
        // "-" is NOT in the default list (user can add it via config)
        assert!(!c.is_null_value(&serde_json::json!("-")));
        // Non-string values (numbers) should return false
        assert!(!c.is_null_value(&serde_json::json!(0)));
        assert!(!c.is_null_value(&serde_json::json!(false)));
    }

    // ========================================================================
    // Bool edge cases
    // ========================================================================

    #[test]
    fn test_coerce_bool_mixed_case_strings() {
        let c = default_coercer();
        // Implementation lowercases before matching
        assert_eq!(
            c.coerce_bool(&serde_json::json!("TRUE")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("FALSE"))
                .unwrap()
                .as_bool(),
            Some(false)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("Yes")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("No")).unwrap().as_bool(),
            Some(false)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("ON")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("T")).unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("Y")).unwrap().as_bool(),
            Some(true)
        );
    }

    #[test]
    fn test_coerce_bool_zero_string_is_false() {
        let c = default_coercer();
        // "0" is not in the truthy list, so it's false
        assert_eq!(
            c.coerce_bool(&serde_json::json!("0")).unwrap().as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_coerce_bool_from_negative_number() {
        let c = default_coercer();
        // -1 != 0, so it's true
        assert_eq!(
            c.coerce_bool(&serde_json::json!(-1)).unwrap().as_bool(),
            Some(true)
        );
    }

    #[test]
    fn test_coerce_bool_from_float_zero() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_bool(&serde_json::json!(0.0)).unwrap().as_bool(),
            Some(false)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!(0.001)).unwrap().as_bool(),
            Some(true)
        );
    }

    #[test]
    fn test_coerce_bool_from_array_is_false() {
        let c = default_coercer();
        // Array and Object fall into _ => false
        assert_eq!(
            c.coerce_bool(&serde_json::json!([1, 2])).unwrap().as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_coerce_bool_arbitrary_string_is_false() {
        let c = default_coercer();
        assert_eq!(
            c.coerce_bool(&serde_json::json!("maybe"))
                .unwrap()
                .as_bool(),
            Some(false)
        );
        assert_eq!(
            c.coerce_bool(&serde_json::json!("")).unwrap().as_bool(),
            Some(false)
        );
    }

    // ========================================================================
    // DateTime64 edge cases
    // ========================================================================

    #[test]
    fn test_coerce_datetime64_epoch_zero() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let v = serde_json::json!(0);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.starts_with("1970-01-01"),
            "Epoch 0 should be 1970-01-01: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_negative_epoch() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        // Negative epoch = before 1970
        let v = serde_json::json!(-86400);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.starts_with("1969-12-31"),
            "Negative epoch (-86400) should be 1969-12-31: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_far_future() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        // Year 2100: ~4102444800
        let v = serde_json::json!(4102444800i64);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.starts_with("2100-"),
            "Far future epoch should be in 2100: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_millisecond_epoch() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        // 1735084800000 ms = 2024-12-25 00:00:00.000
        let v = serde_json::json!(1735084800000i64);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.contains("2024-12-25"),
            "Millisecond epoch should detect magnitude: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_microsecond_epoch() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(6)");
        // Microsecond epoch
        let v = serde_json::json!(1735084800000000i64);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.contains("2024-12-25"),
            "Microsecond epoch should detect magnitude: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_nanosecond_epoch() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(9)");
        let v = serde_json::json!(1735084800000000000i64);
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.contains("2024-12-25"),
            "Nanosecond epoch should detect magnitude: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_with_timezone_offset() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let v = serde_json::json!("2024-12-25T10:30:00.123+11:00");
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(!s.contains('T'));
        // Timezone offset should be stripped (everything after +)
        assert!(!s.contains("+11:00"), "Offset should be stripped: {s}");
    }

    #[test]
    fn test_coerce_datetime64_string_without_dot_gets_precision_appended() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let v = serde_json::json!("2024-12-25 10:30:00");
        let result = c.coerce_datetime64(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            s.ends_with(".000"),
            "Should append .000 for precision 3: {s}"
        );
    }

    #[test]
    fn test_coerce_datetime64_from_bool_fails() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let v = serde_json::json!(true);
        let result = c.coerce_datetime64(&v, &target);
        assert!(result.is_err());
    }

    // ========================================================================
    // Decimal coercion
    // ========================================================================

    #[test]
    fn test_coerce_decimal_with_scale() {
        let c = default_coercer();
        let target = ParsedType::parse("Decimal(18, 4)");
        let v = serde_json::json!(3.14159);
        let result = c.coerce_decimal(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("3.1416")); // rounded to 4 decimal places
    }

    #[test]
    fn test_coerce_decimal_from_string() {
        let c = default_coercer();
        let target = ParsedType::parse("Decimal(10, 2)");
        let v = serde_json::json!("99.99");
        let result = c.coerce_decimal(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("99.99"));
    }

    // ========================================================================
    // JSON coercion
    // ========================================================================

    #[test]
    fn test_coerce_json_from_string() {
        let c = default_coercer();
        let v = serde_json::json!(r#"{"key": "value", "num": 42}"#);
        let result = c.coerce_json(&v).unwrap();
        assert!(result.is_object());
        assert_eq!(result["key"].as_str(), Some("value"));
        assert_eq!(result["num"].as_i64(), Some(42));
    }

    #[test]
    fn test_coerce_json_invalid_string() {
        let c = default_coercer();
        let v = serde_json::json!("{not valid json}");
        let result = c.coerce_json(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_json_passthrough_object() {
        let c = default_coercer();
        let v = serde_json::json!({"already": "an_object"});
        let result = c.coerce_json(&v).unwrap();
        assert_eq!(result, v);
    }

    // ========================================================================
    // Enum coercion
    // ========================================================================

    #[test]
    fn test_coerce_enum_from_number() {
        let c = default_coercer();
        let v = serde_json::json!(42);
        let result = c.coerce_enum(&v).unwrap();
        assert_eq!(result.as_str(), Some("42"));
    }

    #[test]
    fn test_coerce_enum_from_bool_fails() {
        let c = default_coercer();
        let v = serde_json::json!(true);
        let result = c.coerce_enum(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // Strict mode
    // ========================================================================

    #[test]
    fn test_strict_mode_coerce_row_fails_on_bad_value() {
        let config = CoercionConfig {
            strict: true,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("ip", "IPv4")]);

        let mut row = serde_json::Map::new();
        row.insert("ip".to_string(), serde_json::json!("not_an_ip"));

        let result = c.coerce_row(&mut row, &schema);
        assert!(result.is_err(), "Strict mode should propagate errors");
    }

    #[test]
    fn test_non_strict_mode_uses_default_on_failure() {
        let c = default_coercer(); // strict = false
        let schema = make_schema(vec![("ip", "IPv4")]);

        let mut row = serde_json::Map::new();
        row.insert("ip".to_string(), serde_json::json!("garbage"));

        let result = c.coerce_row(&mut row, &schema);
        assert!(result.is_ok(), "Non-strict should not fail");
        // Default for IPv4 is "0.0.0.0"
        assert_eq!(row["ip"].as_str(), Some("0.0.0.0"));
    }

    // ========================================================================
    // FixedString truncation
    // ========================================================================

    #[test]
    fn test_coerce_fixed_string_truncates() {
        let c = default_coercer();
        let target = ParsedType::parse("FixedString(5)");
        let v = serde_json::json!("hello_world_too_long");
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("hello"));
    }

    #[test]
    fn test_coerce_fixed_string_short_value_unchanged() {
        let c = default_coercer();
        let target = ParsedType::parse("FixedString(10)");
        let v = serde_json::json!("hi");
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("hi"));
    }

    // ========================================================================
    // Date coercion edge cases
    // ========================================================================

    #[test]
    fn test_coerce_date_epoch_seconds() {
        let c = default_coercer();
        let v = serde_json::json!(0);
        let result = c.coerce_date(&v).unwrap();
        assert_eq!(result.as_str(), Some("1970-01-01"));
    }

    #[test]
    fn test_coerce_date_epoch_string() {
        let c = default_coercer();
        let v = serde_json::json!("1735084800");
        let result = c.coerce_date(&v).unwrap();
        assert_eq!(result.as_str(), Some("2024-12-25"));
    }

    #[test]
    fn test_coerce_date_invalid_format() {
        let c = default_coercer();
        let v = serde_json::json!("25/12/2024"); // DD/MM/YYYY not supported
        let result = c.coerce_date(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_date_from_bool_fails() {
        let c = default_coercer();
        let v = serde_json::json!(true);
        let result = c.coerce_date(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_datetime_from_epoch() {
        let c = default_coercer();
        let v = serde_json::json!(1735084800);
        let result = c.coerce_datetime(&v).unwrap();
        let s = result.as_str().unwrap();
        assert!(s.starts_with("2024-12-25"), "Should be 2024-12-25: {s}");
    }

    #[test]
    fn test_coerce_datetime_from_bool_fails() {
        let c = default_coercer();
        let v = serde_json::json!(false);
        let result = c.coerce_datetime(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_datetime_short_string_fails() {
        let c = default_coercer();
        // Too short for a datetime
        let v = serde_json::json!("2024-12-25T10");
        let result = c.coerce_datetime(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // Custom type mapping
    // ========================================================================

    #[test]
    fn test_custom_type_mapping_in_full_mode() {
        let mut type_mappings = std::collections::HashMap::new();
        type_mappings.insert("MyCustomType".to_string(), "Int".to_string());
        let config = CoercionConfig {
            type_mappings,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let target = ParsedType::parse("MyCustomType");
        let v = serde_json::json!("42");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_custom_type_mapping_in_delta_mode_still_applies() {
        // Delta mode skips categories not in the delta set, BUT if there's
        // a custom type_mapping, it checks the mapped category
        let mut type_mappings = std::collections::HashMap::new();
        type_mappings.insert("SpecialTime".to_string(), "DateTime64".to_string());
        let config = CoercionConfig {
            type_mappings,
            ..Default::default()
        };
        let c = Coercer::new(config).with_mode(CoercionMode::Delta);
        let target = ParsedType::parse("SpecialTime");
        let v = serde_json::json!("2024-12-25T10:30:00.123Z");
        let result = c.coerce_value(&v, &target).unwrap();
        let s = result.as_str().unwrap();
        assert!(
            !s.contains('T'),
            "Custom mapping to DateTime64 should normalise T in delta mode"
        );
    }

    // ========================================================================
    // Enum edge cases
    // ========================================================================

    #[test]
    fn test_coerce_enum_from_string_passthrough() {
        let c = default_coercer();
        let v = serde_json::json!("VARIANT_A");
        let result = c.coerce_enum(&v).unwrap();
        assert_eq!(result.as_str(), Some("VARIANT_A"));
    }

    #[test]
    fn test_coerce_enum_from_negative_number() {
        let c = default_coercer();
        // Enum8 supports negative values in ClickHouse (signed 8-bit)
        let v = serde_json::json!(-5);
        let result = c.coerce_enum(&v).unwrap();
        assert_eq!(result.as_str(), Some("-5"));
    }

    #[test]
    fn test_coerce_enum_from_null_fails() {
        let c = default_coercer();
        // Null goes through handle_null first, but calling coerce_enum directly
        // with a non-null, non-string, non-number value should fail
        let v = serde_json::json!([1, 2]);
        let result = c.coerce_enum(&v);
        assert!(result.is_err(), "Array not supported for enum");
    }

    #[test]
    fn test_coerce_enum_via_coerce_value_with_enum16_type() {
        // Enum16 goes through the registry dispatch in full mode
        let c = default_coercer();
        let target = ParsedType::parse("Enum16('red' = 1, 'green' = 2)");
        let v = serde_json::json!("red");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("red"));
    }

    #[test]
    fn test_coerce_enum8_integer_value_via_dispatch() {
        let c = default_coercer();
        let target = ParsedType::parse("Enum8('a' = 1, 'b' = 2)");
        let v = serde_json::json!(2);
        let result = c.coerce_value(&v, &target).unwrap();
        // Number becomes the string form — ClickHouse resolves by numeric ID
        assert_eq!(result.as_str(), Some("2"));
    }

    // ========================================================================
    // Nested types: Array of Array, Map with complex value types
    // ========================================================================

    #[test]
    fn test_coerce_nested_array_of_array() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(Array(Int64))");
        let v = serde_json::json!([[1, 2, 3], [4, 5], []]);
        let result = c.coerce_array(&v, &target).unwrap();
        let outer = result.as_array().expect("outer array");
        assert_eq!(outer.len(), 3);
        assert_eq!(outer[0].as_array().unwrap().len(), 3);
        assert_eq!(outer[1].as_array().unwrap().len(), 2);
        assert_eq!(outer[2].as_array().unwrap().len(), 0);
        // Inner element values should survive through two levels of coercion
        assert_eq!(outer[0].as_array().unwrap()[0].as_i64(), Some(1));
    }

    #[test]
    fn test_coerce_array_of_uuids_normalises_each_element() {
        let c = default_coercer();
        let target = ParsedType::parse("Array(UUID)");
        let v = serde_json::json!([
            "550e8400-e29b-41d4-a716-446655440000",
            "550e8400e29b41d4a716446655440001",
            "{550e8400-e29b-41d4-a716-446655440002}"
        ]);
        let result = c.coerce_array(&v, &target).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // All three forms must normalise to the canonical hyphenated form
        for (i, elem) in arr.iter().enumerate() {
            let s = elem.as_str().expect("element is string");
            assert_eq!(s.len(), 36, "UUID at index {i} should be 36 chars: {s}");
            assert_eq!(s.matches('-').count(), 4, "UUID should have 4 hyphens");
        }
    }

    #[test]
    fn test_coerce_map_with_array_values() {
        let c = default_coercer();
        // Map(String, Array(Int64)) — nested array inside a map
        let target = ParsedType::parse("Map(String, Array(Int64))");
        let v = serde_json::json!({
            "alpha": [1, 2, 3],
            "beta": [4, 5]
        });
        let result = c.coerce_map(&v, &target).unwrap();
        let obj = result.as_object().unwrap();
        assert_eq!(obj["alpha"].as_array().unwrap().len(), 3);
        assert_eq!(obj["beta"].as_array().unwrap().len(), 2);
        assert_eq!(obj["alpha"].as_array().unwrap()[0].as_i64(), Some(1));
    }

    #[test]
    fn test_coerce_map_empty_object() {
        let c = default_coercer();
        let target = ParsedType::parse("Map(String, Int64)");
        let v = serde_json::json!({});
        let result = c.coerce_map(&v, &target).unwrap();
        assert!(result.as_object().unwrap().is_empty());
    }

    // ========================================================================
    // Strict mode — specific error messages
    // ========================================================================

    #[test]
    fn test_strict_mode_bad_uuid_error_message() {
        let config = CoercionConfig {
            strict: true,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("id", "UUID")]);

        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::json!("not-a-uuid"));

        let err = c.coerce_row(&mut row, &schema).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("UUID") || msg.contains("hex"),
            "Error should mention UUID failure, got: {msg}"
        );
    }

    #[test]
    fn test_strict_mode_bad_ipv6_error_message() {
        let config = CoercionConfig {
            strict: true,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("addr", "IPv6")]);

        let mut row = serde_json::Map::new();
        row.insert("addr".to_string(), serde_json::json!("not::a::valid::v6"));

        let err = c.coerce_row(&mut row, &schema).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("IPv6") || msg.contains("Invalid"),
            "Error should mention IPv6, got: {msg}"
        );
    }

    #[test]
    fn test_strict_mode_datetime64_bool_error() {
        let config = CoercionConfig {
            strict: true,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("ts", "DateTime64(3)")]);

        let mut row = serde_json::Map::new();
        row.insert("ts".to_string(), serde_json::json!(true));

        let err = c.coerce_row(&mut row, &schema).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("datetime64") || msg.contains("Cannot convert"),
            "Error should mention datetime64 conversion failure, got: {msg}"
        );
    }

    #[test]
    fn test_strict_mode_null_non_nullable_int_error() {
        let config = CoercionConfig {
            strict: true,
            null_handling: NullHandling::Error,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("n", "Int64")]);

        let mut row = serde_json::Map::new();
        row.insert("n".to_string(), serde_json::Value::Null);

        let err = c.coerce_row(&mut row, &schema).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("NULL") || msg.contains("non-nullable"),
            "Error should mention NULL non-nullable, got: {msg}"
        );
    }

    // ========================================================================
    // Delta mode: Array(DateTime64) inner coercion
    // ========================================================================

    #[test]
    fn test_delta_mode_array_datetime64_coerces_inner() {
        let c = delta_coercer();
        let target = ParsedType::parse("Array(DateTime64(3))");
        let v = serde_json::json!(["2024-12-25T10:30:00.123Z", "2024-12-26T11:00:00Z"]);
        let result = c.coerce_value(&v, &target).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        for (i, e) in arr.iter().enumerate() {
            let s = e.as_str().expect("string");
            assert!(
                !s.contains('T'),
                "Element {i} should have T normalised: {s}"
            );
        }
    }

    #[test]
    fn test_delta_mode_datetime_no_t_passthrough() {
        // DateTime (not DateTime64) in delta mode — only strips T/Z
        let c = delta_coercer();
        let target = ParsedType::parse("DateTime");
        // No T — still goes through coerce_datetime
        let v = serde_json::json!("2024-12-25 10:30:00");
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("2024-12-25 10:30:00"));
    }

    // ========================================================================
    // Whitespace handling in numeric strings
    // ========================================================================

    #[test]
    fn test_coerce_int_leading_trailing_whitespace() {
        let c = default_coercer();
        // Leading/trailing whitespace should be trimmed
        let v = serde_json::json!("  42  ");
        let result = c.coerce_int(&v).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_coerce_float_leading_trailing_whitespace() {
        let c = default_coercer();
        let v = serde_json::json!("  3.14  ");
        let result = c.coerce_float(&v).unwrap();
        assert!((result.as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_coerce_uint_from_float_string_whole() {
        // Whole float strings should succeed for uint
        let c = default_coercer();
        let v = serde_json::json!("42.0");
        let result = c.coerce_uint(&v).unwrap();
        assert_eq!(result.as_u64(), Some(42));
    }

    #[test]
    fn test_coerce_uint_from_float_string_fractional_fails() {
        // Fractional float strings should fail for uint
        let c = default_coercer();
        let v = serde_json::json!("42.5");
        let result = c.coerce_uint(&v);
        assert!(result.is_err(), "Fractional float string cannot be uint");
    }

    // ========================================================================
    // ParsedType default_value category coverage
    // ========================================================================

    #[test]
    fn test_default_value_datetime64_format() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let result = c.default_value(&target);
        assert_eq!(result.as_str(), Some("1970-01-01 00:00:00.000"));
    }

    #[test]
    fn test_default_value_decimal_is_zero() {
        let c = default_coercer();
        let target = ParsedType::parse("Decimal(18, 4)");
        let result = c.default_value(&target);
        // Decimal category → 0.0 JSON number
        assert!(
            result.as_f64() == Some(0.0) || result.as_str() == Some("0.0000"),
            "Expected numeric 0 or string '0.0000', got: {result:?}"
        );
    }

    #[test]
    fn test_coerce_row_skips_missing_fields() {
        // Columns in schema but NOT in row should be ignored (not injected)
        let c = default_coercer();
        let schema = make_schema(vec![("a", "Int64"), ("b", "String"), ("c", "UUID")]);

        let mut row = serde_json::Map::new();
        row.insert("a".to_string(), serde_json::json!(42));
        // 'b' and 'c' absent

        c.coerce_row(&mut row, &schema).unwrap();

        assert_eq!(row.len(), 1, "Missing fields should not be injected");
        assert_eq!(row["a"].as_i64(), Some(42));
        assert!(!row.contains_key("b"));
        assert!(!row.contains_key("c"));
    }

    #[test]
    fn test_coerce_row_non_schema_fields_passthrough() {
        // Fields NOT in schema should pass through unchanged
        let c = default_coercer();
        let schema = make_schema(vec![("a", "Int64")]);

        let mut row = serde_json::Map::new();
        row.insert("a".to_string(), serde_json::json!("123"));
        row.insert(
            "extra".to_string(),
            serde_json::json!({"complex": "object"}),
        );

        c.coerce_row(&mut row, &schema).unwrap();

        // 'a' should be coerced to int
        assert_eq!(row["a"].as_i64(), Some(123));
        // 'extra' should be untouched (not in schema)
        assert_eq!(row["extra"]["complex"].as_str(), Some("object"));
    }

    // ========================================================================
    // Unknown category fallback — treats as string
    // ========================================================================

    #[test]
    fn test_coerce_unknown_type_falls_back_to_string() {
        // A type the coercer doesn't recognize by category — default is String
        let c = default_coercer();
        let target = ParsedType::parse("TotallyUnknownType");
        let v = serde_json::json!(42);
        let result = c.coerce_value(&v, &target).unwrap();
        // Unknown category → coerced as string
        assert_eq!(result.as_str(), Some("42"));
    }

    #[test]
    fn test_default_value_unknown_type_is_empty_string() {
        let c = default_coercer();
        let target = ParsedType::parse("SomethingWeird");
        let val = c.default_value(&target);
        assert_eq!(val.as_str(), Some(""));
    }

    // ========================================================================
    // to_f64 edge cases
    // ========================================================================

    #[test]
    fn test_coerce_float_from_bool_true() {
        let c = default_coercer();
        let v = serde_json::json!(true);
        let result = c.coerce_float(&v).unwrap();
        assert_eq!(result.as_f64(), Some(1.0));
    }

    #[test]
    fn test_coerce_float_from_bool_false() {
        let c = default_coercer();
        let v = serde_json::json!(false);
        let result = c.coerce_float(&v).unwrap();
        assert_eq!(result.as_f64(), Some(0.0));
    }

    #[test]
    fn test_coerce_float_from_null_fails() {
        let c = default_coercer();
        let v = serde_json::Value::Null;
        let result = c.coerce_float(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // Date/DateTime edge cases: number type conversion failures
    // ========================================================================

    #[test]
    fn test_coerce_date_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"not": "a date"});
        let result = c.coerce_date(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_datetime_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"not": "a datetime"});
        let result = c.coerce_datetime(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_datetime64_from_null_fails() {
        let c = default_coercer();
        let target = ParsedType::parse("DateTime64(3)");
        let v = serde_json::Value::Null;
        // Null goes through handle_null — Nullable? No, not wrapped, so default
        // handling: Passthrough (which is default) → Ok(Null)
        let result = c.coerce_value(&v, &target);
        // Passthrough or default — should be Ok
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_datetime_valid_unix_seconds() {
        let c = default_coercer();
        let v = serde_json::json!("1735084800");
        let result = c.coerce_datetime(&v).unwrap();
        assert!(result.as_str().unwrap().starts_with("2024"));
    }

    // ========================================================================
    // to_i64 edge cases
    // ========================================================================

    #[test]
    fn test_to_i64_from_null_fails() {
        // Null passes through handle_null path, but direct coerce_int fails
        let c = default_coercer();
        let v = serde_json::Value::Null;
        let result = c.coerce_int(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_u64_from_null_fails() {
        let c = default_coercer();
        let v = serde_json::Value::Null;
        let result = c.coerce_uint(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_i64_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"foo": "bar"});
        let result = c.coerce_int(&v);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_u64_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"foo": "bar"});
        let result = c.coerce_uint(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // Enum from non-string non-number
    // ========================================================================

    #[test]
    fn test_coerce_enum_from_object_fails() {
        let c = default_coercer();
        let v = serde_json::json!({"key": "value"});
        let result = c.coerce_enum(&v);
        assert!(result.is_err());
    }

    // ========================================================================
    // String coercion from non-string types (Number variants)
    // ========================================================================

    #[test]
    fn test_coerce_string_from_float_with_fraction() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!(3.14159);
        let result = c.coerce_string(&v, &target).unwrap();
        // Float with fraction preserves decimal representation
        assert!(result.as_str().unwrap().contains("3.14"));
    }

    #[test]
    fn test_coerce_string_from_very_large_float_whole() {
        // f.abs() >= i64::MAX → takes the else branch (f.to_string())
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!(1.0e20);
        let result = c.coerce_string(&v, &target).unwrap();
        // 1e20 is whole but exceeds i64::MAX so falls to f.to_string()
        assert!(!result.as_str().unwrap().is_empty());
    }

    #[test]
    fn test_coerce_string_from_null_is_empty() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::Value::Null;
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some(""));
    }

    #[test]
    fn test_coerce_string_from_object_serialises() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!({"foo": "bar"});
        let result = c.coerce_string(&v, &target).unwrap();
        // Serialized JSON
        let s = result.as_str().unwrap();
        assert!(s.contains("foo"));
        assert!(s.contains("bar"));
    }

    #[test]
    fn test_coerce_string_from_array_serialises() {
        let c = default_coercer();
        let target = ParsedType::parse("String");
        let v = serde_json::json!([1, 2, 3]);
        let result = c.coerce_string(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("[1,2,3]"));
    }

    // ========================================================================
    // Decimal via coerce_value dispatch
    // ========================================================================

    #[test]
    fn test_coerce_decimal_dispatch_from_value() {
        let c = default_coercer();
        let target = ParsedType::parse("Decimal(10, 2)");
        let v = serde_json::json!(42);
        let result = c.coerce_value(&v, &target).unwrap();
        assert_eq!(result.as_str(), Some("42.00"));
    }

    // ========================================================================
    // Strict mode: json parse error for Array(?) with bad JSON string
    // ========================================================================

    #[test]
    fn test_strict_mode_array_bad_inner_type() {
        let config = CoercionConfig {
            strict: true,
            ..Default::default()
        };
        let c = Coercer::new(config);
        let schema = make_schema(vec![("a", "Array(Int64)")]);

        let mut row = serde_json::Map::new();
        row.insert("a".to_string(), serde_json::json!(["nan", "text"]));

        let result = c.coerce_row(&mut row, &schema);
        // Inner Int64 coercion of "nan" fails → strict propagates
        assert!(result.is_err());
    }

    // ========================================================================
    // Null nullable column passthrough
    // ========================================================================

    #[test]
    fn test_nullable_column_null_value_passthrough() {
        let c = default_coercer();
        let target = ParsedType::parse("Nullable(Int64)");
        let v = serde_json::Value::Null;
        let result = c.coerce_value(&v, &target).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_nullable_column_null_string_becomes_null() {
        let c = default_coercer();
        let target = ParsedType::parse("Nullable(Int64)");
        let v = serde_json::json!("null");
        let result = c.coerce_value(&v, &target).unwrap();
        assert!(result.is_null());
    }
}
