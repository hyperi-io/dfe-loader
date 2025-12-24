//! Native columnar buffer using klickhouse RawRow
//!
//! Stores data directly in klickhouse-native format for zero-copy insert.
//! This avoids the JSON serialization/deserialization overhead of the legacy buffer.
//!
//! ## Design
//!
//! Instead of storing `serde_json::Map<String, Value>` rows that need to be
//! serialized to JSON strings for INSERT, we store `klickhouse::RawRow` directly.
//! This enables `insert_native_block()` which uses the native ClickHouse protocol.
//!
//! ## Performance Benefits
//!
//! 1. **Zero-copy insert**: Data goes directly to ClickHouse in native format
//! 2. **No JSON re-serialization**: Avoid `serde_json::to_string()` per row
//! 3. **Binary protocol**: More compact than text-based JSONEachRow
//! 4. **Type-safe columns**: ClickHouse types are preserved, not string-encoded
//!
//! ## Usage Flow
//!
//! ```text
//! Kafka message → JSON/MsgPack parse → RawRow conversion → NativeBuffer → insert_native_block
//! ```

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::time::Instant;

use chrono_tz::Tz::UTC;
use klickhouse::{Date, DateTime, DynDateTime64, Ipv4, Ipv6, RawRow, Value as KValue};
use serde_json::{Map, Value};

use crate::clickhouse::{ParsedType, TableSchema};
use crate::Result;

/// Native columnar buffer using klickhouse RawRow for zero-copy insert
pub struct NativeBuffer {
    /// Table name this buffer is for
    table: String,
    /// Accumulated rows in klickhouse native format
    rows: Vec<RawRow>,
    /// Estimated size in bytes
    bytes: usize,
    /// Time of first row insertion
    first_insert: Option<Instant>,
    /// Cached table schema for type conversion
    schema: Option<TableSchema>,
}

impl NativeBuffer {
    /// Create a new native buffer for a table
    pub fn new(table: String) -> Self {
        Self {
            table,
            rows: Vec::with_capacity(1000),
            bytes: 0,
            first_insert: None,
            schema: None,
        }
    }

    /// Create a new buffer with a pre-loaded schema
    pub fn with_schema(table: String, schema: TableSchema) -> Self {
        Self {
            table,
            rows: Vec::with_capacity(1000),
            bytes: 0,
            first_insert: None,
            schema: Some(schema),
        }
    }

    /// Set the table schema (for type-aware conversion)
    pub fn set_schema(&mut self, schema: TableSchema) {
        self.schema = Some(schema);
    }

    /// Get the table schema if set
    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Add a row from a JSON Map, converting to RawRow
    ///
    /// If schema is available, uses schema-aware type conversion.
    /// Otherwise, uses best-effort type inference from JSON values.
    pub fn push_json(&mut self, data: Map<String, Value>) -> Result<()> {
        let row = self.json_to_raw_row(data)?;
        let size = self.estimate_row_size(&row);

        if self.first_insert.is_none() {
            self.first_insert = Some(Instant::now());
        }

        self.bytes += size;
        self.rows.push(row);
        Ok(())
    }

    /// Add a row from JSON bytes
    pub fn push_bytes(&mut self, json: &[u8]) -> Result<()> {
        let value: Value = sonic_rs::from_slice(json)
            .map_err(|e| crate::Error::Buffer(format!("JSON parse error: {}", e)))?;

        match value {
            Value::Object(map) => self.push_json(map),
            _ => Err(crate::Error::Buffer("Expected JSON object".into())),
        }
    }

    /// Add a pre-built RawRow directly (for advanced use cases)
    pub fn push_raw(&mut self, row: RawRow) -> Result<()> {
        let size = self.estimate_row_size(&row);

        if self.first_insert.is_none() {
            self.first_insert = Some(Instant::now());
        }

        self.bytes += size;
        self.rows.push(row);
        Ok(())
    }

    /// Get row count
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get byte size estimate
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Get age since first insert (if any)
    pub fn age_secs(&self) -> u64 {
        self.first_insert
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }

    /// Take all rows for insertion, clearing the buffer
    ///
    /// Returns the rows in klickhouse-native format ready for `insert_native_block()`.
    pub fn take(&mut self) -> Vec<RawRow> {
        self.bytes = 0;
        self.first_insert = None;
        std::mem::take(&mut self.rows)
    }

    /// Clear the buffer without returning rows
    pub fn clear(&mut self) {
        self.rows.clear();
        self.bytes = 0;
        self.first_insert = None;
    }

    /// Check if buffer should flush based on thresholds
    pub fn should_flush(&self, max_rows: usize, max_bytes: usize, max_age_secs: u64) -> bool {
        if self.is_empty() {
            return false;
        }

        self.len() >= max_rows || self.bytes >= max_bytes || self.age_secs() >= max_age_secs
    }

    // ========================================================================
    // Conversion helpers
    // ========================================================================

    /// Convert a JSON Map to a klickhouse RawRow
    fn json_to_raw_row(&self, data: Map<String, Value>) -> Result<RawRow> {
        let mut row = RawRow::default();

        for (key, value) in data {
            // Get target type from schema if available
            let target_type = self.schema.as_ref().and_then(|s| s.column(&key));

            let kvalue = if let Some(col) = target_type {
                self.json_to_kvalue_typed(&value, &col.parsed_type)?
            } else {
                // No schema - infer from JSON value
                self.json_to_kvalue_inferred(&value)
            };

            row.set(&key, kvalue);
        }

        Ok(row)
    }

    /// Convert JSON value to klickhouse Value with schema type information
    fn json_to_kvalue_typed(&self, value: &Value, target: &ParsedType) -> Result<KValue> {
        // Handle null
        if value.is_null() {
            if target.nullable {
                return Ok(KValue::Null);
            } else {
                // Return type-appropriate default
                return Ok(self.default_kvalue(target));
            }
        }

        // Convert based on target type category
        match target.coercer_category() {
            "String" => self.to_kvalue_string(value),
            "Int" => self.to_kvalue_int(value),
            "UInt" => self.to_kvalue_uint(value),
            "Float" => self.to_kvalue_float(value),
            "Decimal" => self.to_kvalue_decimal(value, target),
            "Bool" => self.to_kvalue_bool(value),
            "Date" => self.to_kvalue_date(value),
            "DateTime" => self.to_kvalue_datetime(value),
            "DateTime64" => self.to_kvalue_datetime64(value, target),
            "UUID" => self.to_kvalue_uuid(value),
            "IPv4" => self.to_kvalue_ipv4(value),
            "IPv6" => self.to_kvalue_ipv6(value),
            "Array" => self.to_kvalue_array(value, target),
            "Map" => self.to_kvalue_map(value, target),
            _ => {
                // Default: treat as string
                self.to_kvalue_string(value)
            }
        }
    }

    /// Convert JSON value to klickhouse Value by inference (no schema)
    fn json_to_kvalue_inferred(&self, value: &Value) -> KValue {
        match value {
            Value::Null => KValue::Null,
            Value::Bool(b) => KValue::UInt8(if *b { 1 } else { 0 }),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    KValue::Int64(i)
                } else if let Some(u) = n.as_u64() {
                    KValue::UInt64(u)
                } else if let Some(f) = n.as_f64() {
                    KValue::Float64(f)
                } else {
                    KValue::String(n.to_string().into_bytes())
                }
            }
            Value::String(s) => KValue::String(s.clone().into_bytes()),
            Value::Array(arr) => {
                let items: Vec<KValue> =
                    arr.iter().map(|v| self.json_to_kvalue_inferred(v)).collect();
                KValue::Array(items)
            }
            Value::Object(obj) => {
                // Convert object to JSON string for compatibility
                let s = serde_json::to_string(obj).unwrap_or_default();
                KValue::String(s.into_bytes())
            }
        }
    }

    /// Get default klickhouse value for a type
    fn default_kvalue(&self, target: &ParsedType) -> KValue {
        match target.coercer_category() {
            "String" => KValue::String(Vec::new()),
            "Int" => KValue::Int64(0),
            "UInt" => KValue::UInt64(0),
            "Float" => KValue::Float64(0.0),
            "Decimal" => KValue::String(b"0".to_vec()),
            "Bool" => KValue::UInt8(0),
            "Date" => KValue::Date(Date(0)),
            "DateTime" => KValue::DateTime(DateTime(UTC, 0)),
            "DateTime64" => {
                let precision = target.scale.unwrap_or(6) as usize;
                KValue::DateTime64(DynDateTime64(UTC, 0, precision))
            }
            "UUID" => KValue::Uuid(uuid::Uuid::nil()),
            "IPv4" => KValue::Ipv4(Ipv4(Ipv4Addr::new(0, 0, 0, 0))),
            "IPv6" => KValue::Ipv6(Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0))),
            "Array" => KValue::Array(vec![]),
            "Map" => KValue::Map(vec![], vec![]),
            _ => KValue::String(Vec::new()),
        }
    }

    // ========================================================================
    // Type-specific conversions
    // ========================================================================

    fn to_kvalue_string(&self, value: &Value) -> Result<KValue> {
        let s = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Array(_) | Value::Object(_) => {
                serde_json::to_string(value).unwrap_or_default()
            }
            Value::Null => String::new(),
        };
        Ok(KValue::String(s.into_bytes()))
    }

    fn to_kvalue_int(&self, value: &Value) -> Result<KValue> {
        let n = match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i
                } else if let Some(f) = n.as_f64() {
                    f as i64
                } else {
                    0
                }
            }
            Value::String(s) => s.trim().parse().unwrap_or(0),
            Value::Bool(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        };
        Ok(KValue::Int64(n))
    }

    fn to_kvalue_uint(&self, value: &Value) -> Result<KValue> {
        let n = match value {
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    u
                } else if let Some(i) = n.as_i64() {
                    i.max(0) as u64
                } else if let Some(f) = n.as_f64() {
                    f.max(0.0) as u64
                } else {
                    0
                }
            }
            Value::String(s) => s.trim().parse().unwrap_or(0),
            Value::Bool(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        };
        Ok(KValue::UInt64(n))
    }

    fn to_kvalue_float(&self, value: &Value) -> Result<KValue> {
        let f = match value {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            Value::String(s) => s.trim().parse().unwrap_or(0.0),
            Value::Bool(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        };
        Ok(KValue::Float64(f))
    }

    fn to_kvalue_decimal(&self, value: &Value, target: &ParsedType) -> Result<KValue> {
        // Klickhouse uses string for decimal to preserve precision
        let scale = target.scale.unwrap_or(0) as usize;
        let f = match value {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            Value::String(s) => s.trim().parse().unwrap_or(0.0),
            _ => 0.0,
        };
        let s = format!("{:.prec$}", f, prec = scale);
        Ok(KValue::String(s.into_bytes()))
    }

    fn to_kvalue_bool(&self, value: &Value) -> Result<KValue> {
        let b = match value {
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
            Value::String(s) => {
                matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on")
            }
            _ => false,
        };
        Ok(KValue::UInt8(if b { 1 } else { 0 }))
    }

    fn to_kvalue_date(&self, value: &Value) -> Result<KValue> {
        // klickhouse Date(u16) is days since epoch (1970-01-01)
        match value {
            Value::String(s) => {
                // Try to parse YYYY-MM-DD
                if s.len() >= 10 {
                    if let Some(days) = self.parse_date_to_days(&s[..10]) {
                        return Ok(KValue::Date(Date(days)));
                    }
                }
                Ok(KValue::Date(Date(0)))
            }
            Value::Number(n) => {
                let ts = n.as_i64().unwrap_or(0);
                // Auto-detect unit and convert to days
                let days = self.epoch_to_days(ts);
                Ok(KValue::Date(Date(days.clamp(0, u16::MAX as i64) as u16)))
            }
            _ => Ok(KValue::Date(Date(0))),
        }
    }

    fn to_kvalue_datetime(&self, value: &Value) -> Result<KValue> {
        // klickhouse DateTime(Tz, u32) is seconds since epoch
        match value {
            Value::String(s) => {
                // Try to parse as epoch
                if let Ok(ts) = s.parse::<i64>() {
                    let secs = self.normalize_epoch(ts);
                    return Ok(KValue::DateTime(DateTime(
                        UTC,
                        secs.clamp(0, u32::MAX as i64) as u32,
                    )));
                }
                // Let ClickHouse handle string date parsing by storing as string
                // This will be coerced by ClickHouse on insert
                Ok(KValue::String(s.clone().into_bytes()))
            }
            Value::Number(n) => {
                let ts = n.as_i64().unwrap_or(0);
                let secs = self.normalize_epoch(ts);
                Ok(KValue::DateTime(DateTime(
                    UTC,
                    secs.clamp(0, u32::MAX as i64) as u32,
                )))
            }
            _ => Ok(KValue::DateTime(DateTime(UTC, 0))),
        }
    }

    fn to_kvalue_datetime64(&self, value: &Value, target: &ParsedType) -> Result<KValue> {
        // DynDateTime64(Tz, u64, precision)
        // precision: 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds
        let precision = target.scale.unwrap_or(6) as usize;

        match value {
            Value::String(s) => {
                if let Ok(ts) = s.parse::<i64>() {
                    let adjusted = self.adjust_to_precision(ts, precision);
                    return Ok(KValue::DateTime64(DynDateTime64(
                        UTC,
                        adjusted,
                        precision,
                    )));
                }
                // Pass string for ClickHouse to parse
                Ok(KValue::String(s.clone().into_bytes()))
            }
            Value::Number(n) => {
                let ts = n.as_i64().unwrap_or(0);
                let adjusted = self.adjust_to_precision(ts, precision);
                Ok(KValue::DateTime64(DynDateTime64(
                    UTC,
                    adjusted,
                    precision,
                )))
            }
            _ => Ok(KValue::DateTime64(DynDateTime64(UTC, 0, precision))),
        }
    }

    fn to_kvalue_uuid(&self, value: &Value) -> Result<KValue> {
        match value {
            Value::String(s) => {
                // Parse UUID from string (handles multiple formats)
                match uuid::Uuid::parse_str(s) {
                    Ok(u) => Ok(KValue::Uuid(u)),
                    Err(_) => Ok(KValue::Uuid(uuid::Uuid::nil())),
                }
            }
            _ => Ok(KValue::Uuid(uuid::Uuid::nil())),
        }
    }

    fn to_kvalue_ipv4(&self, value: &Value) -> Result<KValue> {
        match value {
            Value::String(s) => match Ipv4Addr::from_str(s) {
                Ok(ip) => Ok(KValue::Ipv4(Ipv4(ip))),
                Err(_) => Ok(KValue::Ipv4(Ipv4(Ipv4Addr::new(0, 0, 0, 0)))),
            },
            Value::Number(n) => {
                let num = n.as_u64().unwrap_or(0) as u32;
                Ok(KValue::Ipv4(Ipv4(Ipv4Addr::from(num))))
            }
            _ => Ok(KValue::Ipv4(Ipv4(Ipv4Addr::new(0, 0, 0, 0)))),
        }
    }

    fn to_kvalue_ipv6(&self, value: &Value) -> Result<KValue> {
        match value {
            Value::String(s) => match Ipv6Addr::from_str(s) {
                Ok(ip) => Ok(KValue::Ipv6(Ipv6(ip))),
                Err(_) => Ok(KValue::Ipv6(Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)))),
            },
            Value::Number(n) => {
                // High 64 bits = 0, low 64 bits = value
                let num = n.as_u64().unwrap_or(0) as u128;
                Ok(KValue::Ipv6(Ipv6(Ipv6Addr::from(num))))
            }
            _ => Ok(KValue::Ipv6(Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)))),
        }
    }

    fn to_kvalue_array(&self, value: &Value, target: &ParsedType) -> Result<KValue> {
        match value {
            Value::Array(arr) => {
                let elem_type = target.array_element.as_ref();
                let items: Result<Vec<KValue>> = arr
                    .iter()
                    .map(|v| {
                        if let Some(et) = elem_type {
                            self.json_to_kvalue_typed(v, et)
                        } else {
                            Ok(self.json_to_kvalue_inferred(v))
                        }
                    })
                    .collect();
                Ok(KValue::Array(items?))
            }
            Value::String(s) if s.starts_with('[') => {
                // Try parsing as JSON array
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
                    self.to_kvalue_array(&Value::Array(arr), target)
                } else {
                    Ok(KValue::Array(vec![]))
                }
            }
            _ => Ok(KValue::Array(vec![])),
        }
    }

    fn to_kvalue_map(&self, value: &Value, target: &ParsedType) -> Result<KValue> {
        // klickhouse Map(Vec<Value>, Vec<Value>) = (keys, values)
        match value {
            Value::Object(obj) => {
                let value_type = target.map_types.as_ref().map(|(_, v)| v.as_ref());
                let mut keys = Vec::with_capacity(obj.len());
                let mut values = Vec::with_capacity(obj.len());

                for (k, v) in obj {
                    keys.push(KValue::String(k.clone().into_bytes()));
                    let val = if let Some(vt) = value_type {
                        self.json_to_kvalue_typed(v, vt)?
                    } else {
                        self.json_to_kvalue_inferred(v)
                    };
                    values.push(val);
                }
                Ok(KValue::Map(keys, values))
            }
            _ => Ok(KValue::Map(vec![], vec![])),
        }
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Estimate row size in bytes
    fn estimate_row_size(&self, row: &RawRow) -> usize {
        // Rough estimate: 100 bytes per column average
        row.len() * 100
    }

    /// Parse YYYY-MM-DD to days since epoch
    fn parse_date_to_days(&self, s: &str) -> Option<u16> {
        // Simple parsing for YYYY-MM-DD
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        let year: i32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        let day: u32 = parts[2].parse().ok()?;

        // Use chrono for accurate calculation
        use chrono::NaiveDate;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let days = (date - epoch).num_days();

        if days >= 0 && days <= u16::MAX as i64 {
            Some(days as u16)
        } else {
            None
        }
    }

    /// Convert epoch timestamp to days since epoch
    fn epoch_to_days(&self, ts: i64) -> i64 {
        let secs = self.normalize_epoch(ts);
        secs / 86400
    }

    /// Normalize timestamp to seconds (detect ms/us/ns)
    fn normalize_epoch(&self, ts: i64) -> i64 {
        if ts > 1_000_000_000_000_000_000 {
            ts / 1_000_000_000 // nanoseconds
        } else if ts > 1_000_000_000_000_000 {
            ts / 1_000_000 // microseconds
        } else if ts > 1_000_000_000_000 {
            ts / 1_000 // milliseconds
        } else {
            ts // seconds
        }
    }

    /// Adjust timestamp to target precision
    /// precision: 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds
    fn adjust_to_precision(&self, ts: i64, precision: usize) -> u64 {
        // First detect the input unit
        let (value, input_precision) = if ts > 1_000_000_000_000_000_000 {
            (ts, 9) // nanoseconds
        } else if ts > 1_000_000_000_000_000 {
            (ts, 6) // microseconds
        } else if ts > 1_000_000_000_000 {
            (ts, 3) // milliseconds
        } else {
            (ts, 0) // seconds
        };

        // Convert to target precision
        let result = if input_precision == precision {
            value
        } else if input_precision > precision {
            // Scale down
            let divisor = 10i64.pow((input_precision - precision) as u32);
            value / divisor
        } else {
            // Scale up
            let multiplier = 10i64.pow((precision - input_precision) as u32);
            value * multiplier
        };

        result.max(0) as u64
    }
}

impl Default for NativeBuffer {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_native_buffer_basic() {
        let mut buffer = NativeBuffer::new("test_table".to_string());
        assert!(buffer.is_empty());
        assert_eq!(buffer.table(), "test_table");

        let row = json!({"key": "value"}).as_object().unwrap().clone();
        buffer.push_json(row).unwrap();

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());
        assert!(buffer.bytes() > 0);
    }

    #[test]
    fn test_native_buffer_push_bytes() {
        let mut buffer = NativeBuffer::new("test".to_string());
        buffer
            .push_bytes(br#"{"event": "login", "user_id": 123}"#)
            .unwrap();

        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_native_buffer_take() {
        let mut buffer = NativeBuffer::new("test".to_string());
        buffer.push_bytes(br#"{"key": "value1"}"#).unwrap();
        buffer.push_bytes(br#"{"key": "value2"}"#).unwrap();

        assert_eq!(buffer.len(), 2);

        let rows = buffer.take();
        assert_eq!(rows.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.bytes(), 0);
    }

    #[test]
    fn test_native_buffer_should_flush() {
        let mut buffer = NativeBuffer::new("test".to_string());

        // Empty buffer should not flush
        assert!(!buffer.should_flush(10, 10000, 5));

        // Add rows
        for i in 0..10 {
            buffer
                .push_bytes(format!(r#"{{"id": {}}}"#, i).as_bytes())
                .unwrap();
        }

        // Should flush now (10 >= 10)
        assert!(buffer.should_flush(10, 100000, 5));
    }

    #[test]
    fn test_json_to_kvalue_inferred() {
        let buffer = NativeBuffer::new("test".to_string());

        // Test various JSON types
        assert!(matches!(
            buffer.json_to_kvalue_inferred(&json!(null)),
            KValue::Null
        ));
        assert!(matches!(
            buffer.json_to_kvalue_inferred(&json!(true)),
            KValue::UInt8(1)
        ));
        assert!(matches!(
            buffer.json_to_kvalue_inferred(&json!(42)),
            KValue::Int64(42)
        ));
        assert!(matches!(
            buffer.json_to_kvalue_inferred(&json!(3.14)),
            KValue::Float64(_)
        ));

        // String check - need to compare bytes
        if let KValue::String(bytes) = buffer.json_to_kvalue_inferred(&json!("hello")) {
            assert_eq!(bytes, b"hello");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_epoch_normalization() {
        let buffer = NativeBuffer::new("test".to_string());

        // Seconds (2024-01-01 00:00:00 UTC)
        assert_eq!(buffer.normalize_epoch(1704067200), 1704067200);

        // Milliseconds
        assert_eq!(buffer.normalize_epoch(1704067200000), 1704067200);

        // Microseconds
        assert_eq!(buffer.normalize_epoch(1704067200000000), 1704067200);

        // Nanoseconds
        assert_eq!(buffer.normalize_epoch(1704067200000000000), 1704067200);
    }

    #[test]
    fn test_parse_date_to_days() {
        let buffer = NativeBuffer::new("test".to_string());

        // 1970-01-01 = day 0
        assert_eq!(buffer.parse_date_to_days("1970-01-01"), Some(0));

        // 1970-01-02 = day 1
        assert_eq!(buffer.parse_date_to_days("1970-01-02"), Some(1));

        // 2024-01-01
        // Days from 1970-01-01 to 2024-01-01: 19723 days
        assert_eq!(buffer.parse_date_to_days("2024-01-01"), Some(19723));
    }

    #[test]
    fn test_adjust_to_precision() {
        let buffer = NativeBuffer::new("test".to_string());

        // Seconds to milliseconds (precision 3)
        assert_eq!(buffer.adjust_to_precision(1704067200, 3), 1704067200000);

        // Seconds to microseconds (precision 6)
        assert_eq!(
            buffer.adjust_to_precision(1704067200, 6),
            1704067200000000
        );

        // Milliseconds to seconds (precision 0)
        assert_eq!(buffer.adjust_to_precision(1704067200000, 0), 1704067200);

        // Microseconds to milliseconds (precision 3)
        assert_eq!(
            buffer.adjust_to_precision(1704067200000000, 3),
            1704067200000
        );
    }

    #[test]
    fn test_ipv4_conversion() {
        let buffer = NativeBuffer::new("test".to_string());

        // String IP
        let result = buffer.to_kvalue_ipv4(&json!("192.168.1.1")).unwrap();
        if let KValue::Ipv4(Ipv4(addr)) = result {
            assert_eq!(addr, Ipv4Addr::new(192, 168, 1, 1));
        } else {
            panic!("Expected Ipv4");
        }

        // Numeric IP (network byte order)
        let result = buffer.to_kvalue_ipv4(&json!(3232235777u64)).unwrap(); // 192.168.1.1
        if let KValue::Ipv4(Ipv4(addr)) = result {
            assert_eq!(addr, Ipv4Addr::new(192, 168, 1, 1));
        } else {
            panic!("Expected Ipv4");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let buffer = NativeBuffer::new("test".to_string());

        // Standard format
        let result = buffer
            .to_kvalue_uuid(&json!("550e8400-e29b-41d4-a716-446655440000"))
            .unwrap();
        if let KValue::Uuid(u) = result {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }

        // Invalid UUID returns nil
        let result = buffer.to_kvalue_uuid(&json!("not-a-uuid")).unwrap();
        if let KValue::Uuid(u) = result {
            assert!(u.is_nil());
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_map_conversion() {
        use crate::clickhouse::ParsedType;

        let buffer = NativeBuffer::new("test".to_string());
        let target = ParsedType::parse("Map(String, Int64)");

        let result = buffer
            .to_kvalue_map(&json!({"a": 1, "b": 2}), &target)
            .unwrap();
        if let KValue::Map(keys, values) = result {
            assert_eq!(keys.len(), 2);
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Map");
        }
    }
}
