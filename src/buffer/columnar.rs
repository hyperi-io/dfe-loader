//! Columnar buffer for batch inserts
//!
//! Accumulates rows for a single table until flush threshold is reached.
//!
//! NOTE: Currently stores rows as serde_json::Map for MVP simplicity.
//! TODO: Migrate to klickhouse-native columnar structure for zero-copy insert.

use std::time::Instant;

use serde_json::{Map, Value};

use crate::Result;

/// Columnar buffer that accumulates rows for batch insert
pub struct ColumnarBuffer {
    /// Table name this buffer is for
    table: String,
    /// Accumulated rows as JSON objects
    rows: Vec<Map<String, Value>>,
    /// Estimated size in bytes
    bytes: usize,
    /// Time of first row insertion
    first_insert: Option<Instant>,
}

impl ColumnarBuffer {
    /// Create a new columnar buffer for a table
    pub fn new(table: String) -> Self {
        Self {
            table,
            rows: Vec::with_capacity(1000),
            bytes: 0,
            first_insert: None,
        }
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Add a row to the buffer
    pub fn push(&mut self, data: Map<String, Value>) -> Result<()> {
        // Estimate size (rough approximation)
        let size_estimate = estimate_json_size(&data);
        self.bytes += size_estimate;

        if self.first_insert.is_none() {
            self.first_insert = Some(Instant::now());
        }

        self.rows.push(data);
        Ok(())
    }

    /// Add a row from JSON bytes
    pub fn push_bytes(&mut self, json: &[u8]) -> Result<()> {
        let value: Value = sonic_rs::from_slice(json)
            .map_err(|e| crate::Error::Buffer(format!("JSON parse error: {}", e)))?;

        match value {
            Value::Object(map) => self.push(map),
            _ => Err(crate::Error::Buffer("Expected JSON object".into())),
        }
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

    /// Take all rows, clearing the buffer
    pub fn take(&mut self) -> Vec<Map<String, Value>> {
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
}

impl Default for ColumnarBuffer {
    fn default() -> Self {
        Self::new(String::new())
    }
}

/// Estimate JSON size in bytes (rough approximation)
fn estimate_json_size(map: &Map<String, Value>) -> usize {
    let mut size = 2; // {}
    for (key, value) in map {
        size += key.len() + 3; // "key":
        size += estimate_value_size(value);
        size += 1; // comma
    }
    size
}

fn estimate_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(n) => n.to_string().len(),
        Value::String(s) => s.len() + 2,
        Value::Array(arr) => {
            2 + arr.iter().map(estimate_value_size).sum::<usize>() + arr.len()
        }
        Value::Object(map) => estimate_json_size(map),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_columnar_buffer_basic() {
        let mut buffer = ColumnarBuffer::new("test_table".to_string());
        assert!(buffer.is_empty());
        assert_eq!(buffer.table(), "test_table");

        let row = json!({"key": "value"}).as_object().unwrap().clone();
        buffer.push(row).unwrap();

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());
        assert!(buffer.bytes() > 0);
    }

    #[test]
    fn test_columnar_buffer_push_bytes() {
        let mut buffer = ColumnarBuffer::new("test".to_string());
        buffer.push_bytes(br#"{"event": "login", "user_id": 123}"#).unwrap();

        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_columnar_buffer_take() {
        let mut buffer = ColumnarBuffer::new("test".to_string());
        buffer.push_bytes(br#"{"key": "value1"}"#).unwrap();
        buffer.push_bytes(br#"{"key": "value2"}"#).unwrap();

        assert_eq!(buffer.len(), 2);

        let rows = buffer.take();
        assert_eq!(rows.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.bytes(), 0);
    }

    #[test]
    fn test_columnar_buffer_should_flush() {
        let mut buffer = ColumnarBuffer::new("test".to_string());

        // Empty buffer should not flush
        assert!(!buffer.should_flush(10, 1000, 5));

        // Add rows
        for i in 0..5 {
            buffer.push_bytes(format!(r#"{{"id": {}}}"#, i).as_bytes()).unwrap();
        }

        // Should not flush yet (5 < 10)
        assert!(!buffer.should_flush(10, 1000, 5));

        // Add more rows to hit threshold
        for i in 5..10 {
            buffer.push_bytes(format!(r#"{{"id": {}}}"#, i).as_bytes()).unwrap();
        }

        // Should flush now (10 >= 10)
        assert!(buffer.should_flush(10, 1000, 5));
    }

    #[test]
    fn test_estimate_json_size() {
        let map = json!({"key": "value", "num": 123}).as_object().unwrap().clone();
        let size = estimate_json_size(&map);
        // Should be reasonable estimate
        assert!(size > 10 && size < 100);
    }
}
