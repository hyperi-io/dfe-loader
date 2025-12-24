//! Buffer manager for multiple tables
//!
//! Manages per-table buffers with configurable flush thresholds.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::buffer::ColumnarBuffer;
use crate::config::BufferConfig;
use crate::Result;

/// Data ready to be flushed to ClickHouse
pub struct FlushBatch {
    pub table: String,
    pub rows: Vec<Map<String, Value>>,
}

/// Manages buffers for multiple destination tables
pub struct BufferManager {
    buffers: HashMap<String, ColumnarBuffer>,
    flush_rows: usize,
    flush_bytes: usize,
    flush_age_secs: u64,
}

impl BufferManager {
    /// Create a new buffer manager with config
    pub fn new(config: &BufferConfig) -> Self {
        Self {
            buffers: HashMap::new(),
            flush_rows: config.flush_rows,
            flush_bytes: config.flush_bytes,
            flush_age_secs: config.flush_age_secs,
        }
    }

    /// Push a row to the appropriate table buffer
    pub fn push(&mut self, table: &str, data: Map<String, Value>) -> Result<()> {
        let buffer = self
            .buffers
            .entry(table.to_string())
            .or_insert_with(|| ColumnarBuffer::new(table.to_string()));

        buffer.push(data)
    }

    /// Push JSON bytes to the appropriate table buffer
    pub fn push_bytes(&mut self, table: &str, json: &[u8]) -> Result<()> {
        let buffer = self
            .buffers
            .entry(table.to_string())
            .or_insert_with(|| ColumnarBuffer::new(table.to_string()));

        buffer.push_bytes(json)
    }

    /// Get or create a buffer for a table
    pub fn get_or_create(&mut self, table: &str) -> &mut ColumnarBuffer {
        self.buffers
            .entry(table.to_string())
            .or_insert_with(|| ColumnarBuffer::new(table.to_string()))
    }

    /// Get buffers ready for flush (meeting any threshold)
    pub fn get_ready_for_flush(&mut self) -> Vec<FlushBatch> {
        let mut batches = Vec::new();

        for buffer in self.buffers.values_mut() {
            if buffer.should_flush(self.flush_rows, self.flush_bytes, self.flush_age_secs) {
                let rows = buffer.take();
                if !rows.is_empty() {
                    batches.push(FlushBatch {
                        table: buffer.table().to_string(),
                        rows,
                    });
                }
            }
        }

        batches
    }

    /// Flush all non-empty buffers (for shutdown)
    pub fn flush_all(&mut self) -> Vec<FlushBatch> {
        let mut batches = Vec::new();

        for buffer in self.buffers.values_mut() {
            if !buffer.is_empty() {
                let rows = buffer.take();
                batches.push(FlushBatch {
                    table: buffer.table().to_string(),
                    rows,
                });
            }
        }

        batches
    }

    /// Get total row count across all buffers
    pub fn total_rows(&self) -> usize {
        self.buffers.values().map(|b| b.len()).sum()
    }

    /// Get total byte estimate across all buffers
    pub fn total_bytes(&self) -> usize {
        self.buffers.values().map(|b| b.bytes()).sum()
    }

    /// Get number of active table buffers
    pub fn table_count(&self) -> usize {
        self.buffers.len()
    }

    /// Get buffer stats for a specific table
    pub fn table_stats(&self, table: &str) -> Option<(usize, usize)> {
        self.buffers.get(table).map(|b| (b.len(), b.bytes()))
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self {
            buffers: HashMap::new(),
            flush_rows: 10_000,
            flush_bytes: 1_048_576, // 1MB
            flush_age_secs: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config() -> BufferConfig {
        BufferConfig {
            flush_bytes: 1024,
            flush_rows: 5,
            flush_age_secs: 10,
        }
    }

    #[test]
    fn test_buffer_manager_push() {
        let mut manager = BufferManager::new(&test_config());

        let row = json!({"event": "login"}).as_object().unwrap().clone();
        manager.push("events", row).unwrap();

        assert_eq!(manager.total_rows(), 1);
        assert_eq!(manager.table_count(), 1);
    }

    #[test]
    fn test_buffer_manager_multiple_tables() {
        let mut manager = BufferManager::new(&test_config());

        manager.push_bytes("events_auth", br#"{"type": "login"}"#).unwrap();
        manager.push_bytes("events_api", br#"{"type": "request"}"#).unwrap();
        manager.push_bytes("events_auth", br#"{"type": "logout"}"#).unwrap();

        assert_eq!(manager.table_count(), 2);
        assert_eq!(manager.total_rows(), 3);

        let (auth_rows, _) = manager.table_stats("events_auth").unwrap();
        assert_eq!(auth_rows, 2);

        let (api_rows, _) = manager.table_stats("events_api").unwrap();
        assert_eq!(api_rows, 1);
    }

    #[test]
    fn test_buffer_manager_flush_threshold() {
        let mut manager = BufferManager::new(&test_config()); // flush at 5 rows

        // Add 4 rows - should not flush
        for i in 0..4 {
            manager.push_bytes("events", format!(r#"{{"id": {}}}"#, i).as_bytes()).unwrap();
        }

        let batches = manager.get_ready_for_flush();
        assert!(batches.is_empty());

        // Add 1 more row - should trigger flush
        manager.push_bytes("events", br#"{"id": 4}"#).unwrap();

        let batches = manager.get_ready_for_flush();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "events");
        assert_eq!(batches[0].rows.len(), 5);

        // Buffer should be empty now
        assert_eq!(manager.total_rows(), 0);
    }

    #[test]
    fn test_buffer_manager_flush_all() {
        let mut manager = BufferManager::new(&test_config());

        manager.push_bytes("table1", br#"{"a": 1}"#).unwrap();
        manager.push_bytes("table2", br#"{"b": 2}"#).unwrap();

        // Not at threshold yet
        assert!(manager.get_ready_for_flush().is_empty());

        // Flush all for shutdown
        let batches = manager.flush_all();
        assert_eq!(batches.len(), 2);
        assert_eq!(manager.total_rows(), 0);
    }
}
