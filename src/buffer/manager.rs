// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/buffer/manager.rs
// Purpose:   Per-table row buffer manager with flush thresholds
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-table row buffer manager for JSONEachRow inserts.
//!
//! Each destination table (db.table) has its own row buffer for schema uniformity.
//! Rows are accumulated as `Map<String, Value>` and flushed to ClickHouse via
//! JSONEachRow format when thresholds are reached.

use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use rustc_hash::FxHashMap;
use serde_json::{Map, Value};
use tracing::debug;

use crate::config::BufferConfig;

type BufferBuildResult = (Vec<Map<String, Value>>, Vec<KafkaOffset>, Vec<Arc<[u8]>>);

/// Kafka offset metadata for at-least-once delivery.
///
/// Uses `Arc<str>` for topic to avoid cloning the topic string for every message.
/// Most messages from the same Kafka partition share the same topic name.
#[derive(Debug, Clone)]
pub struct KafkaOffset {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
}

impl KafkaOffset {
    /// Create a new `KafkaOffset` with an owned topic string
    #[inline]
    pub fn new(topic: impl Into<Arc<str>>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
        }
    }

    /// Create a new `KafkaOffset` sharing an existing topic Arc
    #[inline]
    pub fn with_shared_topic(topic: Arc<str>, partition: i32, offset: i64) -> Self {
        Self {
            topic,
            partition,
            offset,
        }
    }
}

/// Data ready to be flushed to ClickHouse (JSONEachRow).
///
/// Uses `CompactString` for table names (stack-allocated for ≤24 bytes).
/// Typical "db.table" names fit in ~20 bytes, avoiding heap allocation.
///
/// `raw_payloads` is parallel to `rows`: each entry is the original Kafka message
/// bytes (always JSON after format normalisation). Spliced as `_json` at serialisation
/// time — zero-copy for the fast path (no `_json` key collision in source).
pub struct FlushBatch {
    /// Destination table name (db.table) - stack-allocated for short names
    pub table: CompactString,
    /// Promoted schema columns + common header fields (NOT the full payload)
    pub rows: Vec<Map<String, Value>>,
    /// Kafka offsets for acknowledgment after successful insert
    pub offsets: Vec<KafkaOffset>,
    /// Raw payload bytes (JSON) parallel to rows — spliced as `_json` at flush time
    pub raw_payloads: Vec<Arc<[u8]>>,
}

/// Per-table buffer tracking pending rows, raw payloads, and Kafka offsets
struct TableBuffer {
    /// Promoted schema columns + common header fields
    rows: Vec<Map<String, Value>>,
    /// Kafka offsets for messages in this buffer
    offsets: Vec<KafkaOffset>,
    /// Raw JSON bytes parallel to rows — zero-copy Arc for _json splice
    raw_payloads: Vec<Arc<[u8]>>,
    /// Created timestamp
    created_at: Instant,
    /// Target batch size
    batch_size: usize,
}

impl TableBuffer {
    fn new(batch_size: usize) -> Self {
        Self {
            rows: Vec::with_capacity(batch_size),
            offsets: Vec::new(),
            raw_payloads: Vec::with_capacity(batch_size),
            created_at: Instant::now(),
            batch_size,
        }
    }

    fn push(
        &mut self,
        data: Map<String, Value>,
        offset: Option<KafkaOffset>,
        raw: Option<Arc<[u8]>>,
    ) {
        self.rows.push(data);
        if let Some(off) = offset {
            self.offsets.push(off);
        }
        if let Some(r) = raw {
            self.raw_payloads.push(r);
        }
    }

    fn is_ready(&self, flush_rows: usize, flush_age_secs: u64) -> bool {
        self.rows.len() >= flush_rows || self.created_at.elapsed().as_secs() >= flush_age_secs
    }

    fn build(&mut self) -> Option<BufferBuildResult> {
        if self.rows.is_empty() {
            return None;
        }

        let rows = std::mem::take(&mut self.rows);
        let offsets = std::mem::take(&mut self.offsets);
        let raw_payloads = std::mem::take(&mut self.raw_payloads);
        self.rows = Vec::with_capacity(self.batch_size);
        self.raw_payloads = Vec::with_capacity(self.batch_size);
        self.created_at = Instant::now();
        Some((rows, offsets, raw_payloads))
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Buffer statistics
#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    pub pending_rows: usize,
    pub pending_bytes: usize,
    pub pending_chunks: usize,
    pub table_count: usize,
}

/// Per-table row buffer manager.
///
/// Each destination table (db.table) has its own row buffer.
/// Rows are accumulated as `Map<String, Value>` and flushed when
/// row count or age thresholds are reached.
pub struct BufferManager {
    /// Per-table buffers: key is "db.table"
    buffers: FxHashMap<String, TableBuffer>,
    /// Batch size per table buffer
    batch_size: usize,
    /// Flush trigger: row count
    flush_rows: usize,
    /// Flush trigger: age in seconds
    flush_age_secs: u64,
}

impl BufferManager {
    /// Create a new buffer manager with config
    pub fn new(config: &BufferConfig) -> Self {
        Self {
            buffers: FxHashMap::default(),
            batch_size: config.flush_rows.max(100),
            flush_rows: config.flush_rows,
            flush_age_secs: config.flush_age_secs,
        }
    }

    /// Update buffer thresholds from new config (hot-reload safe).
    ///
    /// Updates flush thresholds without clearing existing buffers.
    pub fn update_config(&mut self, config: &BufferConfig) {
        self.batch_size = config.flush_rows.max(100);
        self.flush_rows = config.flush_rows;
        self.flush_age_secs = config.flush_age_secs;
    }

    /// Push a promoted row to the appropriate table buffer.
    ///
    /// `data` contains only schema-promoted columns and common header fields.
    /// `raw` is the original JSON bytes (Arc shared from message receipt) —
    /// carried alongside the promoted row and spliced as `_json` at flush time.
    #[inline]
    pub fn push(
        &mut self,
        table: &str,
        data: Map<String, Value>,
        offset: Option<KafkaOffset>,
        raw: Option<Arc<[u8]>>,
    ) {
        // Fast path: table already exists (common case after first message)
        if let Some(buffer) = self.buffers.get_mut(table) {
            buffer.push(data, offset, raw);
            return;
        }

        // Slow path: new table — allocate key and create buffer
        let mut buffer = TableBuffer::new(self.batch_size);
        buffer.push(data, offset, raw);
        self.buffers.insert(table.to_string(), buffer);
    }

    /// Check if any buffer needs flushing
    pub fn should_flush(&self) -> bool {
        self.buffers
            .values()
            .any(|buf| buf.is_ready(self.flush_rows, self.flush_age_secs))
    }

    /// Get batches ready for flush.
    ///
    /// Single-pass over buffers — no count pre-pass, no `should_flush()` guard needed.
    /// Returns an empty Vec when nothing is ready (caller should check `!batches.is_empty()`).
    pub fn get_ready_for_flush(&mut self) -> Vec<FlushBatch> {
        let flush_rows = self.flush_rows;
        let flush_age_secs = self.flush_age_secs;
        let mut flush_batches = Vec::new();

        for (table, buffer) in self.buffers.iter_mut() {
            if buffer.is_ready(flush_rows, flush_age_secs)
                && let Some((rows, offsets, raw_payloads)) = buffer.build() {
                    debug!(table = %table, rows = rows.len(), "Flushing buffer");
                    flush_batches.push(FlushBatch {
                        table: CompactString::from(table.as_str()),
                        rows,
                        offsets,
                        raw_payloads,
                    });
                }
        }

        flush_batches
    }

    /// Flush all buffers (for shutdown)
    pub fn flush_all(&mut self) -> Vec<FlushBatch> {
        let mut flush_batches = Vec::with_capacity(self.buffers.len());

        for (table, buffer) in self.buffers.iter_mut() {
            if let Some((rows, offsets, raw_payloads)) = buffer.build() {
                flush_batches.push(FlushBatch {
                    table: CompactString::from(table.as_str()),
                    rows,
                    offsets,
                    raw_payloads,
                });
            }
        }

        flush_batches
    }

    /// Get buffer statistics
    pub fn stats(&self) -> BufferStats {
        let mut stats = BufferStats {
            table_count: self.buffers.len(),
            ..Default::default()
        };

        for buffer in self.buffers.values() {
            let rows = buffer.len();
            stats.pending_rows += rows;
            stats.pending_chunks += if buffer.is_empty() { 0 } else { 1 };
            stats.pending_bytes += rows * 200;
        }

        stats
    }

    /// Get total pending row count
    pub fn pending_rows(&self) -> usize {
        self.buffers.values().map(|b| b.len()).sum()
    }

    /// Get pending bytes (estimate)
    pub fn pending_bytes(&self) -> usize {
        // Rough estimate: 200 bytes per row average
        self.pending_rows() * 200
    }

    /// Clear all buffers
    pub fn clear(&mut self) {
        self.buffers.clear();
    }

    /// Get list of tables with pending data
    pub fn tables_with_pending(&self) -> Vec<&str> {
        self.buffers
            .iter()
            .filter(|(_, buf)| !buf.is_empty())
            .map(|(table, _)| table.as_str())
            .collect()
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self {
            buffers: FxHashMap::default(),
            batch_size: 1000,
            flush_rows: 20_000,
            flush_age_secs: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BufferConfig;
    use serde_json::json;
    use std::sync::Arc;

    fn test_config() -> BufferConfig {
        BufferConfig {
            flush_bytes: 1024 * 1024,
            flush_rows: 5,
            flush_age_secs: 10,
        }
    }

    #[test]
    fn test_buffer_manager_push() {
        let mut manager = BufferManager::new(&test_config());

        let data1 = json!({"id": 1, "name": "foo"}).as_object().unwrap().clone();
        let data2 = json!({"id": 2, "name": "bar"}).as_object().unwrap().clone();

        manager.push("db.table_a", data1, None, None);
        manager.push("db.table_b", data2, None, None);

        assert_eq!(manager.pending_rows(), 2);
        assert_eq!(manager.stats().table_count, 2);
    }

    #[test]
    fn test_buffer_manager_flush_threshold() {
        let mut manager = BufferManager::new(&test_config());

        for i in 0..6 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.events", data, None, None);
        }

        let batches = manager.get_ready_for_flush();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "db.events");
        assert_eq!(batches[0].rows.len(), 6);
    }

    #[test]
    fn test_buffer_manager_per_table_isolation() {
        let mut manager = BufferManager::new(&test_config());

        for i in 0..3 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None, None);
        }
        for i in 0..2 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_b", data, None, None);
        }

        assert_eq!(manager.pending_rows(), 5);

        for i in 3..6 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None, None);
        }

        let batches = manager.get_ready_for_flush();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].table, "db.table_a");
        assert_eq!(batches[0].rows.len(), 6);

        // table_b still has 2 pending
        assert_eq!(manager.pending_rows(), 2);
    }

    #[test]
    fn test_buffer_manager_flush_all() {
        let mut manager = BufferManager::new(&test_config());

        for i in 0..3 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_a", data, None, None);
        }
        for i in 0..2 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.table_b", data, None, None);
        }

        let batches = manager.flush_all();
        assert_eq!(batches.len(), 2);

        assert_eq!(manager.pending_rows(), 0);
    }

    #[test]
    fn test_kafka_offset_tracking() {
        let mut manager = BufferManager::new(&test_config());

        let topic: Arc<str> = Arc::from("test");
        let offset1 = KafkaOffset::with_shared_topic(topic.clone(), 0, 100);
        let offset2 = KafkaOffset::with_shared_topic(topic, 0, 101);

        let data1 = json!({"id": 1}).as_object().unwrap().clone();
        let data2 = json!({"id": 2}).as_object().unwrap().clone();

        manager.push("db.events", data1, Some(offset1), None);
        manager.push("db.events", data2, Some(offset2), None);

        for i in 3..7 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.events", data, None, None);
        }

        let batches = manager.get_ready_for_flush();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].offsets.len(), 2);
        assert_eq!(batches[0].offsets[0].offset, 100);
        assert_eq!(batches[0].offsets[1].offset, 101);
    }

    #[test]
    fn test_buffer_manager_update_config() {
        let config = test_config();
        let mut manager = BufferManager::new(&config);

        assert_eq!(manager.flush_rows, 5);

        let data = json!({"id": 1}).as_object().unwrap().clone();
        manager.push("db.events", data, None, None);
        assert_eq!(manager.pending_rows(), 1);

        let new_config = BufferConfig {
            flush_rows: 100,
            flush_bytes: 2 * 1024 * 1024,
            flush_age_secs: 30,
        };
        manager.update_config(&new_config);

        assert_eq!(manager.flush_rows, 100);
        assert_eq!(manager.flush_age_secs, 30);
        assert_eq!(manager.pending_rows(), 1);
    }
}
