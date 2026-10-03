// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/buffer/manager.rs
// Purpose:   Per-table row buffer manager with flush thresholds
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-table row buffer manager for `JSONEachRow` inserts.
//!
//! Each destination table (db.table) has its own row buffer for schema uniformity.
//! Rows are accumulated as `Map<String, Value>` and flushed to `ClickHouse` via
//! `JSONEachRow` format when thresholds are reached.

use std::sync::Arc;
use std::time::{Duration, Instant};

use compact_str::CompactString;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{Map, Value};
use tracing::debug;

use crate::config::BufferConfig;

type BufferBuildResult = (
    Vec<Map<String, Value>>,
    Vec<KafkaOffset>,
    Vec<Arc<[u8]>>,
    Vec<u64>,
);

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

    /// Whether this offset was read from `topic`/`partition`.
    #[inline]
    pub fn is_from(&self, topic: &str, partition: i32) -> bool {
        self.partition == partition && &*self.topic == topic
    }
}

/// What purging one partition took out of the places that hold its rows.
#[derive(Debug, Default)]
pub struct Purged {
    /// Offsets of the records the rows came from: a record split into several
    /// rows counts once.
    offsets: FxHashSet<i64>,
    /// Payload bytes the memory guard tracked for the removed rows.
    pub bytes: u64,
}

impl Purged {
    /// Count one removed row of the record at `offset`, holding `bytes` of
    /// tracked payload.
    pub fn record(&mut self, offset: i64, bytes: u64) {
        self.offsets.insert(offset);
        self.bytes += bytes;
    }

    /// Records the removed rows came from.
    pub fn records(&self) -> u64 {
        self.offsets.len() as u64
    }
}

/// The per-row columns a buffer and a flush batch keep aligned by index.
struct RowColumns<'a> {
    rows: &'a mut Vec<Map<String, Value>>,
    offsets: &'a mut Vec<KafkaOffset>,
    raw: &'a mut Vec<Arc<[u8]>>,
    reserved: &'a mut Vec<u64>,
}

/// Remove every row read from `topic`/`partition`, keeping the columns
/// aligned by index and adding each row's reservation to `purged`.
fn purge_rows(columns: RowColumns<'_>, topic: &str, partition: i32, purged: &mut Purged) {
    let RowColumns {
        rows,
        offsets,
        raw,
        reserved,
    } = columns;
    // A row pushed without an offset names no partition, and leaves `offsets`
    // shorter than `rows`, so no index lines them up.
    let len = rows.len();
    if offsets.len() != len || raw.len() != len || reserved.len() != len {
        return;
    }
    if !offsets.iter().any(|off| off.is_from(topic, partition)) {
        return;
    }
    let mut kept = 0;
    for i in 0..len {
        if offsets[i].is_from(topic, partition) {
            purged.record(offsets[i].offset, reserved[i]);
            continue;
        }
        rows.swap(kept, i);
        offsets.swap(kept, i);
        raw.swap(kept, i);
        reserved.swap(kept, i);
        kept += 1;
    }
    rows.truncate(kept);
    offsets.truncate(kept);
    raw.truncate(kept);
    reserved.truncate(kept);
}

/// Data ready to be flushed to `ClickHouse` (`JSONEachRow`).
///
/// Uses `CompactString` for table names (stack-allocated for ≤24 bytes).
/// Typical "db.table" names fit in ~20 bytes, avoiding heap allocation.
///
/// `raw_payloads` is parallel to `rows`: each entry is the original Kafka message
/// bytes (always JSON after format normalisation). Spliced as `_json` at serialisation
/// time — zero-copy for the fast path (no `_json` key collision in source).
#[derive(Debug)]
pub struct FlushBatch {
    /// Destination table name (db.table) - stack-allocated for short names
    pub table: CompactString,
    /// Promoted schema columns + common header fields (NOT the full payload)
    pub rows: Vec<Map<String, Value>>,
    /// Kafka offsets for acknowledgment after successful insert
    pub offsets: Vec<KafkaOffset>,
    /// Raw payload bytes (JSON) parallel to rows — spliced as `_json` at flush time
    pub raw_payloads: Vec<Arc<[u8]>>,
    /// Bytes the memory guard holds for each row, parallel to rows, released
    /// once the row leaves the loader whatever its capture mode kept.
    pub reserved: Vec<u64>,
}

impl FlushBatch {
    /// Bytes the memory guard holds for the batch's rows.
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved.iter().sum()
    }

    /// Remove every row read from `topic`/`partition`, adding each to `purged`.
    pub fn purge_partition(&mut self, topic: &str, partition: i32, purged: &mut Purged) {
        purge_rows(
            RowColumns {
                rows: &mut self.rows,
                offsets: &mut self.offsets,
                raw: &mut self.raw_payloads,
                reserved: &mut self.reserved,
            },
            topic,
            partition,
            purged,
        );
    }
}

/// Per-table buffer tracking pending rows, raw payloads, and Kafka offsets
struct TableBuffer {
    /// Promoted schema columns + common header fields
    rows: Vec<Map<String, Value>>,
    /// Kafka offsets for messages in this buffer
    offsets: Vec<KafkaOffset>,
    /// Raw JSON bytes parallel to rows — zero-copy Arc for _json splice
    raw_payloads: Vec<Arc<[u8]>>,
    /// Memory-guard bytes held for each row, parallel to rows
    reserved: Vec<u64>,
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
            reserved: Vec::with_capacity(batch_size),
            created_at: Instant::now(),
            batch_size,
        }
    }

    fn push(
        &mut self,
        data: Map<String, Value>,
        offset: Option<KafkaOffset>,
        raw: Option<Arc<[u8]>>,
        reserved: u64,
    ) {
        self.rows.push(data);
        if let Some(off) = offset {
            self.offsets.push(off);
        }
        // Always push to keep raw_payloads parallel with rows.
        // Empty slice for transformer-path rows (they already have _json in the map).
        self.raw_payloads.push(raw.unwrap_or_default());
        self.reserved.push(reserved);
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
        let reserved = std::mem::take(&mut self.reserved);
        self.rows = Vec::with_capacity(self.batch_size);
        self.raw_payloads = Vec::with_capacity(self.batch_size);
        self.reserved = Vec::with_capacity(self.batch_size);
        self.created_at = Instant::now();
        Some((rows, offsets, raw_payloads, reserved))
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn purge_partition(&mut self, topic: &str, partition: i32, purged: &mut Purged) {
        purge_rows(
            RowColumns {
                rows: &mut self.rows,
                offsets: &mut self.offsets,
                raw: &mut self.raw_payloads,
                reserved: &mut self.reserved,
            },
            topic,
            partition,
            purged,
        );
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

    /// Push a promoted row the memory guard holds no bytes for.
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
        self.push_reserved(table, data, offset, raw, 0);
    }

    /// Push a promoted row the memory guard holds `reserved` bytes for, which
    /// the flush or purge that takes the row out releases.
    #[inline]
    pub fn push_reserved(
        &mut self,
        table: &str,
        data: Map<String, Value>,
        offset: Option<KafkaOffset>,
        raw: Option<Arc<[u8]>>,
        reserved: u64,
    ) {
        // Fast path: table already exists (common case after first message)
        if let Some(buffer) = self.buffers.get_mut(table) {
            buffer.push(data, offset, raw, reserved);
            return;
        }

        // Slow path: new table — allocate key and create buffer
        let mut buffer = TableBuffer::new(self.batch_size);
        buffer.push(data, offset, raw, reserved);
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

        for (table, buffer) in &mut self.buffers {
            if buffer.is_ready(flush_rows, flush_age_secs) {
                // Determine trigger reason before consuming buffer
                let trigger = if buffer.len() >= flush_rows {
                    "records"
                } else {
                    "age"
                };
                if let Some((rows, offsets, raw_payloads, reserved)) = buffer.build() {
                    let row_count = rows.len();
                    let byte_estimate = row_count * 200;
                    debug!(
                        table = %table,
                        rows = row_count,
                        bytes = byte_estimate,
                        trigger = trigger,
                        "Buffer flush triggered"
                    );
                    flush_batches.push(FlushBatch {
                        table: CompactString::from(table.as_str()),
                        rows,
                        offsets,
                        raw_payloads,
                        reserved,
                    });
                }
            }
        }

        flush_batches
    }

    /// Flush all buffers (for shutdown)
    pub fn flush_all(&mut self) -> Vec<FlushBatch> {
        self.take_matching(|_| true)
    }

    /// Take every buffer not flushed for at least `age`, whatever its size.
    ///
    /// Rows whose sender is waiting on them go at this shorter age rather than
    /// `flush_age_secs`, so the sender is not held for a batch to fill.
    pub fn take_older_than(&mut self, age: Duration) -> Vec<FlushBatch> {
        self.take_matching(|buffer| buffer.created_at.elapsed() >= age)
    }

    /// Take every non-empty buffer `take` selects.
    fn take_matching(&mut self, take: impl Fn(&TableBuffer) -> bool) -> Vec<FlushBatch> {
        let mut flush_batches = Vec::new();
        for (table, buffer) in &mut self.buffers {
            if !take(buffer) {
                continue;
            }
            if let Some((rows, offsets, raw_payloads, reserved)) = buffer.build() {
                flush_batches.push(FlushBatch {
                    table: CompactString::from(table.as_str()),
                    rows,
                    offsets,
                    raw_payloads,
                    reserved,
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
            stats.pending_chunks += usize::from(!buffer.is_empty());
            stats.pending_bytes += rows * 200;
        }

        stats
    }

    /// Per-table buffer stats for metrics emission.
    ///
    /// Returns `(table_name, rows, estimated_bytes)` for each active buffer.
    pub fn per_table_stats(&self) -> Vec<(&str, usize, usize)> {
        self.buffers
            .iter()
            .map(|(table, buf)| {
                let rows = buf.len();
                (table.as_str(), rows, rows * 200)
            })
            .collect()
    }

    /// Lowest still-buffered offset on each `(topic, partition)`.
    ///
    /// A Kafka commit is a per-partition watermark while a buffer is per table,
    /// so one partition's offsets sit across buffers that age independently and
    /// a watermark committed past a row still only in memory loses it to a
    /// crash.
    ///
    /// Scans the residual buffers once per flush cycle rather than tracking a
    /// minimum on `push`, keeping the per-message path free of the bookkeeping.
    pub fn lowest_pending_offsets(&self) -> Vec<KafkaOffset> {
        let mut lowest: FxHashMap<(&str, i32), &KafkaOffset> = FxHashMap::default();
        for buffer in self.buffers.values() {
            for off in &buffer.offsets {
                lowest
                    .entry((&*off.topic, off.partition))
                    .and_modify(|held| {
                        if off.offset < held.offset {
                            *held = off;
                        }
                    })
                    .or_insert(off);
            }
        }
        lowest.into_values().cloned().collect()
    }

    /// Remove every buffered row read from `topic`/`partition`, from whichever
    /// table buffers it, adding each to `purged`.
    pub fn purge_partition(&mut self, topic: &str, partition: i32, purged: &mut Purged) {
        for buffer in self.buffers.values_mut() {
            buffer.purge_partition(topic, partition, purged);
        }
    }

    /// Get total pending row count
    pub fn pending_rows(&self) -> usize {
        self.buffers.values().map(TableBuffer::len).sum()
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
    fn an_early_take_leaves_buffers_younger_than_its_age() {
        let mut manager = BufferManager::new(&test_config());
        let row = || json!({"id": 1}).as_object().unwrap().clone();
        manager.push("db.old", row(), None, None);
        std::thread::sleep(Duration::from_millis(60));
        manager.push("db.young", row(), None, None);

        let batches = manager.take_older_than(Duration::from_millis(50));
        let tables: Vec<&str> = batches.iter().map(|b| b.table.as_str()).collect();
        assert_eq!(
            tables,
            ["db.old"],
            "one row, far under flush_rows, still goes"
        );
        assert!(
            manager.get_ready_for_flush().is_empty(),
            "neither buffer is due by the configured thresholds"
        );
        assert_eq!(manager.pending_rows(), 1, "the young buffer keeps its row");
        assert_eq!(manager.take_older_than(Duration::ZERO).len(), 1);
        assert!(
            manager.take_older_than(Duration::ZERO).is_empty(),
            "an empty buffer yields no batch"
        );
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

    #[test]
    fn test_raw_payloads_parallel_with_rows() {
        // raw_payloads must always be the same length as rows — even when some
        // rows come via the transformer path (no raw payload). Empty Arc<[u8]>
        // entries mark transformer-path rows.
        let mut manager = BufferManager::new(&test_config());

        // Extractor-path row: has raw payload
        let extractor_row = json!({"severity": "high"}).as_object().unwrap().clone();
        let raw: Arc<[u8]> = Arc::from(br#"{"severity":"high","extra":"data"}"#.as_slice());
        manager.push("db.events", extractor_row, None, Some(raw));

        // Transformer-path row: no raw payload
        let transformer_row = json!({"severity": "low", "_json": "{}"})
            .as_object()
            .unwrap()
            .clone();
        manager.push("db.events", transformer_row, None, None);

        // Another extractor-path row
        let extractor_row2 = json!({"severity": "medium"}).as_object().unwrap().clone();
        let raw2: Arc<[u8]> = Arc::from(br#"{"severity":"medium"}"#.as_slice());
        manager.push("db.events", extractor_row2, None, Some(raw2));

        // Push enough to trigger flush
        for i in 0..4 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            manager.push("db.events", data, None, None);
        }

        let batches = manager.get_ready_for_flush();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(
            batch.rows.len(),
            batch.raw_payloads.len(),
            "raw_payloads must be parallel with rows"
        );

        // Extractor rows have non-empty raw payloads
        assert!(
            !batch.raw_payloads[0].is_empty(),
            "extractor row 0 should have raw payload"
        );
        // Transformer row has empty raw payload
        assert!(
            batch.raw_payloads[1].is_empty(),
            "transformer row 1 should have empty raw payload"
        );
        // Second extractor row
        assert!(
            !batch.raw_payloads[2].is_empty(),
            "extractor row 2 should have raw payload"
        );
        // Remaining transformer rows
        for i in 3..batch.raw_payloads.len() {
            assert!(
                batch.raw_payloads[i].is_empty(),
                "transformer row {i} should have empty raw payload"
            );
        }
    }

    // ========================================================================
    // BufferManager accessors / edge cases
    // ========================================================================

    #[test]
    fn test_buffer_manager_default_instance() {
        let m = BufferManager::default();
        assert_eq!(m.pending_rows(), 0);
        assert!(!m.should_flush());
    }

    #[test]
    fn test_buffer_manager_empty_stats() {
        let m = BufferManager::new(&test_config());
        let s = m.stats();
        assert_eq!(s.pending_rows, 0);
        assert_eq!(s.pending_bytes, 0);
        assert_eq!(s.pending_chunks, 0);
        assert_eq!(s.table_count, 0);
    }

    #[test]
    fn test_buffer_manager_stats_with_data() {
        let mut m = BufferManager::new(&test_config());
        for i in 0..3 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            m.push("db.a", data, None, None);
        }
        for i in 0..2 {
            let data = json!({"id": i}).as_object().unwrap().clone();
            m.push("db.b", data, None, None);
        }

        let s = m.stats();
        assert_eq!(s.table_count, 2);
        assert_eq!(s.pending_rows, 5);
        assert_eq!(s.pending_chunks, 2, "Two non-empty buffers");
        assert!(s.pending_bytes > 0);
    }

    #[test]
    fn test_buffer_manager_clear_removes_all_buffers() {
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );
        assert_eq!(m.pending_rows(), 1);
        m.clear();
        assert_eq!(m.pending_rows(), 0);
        assert_eq!(m.stats().table_count, 0);
    }

    #[test]
    fn test_buffer_manager_per_table_stats() {
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );
        m.push(
            "db.b",
            json!({"id": 2}).as_object().unwrap().clone(),
            None,
            None,
        );
        m.push(
            "db.b",
            json!({"id": 3}).as_object().unwrap().clone(),
            None,
            None,
        );

        let stats: std::collections::HashMap<&str, (usize, usize)> = m
            .per_table_stats()
            .into_iter()
            .map(|(t, r, b)| (t, (r, b)))
            .collect();
        assert_eq!(stats["db.a"], (1, 200));
        assert_eq!(stats["db.b"], (2, 400));
    }

    #[test]
    fn test_buffer_manager_tables_with_pending() {
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );
        m.push(
            "db.b",
            json!({"id": 2}).as_object().unwrap().clone(),
            None,
            None,
        );
        let tables: std::collections::HashSet<&str> = m.tables_with_pending().into_iter().collect();
        assert!(tables.contains("db.a"));
        assert!(tables.contains("db.b"));
    }

    #[test]
    fn test_buffer_manager_should_flush_flag() {
        let mut m = BufferManager::new(&test_config());
        // Empty → false
        assert!(!m.should_flush());
        // One row, threshold 5 → false
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );
        assert!(!m.should_flush());
        // Push enough to exceed threshold
        for i in 0..10 {
            m.push(
                "db.a",
                json!({"id": i}).as_object().unwrap().clone(),
                None,
                None,
            );
        }
        assert!(m.should_flush());
    }

    #[test]
    fn test_buffer_manager_get_ready_for_flush_empty() {
        let mut m = BufferManager::new(&test_config());
        let batches = m.get_ready_for_flush();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_buffer_manager_flush_all_empty() {
        let mut m = BufferManager::new(&test_config());
        let batches = m.flush_all();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_buffer_manager_batch_size_min_100() {
        // BufferManager::new clamps batch_size to at least 100
        let tiny_config = BufferConfig {
            flush_bytes: 1024,
            flush_rows: 5, // < 100
            flush_age_secs: 10,
        };
        let m = BufferManager::new(&tiny_config);
        assert_eq!(m.batch_size, 100, "batch_size should be clamped to 100");
    }

    #[test]
    fn test_buffer_manager_pending_bytes_estimate() {
        let mut m = BufferManager::new(&test_config());
        // 5 rows × 200 bytes each = 1000
        for i in 0..5 {
            m.push(
                "db.a",
                json!({"id": i}).as_object().unwrap().clone(),
                None,
                None,
            );
        }
        assert_eq!(m.pending_bytes(), 1000);
    }

    #[test]
    fn test_buffer_manager_flush_leaves_buffer_empty_but_table_key() {
        // After flush, the buffer is emptied but the key still exists.
        // tables_with_pending() should filter out empty buffers.
        let mut m = BufferManager::new(&test_config());
        for i in 0..6 {
            m.push(
                "db.a",
                json!({"id": i}).as_object().unwrap().clone(),
                None,
                None,
            );
        }
        let _ = m.get_ready_for_flush();
        // Buffer exists but empty
        let pending_tables = m.tables_with_pending();
        assert_eq!(pending_tables, [] as [&str; 0]);
    }

    // ========================================================================
    // lowest_pending_offsets — the floor a Kafka commit must not pass
    // ========================================================================

    #[test]
    fn lowest_pending_offsets_is_empty_with_nothing_buffered() {
        let m = BufferManager::new(&test_config());
        assert!(m.lowest_pending_offsets().is_empty());
    }

    #[test]
    fn lowest_pending_offsets_reports_the_minimum_per_partition() {
        let mut m = BufferManager::new(&test_config());
        let topic: Arc<str> = Arc::from("t");
        for off in [40i64, 12, 77] {
            m.push(
                "db.a",
                json!({"id": off}).as_object().unwrap().clone(),
                Some(KafkaOffset::with_shared_topic(topic.clone(), 3, off)),
                None,
            );
        }

        let floor = m.lowest_pending_offsets();
        assert_eq!(floor.len(), 1);
        assert_eq!(floor[0].partition, 3);
        assert_eq!(floor[0].offset, 12);
    }

    #[test]
    fn lowest_pending_offsets_spans_tables_that_buffer_independently() {
        // Two tables fed by the same partition: the floor is the lower of the
        // two, whichever buffer happens to hold it.
        let mut m = BufferManager::new(&test_config());
        let topic: Arc<str> = Arc::from("t");
        m.push(
            "db.foo",
            json!({"id": 1}).as_object().unwrap().clone(),
            Some(KafkaOffset::with_shared_topic(topic.clone(), 0, 102)),
            None,
        );
        m.push(
            "db.bar",
            json!({"id": 2}).as_object().unwrap().clone(),
            Some(KafkaOffset::with_shared_topic(topic.clone(), 0, 101)),
            None,
        );
        m.push(
            "db.bar",
            json!({"id": 3}).as_object().unwrap().clone(),
            Some(KafkaOffset::with_shared_topic(topic, 1, 7)),
            None,
        );

        let mut floor: Vec<(i32, i64)> = m
            .lowest_pending_offsets()
            .into_iter()
            .map(|o| (o.partition, o.offset))
            .collect();
        floor.sort_unstable();
        assert_eq!(floor, vec![(0, 101), (1, 7)]);
    }

    #[test]
    fn lowest_pending_offsets_keeps_topics_apart() {
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            Some(KafkaOffset::new("alpha", 0, 500)),
            None,
        );
        m.push(
            "db.a",
            json!({"id": 2}).as_object().unwrap().clone(),
            Some(KafkaOffset::new("beta", 0, 9)),
            None,
        );

        let mut floor: Vec<(String, i64)> = m
            .lowest_pending_offsets()
            .into_iter()
            .map(|o| (o.topic.to_string(), o.offset))
            .collect();
        floor.sort_unstable();
        assert_eq!(
            floor,
            vec![("alpha".to_string(), 500), ("beta".to_string(), 9)]
        );
    }

    #[test]
    fn lowest_pending_offsets_ignores_what_a_flush_already_took() {
        // The caller reads the floor after the take, so a flushed buffer must
        // no longer hold the watermark down.
        let mut m = BufferManager::new(&test_config());
        let topic: Arc<str> = Arc::from("t");
        for off in 0..6i64 {
            m.push(
                "db.ready",
                json!({"id": off}).as_object().unwrap().clone(),
                Some(KafkaOffset::with_shared_topic(topic.clone(), 0, off)),
                None,
            );
        }
        m.push(
            "db.waiting",
            json!({"id": 99}).as_object().unwrap().clone(),
            Some(KafkaOffset::with_shared_topic(topic, 0, 99)),
            None,
        );

        let batches = m.get_ready_for_flush();
        assert_eq!(batches.len(), 1, "only db.ready crossed the row threshold");

        let floor = m.lowest_pending_offsets();
        assert_eq!(floor.len(), 1);
        assert_eq!(
            floor[0].offset, 99,
            "the taken batch's offsets are no longer buffered"
        );
    }

    #[test]
    fn lowest_pending_offsets_ignores_rows_pushed_without_an_offset() {
        // `push` accepts a row with no offset, and such a row names nothing a
        // watermark could be held below.
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );
        assert!(m.lowest_pending_offsets().is_empty());
    }

    // ========================================================================
    // purge_partition -- a revoked partition's rows leave every buffer
    // ========================================================================

    /// Push one row of `payload` read from `topic`/`partition` at `offset`, reserving its length.
    fn push_read(m: &mut BufferManager, table: &str, at: (&str, i32, i64), payload: &[u8]) {
        let (topic, partition, offset) = at;
        m.push_reserved(
            table,
            json!({"offset": offset}).as_object().unwrap().clone(),
            Some(KafkaOffset::new(topic, partition, offset)),
            Some(Arc::from(payload)),
            payload.len() as u64,
        );
    }

    #[test]
    fn purge_partition_takes_the_partitions_rows_from_every_table() {
        let mut m = BufferManager::new(&test_config());
        push_read(&mut m, "db.a", ("t", 0, 10), b"a10");
        push_read(&mut m, "db.a", ("t", 1, 20), b"a20");
        push_read(&mut m, "db.b", ("t", 0, 11), b"b11xx");
        push_read(&mut m, "db.b", ("u", 0, 30), b"b30");

        let mut purged = Purged::default();
        m.purge_partition("t", 0, &mut purged);

        assert_eq!(purged.records(), 2);
        assert_eq!(purged.bytes, 3 + 5, "the reserved bytes of both rows");
        assert_eq!(m.pending_rows(), 2);
        let mut kept: Vec<(String, i32, i64)> = m
            .flush_all()
            .into_iter()
            .flat_map(|batch| batch.offsets)
            .map(|o| (o.topic.to_string(), o.partition, o.offset))
            .collect();
        kept.sort_unstable();
        assert_eq!(
            kept,
            vec![("t".to_string(), 1, 20), ("u".to_string(), 0, 30)],
            "another partition of the topic, and the same partition of another topic, stay"
        );
    }

    #[test]
    fn purge_partition_keeps_rows_offsets_and_payloads_in_line() {
        let mut m = BufferManager::new(&test_config());
        for (partition, offset) in [(0, 1), (1, 2), (0, 3), (1, 4), (1, 5), (0, 6)] {
            push_read(&mut m, "db.a", ("t", partition, offset), &[offset as u8]);
        }

        let mut purged = Purged::default();
        m.purge_partition("t", 0, &mut purged);
        assert_eq!(purged.records(), 3);

        let batch = m.flush_all().pop().expect("the kept rows");
        let offsets: Vec<i64> = batch.offsets.iter().map(|o| o.offset).collect();
        assert_eq!(offsets, [2, 4, 5], "the kept rows keep their order");
        for ((row, offset), raw) in batch
            .rows
            .iter()
            .zip(&batch.offsets)
            .zip(&batch.raw_payloads)
        {
            assert_eq!(
                row["offset"], offset.offset,
                "a row moved away from its offset"
            );
            assert_eq!(
                **raw,
                [offset.offset as u8],
                "a payload moved away from its row"
            );
        }
    }

    #[test]
    fn purge_partition_counts_a_split_record_once() {
        // One record fanned out into rows shares its offset.
        let mut m = BufferManager::new(&test_config());
        push_read(&mut m, "db.a", ("t", 0, 7), b"first");
        push_read(&mut m, "db.b", ("t", 0, 7), b"second");

        let mut purged = Purged::default();
        m.purge_partition("t", 0, &mut purged);
        assert_eq!(purged.records(), 1);
        assert_eq!(purged.bytes, 11);
        assert_eq!(m.pending_rows(), 0);
    }

    #[test]
    fn purge_partition_of_a_partition_with_nothing_buffered_changes_nothing() {
        let mut m = BufferManager::new(&test_config());
        push_read(&mut m, "db.a", ("t", 1, 20), b"a20");

        let mut purged = Purged::default();
        m.purge_partition("t", 0, &mut purged);
        assert_eq!(purged.records(), 0);
        assert_eq!(purged.bytes, 0);
        assert_eq!(m.pending_rows(), 1);
    }

    #[test]
    fn purge_partition_leaves_rows_pushed_without_an_offset() {
        let mut m = BufferManager::new(&test_config());
        m.push(
            "db.a",
            json!({"id": 1}).as_object().unwrap().clone(),
            None,
            None,
        );

        let mut purged = Purged::default();
        m.purge_partition("t", 0, &mut purged);
        assert_eq!(purged.records(), 0);
        assert_eq!(m.pending_rows(), 1);
    }

    #[test]
    fn a_flush_batch_purge_drops_only_the_partitions_rows() {
        let mut batch = FlushBatch {
            table: CompactString::from("db.a"),
            rows: vec![Map::new(), Map::new(), Map::new()],
            offsets: vec![
                KafkaOffset::new("t", 0, 1),
                KafkaOffset::new("t", 2, 2),
                KafkaOffset::new("t", 0, 3),
            ],
            raw_payloads: vec![
                Arc::from(&b"aa"[..]),
                Arc::from(&b"b"[..]),
                Arc::from(&b"ccc"[..]),
            ],
            reserved: vec![20, 10, 30],
        };

        let mut purged = Purged::default();
        batch.purge_partition("t", 0, &mut purged);
        assert_eq!(purged.records(), 2);
        assert_eq!(purged.bytes, 50, "the reservation, not the kept payload");
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.offsets[0].offset, 2);
        assert_eq!(&*batch.raw_payloads[0], b"b");
        assert_eq!(batch.reserved, [10]);
        assert_eq!(batch.reserved_bytes(), 10);
    }

    #[test]
    fn test_table_buffer_push_and_build_preserves_offsets() {
        // Directly test TableBuffer via BufferManager.push with offsets
        let mut m = BufferManager::new(&test_config());
        let topic: Arc<str> = Arc::from("t");
        for i in 0..5 {
            let off = KafkaOffset::with_shared_topic(topic.clone(), 0, i * 10);
            m.push(
                "db.a",
                json!({"id": i}).as_object().unwrap().clone(),
                Some(off),
                None,
            );
        }
        // threshold is 5
        let batches = m.get_ready_for_flush();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].offsets.len(), 5);
        // Offsets preserved in insertion order
        for (i, o) in batches[0].offsets.iter().enumerate() {
            assert_eq!(o.offset, (i as i64) * 10);
        }
    }
}
