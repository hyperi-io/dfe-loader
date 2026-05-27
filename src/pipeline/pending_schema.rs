// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pending-schema buffer.
//!
//! Holds messages whose destination table's schema is not yet cached.
//! Drained when the background resolver populates the schema. Caps and
//! age limits route overflow / stale messages to DLQ with security events.
//!
//! `last_requested_at` drives two things: the first-request signal
//! (`EnqueueOutcome::NeedsResolution`) so the caller kicks off resolution,
//! and periodic re-requests (added in a later task) so a table whose
//! resolution failed — e.g. a transient ClickHouse outage — is retried
//! instead of silently ageing out to the DLQ.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use crate::clickhouse::SchemaCache;
use crate::kafka::KafkaMessage;

/// Per-buffer configuration.
#[derive(Debug, Clone)]
pub(crate) struct PendingSchemaConfig {
    pub max_per_table: usize,
    pub max_total: usize,
    pub max_age: Duration,
}

impl PendingSchemaConfig {
    #[allow(dead_code)]
    pub fn from_schema_config(c: &crate::config::SchemaConfig) -> Self {
        Self {
            max_per_table: c.pending_max_per_table,
            max_total: c.pending_max_total,
            max_age: Duration::from_secs(c.pending_max_age_secs),
        }
    }
}

/// Outcome of a successful `enqueue` call.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EnqueueOutcome {
    /// Enqueued; resolution has already been requested for this table.
    Enqueued,
    /// Enqueued; caller must push the table to the resolver channel.
    NeedsResolution,
}

/// Error returned by `enqueue` when caps are hit.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PendingOverflow {
    /// Per-table cap exceeded for this table.
    PerTable(String),
}

/// Reason a message was returned by `expire` / `drain_all`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ExpireReason {
    AgeExceeded { age_ms: u128, table: String },
    GlobalCapEviction { table: String },
    Shutdown { table: String },
}

struct PendingMsg {
    msg: KafkaMessage,
    table: String,
    enqueued_at: Instant,
}

pub(crate) struct PendingSchemaBuffer {
    per_table: FxHashMap<String, VecDeque<PendingMsg>>,
    /// Last time resolution was requested for a table. Drives the
    /// first-request signal AND periodic re-requests. Cleared for a table
    /// when `take_ready` drains it.
    last_requested_at: FxHashMap<String, Instant>,
    total_count: usize,
    /// Messages evicted by global-cap overflow, drained on the next
    /// `expire()` call (added in a later task).
    evicted: VecDeque<(KafkaMessage, ExpireReason)>,
    config: PendingSchemaConfig,
}

impl PendingSchemaBuffer {
    pub fn new(config: PendingSchemaConfig) -> Self {
        Self {
            per_table: FxHashMap::default(),
            last_requested_at: FxHashMap::default(),
            total_count: 0,
            evicted: VecDeque::new(),
            config,
        }
    }

    pub fn len(&self) -> usize {
        self.total_count
    }

    pub fn len_per_table(&self, table: &str) -> usize {
        self.per_table.get(table).map_or(0, VecDeque::len)
    }

    /// Enqueue a message awaiting schema resolution.
    ///
    /// Returns `NeedsResolution` the first time a table is seen (caller must
    /// push the table to the resolver channel); `Enqueued` otherwise.
    ///
    /// Per-table cap: returns `Err(PerTable)` and does NOT enqueue — the
    /// caller routes the current message to DLQ. Global cap: evicts the
    /// oldest message across all tables (queued in `evicted`) before inserting.
    pub fn enqueue(
        &mut self,
        table: String,
        msg: KafkaMessage,
    ) -> Result<EnqueueOutcome, PendingOverflow> {
        if self.len_per_table(&table) >= self.config.max_per_table {
            return Err(PendingOverflow::PerTable(table));
        }

        if self.total_count >= self.config.max_total {
            self.evict_oldest();
        }

        let queue = self.per_table.entry(table.clone()).or_default();
        queue.push_back(PendingMsg {
            msg,
            table: table.clone(),
            enqueued_at: Instant::now(),
        });
        self.total_count += 1;

        // First time we see this table -> caller must request resolution.
        if self.last_requested_at.contains_key(&table) {
            Ok(EnqueueOutcome::Enqueued)
        } else {
            self.last_requested_at.insert(table, Instant::now());
            Ok(EnqueueOutcome::NeedsResolution)
        }
    }

    /// Drain messages whose table now has a cached schema.
    ///
    /// Clears the per-table request timestamp so a later miss for the same
    /// table triggers a fresh resolution request.
    pub fn take_ready(&mut self, schema_cache: &SchemaCache) -> Vec<KafkaMessage> {
        let mut out = Vec::new();
        let ready_tables: Vec<String> = self
            .per_table
            .keys()
            .filter(|t| schema_cache.get(t).is_some())
            .cloned()
            .collect();
        for table in ready_tables {
            if let Some(queue) = self.per_table.remove(&table) {
                self.total_count -= queue.len();
                self.last_requested_at.remove(&table);
                out.extend(queue.into_iter().map(|p| p.msg));
            }
        }
        out
    }

    /// Evict the oldest message across all tables into the eviction queue.
    /// Drained by `expire()` (added in a later task).
    fn evict_oldest(&mut self) {
        let oldest_table = self
            .per_table
            .iter()
            .filter_map(|(t, q)| q.front().map(|m| (t.clone(), m.enqueued_at)))
            .min_by_key(|(_, ts)| *ts)
            .map(|(t, _)| t);

        if let Some(table) = oldest_table
            && let Some(queue) = self.per_table.get_mut(&table)
            && let Some(p) = queue.pop_front()
        {
            self.total_count -= 1;
            self.evicted
                .push_back((p.msg, ExpireReason::GlobalCapEviction { table: p.table }));
            if queue.is_empty() {
                self.per_table.remove(&table);
                self.last_requested_at.remove(&table);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::clickhouse::schema::SchemaCache;
    use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

    fn make_msg(payload: &[u8]) -> KafkaMessage {
        KafkaMessage {
            payload: payload.to_vec(),
            topic: Arc::from("t"),
            partition: 0,
            offset: 0,
            key: None,
            timestamp_ms: None,
        }
    }

    fn small_cfg() -> PendingSchemaConfig {
        PendingSchemaConfig {
            max_per_table: 3,
            max_total: 10,
            max_age: Duration::from_secs(30),
        }
    }

    fn dummy_schema(table: &str) -> TableSchema {
        let (db, t) = table.split_once('.').unwrap_or(("default", table));
        TableSchema {
            database: db.into(),
            table: t.into(),
            columns: vec![ColumnInfo {
                name: "c".into(),
                type_name: "String".into(),
                parsed_type: ParsedType::parse("String"),
                position: 0,
                default_kind: String::new(),
                default_expression: String::new(),
                comment: String::new(),
                is_in_primary_key: false,
                is_in_sorting_key: false,
            }],
            comment: String::new(),
        }
    }

    #[test]
    fn enqueue_first_time_needs_resolution() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        let out = buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        assert_eq!(out, EnqueueOutcome::NeedsResolution);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.len_per_table("dfe.t1"), 1);
    }

    #[test]
    fn enqueue_second_time_returns_enqueued() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        let out = buf.enqueue("dfe.t1".into(), make_msg(b"b")).unwrap();
        assert_eq!(out, EnqueueOutcome::Enqueued);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn take_ready_drains_when_schema_present() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        buf.enqueue("dfe.t1".into(), make_msg(b"b")).unwrap();
        buf.enqueue("dfe.t2".into(), make_msg(b"c")).unwrap();

        let cache = SchemaCache::new(300);
        cache.insert("dfe.t1".into(), dummy_schema("dfe.t1"));

        let ready = buf.take_ready(&cache);
        assert_eq!(ready.len(), 2);
        assert_eq!(buf.len(), 1); // t2 still pending
        assert_eq!(buf.len_per_table("dfe.t1"), 0);

        // Re-enqueue t1 -> resolution must be requested again.
        let out = buf.enqueue("dfe.t1".into(), make_msg(b"d")).unwrap();
        assert_eq!(out, EnqueueOutcome::NeedsResolution);
    }

    #[test]
    fn enqueue_per_table_overflow_returns_err() {
        let mut buf = PendingSchemaBuffer::new(small_cfg()); // max_per_table = 3
        buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        buf.enqueue("dfe.t1".into(), make_msg(b"b")).unwrap();
        buf.enqueue("dfe.t1".into(), make_msg(b"c")).unwrap();
        let err = buf.enqueue("dfe.t1".into(), make_msg(b"d")).unwrap_err();
        assert_eq!(err, PendingOverflow::PerTable("dfe.t1".into()));
        assert_eq!(buf.len(), 3); // unchanged
    }
}
