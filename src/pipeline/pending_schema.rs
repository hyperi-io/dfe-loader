// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pending-schema buffer.
//!
//! Holds messages whose destination table's schema is not yet cached.
//! Drained when the background resolver populates the schema. Age limits route
//! stale messages to the DLQ with security events. A full buffer either routes
//! its overflow to the DLQ or admits it and reports itself full, so the caller
//! stops intake -- see [`OnFull`].
//!
//! `last_requested_at` drives two things: the first-request signal
//! (`EnqueueOutcome::NeedsResolution`) so the caller kicks off resolution,
//! and periodic re-requests (added in a later task) so a table whose
//! resolution failed — e.g. a transient ClickHouse outage — is retried
//! instead of silently ageing out to the DLQ.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use crate::buffer::KafkaOffset;
use crate::clickhouse::SchemaCache;
use crate::kafka::KafkaMessage;

/// Per-buffer configuration.
///
/// The `max_` prefix on every field is intentional and reads clearly at the
/// call sites (`config.max_per_table`); the shared prefix is not noise here.
#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub(crate) struct PendingSchemaConfig {
    pub max_per_table: usize,
    pub max_total: usize,
    pub max_age: Duration,
    pub on_full: OnFull,
}

impl PendingSchemaConfig {
    pub fn from_schema_config(c: &crate::config::SchemaConfig, on_full: OnFull) -> Self {
        Self {
            max_per_table: c.pending_max_per_table,
            max_total: c.pending_max_total,
            max_age: Duration::from_secs(c.pending_max_age_secs),
            on_full,
        }
    }
}

/// What a message does when the buffer has hit a cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnFull {
    /// Route the overflow to the DLQ and keep taking messages.
    DeadLetter,
    /// Take the message anyway; the caller stops intake while
    /// [`PendingSchemaBuffer::is_full`] holds, which is the only answer where
    /// the buffer is a message's last copy.
    Backpressure,
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

    /// Whether any cap is reached: the global one, or any single table's.
    pub fn is_full(&self) -> bool {
        self.total_count >= self.config.max_total
            || self
                .per_table
                .values()
                .any(|q| q.len() >= self.config.max_per_table)
    }

    /// The lowest offset held on each partition, evictions not yet collected
    /// included: a Kafka commit must not pass a message whose only copy is here.
    pub fn lowest_offsets(&self) -> Vec<KafkaOffset> {
        let held = self
            .per_table
            .values()
            .flatten()
            .map(|p| &p.msg)
            .chain(self.evicted.iter().map(|(msg, _)| msg));
        let mut lowest: FxHashMap<(&str, i32), &KafkaMessage> = FxHashMap::default();
        for msg in held {
            lowest
                .entry((&*msg.topic, msg.partition))
                .and_modify(|low| {
                    if msg.offset < low.offset {
                        *low = msg;
                    }
                })
                .or_insert(msg);
        }
        lowest
            .into_values()
            .map(|msg| {
                KafkaOffset::with_shared_topic(Arc::clone(&msg.topic), msg.partition, msg.offset)
            })
            .collect()
    }

    /// The `limit` tables holding the most messages, deepest first.
    pub fn deepest_tables(&self, limit: usize) -> Vec<(&str, usize)> {
        let mut depths: Vec<(&str, usize)> = self
            .per_table
            .iter()
            .map(|(table, queue)| (table.as_str(), queue.len()))
            .collect();
        depths.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        depths.truncate(limit);
        depths
    }

    /// Enqueue a message awaiting schema resolution.
    ///
    /// Returns `NeedsResolution` the first time a table is seen (caller must
    /// push the table to the resolver channel); `Enqueued` otherwise.
    ///
    /// Under [`OnFull::DeadLetter`], the per-table cap returns `Err(PerTable)`
    /// and does NOT enqueue -- the caller routes the current message to DLQ --
    /// and the global cap evicts the oldest message across all tables (queued
    /// in `evicted`) before inserting. Under [`OnFull::Backpressure`] every
    /// message is enqueued and neither cap drops anything.
    pub fn enqueue(
        &mut self,
        table: String,
        msg: KafkaMessage,
    ) -> Result<EnqueueOutcome, PendingOverflow> {
        if self.config.on_full == OnFull::DeadLetter {
            if self.len_per_table(&table) >= self.config.max_per_table {
                return Err(PendingOverflow::PerTable(table));
            }
            if self.total_count >= self.config.max_total {
                self.evict_oldest();
            }
        }

        let queue = self.per_table.entry(table.clone()).or_default();
        queue.push_back(PendingMsg {
            msg,
            table: table.clone(),
            enqueued_at: Instant::now(),
        });
        self.total_count += 1;

        // First time we see this table -> caller must request resolution.
        match self.last_requested_at.entry(table) {
            std::collections::hash_map::Entry::Occupied(_) => Ok(EnqueueOutcome::Enqueued),
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(Instant::now());
                Ok(EnqueueOutcome::NeedsResolution)
            }
        }
    }

    /// Drain messages whose table now has a cached schema, or whose table
    /// ClickHouse has confirmed absent.
    ///
    /// An absent table's messages are released so the processor can re-route
    /// them to the default table; holding them would only age them out to DLQ.
    /// Clears the per-table request timestamp so a later miss for the same
    /// table triggers a fresh resolution request.
    pub fn take_ready(
        &mut self,
        schema_cache: &SchemaCache,
        absent: &super::types::AbsentTables,
    ) -> Vec<KafkaMessage> {
        let mut out = Vec::new();
        let ready_tables: Vec<String> = self
            .per_table
            .keys()
            .filter(|t| schema_cache.get(t).is_some() || absent.contains(t))
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

    /// Return messages older than `config.max_age`, plus any global-cap
    /// evictions queued since the last call. Caller routes them to DLQ.
    pub fn expire(&mut self, now: Instant) -> Vec<(KafkaMessage, ExpireReason)> {
        let mut out: Vec<(KafkaMessage, ExpireReason)> = self.evicted.drain(..).collect();

        let max_age = self.config.max_age;
        let mut empty_tables = Vec::new();
        for (table, queue) in &mut self.per_table {
            while let Some(front) = queue.front() {
                let age = now.saturating_duration_since(front.enqueued_at);
                if age < max_age {
                    break;
                }
                let p = queue.pop_front().expect("front checked above");
                self.total_count -= 1;
                out.push((
                    p.msg,
                    ExpireReason::AgeExceeded {
                        age_ms: age.as_millis(),
                        table: p.table,
                    },
                ));
            }
            if queue.is_empty() {
                empty_tables.push(table.clone());
            }
        }
        for t in empty_tables {
            self.per_table.remove(&t);
            self.last_requested_at.remove(&t);
        }
        out
    }

    /// Drain all pending messages with `Shutdown` reason. Caller routes to DLQ.
    pub fn drain_all(&mut self) -> Vec<(KafkaMessage, ExpireReason)> {
        let mut out: Vec<(KafkaMessage, ExpireReason)> = self.evicted.drain(..).collect();
        for (table, queue) in self.per_table.drain() {
            for p in queue {
                out.push((
                    p.msg,
                    ExpireReason::Shutdown {
                        table: table.clone(),
                    },
                ));
            }
        }
        self.total_count = 0;
        self.last_requested_at.clear();
        out
    }

    /// Tables still pending whose last resolution request is older than
    /// `interval`. Re-stamps them to `now`. The caller re-sends each to the
    /// resolver — so a table whose resolution failed (e.g. a transient
    /// ClickHouse outage) is retried instead of silently ageing out to DLQ.
    pub fn tables_needing_rerequest(&mut self, now: Instant, interval: Duration) -> Vec<String> {
        let due: Vec<String> = self
            .last_requested_at
            .iter()
            .filter(|&(_, &ts)| now.saturating_duration_since(ts) >= interval)
            .map(|(t, _)| t.clone())
            .collect();
        for t in &due {
            self.last_requested_at.insert(t.clone(), now);
        }
        due
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

    fn no_absent_tables() -> crate::pipeline::types::AbsentTables {
        crate::pipeline::types::AbsentTables::new(Duration::from_secs(60), 16)
    }

    fn small_cfg() -> PendingSchemaConfig {
        PendingSchemaConfig {
            max_per_table: 3,
            max_total: 10,
            max_age: Duration::from_secs(30),
            on_full: OnFull::DeadLetter,
        }
    }

    fn backpressure_cfg() -> PendingSchemaConfig {
        PendingSchemaConfig {
            on_full: OnFull::Backpressure,
            ..small_cfg()
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

        let ready = buf.take_ready(&cache, &no_absent_tables());
        assert_eq!(ready.len(), 2);
        assert_eq!(buf.len(), 1); // t2 still pending
        assert_eq!(buf.len_per_table("dfe.t1"), 0);

        // Re-enqueue t1 -> resolution must be requested again.
        let out = buf.enqueue("dfe.t1".into(), make_msg(b"d")).unwrap();
        assert_eq!(out, EnqueueOutcome::NeedsResolution);
    }

    #[test]
    fn take_ready_releases_tables_confirmed_absent() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.acme_widgets".into(), make_msg(b"a"))
            .unwrap();
        buf.enqueue("dfe.acme_widgets".into(), make_msg(b"b"))
            .unwrap();
        buf.enqueue("dfe.unreachable".into(), make_msg(b"c"))
            .unwrap();

        let cache = SchemaCache::new(300);
        let mut absent = no_absent_tables();
        absent.insert("dfe.acme_widgets", std::time::Instant::now());

        let ready = buf.take_ready(&cache, &absent);
        assert_eq!(ready.len(), 2, "the absent table's messages are released");
        assert_eq!(
            buf.len(),
            1,
            "a table ClickHouse never answered for stays buffered"
        );
        assert_eq!(buf.len_per_table("dfe.unreachable"), 1);
    }

    #[test]
    fn unreachable_table_stays_pending_then_expires_to_dlq() {
        // A ClickHouse outage must keep buffering and retrying, and only the age
        // cap may give up — never a fallback that would dump the source into
        // the default table.
        let cfg = PendingSchemaConfig {
            max_per_table: 100,
            max_total: 100,
            max_age: Duration::from_millis(20),
            on_full: OnFull::DeadLetter,
        };
        let mut buf = PendingSchemaBuffer::new(cfg);
        buf.enqueue("dfe.unreachable".into(), make_msg(b"x"))
            .unwrap();

        let cache = SchemaCache::new(300);
        let absent = no_absent_tables();
        assert!(
            buf.take_ready(&cache, &absent).is_empty(),
            "nothing is released while resolution is merely failing"
        );
        assert_eq!(buf.len(), 1);

        // The re-request loop keeps asking while the buffer holds the message.
        let later = Instant::now() + Duration::from_secs(3);
        assert_eq!(
            buf.tables_needing_rerequest(later, Duration::from_secs(2)),
            vec!["dfe.unreachable".to_string()]
        );

        std::thread::sleep(Duration::from_millis(40));
        let expired = buf.expire(Instant::now());
        assert_eq!(expired.len(), 1);
        assert!(matches!(expired[0].1, ExpireReason::AgeExceeded { .. }));
        assert_eq!(buf.len(), 0);
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

    #[test]
    fn enqueue_global_overflow_evicts_oldest_across_tables() {
        let cfg = PendingSchemaConfig {
            max_per_table: 100,
            max_total: 3,
            max_age: Duration::from_secs(30),
            on_full: OnFull::DeadLetter,
        };
        let mut buf = PendingSchemaBuffer::new(cfg);
        buf.enqueue("a".into(), make_msg(b"1")).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        buf.enqueue("b".into(), make_msg(b"2")).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        buf.enqueue("a".into(), make_msg(b"3")).unwrap();
        // Full. Next enqueue evicts the oldest (table "a", payload b"1").
        buf.enqueue("c".into(), make_msg(b"4")).unwrap();
        assert_eq!(buf.len(), 3);

        // The eviction is drained via expire().
        let expired = buf.expire(Instant::now());
        assert_eq!(expired.len(), 1);
        let (msg, reason) = &expired[0];
        assert_eq!(msg.payload, b"1");
        assert!(matches!(reason, ExpireReason::GlobalCapEviction { table } if table == "a"));
    }

    #[test]
    fn expire_returns_messages_older_than_max_age() {
        let cfg = PendingSchemaConfig {
            max_per_table: 100,
            max_total: 100,
            max_age: Duration::from_millis(20),
            on_full: OnFull::DeadLetter,
        };
        let mut buf = PendingSchemaBuffer::new(cfg);
        buf.enqueue("a".into(), make_msg(b"old")).unwrap();
        std::thread::sleep(Duration::from_millis(40));
        buf.enqueue("a".into(), make_msg(b"new")).unwrap();

        let expired = buf.expire(Instant::now());
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0.payload, b"old");
        assert!(matches!(expired[0].1, ExpireReason::AgeExceeded { .. }));
        assert_eq!(buf.len(), 1); // "new" still pending
    }

    #[test]
    fn drain_all_returns_everything_with_shutdown_reason() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("a".into(), make_msg(b"1")).unwrap();
        buf.enqueue("b".into(), make_msg(b"2")).unwrap();
        let drained = buf.drain_all();
        assert_eq!(drained.len(), 2);
        assert!(
            drained
                .iter()
                .all(|(_, r)| matches!(r, ExpireReason::Shutdown { .. }))
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn under_backpressure_a_full_table_takes_every_message_and_reports_full() {
        let mut buf = PendingSchemaBuffer::new(backpressure_cfg()); // max_per_table = 3
        for n in 0..5u8 {
            buf.enqueue("dfe.t1".into(), make_msg(&[n]))
                .expect("backpressure never refuses a message");
        }
        assert_eq!(buf.len_per_table("dfe.t1"), 5, "nothing was turned away");
        assert!(buf.is_full(), "a table past its cap must stop intake");
        assert!(
            buf.expire(Instant::now()).is_empty(),
            "nothing was evicted to the DLQ"
        );
    }

    #[test]
    fn under_backpressure_the_global_cap_evicts_nothing() {
        let cfg = PendingSchemaConfig {
            max_per_table: 100,
            max_total: 3,
            ..backpressure_cfg()
        };
        let mut buf = PendingSchemaBuffer::new(cfg);
        for table in ["a", "b", "c", "d"] {
            buf.enqueue(table.into(), make_msg(table.as_bytes()))
                .expect("backpressure never refuses a message");
        }
        assert_eq!(buf.len(), 4);
        assert!(buf.is_full());
        assert!(buf.expire(Instant::now()).is_empty(), "no eviction queued");
    }

    #[test]
    fn a_buffer_drained_below_its_caps_is_no_longer_full() {
        let mut buf = PendingSchemaBuffer::new(backpressure_cfg());
        for n in 0..3u8 {
            buf.enqueue("dfe.t1".into(), make_msg(&[n])).unwrap();
        }
        assert!(buf.is_full());

        let cache = SchemaCache::new(300);
        cache.insert("dfe.t1".into(), dummy_schema("dfe.t1"));
        assert_eq!(buf.take_ready(&cache, &no_absent_tables()).len(), 3);
        assert!(!buf.is_full(), "a drained buffer must let intake run again");
    }

    #[test]
    fn a_dead_letter_buffer_below_its_caps_is_not_full() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        assert!(!buf.is_full());
    }

    fn msg_at(topic: &str, partition: i32, offset: i64) -> KafkaMessage {
        KafkaMessage {
            topic: Arc::from(topic),
            partition,
            offset,
            ..make_msg(b"x")
        }
    }

    /// `(topic, partition, offset)` of each floor, sorted.
    fn floors(buf: &PendingSchemaBuffer) -> Vec<(String, i32, i64)> {
        let mut out: Vec<_> = buf
            .lowest_offsets()
            .into_iter()
            .map(|o| (o.topic.to_string(), o.partition, o.offset))
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn nothing_held_holds_no_offset_down() {
        assert!(
            PendingSchemaBuffer::new(small_cfg())
                .lowest_offsets()
                .is_empty()
        );
    }

    #[test]
    fn the_floor_is_the_lowest_held_offset_per_partition_across_tables() {
        let mut buf = PendingSchemaBuffer::new(PendingSchemaConfig {
            max_per_table: 100,
            max_total: 100,
            ..small_cfg()
        });
        buf.enqueue("dfe.a".into(), msg_at("t", 0, 40)).unwrap();
        buf.enqueue("dfe.b".into(), msg_at("t", 0, 12)).unwrap();
        buf.enqueue("dfe.a".into(), msg_at("t", 1, 7)).unwrap();
        buf.enqueue("dfe.b".into(), msg_at("u", 0, 3)).unwrap();
        assert_eq!(
            floors(&buf),
            vec![
                ("t".to_string(), 0, 12),
                ("t".to_string(), 1, 7),
                ("u".to_string(), 0, 3)
            ]
        );
    }

    #[test]
    fn an_eviction_not_yet_collected_still_holds_its_offset_down() {
        let mut buf = PendingSchemaBuffer::new(PendingSchemaConfig {
            max_per_table: 100,
            max_total: 1,
            ..small_cfg()
        });
        buf.enqueue("dfe.a".into(), msg_at("t", 0, 5)).unwrap();
        // The global cap evicts offset 5 into the queue `expire` drains.
        buf.enqueue("dfe.b".into(), msg_at("t", 0, 9)).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(floors(&buf), vec![("t".to_string(), 0, 5)]);

        let handed_on = buf.expire(Instant::now());
        assert_eq!(handed_on.len(), 1);
        assert_eq!(floors(&buf), vec![("t".to_string(), 0, 9)]);
    }

    #[test]
    fn a_message_released_by_its_schema_no_longer_holds_the_floor() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.t1".into(), msg_at("t", 0, 4)).unwrap();
        let cache = SchemaCache::new(300);
        cache.insert("dfe.t1".into(), dummy_schema("dfe.t1"));
        assert_eq!(buf.take_ready(&cache, &no_absent_tables()).len(), 1);
        assert!(buf.lowest_offsets().is_empty());
    }

    #[test]
    fn the_deepest_tables_come_first_and_stop_at_the_limit() {
        let mut buf = PendingSchemaBuffer::new(PendingSchemaConfig {
            max_per_table: 100,
            max_total: 100,
            ..small_cfg()
        });
        for (table, n) in [("dfe.one", 1), ("dfe.three", 3), ("dfe.two", 2)] {
            for _ in 0..n {
                buf.enqueue(table.into(), make_msg(b"x")).unwrap();
            }
        }
        assert_eq!(
            buf.deepest_tables(2),
            vec![("dfe.three", 3), ("dfe.two", 2)]
        );
        assert!(buf.deepest_tables(0).is_empty());
    }

    #[test]
    fn tables_needing_rerequest_after_interval() {
        let mut buf = PendingSchemaBuffer::new(small_cfg());
        buf.enqueue("dfe.t1".into(), make_msg(b"a")).unwrap();
        // Just enqueued -> not yet due.
        assert!(
            buf.tables_needing_rerequest(Instant::now(), Duration::from_secs(2))
                .is_empty()
        );
        // After the interval -> due, and re-stamped.
        let later = Instant::now() + Duration::from_secs(3);
        let due = buf.tables_needing_rerequest(later, Duration::from_secs(2));
        assert_eq!(due, vec!["dfe.t1".to_string()]);
        // Immediately asking again at the same instant -> nothing (re-stamped).
        assert!(
            buf.tables_needing_rerequest(later, Duration::from_secs(2))
                .is_empty()
        );
    }
}
