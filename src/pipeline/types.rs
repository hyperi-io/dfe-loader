// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared types for the pipeline processing stages.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::Value;

use crate::buffer::KafkaOffset;

/// Result of processing a single message through the parallel phase.
///
/// Carries all data needed by the sequential phase (buffer push, mark_pending).
/// Produced by `super::processor::MessageProcessor::process`, consumed by
/// `super::coordinator::BatchCoordinator::apply_results`.
pub struct ProcessedMessage {
    /// Destination table (db.table) from routing.
    pub table: String,
    /// Transformed field map ready for ClickHouse insertion.
    pub data: serde_json::Map<String, Value>,
    /// Raw payload bytes for zero-copy `_json` splice (json_primary path only).
    pub raw_payload: Option<Arc<[u8]>>,
    /// Kafka offset for commit tracking.
    pub kafka_offset: KafkaOffset,
    /// The table routing chose, when the destination was rewritten because
    /// `ClickHouse` had confirmed it absent. `None` on the ordinary path.
    pub fell_back_from: Option<String>,
}

/// Tables `ClickHouse` confirmed absent, held only while that answer is fresh.
///
/// Absence is not a permanent fact. A source commonly produces before
/// dfe-engine has created its table, and `system.columns` is access-filtered,
/// so a loader identity missing a GRANT also reads as "table absent". Each
/// entry expires and is re-resolved, which is what lets a table that appears
/// start receiving its own data without a pod restart.
pub(crate) struct AbsentTables {
    entries: FxHashMap<String, Instant>,
    /// Tables whose entry aged out and are being re-resolved. The same "does
    /// not exist" coming back is the answer already held, not news, so without
    /// this memory a dead source re-announces itself once a TTL forever (#129).
    rechecking: FxHashSet<String>,
    ttl: Duration,
    /// Upper bound on distinct entries. The routed table name comes from a
    /// payload field with no allowlist, so untrusted input would otherwise
    /// grow this map -- and the per-table metric label set with it -- without
    /// limit.
    capacity: usize,
}

/// What [`AbsentTables::insert`] did with a table.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AbsentOutcome {
    /// The table has moved into absence -- the transition an operator is told
    /// about.
    Recorded,
    /// Already known absent, whether still tracked or just re-resolved.
    Refreshed,
    /// The cap is full. The table is untracked, so its messages keep buffering
    /// until the pending-schema age cap routes them to the DLQ.
    Rejected,
}

impl AbsentTables {
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: FxHashMap::default(),
            rechecking: FxHashSet::default(),
            ttl,
            capacity,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, table: &str) -> bool {
        self.entries.contains_key(table)
    }

    /// Mark a table absent, or refresh an answer already held.
    pub fn insert(&mut self, table: &str, now: Instant) -> AbsentOutcome {
        if let Some(slot) = self.entries.get_mut(table) {
            *slot = now;
            return AbsentOutcome::Refreshed;
        }
        if self.entries.len() >= self.capacity {
            return AbsentOutcome::Rejected;
        }
        self.entries.insert(table.to_string(), now);
        if self.rechecking.remove(table) {
            AbsentOutcome::Refreshed
        } else {
            AbsentOutcome::Recorded
        }
    }

    /// Forget a table -- `ClickHouse` has since resolved its schema.
    pub fn remove(&mut self, table: &str) {
        self.entries.remove(table);
        self.rechecking.remove(table);
    }

    /// Remove and return the entries whose TTL has passed, remembering each as
    /// being re-resolved. The caller re-requests resolution for each.
    pub fn expired(&mut self, now: Instant) -> Vec<String> {
        let ttl = self.ttl;
        let due: Vec<String> = self
            .entries
            .iter()
            .filter(|&(_, &marked)| now.saturating_duration_since(marked) >= ttl)
            .map(|(t, _)| t.clone())
            .collect();
        // A table whose re-resolution never answers stays here, so the recheck
        // memory is dropped wholesale at the cap the entries are bounded by.
        if self.rechecking.len() >= self.capacity {
            self.rechecking.clear();
        }
        for t in &due {
            self.entries.remove(t);
            self.rechecking.insert(t.clone());
        }
        due
    }
}

/// Outcome of one schema-resolution attempt for a table.
///
/// The two failure arms must stay apart: an absent table falls back to the
/// default table, while an unreachable ClickHouse keeps buffering and retrying.
/// Collapsing them would dump every source's events into the default table
/// during an outage.
pub enum SchemaResolution {
    /// Schema fetched.
    Resolved(crate::clickhouse::TableSchema),
    /// The table does not exist — the query answered, with no columns.
    TableNotFound,
    /// The fetch failed; whether the table exists is unknown.
    Unavailable,
}

/// Result of async per-table schema resolution.
///
/// Fetched by a background resolver task. Applied to per-table caches
/// (capture overrides, field mapping, computed columns, schema cache)
/// when received via the schema result channel.
pub struct TableResolutionResult {
    /// Destination table (db.table).
    pub table: String,
    /// Table-level COMMENT string (for DDL capture tags).
    pub comment: String,
    /// Schema from `system.columns` (for field mapping + `SharedSchemaCache`),
    /// or why it could not be fetched.
    pub schema: SchemaResolution,
    /// Parsed per-column directives (skip/default/renamed/computed/coerce).
    pub column_directives: rustc_hash::FxHashMap<String, crate::column_meta::ColumnDirectives>,
}

#[cfg(test)]
mod tests {
    use super::{AbsentOutcome, AbsentTables};
    use std::time::{Duration, Instant};

    #[test]
    fn an_absent_table_is_forgotten_once_its_ttl_passes() {
        // Standard onboarding: a source produces before dfe-engine has created
        // its table. Without expiry that source lands in the default table
        // until the pod restarts.
        let mut absent = AbsentTables::new(Duration::from_secs(60), 8);
        let t0 = Instant::now();

        assert_eq!(absent.insert("dfe.acme", t0), AbsentOutcome::Recorded);
        assert!(absent.contains("dfe.acme"));

        assert!(
            absent.expired(t0 + Duration::from_secs(59)).is_empty(),
            "a fresh entry must still divert its messages"
        );
        assert!(absent.contains("dfe.acme"));

        let due = absent.expired(t0 + Duration::from_secs(60));
        assert_eq!(due, vec!["dfe.acme".to_string()]);
        assert!(
            !absent.contains("dfe.acme"),
            "an expired table must be re-resolved, not diverted forever"
        );
    }

    #[test]
    fn a_resolved_table_is_forgotten_immediately() {
        let mut absent = AbsentTables::new(Duration::from_secs(60), 8);
        absent.insert("dfe.acme", Instant::now());
        absent.remove("dfe.acme");
        assert!(!absent.contains("dfe.acme"));
        assert!(absent.is_empty());
    }

    #[test]
    fn re_marking_refreshes_rather_than_duplicating() {
        let mut absent = AbsentTables::new(Duration::from_secs(60), 8);
        let t0 = Instant::now();
        assert_eq!(absent.insert("dfe.acme", t0), AbsentOutcome::Recorded);
        assert_eq!(
            absent.insert("dfe.acme", t0 + Duration::from_secs(30)),
            AbsentOutcome::Refreshed
        );
        assert_eq!(absent.len(), 1);
        assert!(
            absent.expired(t0 + Duration::from_secs(80)).is_empty(),
            "the refresh must move the expiry"
        );
    }

    #[test]
    fn the_same_answer_after_a_re_resolve_is_not_news() {
        // The entry is dropped every TTL and re-recorded, so an outcome that
        // reads the state instead of the transition warns once a minute per
        // dead source name forever (#129).
        let mut absent = AbsentTables::new(Duration::from_secs(60), 8);
        let t0 = Instant::now();

        assert_eq!(absent.insert("dfe.harness01", t0), AbsentOutcome::Recorded);
        let due = absent.expired(t0 + Duration::from_secs(60));
        assert_eq!(due, vec!["dfe.harness01".to_string()]);

        assert_eq!(
            absent.insert("dfe.harness01", t0 + Duration::from_secs(60)),
            AbsentOutcome::Refreshed,
            "the re-resolved answer is the one already held"
        );
        assert!(absent.contains("dfe.harness01"), "diversion resumes");

        // And it stays quiet for every round after that.
        let due = absent.expired(t0 + Duration::from_secs(120));
        assert_eq!(due, vec!["dfe.harness01".to_string()]);
        assert_eq!(
            absent.insert("dfe.harness01", t0 + Duration::from_secs(120)),
            AbsentOutcome::Refreshed
        );
    }

    #[test]
    fn a_table_that_appears_and_goes_again_is_news_twice() {
        let mut absent = AbsentTables::new(Duration::from_secs(60), 8);
        let t0 = Instant::now();

        assert_eq!(absent.insert("dfe.acme", t0), AbsentOutcome::Recorded);
        absent.expired(t0 + Duration::from_secs(60));
        // The table was created, so the re-resolve resolved it.
        absent.remove("dfe.acme");

        assert_eq!(
            absent.insert("dfe.acme", t0 + Duration::from_secs(600)),
            AbsentOutcome::Recorded,
            "a table that was dropped again is a new transition"
        );
    }

    #[test]
    fn the_recheck_memory_is_bounded_like_the_entries() {
        // A re-resolution that never answers leaves its table here, so the set
        // must not grow past the cap on untrusted table names.
        let mut absent = AbsentTables::new(Duration::from_secs(60), 2);
        let t0 = Instant::now();

        absent.insert("dfe.a", t0);
        absent.insert("dfe.b", t0);
        absent.expired(t0 + Duration::from_secs(60));

        // Two fresh names fill the entries, and their expiry sweep finds the
        // recheck memory already at the cap.
        absent.insert("dfe.c", t0 + Duration::from_secs(60));
        absent.insert("dfe.d", t0 + Duration::from_secs(60));
        absent.expired(t0 + Duration::from_secs(120));

        assert_eq!(
            absent.insert("dfe.c", t0 + Duration::from_secs(120)),
            AbsentOutcome::Refreshed,
            "the tables just swept are still remembered"
        );
        assert_eq!(
            absent.insert("dfe.a", t0 + Duration::from_secs(120)),
            AbsentOutcome::Recorded,
            "the older half was dropped at the cap and re-announces once"
        );
    }

    #[test]
    fn the_cap_bounds_what_untrusted_routing_can_grow() {
        // The routed table name comes from a payload field, so the cap is what
        // stops a hostile producer growing the map and the metric label set.
        let mut absent = AbsentTables::new(Duration::from_secs(60), 2);
        let now = Instant::now();
        assert_eq!(absent.insert("dfe.a", now), AbsentOutcome::Recorded);
        assert_eq!(absent.insert("dfe.b", now), AbsentOutcome::Recorded);
        assert_eq!(absent.insert("dfe.c", now), AbsentOutcome::Rejected);
        assert_eq!(absent.len(), 2);
        assert!(!absent.contains("dfe.c"));
        // A table already tracked is still refreshable at the cap.
        assert_eq!(absent.insert("dfe.a", now), AbsentOutcome::Refreshed);
    }
}
