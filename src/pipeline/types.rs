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
///
/// A table the live routing names is expected to appear: the engine pushes
/// routing when a source is created and deploys its table seconds later. Such
/// a table is re-resolved on a short, doubling wait up to `live_cap`, so it
/// starts receiving its data within seconds. Any other table keeps the full
/// `ttl`.
pub(crate) struct AbsentTables {
    entries: FxHashMap<String, Held>,
    /// Tables whose entry aged out and are being re-resolved, with the wait
    /// their last entry carried. The same "does not exist" coming back is the
    /// answer already held, not news, so without this memory a dead source
    /// re-announces itself once a TTL forever (#129). The wait seeds the
    /// next entry's backoff.
    rechecking: FxHashMap<String, Duration>,
    /// Tables the live routing names.
    live: FxHashSet<String>,
    /// Tables the live routing named until a reload removed them: their source
    /// is gone, so a batch held for one is not waiting on a table to appear.
    retired: FxHashSet<String>,
    ttl: Duration,
    live_first: Duration,
    live_cap: Duration,
    /// Upper bound on distinct entries. The routed table name comes from a
    /// payload field with no allowlist, so untrusted input would otherwise
    /// grow this map -- and the per-table metric label set with it -- without
    /// limit.
    capacity: usize,
}

/// One absent table: when it was marked and how long that answer is trusted.
struct Held {
    marked: Instant,
    wait: Duration,
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
    /// Every table waits `ttl` until [`Self::with_live_recheck`] says otherwise.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: FxHashMap::default(),
            rechecking: FxHashMap::default(),
            live: FxHashSet::default(),
            retired: FxHashSet::default(),
            ttl,
            live_first: ttl,
            live_cap: ttl,
            capacity,
        }
    }

    /// Re-resolve a table the live routing names after `first`, doubling the
    /// wait each time it is still absent, up to `cap` (and never past `ttl`).
    #[must_use]
    pub fn with_live_recheck(mut self, first: Duration, cap: Duration) -> Self {
        self.live_cap = cap.min(self.ttl);
        self.live_first = first.min(self.live_cap);
        self
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

    /// Replace the set of tables the live routing names, and drop the absent
    /// mark of each one. A routing reload naming a table says it is meant to
    /// exist, and the mark may predate the table being created. The caller
    /// resolves the cleared tables again.
    ///
    /// A cleared table is remembered as being re-resolved, so a table that is
    /// still absent is not announced a second time, and its next mark starts
    /// at the short wait.
    ///
    /// A table the previous routing named and this one does not is remembered
    /// as retired until a later reload names it again.
    pub fn set_live(&mut self, tables: impl IntoIterator<Item = String>) {
        let live: FxHashSet<String> = tables.into_iter().collect();
        let dropped: Vec<String> = self.live.difference(&live).cloned().collect();
        self.retired.retain(|t| !live.contains(t));
        if self.retired.len() + dropped.len() > self.capacity {
            self.retired.clear();
        }
        self.retired.extend(dropped);
        self.live = live;
        let cleared: Vec<String> = self
            .entries
            .keys()
            .filter(|t| self.live.contains(*t))
            .cloned()
            .collect();
        if !cleared.is_empty() && self.rechecking.len() >= self.capacity {
            self.rechecking.clear();
        }
        for t in cleared {
            self.entries.remove(&t);
            self.rechecking.insert(t, Duration::ZERO);
        }
    }

    /// Whether a routing reload removed `table`, and none since has named it.
    pub fn is_retired(&self, table: &str) -> bool {
        self.retired.contains(table)
    }

    /// How long to trust a fresh "does not exist" for `table`.
    fn wait_for(&self, table: &str) -> Duration {
        if !self.live.contains(table) {
            return self.ttl;
        }
        match self.rechecking.get(table) {
            Some(prev) => prev.saturating_mul(2).clamp(self.live_first, self.live_cap),
            None => self.live_first,
        }
    }

    /// Mark a table absent, or refresh an answer already held.
    pub fn insert(&mut self, table: &str, now: Instant) -> AbsentOutcome {
        if let Some(held) = self.entries.get_mut(table) {
            held.marked = now;
            return AbsentOutcome::Refreshed;
        }
        if self.entries.len() >= self.capacity {
            return AbsentOutcome::Rejected;
        }
        let wait = self.wait_for(table);
        self.entries
            .insert(table.to_string(), Held { marked: now, wait });
        if self.rechecking.remove(table).is_some() {
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

    /// Remove and return the entries whose wait has passed, remembering each as
    /// being re-resolved. The caller re-requests resolution for each.
    pub fn expired(&mut self, now: Instant) -> Vec<String> {
        let due: Vec<(String, Duration)> = self
            .entries
            .iter()
            .filter(|&(_, held)| now.saturating_duration_since(held.marked) >= held.wait)
            .map(|(t, held)| (t.clone(), held.wait))
            .collect();
        // A table whose re-resolution never answers stays here, so the recheck
        // memory is dropped wholesale at the cap the entries are bounded by.
        // Only on a sweep that adds to it, or a full set would clear every tick
        // and the warning it suppresses would come back.
        if !due.is_empty() && self.rechecking.len() >= self.capacity {
            self.rechecking.clear();
        }
        let mut tables = Vec::with_capacity(due.len());
        for (t, wait) in due {
            self.entries.remove(&t);
            self.rechecking.insert(t.clone(), wait);
            tables.push(t);
        }
        tables
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

    fn live_absent(ttl_secs: u64, first_secs: u64, cap_secs: u64) -> AbsentTables {
        AbsentTables::new(Duration::from_secs(ttl_secs), 8).with_live_recheck(
            Duration::from_secs(first_secs),
            Duration::from_secs(cap_secs),
        )
    }

    #[test]
    fn a_table_the_routing_names_is_rechecked_on_a_short_doubling_wait() {
        // The engine pushes routing at source create and deploys the table a
        // couple of seconds later. Trusting the first "absent" for the full
        // TTL sent that minute of rows to the default table (#275).
        // (live?, still-absent re-marks, expected waits in seconds)
        let cases: [(bool, usize, &[u64]); 3] = [
            (true, 6, &[1, 2, 4, 8, 8, 8]),
            (false, 3, &[60, 60, 60]),
            (true, 1, &[1]),
        ];
        for (live, marks, waits) in cases {
            let mut absent = live_absent(60, 1, 8);
            if live {
                absent.set_live(["dfe.acme".to_string()]);
            }
            let mut now = Instant::now();
            for (n, want) in waits.iter().take(marks).enumerate() {
                absent.insert("dfe.acme", now);
                let wait = Duration::from_secs(*want);
                assert_eq!(
                    absent.expired(now + wait.saturating_sub(Duration::from_millis(1))),
                    Vec::<String>::new(),
                    "live={live} mark {n}: still trusted just before {want}s"
                );
                assert_eq!(
                    absent.expired(now + wait),
                    vec!["dfe.acme".to_string()],
                    "live={live} mark {n}: re-resolved at {want}s"
                );
                now += wait;
            }
        }
    }

    #[test]
    fn a_table_that_appears_after_the_mark_is_picked_up_within_the_short_wait() {
        // Marked absent at t0, created 2 s later. The recheck at 1 s finds
        // nothing, the one at 3 s finds it. The first-ever mark was 60 s.
        let mut absent = live_absent(60, 1, 30);
        absent.set_live(["dfe.acme".to_string()]);
        let t0 = Instant::now();

        absent.insert("dfe.acme", t0);
        assert_eq!(
            absent.expired(t0 + Duration::from_secs(1)),
            vec!["dfe.acme".to_string()]
        );
        assert_eq!(
            absent.insert("dfe.acme", t0 + Duration::from_secs(1)),
            AbsentOutcome::Refreshed,
            "the same answer is not announced again"
        );
        assert_eq!(
            absent.expired(t0 + Duration::from_secs(3)),
            vec!["dfe.acme".to_string()]
        );
        // ClickHouse now answers with a schema: the orchestrator removes the
        // table, and nothing is left to divert or re-check.
        absent.remove("dfe.acme");
        assert!(absent.is_empty());
        assert_eq!(
            absent.expired(t0 + Duration::from_secs(600)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_table_still_absent_is_looked_up_once_per_wait_not_per_message() {
        // Fresh answers are held: between rechecks `expired` hands out
        // nothing, however many ticks (batches) pass, so the lookup count
        // is bounded by the schedule.
        let mut absent = live_absent(60, 1, 30);
        absent.set_live(["dfe.gone".to_string()]);
        let t0 = Instant::now();
        let mut now = t0;
        absent.insert("dfe.gone", now);
        let mut lookups = 0;
        while now < t0 + Duration::from_secs(120) {
            now += Duration::from_millis(10);
            if !absent.expired(now).is_empty() {
                lookups += 1;
                absent.insert("dfe.gone", now);
            }
        }
        // 1, 2, 4, 8, 16, 30, 30, 30 ... over 120 s.
        assert!(
            (6..=10).contains(&lookups),
            "12000 ticks made {lookups} lookups"
        );
    }

    #[test]
    fn a_reload_naming_a_marked_table_clears_the_mark() {
        // (marked, named by the reload, mark survives?)
        let cases = [
            ("dfe.acme", "dfe.acme", false),
            ("dfe.acme", "dfe.other", true),
        ];
        for (marked, named, survives) in cases {
            let mut absent = live_absent(60, 1, 30);
            absent.insert(marked, Instant::now());
            absent.set_live([named.to_string()]);
            assert_eq!(
                absent.contains(marked),
                survives,
                "marked {marked}, reload names {named}"
            );
        }
    }

    #[test]
    fn a_cleared_table_that_is_still_absent_restarts_on_the_short_wait_without_a_second_warning() {
        let mut absent = live_absent(60, 1, 30);
        let t0 = Instant::now();
        // Marked as an unrouted name: the full TTL.
        absent.insert("dfe.acme", t0);
        assert_eq!(
            absent.expired(t0 + Duration::from_secs(1)),
            Vec::<String>::new()
        );

        absent.set_live(["dfe.acme".to_string()]);
        assert!(!absent.contains("dfe.acme"));
        assert_eq!(
            absent.insert("dfe.acme", t0 + Duration::from_secs(2)),
            AbsentOutcome::Refreshed
        );
        assert_eq!(
            absent.expired(t0 + Duration::from_secs(3)),
            vec!["dfe.acme".to_string()],
            "short wait after the reload, not the 60 s it held before"
        );
    }

    #[test]
    fn a_table_a_reload_stops_naming_is_retired_until_one_names_it_again() {
        // (routing reloads in order, table, retired after the last?)
        let cases: [(&[&[&str]], &str, bool); 6] = [
            (&[&["dfe.a", "dfe.b"], &["dfe.a"]], "dfe.b", true),
            (&[&["dfe.a", "dfe.b"], &["dfe.a"]], "dfe.a", false),
            (
                &[&["dfe.a", "dfe.b"], &["dfe.a"], &["dfe.a", "dfe.b"]],
                "dfe.b",
                false,
            ),
            (
                &[&["dfe.a", "dfe.b"], &["dfe.a"], &["dfe.a", "dfe.c"]],
                "dfe.b",
                true,
            ),
            // Never named: a payload-routed table is not retired.
            (&[&["dfe.a"], &[]], "dfe.x", false),
            (&[&["dfe.a"]], "dfe.a", false),
        ];
        for (reloads, table, retired) in cases {
            let mut absent = live_absent(60, 1, 30);
            for routing in reloads {
                absent.set_live(routing.iter().map(|t| (*t).to_string()));
            }
            assert_eq!(
                absent.is_retired(table),
                retired,
                "{table} after {reloads:?}"
            );
        }
    }

    #[test]
    fn the_retired_set_is_bounded_like_the_entries() {
        let mut absent = AbsentTables::new(Duration::from_secs(60), 4);
        for n in 0..10 {
            absent.set_live([format!("dfe.t{n}")]);
            absent.set_live(std::iter::empty());
        }
        assert!(
            absent.retired.len() <= 4,
            "{} retired",
            absent.retired.len()
        );
        assert!(absent.is_retired("dfe.t9"), "the latest removal is kept");
    }
}
