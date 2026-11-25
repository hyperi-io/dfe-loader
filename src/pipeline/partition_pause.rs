// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka partitions held paused while a table waits on its schema.
//!
//! On Kafka a table over its pending-schema cap does not dead-letter its
//! overflow. The partition carrying it is paused and sought back to its first
//! record not kept, so the records wait in Kafka. A partition never stays
//! paused: it resumes once every table holding it resolves, at
//! `schema.pending_max_age_secs` whatever else happens, on a revoke, on a
//! config reload, and at once when its seek fails. A table whose hold aged out
//! dead-letters its overflow, as it did before, until a resolution succeeds,
//! so a schema that never arrives cannot pause the partition again and again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{info, warn};

use crate::buffer::KafkaOffset;
use crate::kafka::transport::ControlResult;
use crate::kafka::{KafkaMessage, TransportBackend};
use crate::metrics::names;

/// A partition, keyed as the transport names it.
type PartitionKey = (Arc<str>, i32);

/// The least time between two warnings that a partition is still paused.
const STILL_PAUSED_WARN: Duration = Duration::from_secs(10);

/// A message the pending-schema buffer refused, and why.
#[derive(Debug)]
pub(crate) struct Overflow {
    pub table: String,
    /// The DLQ reason, should the message be dead-lettered after all.
    pub reason: String,
    pub msg: KafkaMessage,
}

/// Why a held partition resumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResumeReason {
    /// Every table holding it has its schema, or is confirmed absent.
    Resolved,
    /// It was held for `schema.pending_max_age_secs`.
    AgedOut,
    /// A revoke or a consumer rebuild ended the hold.
    Rebalance,
    /// A config reload released it.
    Reload,
    /// Its seek failed, so its overflow went to the DLQ instead.
    SeekError,
}

impl ResumeReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::AgedOut => "aged_out",
            Self::Rebalance => "rebalance",
            Self::Reload => "reload",
            Self::SeekError => "seek_error",
        }
    }
}

/// Where a held partition stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Paused,
    /// Its resume was refused, and the next upkeep tries again.
    Resuming(ResumeReason),
}

/// One held partition.
struct Hold {
    /// Tables whose refused records it carries.
    tables: BTreeSet<String>,
    /// The first record not kept, which the partition reads again from.
    seek_to: i64,
    paused_at: Instant,
    state: State,
}

/// The pause, resume and seek the holds make, so they are tested without a broker.
pub(crate) trait PartitionControl {
    fn pause(&self, topic: &str, partition: i32) -> ControlResult;
    fn resume(&self, topic: &str, partition: i32) -> ControlResult;
    async fn seek(&self, topic: &str, partition: i32, offset: i64) -> ControlResult;
    /// The partitions the transport holds paused now. A revoke ends a hold.
    fn paused(&self) -> Vec<PartitionKey>;
}

impl PartitionControl for TransportBackend {
    fn pause(&self, topic: &str, partition: i32) -> ControlResult {
        self.pause_partition(topic, partition)
    }

    fn resume(&self, topic: &str, partition: i32) -> ControlResult {
        self.resume_partition(topic, partition)
    }

    async fn seek(&self, topic: &str, partition: i32, offset: i64) -> ControlResult {
        self.seek_partition(topic, partition, offset).await
    }

    fn paused(&self) -> Vec<PartitionKey> {
        self.paused_partitions()
    }
}

/// The partitions held paused for tables waiting on their schema.
pub(crate) struct PartitionPauses {
    /// The longest a partition stays held, whatever its tables wait on.
    max_pause: Duration,
    holds: FxHashMap<PartitionKey, Hold>,
    /// Seek points no commit passes until the partition delivers a record again.
    floors: FxHashMap<PartitionKey, i64>,
    /// Tables whose hold aged out. Their overflow is dead-lettered until a
    /// resolution succeeds.
    failed: FxHashSet<String>,
    /// When the last still-paused warning was logged.
    warned_at: Option<Instant>,
}

impl PartitionPauses {
    pub fn new(max_pause: Duration) -> Self {
        Self {
            max_pause,
            holds: FxHashMap::default(),
            floors: FxHashMap::default(),
            failed: FxHashSet::default(),
            warned_at: None,
        }
    }

    /// Whether any partition is held, so the upkeep tick has work.
    pub fn is_holding(&self) -> bool {
        !self.holds.is_empty()
    }

    /// Whether `table`'s overflow goes to the DLQ: its last hold aged out and
    /// no resolution has succeeded since.
    pub fn dead_letters(&self, table: &str) -> bool {
        self.failed.contains(table)
    }

    /// A resolution for `table` succeeded: its overflow pauses again.
    pub fn resolved(&mut self, table: &str) {
        self.failed.remove(table);
    }

    /// Pause each partition carrying `overflow` and seek it back to its first
    /// record not kept. Returns the overflow whose partition could not be
    /// paused or sought, for the DLQ.
    pub async fn hold(
        &mut self,
        control: &impl PartitionControl,
        overflow: Vec<Overflow>,
        now: Instant,
    ) -> Vec<Overflow> {
        let mut by_partition: BTreeMap<PartitionKey, Vec<Overflow>> = BTreeMap::new();
        for refused in overflow {
            let key = (Arc::clone(&refused.msg.topic), refused.msg.partition);
            by_partition.entry(key).or_default().push(refused);
        }
        let mut dead = Vec::new();
        for (key, refused) in by_partition {
            let seek_to = refused
                .iter()
                .map(|r| r.msg.offset)
                .min()
                .unwrap_or(i64::MAX);
            let tables: BTreeSet<String> = refused.iter().map(|r| r.table.clone()).collect();
            if let Some(hold) = self.holds.get_mut(&key) {
                hold.tables.extend(tables);
                if seek_to < hold.seek_to {
                    match control.seek(&key.0, key.1, seek_to).await {
                        Ok(()) => {
                            hold.seek_to = seek_to;
                            self.floors.insert(key, seek_to);
                        }
                        Err(e) => {
                            warn!(
                                topic = %key.0,
                                partition = key.1,
                                offset = seek_to,
                                error = %e,
                                "Partition seek failed, dead-lettering its overflow"
                            );
                            dead.extend(refused);
                        }
                    }
                }
                continue;
            }
            if let Err(e) = control.pause(&key.0, key.1) {
                warn!(
                    topic = %key.0,
                    partition = key.1,
                    tables = ?tables,
                    error = %e,
                    "Partition pause failed, dead-lettering its overflow -- the next overflow tries again"
                );
                dead.extend(refused);
                continue;
            }
            let hold = Hold {
                tables,
                seek_to,
                paused_at: now,
                state: State::Paused,
            };
            if let Err(e) = control.seek(&key.0, key.1, seek_to).await {
                warn!(
                    topic = %key.0,
                    partition = key.1,
                    offset = seek_to,
                    error = %e,
                    "Partition seek failed, dead-lettering its overflow and resuming it"
                );
                dead.extend(refused);
                note_paused(&key);
                self.resume_hold(control, key, hold, ResumeReason::SeekError, now);
                continue;
            }
            warn!(
                topic = %key.0,
                partition = key.1,
                tables = ?hold.tables,
                offset = seek_to,
                reason = "pending_schema",
                max_pause_secs = self.max_pause.as_secs(),
                "Partition paused until its tables' schemas resolve -- it reads again from the offset"
            );
            note_paused(&key);
            self.floors
                .entry(key.clone())
                .and_modify(|f| *f = (*f).min(seek_to))
                .or_insert(seek_to);
            self.holds.insert(key, hold);
        }
        dead
    }

    /// Resume what may resume: a hold whose tables all resolved, one held
    /// past the bound, and one whose resume was refused before. A hold the
    /// transport no longer has, after a revoke, is dropped with its floor.
    /// `releases` says whether a table's waiting records are released.
    pub fn upkeep(
        &mut self,
        control: &impl PartitionControl,
        releases: impl Fn(&str) -> bool,
        now: Instant,
    ) {
        if self.holds.is_empty() {
            return;
        }
        let paused: FxHashSet<PartitionKey> = control.paused().into_iter().collect();
        let keys: Vec<PartitionKey> = self.holds.keys().cloned().collect();
        for key in keys {
            let Some(hold) = self.holds.remove(&key) else {
                continue;
            };
            if !paused.contains(&key) {
                self.floors.remove(&key);
                note_resumed(&key, &hold, ResumeReason::Rebalance, now);
                continue;
            }
            if let State::Resuming(reason) = hold.state {
                self.resume_hold(control, key, hold, reason, now);
                continue;
            }
            if hold.tables.iter().all(|t| releases(t)) {
                self.resume_hold(control, key, hold, ResumeReason::Resolved, now);
                continue;
            }
            if now.saturating_duration_since(hold.paused_at) >= self.max_pause {
                self.failed.extend(hold.tables.iter().cloned());
                self.resume_hold(control, key, hold, ResumeReason::AgedOut, now);
                continue;
            }
            self.holds.insert(key, hold);
        }
        self.warn_still_paused(now);
    }

    /// Release every hold: a config reload may have removed or moved the
    /// tables they wait on.
    pub fn release_all(&mut self, control: &impl PartitionControl, now: Instant) {
        let holds: Vec<(PartitionKey, Hold)> = self.holds.drain().collect();
        for (key, hold) in holds {
            self.resume_hold(control, key, hold, ResumeReason::Reload, now);
        }
    }

    /// Drop the hold and floor of a partition whose lease ended: its next
    /// owner reads it, and commits it, from the committed offset.
    pub fn forget(&mut self, topic: &str, partition: i32, now: Instant) {
        self.floors
            .retain(|(t, p), _| !(*p == partition && &**t == topic));
        let ended: Vec<PartitionKey> = self
            .holds
            .keys()
            .filter(|(t, p)| *p == partition && &**t == topic)
            .cloned()
            .collect();
        for key in ended {
            if let Some(hold) = self.holds.remove(&key) {
                note_resumed(&key, &hold, ResumeReason::Rebalance, now);
            }
        }
    }

    /// A partition that delivers a record after its seek is read again from
    /// there, and its records in memory hold the commit from then on.
    pub fn received(&mut self, messages: &[KafkaMessage]) {
        if self.floors.is_empty() {
            return;
        }
        let holds = &self.holds;
        self.floors.retain(|key, _| {
            holds.contains_key(key)
                || !messages
                    .iter()
                    .any(|m| m.partition == key.1 && *m.topic == *key.0)
        });
    }

    /// Each sought partition's seek point, which no commit may pass until the
    /// partition delivers again.
    pub fn floors(&self) -> Vec<KafkaOffset> {
        self.floors
            .iter()
            .map(|((topic, partition), offset)| {
                KafkaOffset::with_shared_topic(Arc::clone(topic), *partition, *offset)
            })
            .collect()
    }

    /// A seek point on `topic`/`partition` with no hold, as one stands once
    /// its partition resumed and before it delivers again.
    #[cfg(test)]
    pub(crate) fn with_seek_point(mut self, topic: &str, partition: i32, offset: i64) -> Self {
        self.floors.insert((Arc::from(topic), partition), offset);
        self
    }

    /// Resume `key`, or keep the hold for the next upkeep when the resume is refused.
    fn resume_hold(
        &mut self,
        control: &impl PartitionControl,
        key: PartitionKey,
        mut hold: Hold,
        reason: ResumeReason,
        now: Instant,
    ) {
        match control.resume(&key.0, key.1) {
            Ok(()) => note_resumed(&key, &hold, reason, now),
            Err(e) => {
                warn!(
                    topic = %key.0,
                    partition = key.1,
                    tables = ?hold.tables,
                    reason = reason.as_str(),
                    paused_for_secs = now.saturating_duration_since(hold.paused_at).as_secs_f64(),
                    error = %e,
                    "Partition resume failed, retrying on the next tick"
                );
                hold.state = State::Resuming(reason);
                self.holds.insert(key, hold);
            }
        }
    }

    /// Warn, at most once per [`STILL_PAUSED_WARN`], while any partition is
    /// held past half the bound.
    fn warn_still_paused(&mut self, now: Instant) {
        if self
            .warned_at
            .is_some_and(|at| now.saturating_duration_since(at) < STILL_PAUSED_WARN)
        {
            return;
        }
        let half = self.max_pause / 2;
        let long: Vec<(String, i32, f64)> = self
            .holds
            .iter()
            .filter(|(_, h)| now.saturating_duration_since(h.paused_at) >= half)
            .map(|((topic, partition), h)| {
                (
                    topic.to_string(),
                    *partition,
                    now.saturating_duration_since(h.paused_at).as_secs_f64(),
                )
            })
            .collect();
        if !long.is_empty() {
            warn!(
                partitions = ?long,
                max_pause_secs = self.max_pause.as_secs(),
                "Partitions still paused waiting on table schemas (max 1 per 10s)"
            );
            self.warned_at = Some(now);
        }
    }
}

/// Count a pause and mark the partition paused.
fn note_paused(key: &PartitionKey) {
    metrics::counter!(names::PARTITION_PAUSES_TOTAL).increment(1);
    metrics::gauge!(
        names::PARTITION_PAUSED,
        "topic" => key.0.to_string(),
        "partition" => key.1.to_string()
    )
    .set(1.0);
}

/// Log and count a hold that ended.
fn note_resumed(key: &PartitionKey, hold: &Hold, reason: ResumeReason, now: Instant) {
    let paused_for = now.saturating_duration_since(hold.paused_at);
    info!(
        topic = %key.0,
        partition = key.1,
        tables = ?hold.tables,
        reason = reason.as_str(),
        paused_for_secs = paused_for.as_secs_f64(),
        "Partition resumed"
    );
    metrics::counter!(names::PARTITION_RESUMES_TOTAL, "reason" => reason.as_str()).increment(1);
    metrics::histogram!(names::PARTITION_PAUSED_SECONDS).record(paused_for.as_secs_f64());
    metrics::gauge!(
        names::PARTITION_PAUSED,
        "topic" => key.0.to_string(),
        "partition" => key.1.to_string()
    )
    .set(0.0);
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use scalo::dlq::DlqEntry;
    use scalo::memory::{MemoryGuard, MemoryGuardConfig};
    use scalo::transport::kafka::{PartitionControlError, PartitionOp};

    use super::*;
    use crate::buffer::BufferManager;
    use crate::clickhouse::{SchemaCache, TableSchema};
    use crate::config::{BufferConfig, ComputedColumnsConfig, MetadataConfig};
    use crate::metrics::counting::CountingRecorder;
    use crate::pipeline::capture::CaptureOverrides;
    use crate::pipeline::coordinator::{BatchCoordinator, dead_letter_overflow};
    use crate::pipeline::pending_schema::{OnFull, PendingSchemaBuffer, PendingSchemaConfig};
    use crate::pipeline::types::{AbsentTables, ProcessedMessage};
    use crate::transform::ComputedColumnCache;

    const TOPIC: &str = "events";
    const MAX_PAUSE: Duration = Duration::from_secs(30);

    /// A Kafka topic in memory: each partition a log of table names, read
    /// from a position that pause and seek move as librdkafka's do.
    #[derive(Default)]
    struct FakeKafka {
        log: BTreeMap<i32, Vec<String>>,
        position: RefCell<BTreeMap<i32, i64>>,
        paused: RefCell<BTreeSet<i32>>,
        refuse_pause: Cell<bool>,
        refuse_seek: Cell<bool>,
        refuse_resume: Cell<bool>,
        pauses: Cell<u32>,
    }

    impl FakeKafka {
        fn with(partitions: &[(i32, &str, usize)]) -> Self {
            let mut kafka = Self::default();
            for &(partition, table, records) in partitions {
                kafka
                    .log
                    .entry(partition)
                    .or_default()
                    .extend(std::iter::repeat_n(table.to_string(), records));
            }
            kafka
        }

        /// Up to `max` records, partition by partition, from those not paused.
        fn recv(&self, max: usize) -> Vec<KafkaMessage> {
            let mut out = Vec::new();
            for (&partition, log) in &self.log {
                if self.paused.borrow().contains(&partition) {
                    continue;
                }
                let mut positions = self.position.borrow_mut();
                let at = positions.entry(partition).or_insert(0);
                while out.len() < max && usize::try_from(*at).is_ok_and(|i| i < log.len()) {
                    out.push(record(
                        partition,
                        *at,
                        &log[usize::try_from(*at).unwrap_or(0)],
                    ));
                    *at += 1;
                }
            }
            out
        }

        /// A revoke: librdkafka forgets the pause and the transport drops the hold.
        fn revoke(&self, partition: i32) {
            self.paused.borrow_mut().remove(&partition);
        }

        fn is_paused(&self, partition: i32) -> bool {
            self.paused.borrow().contains(&partition)
        }

        fn refused(op: PartitionOp) -> ControlResult {
            Err(PartitionControlError::Refused {
                op,
                detail: "broker unavailable".into(),
            })
        }
    }

    impl PartitionControl for FakeKafka {
        fn pause(&self, _: &str, partition: i32) -> ControlResult {
            if self.refuse_pause.get() {
                return Self::refused(PartitionOp::Pause);
            }
            self.pauses.set(self.pauses.get() + 1);
            self.paused.borrow_mut().insert(partition);
            Ok(())
        }

        fn resume(&self, _: &str, partition: i32) -> ControlResult {
            if self.refuse_resume.get() {
                return Self::refused(PartitionOp::Resume);
            }
            self.paused.borrow_mut().remove(&partition);
            Ok(())
        }

        fn seek(
            &self,
            _: &str,
            partition: i32,
            offset: i64,
        ) -> impl std::future::Future<Output = ControlResult> {
            if self.refuse_seek.get() {
                return std::future::ready(Self::refused(PartitionOp::Seek));
            }
            self.position.borrow_mut().insert(partition, offset);
            std::future::ready(Ok(()))
        }

        fn paused(&self) -> Vec<PartitionKey> {
            self.paused
                .borrow()
                .iter()
                .map(|&p| (Arc::from(TOPIC), p))
                .collect()
        }
    }

    /// A record for `table`, which its payload names.
    fn record(partition: i32, offset: i64, table: &str) -> KafkaMessage {
        KafkaMessage {
            payload: table.as_bytes().to_vec(),
            topic: Arc::from(TOPIC),
            partition,
            offset,
            key: None,
            timestamp_ms: None,
        }
    }

    fn overflow(table: &str, partition: i32, offset: i64) -> Overflow {
        Overflow {
            table: table.to_string(),
            reason: format!("pending_schema_per_table_overflow table={table}"),
            msg: record(partition, offset, table),
        }
    }

    fn table_schema(table: &str) -> TableSchema {
        let (database, name) = table.split_once('.').unwrap_or(("dfe", table));
        TableSchema {
            database: database.into(),
            table: name.into(),
            columns: vec![],
            comment: String::new(),
        }
    }

    fn held(pauses: &PartitionPauses) -> Vec<i32> {
        let mut held: Vec<i32> = pauses.holds.keys().map(|(_, p)| *p).collect();
        held.sort_unstable();
        held
    }

    fn seek_points(pauses: &PartitionPauses) -> Vec<(i32, i64)> {
        let mut points: Vec<(i32, i64)> = pauses
            .floors()
            .iter()
            .map(|o| (o.partition, o.offset))
            .collect();
        points.sort_unstable();
        points
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime")
            .block_on(future)
    }

    #[test]
    fn a_held_partition_resumes_on_the_tick_with_no_record_arriving() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        let recorder = CountingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let dead = block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
            assert!(dead.is_empty());
            assert!(kafka.is_paused(0));

            // A ClickHouse outage: no resolution succeeds, and no record arrives.
            pauses.upkeep(&kafka, |_| false, t0 + MAX_PAUSE / 2);
            assert_eq!(held(&pauses), vec![0], "within the bound it stays held");
            pauses.upkeep(&kafka, |_| false, t0 + MAX_PAUSE);
        });
        assert!(
            !kafka.is_paused(0),
            "the bound resumed it with nothing arriving"
        );
        assert_eq!(held(&pauses), Vec::<i32>::new());
        assert!(
            pauses.dead_letters("dfe.t"),
            "its overflow now goes to the DLQ"
        );
        assert_eq!(recorder.total(names::PARTITION_PAUSES_TOTAL), 1);
        assert_eq!(
            recorder.labelled(names::PARTITION_RESUMES_TOTAL, "reason", "aged_out"),
            1
        );
        assert_eq!(
            seek_points(&pauses),
            vec![(0, 4)],
            "read again from the seek point"
        );
    }

    #[test]
    fn a_partition_shared_by_tables_resumes_once_every_one_resolves() {
        let kafka = FakeKafka::with(&[(0, "dfe.a", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        let dead = block_on(pauses.hold(
            &kafka,
            vec![overflow("dfe.a", 0, 6), overflow("dfe.b", 0, 3)],
            t0,
        ));
        assert!(dead.is_empty());
        assert_eq!(
            seek_points(&pauses),
            vec![(0, 3)],
            "the first record not kept"
        );

        let resolved = RefCell::new(BTreeSet::from(["dfe.a".to_string()]));
        pauses.upkeep(&kafka, |t| resolved.borrow().contains(t), t0);
        assert!(kafka.is_paused(0), "dfe.b still waits");
        resolved.borrow_mut().insert("dfe.b".into());
        pauses.upkeep(&kafka, |t| resolved.borrow().contains(t), t0);
        assert!(!kafka.is_paused(0));
        assert_eq!(held(&pauses), Vec::<i32>::new(), "no hold leaked");

        // A table's overflow on a partition already held joins its hold, and
        // a lower first record moves the seek point down.
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        block_on(pauses.hold(&kafka, vec![overflow("dfe.a", 0, 6)], t0));
        block_on(pauses.hold(&kafka, vec![overflow("dfe.b", 0, 2)], t0));
        assert_eq!(kafka.pauses.get(), 2, "one pause per hold, not per table");
        assert_eq!(seek_points(&pauses), vec![(0, 2)]);
        pauses.upkeep(&kafka, |t| t == "dfe.a", t0);
        assert_eq!(held(&pauses), vec![0], "dfe.b still holds it");
    }

    #[test]
    fn a_revoke_while_held_drops_the_hold_and_its_seek_point() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10), (1, "dfe.t", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        let recorder = CountingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            block_on(pauses.hold(
                &kafka,
                vec![overflow("dfe.t", 0, 4), overflow("dfe.t", 1, 7)],
                t0,
            ));
            kafka.revoke(0);
            pauses.upkeep(&kafka, |_| false, t0);
            // A lease that ended on the other partition, seen by the purge.
            pauses.forget(TOPIC, 1, t0);
        });
        assert_eq!(held(&pauses), Vec::<i32>::new());
        assert_eq!(
            seek_points(&pauses),
            Vec::<(i32, i64)>::new(),
            "the next owner commits it from the committed offset"
        );
        assert_eq!(
            recorder.labelled(names::PARTITION_RESUMES_TOTAL, "reason", "rebalance"),
            2
        );
    }

    #[test]
    fn a_reload_resumes_every_hold() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10), (1, "dfe.u", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        block_on(pauses.hold(
            &kafka,
            vec![overflow("dfe.t", 0, 1), overflow("dfe.u", 1, 1)],
            t0,
        ));
        pauses.release_all(&kafka, t0);
        assert!(!kafka.is_paused(0) && !kafka.is_paused(1));
        assert_eq!(held(&pauses), Vec::<i32>::new());
        assert_eq!(
            seek_points(&pauses),
            vec![(0, 1), (1, 1)],
            "both read again from their seek points"
        );
    }

    /// The seek is refused, and the resume after it taken or refused once.
    #[test]
    fn a_failed_seek_resumes_at_once_and_dead_letters_the_overflow() {
        for (case, refuse_resume) in [("resume taken", false), ("resume refused once", true)] {
            let kafka = FakeKafka::with(&[(0, "dfe.t", 10)]);
            kafka.refuse_seek.set(true);
            kafka.refuse_resume.set(refuse_resume);
            let mut pauses = PartitionPauses::new(MAX_PAUSE);
            let t0 = Instant::now();
            let dead = block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
            assert_eq!(dead.len(), 1, "{case}: the overflow goes to the DLQ");
            assert_eq!(
                seek_points(&pauses),
                Vec::<(i32, i64)>::new(),
                "{case}: nothing to read again"
            );
            if refuse_resume {
                assert!(kafka.is_paused(0), "{case}");
                assert_eq!(held(&pauses), vec![0], "{case}: the resume is scheduled");
                kafka.refuse_resume.set(false);
                pauses.upkeep(&kafka, |_| false, t0);
            }
            assert!(!kafka.is_paused(0), "{case}: never left paused");
            assert_eq!(held(&pauses), Vec::<i32>::new(), "{case}");
        }
    }

    #[test]
    fn a_refused_pause_dead_letters_and_a_refused_resume_is_retried() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        kafka.refuse_pause.set(true);
        let dead = block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
        assert_eq!(
            dead.len(),
            1,
            "a refused pause hands the overflow to the DLQ"
        );
        assert_eq!(held(&pauses), Vec::<i32>::new(), "and holds nothing");

        kafka.refuse_pause.set(false);
        block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
        kafka.refuse_resume.set(true);
        pauses.upkeep(&kafka, |_| true, t0);
        assert_eq!(held(&pauses), vec![0], "a refused resume keeps the hold");
        pauses.upkeep(&kafka, |_| true, t0 + Duration::from_secs(1));
        assert_eq!(held(&pauses), vec![0], "and tries again each tick");
        kafka.refuse_resume.set(false);
        pauses.upkeep(&kafka, |_| true, t0 + Duration::from_secs(2));
        assert!(!kafka.is_paused(0));
        assert_eq!(held(&pauses), Vec::<i32>::new());
    }

    #[test]
    fn a_seek_point_stands_until_its_partition_delivers_again() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10), (1, "dfe.u", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
        pauses.received(&[record(1, 0, "dfe.u")]);
        assert_eq!(
            seek_points(&pauses),
            vec![(0, 4)],
            "another partition's record"
        );
        pauses.received(&[record(0, 4, "dfe.t")]);
        assert_eq!(seek_points(&pauses), vec![(0, 4)], "still held");
        pauses.upkeep(&kafka, |_| true, t0);
        assert_eq!(seek_points(&pauses), vec![(0, 4)], "resumed, not read yet");
        pauses.received(&[record(0, 4, "dfe.t")]);
        assert_eq!(seek_points(&pauses), Vec::<(i32, i64)>::new());
    }

    /// Fields of every event at `warn` or `info` that names a partition.
    #[derive(Clone, Default)]
    struct PartitionEvents(Arc<std::sync::Mutex<Vec<BTreeMap<String, String>>>>);

    struct Fields(BTreeMap<String, String>);

    impl tracing::field::Visit for Fields {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), format!("{value:?}"));
        }
    }

    impl tracing::Subscriber for PartitionEvents {
        fn register_callsite(
            &self,
            _: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            if fields.0.contains_key("partition") || fields.0.contains_key("partitions") {
                self.0.lock().expect("events lock").push(fields.0);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn each_pause_resume_and_failed_resume_logs_one_line_naming_the_partition() {
        let kafka = FakeKafka::with(&[(0, "dfe.t", 10)]);
        let mut pauses = PartitionPauses::new(MAX_PAUSE);
        let t0 = Instant::now();
        let events = PartitionEvents::default();
        tracing::subscriber::with_default(events.clone(), || {
            block_on(pauses.hold(&kafka, vec![overflow("dfe.t", 0, 4)], t0));
            kafka.refuse_resume.set(true);
            pauses.upkeep(&kafka, |_| true, t0 + MAX_PAUSE / 2);
            kafka.refuse_resume.set(false);
            pauses.upkeep(&kafka, |_| true, t0 + MAX_PAUSE / 2);
        });
        let events = events.0.lock().expect("events lock").clone();
        let messages: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("message").map(String::as_str))
            .collect();
        assert_eq!(
            messages,
            vec![
                "Partition paused until its tables' schemas resolve -- it reads again from the offset",
                "Partition resume failed, retrying on the next tick",
                "Partitions still paused waiting on table schemas (max 1 per 10s)",
                "Partition resumed",
            ],
            "{events:?}"
        );
        for event in [&events[0], &events[1], &events[3]] {
            assert_eq!(
                event.get("topic").map(String::as_str),
                Some(TOPIC),
                "{event:?}"
            );
            assert_eq!(event.get("partition").map(String::as_str), Some("0"));
            assert_eq!(
                event.get("tables").map(String::as_str),
                Some("{\"dfe.t\"}"),
                "{event:?}"
            );
            assert!(event.contains_key("reason"), "{event:?}");
        }
        assert!(events[1].contains_key("paused_for_secs"));
        assert_eq!(
            events[3].get("reason").map(String::as_str),
            Some("resolved")
        );
        assert!(events[3].contains_key("paused_for_secs"));
    }

    /// The pending-schema buffer, a coordinator and the holds over a fake
    /// Kafka topic, as the orchestrator's intake runs them.
    struct Intake {
        kafka: FakeKafka,
        pending: PendingSchemaBuffer,
        pauses: PartitionPauses,
        buffers: BufferManager,
        capture: CaptureOverrides,
        computed: ComputedColumnCache,
        guard: MemoryGuard,
        schemas: SchemaCache,
        absent: AbsentTables,
        dead: Vec<DlqEntry>,
    }

    impl Intake {
        fn new(kafka: FakeKafka, max_per_table: usize, max_total: usize) -> Self {
            Self {
                kafka,
                pending: PendingSchemaBuffer::new(PendingSchemaConfig {
                    max_per_table,
                    max_total,
                    max_age: MAX_PAUSE,
                    on_full: OnFull::Refuse,
                }),
                pauses: PartitionPauses::new(MAX_PAUSE),
                buffers: BufferManager::new(&BufferConfig::default()),
                capture: CaptureOverrides::new(&MetadataConfig::default()),
                computed: ComputedColumnCache::new(ComputedColumnsConfig::default()),
                guard: MemoryGuard::new(MemoryGuardConfig {
                    limit_bytes: 64 * 1024 * 1024,
                    ..Default::default()
                }),
                schemas: SchemaCache::new(300),
                absent: AbsentTables::new(Duration::from_secs(60), 16),
                dead: Vec::new(),
            }
        }

        /// A schema answer for `table`: found, or the fetch failed.
        fn answer(&mut self, table: &str, found: bool) {
            self.pending.record_answer(table);
            if found {
                self.schemas.insert(table.into(), table_schema(table));
                self.pauses.resolved(table);
            }
        }

        /// One intake turn at `now`, then the upkeep tick. The age cap on
        /// buffered records reads the real clock, so it is left out.
        async fn turn(&mut self, now: Instant) {
            let fresh = self.kafka.recv(500);
            self.pauses.received(&fresh);
            let mut batch = self.pending.take_ready(&self.schemas, &self.absent);
            batch.extend(fresh);
            if !batch.is_empty() {
                let results: Vec<crate::Result<ProcessedMessage>> = batch
                    .iter()
                    .map(|msg| {
                        let table = String::from_utf8_lossy(&msg.payload).into_owned();
                        if self.schemas.get(&table).is_some() {
                            Ok(ProcessedMessage {
                                table,
                                data: serde_json::Map::new(),
                                raw_payload: None,
                                kafka_offset: KafkaOffset::with_shared_topic(
                                    Arc::clone(&msg.topic),
                                    msg.partition,
                                    msg.offset,
                                ),
                                fell_back_from: None,
                            })
                        } else {
                            Err(crate::Error::SchemaPending { table })
                        }
                    })
                    .collect();
                let mut fmc: Option<crate::transform::FieldMappingCache> = None;
                let outcome = BatchCoordinator {
                    buffer_manager: &mut self.buffers,
                    capture_overrides: &mut self.capture,
                    field_mapping_cache: &mut fmc,
                    computed_column_cache: &mut self.computed,
                    metrics: &None,
                    memory_guard: &self.guard,
                    pending_schema: &mut self.pending,
                    absent_tables: &self.absent,
                    pauses: Some(&self.pauses),
                }
                .apply_results(results, &batch);
                self.dead.extend(outcome.dead_letters);
                let refused = self.pauses.hold(&self.kafka, outcome.overflow, now).await;
                self.dead.extend(
                    refused
                        .iter()
                        .map(|o| dead_letter_overflow(&o.msg, &o.table, o.reason.clone(), &None)),
                );
            }
            let (pending, schemas, absent) = (&self.pending, &self.schemas, &self.absent);
            self.pauses
                .upkeep(&self.kafka, |t| pending.releases(t, schemas, absent), now);
        }

        /// How many times each `(partition, offset)` landed in a buffer.
        fn landed(&mut self) -> BTreeMap<(i32, i64), u32> {
            let mut landed = BTreeMap::new();
            for batch in self.buffers.flush_all() {
                for offset in batch.offsets {
                    *landed.entry((offset.partition, offset.offset)).or_insert(0) += 1;
                }
            }
            landed
        }
    }

    /// Issue #362: a burst over the cap before the table's schema resolves,
    /// its first answer a failed fetch, lands whole with nothing in the DLQ.
    #[tokio::test]
    async fn a_burst_over_the_cap_before_its_schema_resolves_lands_with_no_dead_letter() {
        // (case, per-table cap, total cap, first answer is a failed fetch)
        let cases = [
            ("per-table cap after a failed first fetch", 100, 1_000, true),
            ("total cap before any answer", 1_000, 300, false),
        ];
        for (case, max_per_table, max_total, failed_first) in cases {
            let kafka = FakeKafka::with(&[(0, "dfe.burst", 600), (1, "dfe.live", 200)]);
            let mut intake = Intake::new(kafka, max_per_table, max_total);
            intake.answer("dfe.live", true);
            let t0 = Instant::now();
            intake.turn(t0).await;
            if failed_first {
                intake.answer("dfe.burst", false);
            }
            for step in 1..4 {
                intake.turn(t0 + Duration::from_secs(step)).await;
            }
            assert!(
                intake.kafka.is_paused(0),
                "{case}: the burst's partition waits"
            );
            assert!(!intake.kafka.is_paused(1), "{case}: the other flows");
            intake.answer("dfe.burst", true);
            for step in 4..8 {
                intake.turn(t0 + Duration::from_secs(step)).await;
            }
            assert!(
                intake.dead.is_empty(),
                "{case}: {} dead letters",
                intake.dead.len()
            );
            let landed = intake.landed();
            let missing: Vec<(i32, i64)> = (0..600)
                .map(|o| (0, o))
                .chain((0..200).map(|o| (1, o)))
                .filter(|k| !landed.contains_key(k))
                .collect();
            assert_eq!(missing, Vec::<(i32, i64)>::new(), "{case}: lost or skipped");
            assert!(!intake.kafka.is_paused(0), "{case}");
            assert_eq!(
                seek_points(&intake.pauses),
                Vec::<(i32, i64)>::new(),
                "{case}"
            );
        }
    }

    /// A schema that never arrives: the partition pauses once, ages out, and
    /// its re-read overflow goes to the DLQ rather than pausing it again,
    /// until a fetch succeeds and the next overflow pauses it again.
    #[tokio::test]
    async fn after_an_age_out_the_overflow_is_dead_lettered_until_a_fetch_succeeds() {
        let kafka = FakeKafka::with(&[(0, "dfe.stuck", 1_000)]);
        let mut intake = Intake::new(kafka, 100, 1_000);
        let t0 = Instant::now();
        intake.turn(t0).await;
        intake.answer("dfe.stuck", false);
        intake.turn(t0 + Duration::from_secs(1)).await;
        assert!(intake.kafka.is_paused(0));
        assert_eq!(intake.kafka.pauses.get(), 1);

        let aged = t0 + MAX_PAUSE + Duration::from_secs(1);
        intake.turn(aged).await;
        assert!(!intake.kafka.is_paused(0), "aged out, it is read on");
        intake.turn(aged + Duration::from_secs(1)).await;
        assert_eq!(intake.kafka.pauses.get(), 1, "no second pause: no livelock");
        assert_eq!(
            intake
                .dead
                .iter()
                .filter(|e| e.reason.contains("pending_schema_per_table_overflow"))
                .count(),
            500,
            "the overflow read again went to the DLQ"
        );

        intake.answer("dfe.stuck", true);
        assert!(
            !intake.pauses.dead_letters("dfe.stuck"),
            "a fetch succeeded"
        );
        intake.schemas.invalidate("dfe.stuck");
        intake.kafka.position.borrow_mut().insert(0, 0);
        intake.turn(aged + Duration::from_secs(2)).await;
        assert_eq!(intake.kafka.pauses.get(), 2, "its overflow pauses again");
    }
}
