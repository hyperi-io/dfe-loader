// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Rows the loader could not place yet, held until something takes them.
//!
//! Two kinds wait here: batches whose insert failed, which go back to
//! `ClickHouse`, and dead letters `ClickHouse` will never take, which go back to
//! the DLQ. Both are retried on scalo's jittered exponential schedule until they
//! land. A Kafka consumer has already read past a held row and never reads it
//! again, so its offset keeps the commit below it until it lands.
//!
//! A gRPC Push with `grpc.acknowledgements.enabled` (the default) is answered
//! only once its rows land, so a row that fails is released for its sender to
//! retry and never held here. With acknowledgements off a Push is answered once
//! its record is queued, so a held row exists nowhere else and intake pauses
//! while anything is held.

use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use rustc_hash::FxHashMap;
use scalo::dlq::DlqEntry;
use scalo::sink_stack::SinkStackConfig;
use tokio::time::Instant;

use crate::buffer::{FlushBatch, KafkaOffset};

/// The delay schedule `ExponentialBuilder` produces.
type Schedule = <ExponentialBuilder as BackoffBuilder>::Backoff;

/// Held rows and the schedule their next attempt runs on.
pub(crate) struct Unsettled {
    batches: Vec<FlushBatch>,
    dead_letters: Vec<DlqEntry>,
    /// The lowest offset per partition of rows this process no longer holds,
    /// which only a re-read from Kafka after a restart can place.
    awaiting_reread: Vec<KafkaOffset>,
    builder: ExponentialBuilder,
    schedule: Schedule,
    retry_at: Option<Instant>,
}

/// Everything taken off the hold for one more attempt.
pub(crate) struct Held {
    /// Batches to insert into `ClickHouse` again.
    pub(crate) batches: Vec<FlushBatch>,
    /// Rows `ClickHouse` rejected for good, to hand to the DLQ again.
    pub(crate) dead_letters: Vec<DlqEntry>,
}

impl Held {
    /// Payload bytes across the held dead letters.
    pub(crate) fn dead_letter_bytes(&self) -> u64 {
        payload_bytes(&self.dead_letters)
    }
}

/// Payload bytes across `dead_letters`.
pub(crate) fn payload_bytes(dead_letters: &[DlqEntry]) -> u64 {
    dead_letters.iter().map(|e| e.payload.len() as u64).sum()
}

/// The Kafka offset a dead letter came from; an inbound-filter reject names none.
fn kafka_source(entry: &DlqEntry) -> Option<KafkaOffset> {
    let source = entry.source.as_ref()?;
    Some(KafkaOffset::new(
        source.topic.as_deref()?,
        source.partition?,
        source.offset?,
    ))
}

/// The lowest of `offsets` on each topic partition.
fn lowest_per_partition<'a>(
    offsets: impl IntoIterator<Item = &'a KafkaOffset>,
) -> Vec<KafkaOffset> {
    let mut lowest: FxHashMap<(&str, i32), &KafkaOffset> = FxHashMap::default();
    for off in offsets {
        lowest
            .entry((&*off.topic, off.partition))
            .and_modify(|held| {
                if off.offset < held.offset {
                    *held = off;
                }
            })
            .or_insert(off);
    }
    lowest.into_values().cloned().collect()
}

impl Unsettled {
    /// An empty hold whose attempts are never further apart than `max_delay`
    /// before jitter.
    pub(crate) fn new(max_delay: Duration) -> Self {
        let config = SinkStackConfig {
            max_backoff_ms: u64::try_from(max_delay.as_millis()).unwrap_or(u64::MAX),
            ..SinkStackConfig::default()
        };
        // A held row has no other copy, so the schedule never runs out.
        let builder = config.backoff().without_max_times();
        Self {
            batches: Vec::new(),
            dead_letters: Vec::new(),
            awaiting_reread: Vec::new(),
            builder,
            schedule: builder.build(),
            retry_at: None,
        }
    }

    /// Hold a batch whose insert failed.
    pub(crate) fn hold(&mut self, batch: FlushBatch) {
        self.batches.push(batch);
    }

    /// Hold dead letters the DLQ did not prove written.
    pub(crate) fn hold_dead_letters(&mut self, dead_letters: Vec<DlqEntry>) {
        self.dead_letters.extend(dead_letters);
    }

    /// Keep the commit below `offsets` for as long as this process runs: their
    /// rows are gone from memory, so only Kafka still has them.
    pub(crate) fn await_reread(&mut self, offsets: &[KafkaOffset]) {
        self.awaiting_reread = lowest_per_partition(self.awaiting_reread.iter().chain(offsets));
    }

    /// Whether nothing is held for another attempt, which is when intake may
    /// run. Rows awaiting a re-read have nothing to attempt.
    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty() && self.dead_letters.is_empty()
    }

    /// Whether dead letters are held, so the DLQ refused the last attempt.
    pub(crate) fn holds_dead_letters(&self) -> bool {
        !self.dead_letters.is_empty()
    }

    /// The lowest Kafka offset held on each partition, across held batches,
    /// held dead letters and rows awaiting a re-read: a commit must stop below
    /// each until its row lands.
    pub(crate) fn offsets(&self) -> Vec<KafkaOffset> {
        let dead_letters: Vec<KafkaOffset> =
            self.dead_letters.iter().filter_map(kafka_source).collect();
        lowest_per_partition(
            self.batches
                .iter()
                .flat_map(|batch| &batch.offsets)
                .chain(&dead_letters)
                .chain(&self.awaiting_reread),
        )
    }

    /// Rows across every held batch and dead letter.
    pub(crate) fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.rows.len()).sum::<usize>() + self.dead_letters.len()
    }

    /// Take everything held for another attempt.
    pub(crate) fn take(&mut self) -> Held {
        self.retry_at = None;
        Held {
            batches: std::mem::take(&mut self.batches),
            dead_letters: std::mem::take(&mut self.dead_letters),
        }
    }

    /// Schedule the next attempt while anything is held; start the schedule
    /// over once nothing is.
    pub(crate) fn rearm(&mut self) {
        if self.is_empty() {
            self.retry_at = None;
            self.schedule = self.builder.build();
        } else if self.retry_at.is_none() {
            // `without_max_times` makes the schedule endless; the fallback is
            // unreachable and only keeps the hold from spinning if it were not.
            let delay = self.schedule.next().unwrap_or(Duration::from_secs(1));
            self.retry_at = Some(Instant::now() + delay);
        }
    }

    /// Resolves when the next attempt is due, and never while nothing is held.
    pub(crate) async fn due(&self) {
        match self.retry_at {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use compact_str::CompactString;

    use super::*;

    fn batch(rows: usize) -> FlushBatch {
        FlushBatch {
            table: CompactString::from("default.t"),
            rows: vec![serde_json::Map::new(); rows],
            offsets: Vec::new(),
            raw_payloads: vec![Arc::from(&b"{}"[..]); rows],
        }
    }

    fn dead_letter(payload: &[u8]) -> DlqEntry {
        DlqEntry::new("loader", "clickhouse_permanent_reject", payload.to_vec())
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_held_is_never_due() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.rearm();
        let due = tokio::time::timeout(Duration::from_secs(3600), held.due()).await;
        assert!(due.is_err(), "an empty hold asked for an insert attempt");
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_batch_comes_due_within_the_jittered_cap() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.hold(batch(3));
        held.rearm();
        // Jitter adds up to the delay again, so the first attempt lands inside
        // twice the schedule's first step.
        tokio::time::timeout(Duration::from_millis(200), held.due())
            .await
            .expect("the first attempt was not due within twice the minimum delay");
        assert_eq!(held.rows(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn attempts_back_off_but_never_past_twice_the_cap() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        let mut previous = Duration::ZERO;
        let mut grew = false;
        for _ in 0..20 {
            held.hold(batch(1));
            held.rearm();
            let start = Instant::now();
            held.due().await;
            let waited = start.elapsed();
            assert!(waited <= Duration::from_secs(10), "waited {waited:?}");
            grew |= waited > previous;
            previous = waited;
            let _ = held.take();
        }
        assert!(grew, "the delay never grew");
    }

    #[test]
    fn taking_empties_the_hold_and_an_empty_rearm_restarts_the_schedule() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.hold(batch(2));
        held.hold(batch(4));
        assert_eq!(held.rows(), 6);
        assert_eq!(held.take().batches.len(), 2);
        assert!(held.is_empty());
        held.rearm();
        assert!(held.retry_at.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_dead_letter_pauses_intake_and_comes_due() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.hold_dead_letters(vec![dead_letter(b"{\"id\":1}"), dead_letter(b"{\"id\":2}")]);
        assert!(
            !held.is_empty(),
            "a dead letter the DLQ refused left intake open"
        );
        assert_eq!(held.rows(), 2);
        held.rearm();
        tokio::time::timeout(Duration::from_millis(200), held.due())
            .await
            .expect("a held dead letter was never retried");
    }

    /// `(topic, partition, offset)` of each floor the hold names, sorted.
    fn floors(held: &Unsettled) -> Vec<(String, i32, i64)> {
        let mut out: Vec<_> = held
            .offsets()
            .into_iter()
            .map(|o| (o.topic.to_string(), o.partition, o.offset))
            .collect();
        out.sort_unstable();
        out
    }

    fn batch_at(topic: &str, partition: i32, offsets: std::ops::Range<i64>) -> FlushBatch {
        let rows = offsets.clone().count();
        FlushBatch {
            offsets: offsets
                .map(|off| KafkaOffset::new(topic, partition, off))
                .collect(),
            ..batch(rows)
        }
    }

    #[test]
    fn a_held_dead_letter_from_kafka_names_the_offset_a_commit_stops_below() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        assert!(!held.holds_dead_letters());
        held.hold_dead_letters(vec![
            dead_letter(b"{}").with_source(scalo::dlq::DlqSource::kafka("t", 2, 41)),
            // An inbound-filter reject carries no source and names no offset.
            dead_letter(b"{}"),
        ]);
        assert!(held.holds_dead_letters());
        assert_eq!(floors(&held), vec![("t".to_string(), 2, 41)]);

        let _ = held.take();
        assert!(held.offsets().is_empty());
        assert!(!held.holds_dead_letters());
    }

    #[test]
    fn a_held_batch_names_its_lowest_offset_on_each_partition() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.hold(batch_at("t", 0, 107..112));
        held.hold(batch_at("t", 0, 100..105));
        held.hold(batch_at("t", 1, 9..12));
        held.hold_dead_letters(vec![
            dead_letter(b"{}").with_source(scalo::dlq::DlqSource::kafka("t", 1, 4)),
        ]);
        assert_eq!(
            floors(&held),
            vec![("t".to_string(), 0, 100), ("t".to_string(), 1, 4)]
        );

        // Taken for another attempt, a batch no longer bounds the commit: the
        // attempt either lands it or holds it again.
        let taken = held.take();
        assert_eq!(taken.batches.len(), 3);
        assert!(held.offsets().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn rows_awaiting_a_reread_hold_the_floor_and_are_never_due() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        let lost = [
            KafkaOffset::new("t", 0, 30),
            KafkaOffset::new("t", 0, 20),
            KafkaOffset::new("t", 3, 8),
        ];
        held.await_reread(&lost);
        held.await_reread(&[KafkaOffset::new("t", 0, 25)]);
        assert_eq!(
            floors(&held),
            vec![("t".to_string(), 0, 20), ("t".to_string(), 3, 8)]
        );

        // Nothing is left to attempt, so intake stays open and no retry fires.
        assert!(held.is_empty());
        held.rearm();
        let due = tokio::time::timeout(Duration::from_secs(3600), held.due()).await;
        assert!(
            due.is_err(),
            "rows with nothing to retry asked for an attempt"
        );

        // Only a restart re-reads them, so taking the hold leaves them in place.
        let _ = held.take();
        assert_eq!(floors(&held).len(), 2);
    }

    #[test]
    fn taking_hands_back_batches_and_dead_letters_together() {
        let mut held = Unsettled::new(Duration::from_secs(5));
        held.hold(batch(3));
        held.hold_dead_letters(vec![dead_letter(b"12345"), dead_letter(b"678")]);
        assert_eq!(held.rows(), 5);

        let taken = held.take();
        assert_eq!(taken.batches.len(), 1);
        assert_eq!(taken.dead_letters.len(), 2);
        assert_eq!(taken.dead_letter_bytes(), 8);
        assert!(held.is_empty());
        assert_eq!(held.rows(), 0);
    }
}
