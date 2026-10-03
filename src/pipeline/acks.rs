// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Push answers held until every row built from a record is placed.
//!
//! One received record can become several rows, since a batched message splits
//! into one row per element, and each row settles on its own: inserted, dead
//! lettered, dropped or failed. A record is released once its last row settles,
//! with the worst outcome among them, so a sender hears its Push landed only
//! once all of it has.

use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;
use scalo::transport::DeliveryStatus;

/// Rows of one record not yet settled, and the worst outcome so far.
struct Open {
    rows: u32,
    worst: DeliveryStatus,
}

/// Records whose answer waits on their rows, keyed by sequence number.
#[derive(Default)]
pub(crate) struct AckLedger {
    open: FxHashMap<u64, Open>,
    settled: Vec<(u64, DeliveryStatus)>,
}

impl AckLedger {
    /// One more row built from record `seq`.
    pub(crate) fn admit(&mut self, seq: u64) {
        let open = self.open.entry(seq).or_insert(Open {
            rows: 0,
            worst: DeliveryStatus::Delivered,
        });
        open.rows += 1;
    }

    /// One row of record `seq` settled with `status`. A sequence number never
    /// admitted is ignored.
    pub(crate) fn settle(&mut self, seq: u64, status: DeliveryStatus) {
        let Entry::Occupied(mut entry) = self.open.entry(seq) else {
            return;
        };
        let open = entry.get_mut();
        open.worst = open.worst.max(status);
        open.rows -= 1;
        if open.rows == 0 {
            let worst = entry.remove().worst;
            self.settled.push((seq, worst));
        }
    }

    /// Records whose last row has settled since the last call, grouped by the
    /// outcome each is released with.
    pub(crate) fn take_settled(&mut self) -> Vec<(DeliveryStatus, Vec<u64>)> {
        let mut groups: Vec<(DeliveryStatus, Vec<u64>)> = Vec::new();
        for (seq, status) in self.settled.drain(..) {
            match groups.iter_mut().find(|(s, _)| *s == status) {
                Some((_, seqs)) => seqs.push(seq),
                None => groups.push((status, vec![seq])),
            }
        }
        groups
    }

    /// Records with a row not yet settled.
    pub(crate) fn open(&self) -> usize {
        self.open.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settled(ledger: &mut AckLedger) -> Vec<(DeliveryStatus, Vec<u64>)> {
        let mut groups = ledger.take_settled();
        for (_, seqs) in &mut groups {
            seqs.sort_unstable();
        }
        groups.sort_unstable_by_key(|(status, _)| *status);
        groups
    }

    #[test]
    fn a_record_is_released_only_after_its_last_row() {
        let mut ledger = AckLedger::default();
        ledger.admit(7);
        ledger.admit(7);
        ledger.admit(7);

        ledger.settle(7, DeliveryStatus::Delivered);
        ledger.settle(7, DeliveryStatus::Delivered);
        assert!(settled(&mut ledger).is_empty(), "one row is still out");
        assert_eq!(ledger.open(), 1);

        ledger.settle(7, DeliveryStatus::Delivered);
        assert_eq!(
            settled(&mut ledger),
            vec![(DeliveryStatus::Delivered, vec![7])]
        );
        assert_eq!(ledger.open(), 0);
    }

    #[test]
    fn the_worst_row_decides_the_answer() {
        let mut ledger = AckLedger::default();
        for _ in 0..3 {
            ledger.admit(1);
        }
        ledger.settle(1, DeliveryStatus::Errored);
        ledger.settle(1, DeliveryStatus::Delivered);
        ledger.settle(1, DeliveryStatus::Rejected);
        assert_eq!(
            settled(&mut ledger),
            vec![(DeliveryStatus::Errored, vec![1])],
            "one failed row makes the sender retry the whole record"
        );
    }

    #[test]
    fn records_are_grouped_by_outcome() {
        let mut ledger = AckLedger::default();
        for seq in 0..5 {
            ledger.admit(seq);
        }
        ledger.settle(0, DeliveryStatus::Delivered);
        ledger.settle(1, DeliveryStatus::Errored);
        ledger.settle(2, DeliveryStatus::Delivered);
        ledger.settle(3, DeliveryStatus::Dropped);
        ledger.settle(4, DeliveryStatus::Rejected);
        assert_eq!(
            settled(&mut ledger),
            vec![
                (DeliveryStatus::Delivered, vec![0, 2]),
                (DeliveryStatus::Dropped, vec![3]),
                (DeliveryStatus::Rejected, vec![4]),
                (DeliveryStatus::Errored, vec![1]),
            ]
        );
        assert!(
            ledger.take_settled().is_empty(),
            "each record is released once"
        );
    }

    #[test]
    fn a_seq_never_admitted_or_already_released_is_ignored() {
        let mut ledger = AckLedger::default();
        ledger.settle(9, DeliveryStatus::Errored);
        assert_eq!(settled(&mut ledger), [] as [(DeliveryStatus, Vec<u64>); 0]);

        ledger.admit(3);
        ledger.settle(3, DeliveryStatus::Delivered);
        ledger.settle(3, DeliveryStatus::Errored);
        assert_eq!(
            settled(&mut ledger),
            vec![(DeliveryStatus::Delivered, vec![3])],
            "a late settle cannot turn a released record into a failure"
        );
    }
}
