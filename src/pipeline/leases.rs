// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The Kafka partition lease each partition's buffered rows were read under.
//!
//! A rebalance that moves a partition to another member ends this consumer's
//! lease on it, and the new owner reads the partition again from the committed
//! offset. A row read under the ended lease is then a duplicate of one the new
//! owner writes, so it is discarded before any write rather than written.

use std::sync::Arc;

use rustc_hash::FxHashMap;

/// The lease each partition's buffered rows were read under.
///
/// Generic so tests can stand in for `PartitionLease`, which only a live
/// consumer hands out.
pub(crate) struct Leases<L> {
    held: FxHashMap<(Arc<str>, i32), L>,
}

impl<L> Default for Leases<L> {
    fn default() -> Self {
        Self {
            held: FxHashMap::default(),
        }
    }
}

impl<L: Copy + Eq> Leases<L> {
    /// Record the lease `topic`/`partition` has as a batch of its records
    /// arrives, `None` where this consumer no longer holds it.
    ///
    /// Returns whether rows the partition buffered under an earlier lease must
    /// go: that lease has ended, so the partition's next owner reads them again.
    pub(crate) fn observe(&mut self, topic: &Arc<str>, partition: i32, lease: Option<L>) -> bool {
        let key = (Arc::clone(topic), partition);
        match lease {
            Some(lease) => self
                .held
                .insert(key, lease)
                .is_some_and(|before| before != lease),
            None => self.held.remove(&key).is_some(),
        }
    }

    /// Take out every partition whose lease `holds` no longer accepts.
    pub(crate) fn take_ended(
        &mut self,
        holds: impl Fn(&str, i32, L) -> bool,
    ) -> Vec<(Arc<str>, i32)> {
        let mut ended = Vec::new();
        self.held.retain(|(topic, partition), lease| {
            let stands = holds(topic, *partition, *lease);
            if !stands {
                ended.push((Arc::clone(topic), *partition));
            }
            stands
        });
        ended
    }

    /// Whether no partition's lease is recorded.
    pub(crate) fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> Arc<str> {
        Arc::from("events_land")
    }

    #[test]
    fn a_first_lease_purges_nothing() {
        let mut leases = Leases::default();
        assert!(!leases.observe(&topic(), 0, Some(7_u32)));
        assert!(!leases.is_empty());
    }

    #[test]
    fn the_same_lease_again_purges_nothing() {
        let mut leases = Leases::default();
        leases.observe(&topic(), 0, Some(7_u32));
        assert!(!leases.observe(&topic(), 0, Some(7)));
    }

    #[test]
    fn a_new_lease_on_the_partition_purges_what_the_old_one_read() {
        // An eager rebalance revokes the partition and hands it straight back.
        let mut leases = Leases::default();
        leases.observe(&topic(), 0, Some(7_u32));
        assert!(leases.observe(&topic(), 0, Some(8)));
        assert!(
            !leases.observe(&topic(), 0, Some(8)),
            "the new lease is now the one recorded"
        );
    }

    #[test]
    fn a_partition_no_longer_held_purges_and_forgets_its_lease() {
        let mut leases = Leases::default();
        leases.observe(&topic(), 0, Some(7_u32));
        assert!(leases.observe(&topic(), 0, None));
        assert!(leases.is_empty());
        assert!(
            !leases.observe(&topic(), 0, None),
            "nothing was read under a lease to purge"
        );
    }

    #[test]
    fn a_lease_on_another_partition_or_topic_leaves_this_one_alone() {
        let mut leases = Leases::default();
        leases.observe(&topic(), 0, Some(7_u32));
        assert!(!leases.observe(&topic(), 1, Some(9)));
        assert!(!leases.observe(&Arc::from("other_land"), 0, Some(9)));
        assert!(!leases.observe(&topic(), 0, Some(7)));
    }

    #[test]
    fn take_ended_takes_only_the_partitions_holds_rejects() {
        let mut leases = Leases::default();
        leases.observe(&topic(), 0, Some(7_u32));
        leases.observe(&topic(), 1, Some(7));
        leases.observe(&topic(), 2, Some(9));

        let mut ended = leases.take_ended(|_, partition, lease| partition != 1 && lease == 7);
        ended.sort_unstable_by_key(|(_, partition)| *partition);
        let ended: Vec<i32> = ended.into_iter().map(|(_, partition)| partition).collect();
        assert_eq!(ended, [1, 2]);

        assert_eq!(
            leases.take_ended(|_, _, _| false).len(),
            1,
            "only the lease that stood is still recorded"
        );
        assert!(leases.is_empty());
    }
}
