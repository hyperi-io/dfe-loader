// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Sequential batch coordinator.
//!
//! Applies results from the parallel processing phase to mutable state:
//! buffer push, mark_pending, stats, DLQ routing. Called after
//! `super::processor::MessageProcessor` completes and its borrows are released.

use tokio::sync::mpsc;
use tracing::{debug, warn};

use scalo::dlq::{DlqEntry, DlqSource};
use scalo::memory::MemoryGuard;

use crate::buffer::BufferManager;
use crate::kafka::KafkaMessage;
use crate::metrics::Metrics;
use crate::transform::{ComputedColumnCache, FieldMappingCache};

use super::capture::CaptureOverrides;
use super::pending_schema::{EnqueueOutcome, PendingOverflow, PendingSchemaBuffer};
use super::types::ProcessedMessage;

/// Counters returned from `apply_results` for the orchestrator to update stats.
#[derive(Debug, Default)]
pub struct BatchOutcome {
    pub processed: u64,
    pub errors: u64,
    pub dlq: u64,
    /// Messages routed into the pending-schema buffer (awaiting resolution).
    pub pending: u64,
    /// Tables seen for the first time that need a resolution request kicked
    /// off by the orchestrator.
    pub needs_resolution: Vec<String>,
}

/// Applies parallel processing results to mutable state.
///
/// Created per-batch in the orchestrator event loop. Holds `&mut` references
/// to caches, buffers, and stats. The borrow checker ensures this is only
/// constructed after the parallel phase completes (processor dropped).
pub(crate) struct BatchCoordinator<'a> {
    pub buffer_manager: &'a mut BufferManager,
    pub capture_overrides: &'a mut CaptureOverrides,
    pub field_mapping_cache: &'a mut Option<FieldMappingCache>,
    pub computed_column_cache: &'a mut ComputedColumnCache,
    pub metrics: &'a Option<Metrics>,
    pub dlq_tx: &'a mpsc::Sender<DlqEntry>,
    pub dlq_enabled: bool,
    pub memory_guard: &'a MemoryGuard,
    pub pending_schema: &'a mut PendingSchemaBuffer,
}

impl BatchCoordinator<'_> {
    /// Apply parallel processing results sequentially.
    ///
    /// For each `Ok(processed)`: ensure cache entry, mark_pending, buffer push.
    /// For each `Err(e)`: DLQ routing, memory release.
    ///
    /// Returns counters for the orchestrator to update its own stats.
    pub fn apply_results(
        &mut self,
        results: Vec<crate::Result<ProcessedMessage>>,
        messages: &[KafkaMessage],
    ) -> BatchOutcome {
        self.apply_results_inner(results, messages.iter())
    }

    /// Apply results when the batch was pre-route filtered (references to original messages).
    pub fn apply_results_refs(
        &mut self,
        results: Vec<crate::Result<ProcessedMessage>>,
        messages: &[&KafkaMessage],
    ) -> BatchOutcome {
        self.apply_results_inner(results, messages.iter().copied())
    }

    fn apply_results_inner<'a>(
        &mut self,
        results: Vec<crate::Result<ProcessedMessage>>,
        messages: impl Iterator<Item = &'a KafkaMessage>,
    ) -> BatchOutcome {
        let mut outcome = BatchOutcome::default();

        for (msg, result) in messages.zip(results) {
            match result {
                Ok(processed) => {
                    // mark_pending BEFORE ensure_cached — new tables must be queued
                    // for DDL tag resolution before their config-list defaults are cached.
                    // Reversing this order would silently break DDL tag application.
                    self.capture_overrides.mark_pending(&processed.table);
                    self.capture_overrides.ensure_cached(&processed.table);
                    if let Some(fm) = self.field_mapping_cache.as_mut() {
                        fm.mark_pending(&processed.table);
                    }
                    self.computed_column_cache.mark_pending(&processed.table);

                    // Buffer push (sequential — needs &mut BufferManager)
                    self.buffer_manager.push(
                        &processed.table,
                        processed.data,
                        Some(processed.kafka_offset),
                        processed.raw_payload,
                    );

                    outcome.processed += 1;
                    if let Some(m) = self.metrics {
                        m.record_processed(&processed.table);
                    }
                }
                Err(crate::Error::SchemaPending { table }) => {
                    match self
                        .pending_schema
                        .enqueue(table.clone(), msg.clone_for_pending())
                    {
                        Ok(EnqueueOutcome::Enqueued) => {
                            outcome.pending += 1;
                        }
                        Ok(EnqueueOutcome::NeedsResolution) => {
                            outcome.pending += 1;
                            outcome.needs_resolution.push(table);
                        }
                        Err(PendingOverflow::PerTable(t)) => {
                            // Per-table cap hit — this message overflows to DLQ.
                            // Memory is released (it won't be flushed). Other
                            // buffered messages for the table stay put.
                            outcome.dlq += 1;
                            if let Some(m) = self.metrics {
                                m.record_dlq();
                                m.record_pending_schema_overflow();
                            }
                            if self.dlq_enabled {
                                let entry =
                                    DlqEntry::new(
                                        "loader",
                                        format!("pending_schema_per_table_overflow table={t}"),
                                        msg.payload.clone(),
                                    )
                                    .with_source(
                                        DlqSource::kafka(&*msg.topic, msg.partition, msg.offset),
                                    );
                                let _ = self.dlq_tx.try_send(entry);
                            }
                            scalo::logger::security::record_dlq(
                                "pending_schema_overflow",
                                &format!("per-table cap exceeded for {t}"),
                                None,
                            );
                            self.memory_guard.release(msg.payload.len() as u64);
                        }
                    }
                }
                Err(e) => {
                    outcome.errors += 1;
                    if let Some(m) = self.metrics {
                        m.record_dlq();
                    }

                    if self.dlq_enabled {
                        let entry = DlqEntry::new("loader", e.to_string(), msg.payload.clone())
                            .with_source(DlqSource::kafka(&*msg.topic, msg.partition, msg.offset));

                        match self.dlq_tx.try_send(entry) {
                            Ok(()) => {
                                outcome.dlq += 1;
                                scalo::logger::security::record_dlq(
                                    "processing",
                                    &e.to_string(),
                                    Some(&format!(
                                        "topic: {}, partition: {}, offset: {}",
                                        msg.topic, msg.partition, msg.offset
                                    )),
                                );
                                debug!(error = %e, "Message queued for DLQ");
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                static DLQ_FULL_TS: std::sync::atomic::AtomicU64 =
                                    std::sync::atomic::AtomicU64::new(0);
                                if scalo::logger::log_debounced(&DLQ_FULL_TS, 5000) {
                                    warn!(error = %e, "DLQ channel full, messages dropped (max 1 per 5s)");
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                static DLQ_CLOSED_TS: std::sync::atomic::AtomicU64 =
                                    std::sync::atomic::AtomicU64::new(0);
                                if scalo::logger::log_debounced(&DLQ_CLOSED_TS, 5000) {
                                    warn!(error = %e, "DLQ channel closed (max 1 per 5s)");
                                }
                            }
                        }
                    } else {
                        warn!(error = %e, "Message processing failed, DLQ disabled");
                    }

                    // Release memory for failed messages (they won't be flushed)
                    self.memory_guard.release(msg.payload.len() as u64);
                }
            }
        }

        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use scalo::dlq::DlqEntry;
    use scalo::memory::{MemoryGuard, MemoryGuardConfig};

    use crate::buffer::{BufferManager, KafkaOffset};
    use crate::config::{BufferConfig, ComputedColumnsConfig, MetadataConfig};
    use crate::kafka::KafkaMessage;
    use crate::pipeline::capture::CaptureOverrides;
    use crate::pipeline::types::ProcessedMessage;
    use crate::transform::ComputedColumnCache;

    use super::{BatchCoordinator, BatchOutcome};

    fn make_kafka_message(
        payload: &[u8],
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> KafkaMessage {
        KafkaMessage {
            payload: payload.to_vec(),
            topic: Arc::from(topic),
            partition,
            offset,
            key: None,
            timestamp_ms: None,
        }
    }

    fn make_pending() -> crate::pipeline::pending_schema::PendingSchemaBuffer {
        crate::pipeline::pending_schema::PendingSchemaBuffer::new(
            crate::pipeline::pending_schema::PendingSchemaConfig {
                max_per_table: 100,
                max_total: 1000,
                max_age: std::time::Duration::from_secs(30),
            },
        )
    }

    fn make_processed(table: &str) -> ProcessedMessage {
        ProcessedMessage {
            table: table.to_string(),
            data: serde_json::Map::new(),
            raw_payload: None,
            kafka_offset: KafkaOffset {
                topic: Arc::from("test-topic"),
                partition: 0,
                offset: 1,
            },
        }
    }

    fn make_coordinator<'a>(
        buffer_manager: &'a mut BufferManager,
        capture_overrides: &'a mut CaptureOverrides,
        field_mapping_cache: &'a mut Option<crate::transform::FieldMappingCache>,
        computed_column_cache: &'a mut ComputedColumnCache,
        metrics: &'a Option<crate::metrics::Metrics>,
        dlq_tx: &'a mpsc::Sender<DlqEntry>,
        dlq_enabled: bool,
        memory_guard: &'a MemoryGuard,
        pending_schema: &'a mut crate::pipeline::pending_schema::PendingSchemaBuffer,
    ) -> BatchCoordinator<'a> {
        BatchCoordinator {
            buffer_manager,
            capture_overrides,
            field_mapping_cache,
            computed_column_cache,
            metrics,
            dlq_tx,
            dlq_enabled,
            memory_guard,
            pending_schema,
        }
    }

    // ---- BatchOutcome tests ----

    #[test]
    fn batch_outcome_default_is_all_zeros() {
        let outcome = BatchOutcome::default();
        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.dlq, 0);
        assert_eq!(outcome.pending, 0);
        assert!(outcome.needs_resolution.is_empty());
    }

    // ---- BatchCoordinator tests ----

    #[test]
    fn apply_results_empty_batch() {
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages: Vec<KafkaMessage> = vec![];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.dlq, 0);
    }

    #[test]
    fn apply_results_all_success() {
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![
            make_kafka_message(b"msg1", "topic", 0, 1),
            make_kafka_message(b"msg2", "topic", 0, 2),
            make_kafka_message(b"msg3", "topic", 0, 3),
        ];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Ok(make_processed("dfe.events")),
            Ok(make_processed("dfe.events")),
            Ok(make_processed("dfe.metrics")),
        ];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 3);
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.dlq, 0);
    }

    #[test]
    fn apply_results_all_errors_dlq_enabled() {
        let (dlq_tx, mut dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![
            make_kafka_message(b"bad1", "topic", 0, 10),
            make_kafka_message(b"bad2", "topic", 0, 11),
        ];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Err(crate::Error::Json("parse failed".to_string())),
            Err(crate::Error::Transform("missing field".to_string())),
        ];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.errors, 2);
        assert_eq!(outcome.dlq, 2);

        // Verify DLQ entries were sent
        let entry1 = dlq_rx.try_recv().expect("DLQ entry 1");
        assert_eq!(entry1.service, "loader");
        assert!(entry1.reason.contains("parse failed"));

        let entry2 = dlq_rx.try_recv().expect("DLQ entry 2");
        assert!(entry2.reason.contains("missing field"));
    }

    #[test]
    fn apply_results_errors_dlq_disabled() {
        let (dlq_tx, mut dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            false, // DLQ disabled
            &guard,
            &mut pending,
        );

        let messages = vec![make_kafka_message(b"bad", "topic", 0, 1)];
        let results: Vec<crate::Result<ProcessedMessage>> =
            vec![Err(crate::Error::Json("boom".to_string()))];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.errors, 1);
        assert_eq!(outcome.dlq, 0); // Nothing sent to DLQ

        // Channel should be empty
        assert!(dlq_rx.try_recv().is_err());
    }

    #[test]
    fn apply_results_mixed_success_and_failure() {
        let (dlq_tx, mut dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![
            make_kafka_message(b"good1", "topic", 0, 1),
            make_kafka_message(b"bad1", "topic", 0, 2),
            make_kafka_message(b"good2", "topic", 0, 3),
            make_kafka_message(b"bad2", "topic", 0, 4),
            make_kafka_message(b"good3", "topic", 0, 5),
        ];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Ok(make_processed("dfe.events")),
            Err(crate::Error::Json("parse error".to_string())),
            Ok(make_processed("dfe.metrics")),
            Err(crate::Error::Schema("schema mismatch".to_string())),
            Ok(make_processed("dfe.events")),
        ];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 3);
        assert_eq!(outcome.errors, 2);
        assert_eq!(outcome.dlq, 2);

        // Verify exactly 2 DLQ entries
        assert!(dlq_rx.try_recv().is_ok());
        assert!(dlq_rx.try_recv().is_ok());
        assert!(dlq_rx.try_recv().is_err()); // No more
    }

    #[test]
    fn apply_results_dlq_channel_full() {
        // Channel capacity 1, send 3 errors — first succeeds, rest drop
        let (dlq_tx, _dlq_rx) = mpsc::channel(1);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![
            make_kafka_message(b"e1", "topic", 0, 1),
            make_kafka_message(b"e2", "topic", 0, 2),
            make_kafka_message(b"e3", "topic", 0, 3),
        ];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Err(crate::Error::Json("err1".to_string())),
            Err(crate::Error::Json("err2".to_string())),
            Err(crate::Error::Json("err3".to_string())),
        ];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.errors, 3);
        // Only 1 fits in the channel, remaining 2 are dropped
        assert_eq!(outcome.dlq, 1);
    }

    #[test]
    fn apply_results_memory_released_on_error() {
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        // Pre-acquire memory matching the payload sizes
        guard.add_bytes(100);
        let before = guard.current_bytes();

        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            false,
            &guard,
            &mut pending,
        );

        // 50-byte payload — memory should be released on error
        let messages = vec![make_kafka_message(&[0u8; 50], "topic", 0, 1)];
        let results: Vec<crate::Result<ProcessedMessage>> =
            vec![Err(crate::Error::Json("fail".to_string()))];

        coord.apply_results(results, &messages);
        let after = guard.current_bytes();
        assert!(
            after < before,
            "Memory should be released on error: before={before}, after={after}"
        );
    }

    #[test]
    fn apply_results_refs_works_like_apply_results() {
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let msg1 = make_kafka_message(b"ref1", "topic", 0, 1);
        let msg2 = make_kafka_message(b"ref2", "topic", 0, 2);
        let messages: Vec<&KafkaMessage> = vec![&msg1, &msg2];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Ok(make_processed("dfe.events")),
            Err(crate::Error::Json("parse fail".to_string())),
        ];

        let outcome = coord.apply_results_refs(results, &messages);
        assert_eq!(outcome.processed, 1);
        assert_eq!(outcome.errors, 1);
        assert_eq!(outcome.dlq, 1);
    }

    #[test]
    fn apply_results_more_results_than_messages_ignores_extras() {
        // zip() stops at the shorter iterator — excess results are ignored
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![make_kafka_message(b"only_one", "topic", 0, 1)];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![
            Ok(make_processed("dfe.events")),
            Ok(make_processed("dfe.metrics")), // excess — should be ignored
            Ok(make_processed("dfe.metrics")), // excess — should be ignored
        ];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 1); // Only 1 message matched
    }

    #[test]
    fn apply_results_more_messages_than_results_ignores_extras() {
        let (dlq_tx, _dlq_rx) = mpsc::channel(16);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = make_pending();

        let mut coord = make_coordinator(
            &mut buffer_manager,
            &mut capture_overrides,
            &mut field_mapping_cache,
            &mut computed_column_cache,
            &None,
            &dlq_tx,
            true,
            &guard,
            &mut pending,
        );

        let messages = vec![
            make_kafka_message(b"m1", "topic", 0, 1),
            make_kafka_message(b"m2", "topic", 0, 2),
            make_kafka_message(b"m3", "topic", 0, 3), // excess
        ];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![Ok(make_processed("dfe.events"))];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 1);
    }

    #[test]
    fn schema_pending_routes_to_buffer_not_dlq() {
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut fmc: Option<crate::transform::FieldMappingCache> = None;
        let mut ccc = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let metrics: Option<crate::metrics::Metrics> = None;
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<DlqEntry>(8);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 64 * 1024 * 1024,
            ..Default::default()
        });
        let mut pending = crate::pipeline::pending_schema::PendingSchemaBuffer::new(
            crate::pipeline::pending_schema::PendingSchemaConfig {
                max_per_table: 100,
                max_total: 1000,
                max_age: std::time::Duration::from_secs(30),
            },
        );

        let outcome = {
            let mut coord = make_coordinator(
                &mut buffer_manager,
                &mut capture_overrides,
                &mut fmc,
                &mut ccc,
                &metrics,
                &dlq_tx,
                true,
                &guard,
                &mut pending,
            );
            let messages = [make_kafka_message(b"a", "t", 0, 0)];
            let results: Vec<crate::Result<ProcessedMessage>> =
                vec![Err(crate::Error::SchemaPending {
                    table: "dfe.t1".into(),
                })];
            coord.apply_results(results, &messages)
        };

        assert_eq!(outcome.pending, 1);
        assert_eq!(outcome.dlq, 0);
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.needs_resolution, vec!["dfe.t1".to_string()]);
        assert_eq!(pending.len(), 1);
        assert!(dlq_rx.try_recv().is_err(), "nothing should go to DLQ");
    }

    #[test]
    fn schema_pending_per_table_overflow_goes_to_dlq() {
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut fmc: Option<crate::transform::FieldMappingCache> = None;
        let mut ccc = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let metrics: Option<crate::metrics::Metrics> = None;
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<DlqEntry>(8);
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 64 * 1024 * 1024,
            ..Default::default()
        });
        let mut pending = crate::pipeline::pending_schema::PendingSchemaBuffer::new(
            crate::pipeline::pending_schema::PendingSchemaConfig {
                max_per_table: 1, // second message for the table overflows
                max_total: 1000,
                max_age: std::time::Duration::from_secs(30),
            },
        );

        let outcome = {
            let mut coord = make_coordinator(
                &mut buffer_manager,
                &mut capture_overrides,
                &mut fmc,
                &mut ccc,
                &metrics,
                &dlq_tx,
                true,
                &guard,
                &mut pending,
            );
            let messages = [
                make_kafka_message(b"a", "t", 0, 0),
                make_kafka_message(b"b", "t", 0, 1),
            ];
            let results: Vec<crate::Result<ProcessedMessage>> = vec![
                Err(crate::Error::SchemaPending {
                    table: "dfe.t1".into(),
                }),
                Err(crate::Error::SchemaPending {
                    table: "dfe.t1".into(),
                }),
            ];
            coord.apply_results(results, &messages)
        };

        assert_eq!(outcome.pending, 1, "first fits");
        assert_eq!(outcome.dlq, 1, "second overflows to DLQ");
        let entry = dlq_rx.try_recv().expect("overflow DLQ entry");
        assert!(entry.reason.contains("pending_schema"));
    }
}
