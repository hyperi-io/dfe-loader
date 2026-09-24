// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Sequential batch coordinator.
//!
//! Applies results from the parallel processing phase to mutable state:
//! buffer push, mark_pending, stats, dead letters. Called after
//! `super::processor::MessageProcessor` completes and its borrows are released.

use tracing::debug;

use scalo::dlq::{DlqEntry, DlqSource};
use scalo::memory::MemoryGuard;

use crate::buffer::BufferManager;
use crate::kafka::KafkaMessage;
use crate::metrics::Metrics;
use crate::transform::{ComputedColumnCache, FieldMappingCache};

use super::capture::CaptureOverrides;
use super::pending_schema::{EnqueueOutcome, PendingOverflow, PendingSchemaBuffer};
use super::types::ProcessedMessage;

/// Emit the `data.dlq_routed` security event, naming what was dead-lettered.
///
/// Built here rather than through scalo's `record_dlq` because that passes its
/// context as `detail`, which scalo's failure-level event never writes
/// (scalo-rs#145). `resource` is written, so the location goes there.
pub(crate) fn record_dlq_routed(action: &str, reason: &str, resource: &str) {
    use scalo::logger::security::{SecurityEvent, SecurityOutcome};

    SecurityEvent::new("data.dlq_routed", action, SecurityOutcome::Failure)
        .reason(reason)
        .resource(resource)
        .emit();
}

/// The DLQ entry for a message only the DLQ can take, naming where it came from.
pub(crate) fn dead_letter(msg: &KafkaMessage, reason: impl Into<String>) -> DlqEntry {
    DlqEntry::new("loader", reason, msg.payload.clone()).with_source(DlqSource::kafka(
        &*msg.topic,
        msg.partition,
        msg.offset,
    ))
}

/// Counters returned from `apply_results` for the orchestrator to update stats.
#[derive(Debug, Default)]
pub struct BatchOutcome {
    pub processed: u64,
    pub errors: u64,
    /// Messages routed into the pending-schema buffer (awaiting resolution).
    pub pending: u64,
    /// Tables seen for the first time that need a resolution request kicked
    /// off by the orchestrator.
    pub needs_resolution: Vec<String>,
    /// Messages only the DLQ can take, for the orchestrator to hand over and
    /// hold until it does.
    pub dead_letters: Vec<DlqEntry>,
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
    pub memory_guard: &'a MemoryGuard,
    pub pending_schema: &'a mut PendingSchemaBuffer,
}

impl BatchCoordinator<'_> {
    /// Apply parallel processing results sequentially.
    ///
    /// For each `Ok(processed)`: ensure cache entry, mark_pending, buffer push.
    /// For each `Err(e)`: a dead letter in the outcome, memory release.
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
        // Aggregated per table so the counter is touched once per batch, not
        // once per message.
        let mut fallbacks: rustc_hash::FxHashMap<String, u64> = rustc_hash::FxHashMap::default();

        for (msg, result) in messages.zip(results) {
            match result {
                Ok(mut processed) => {
                    if let Some(original) = processed.fell_back_from.take() {
                        *fallbacks.entry(original).or_insert(0) += 1;
                    }
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
                            // Per-table cap hit: this message overflows to the
                            // DLQ. Other buffered messages for the table stay put.
                            if let Some(m) = self.metrics {
                                m.record_pending_schema_overflow();
                            }
                            record_dlq_routed(
                                "pending_schema_overflow",
                                &format!("per-table cap exceeded for {t}"),
                                &msg.location(),
                            );
                            outcome.dead_letters.push(dead_letter(
                                msg,
                                format!("pending_schema_per_table_overflow table={t}"),
                            ));
                            self.memory_guard.release(msg.payload.len() as u64);
                        }
                    }
                }
                Err(e) => {
                    outcome.errors += 1;
                    let reason = e.to_string();
                    record_dlq_routed("processing", &reason, &msg.location());
                    debug!(error = %e, "Message queued for DLQ");
                    outcome.dead_letters.push(dead_letter(msg, reason));
                    self.memory_guard.release(msg.payload.len() as u64);
                }
            }
        }

        if let Some(m) = self.metrics {
            for (table, n) in fallbacks {
                m.record_unknown_table_fallback_n(&table, n);
            }
        }

        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
                on_full: crate::pipeline::pending_schema::OnFull::DeadLetter,
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
            fell_back_from: None,
        }
    }

    fn make_coordinator<'a>(
        buffer_manager: &'a mut BufferManager,
        capture_overrides: &'a mut CaptureOverrides,
        field_mapping_cache: &'a mut Option<crate::transform::FieldMappingCache>,
        computed_column_cache: &'a mut ComputedColumnCache,
        metrics: &'a Option<crate::metrics::Metrics>,
        memory_guard: &'a MemoryGuard,
        pending_schema: &'a mut crate::pipeline::pending_schema::PendingSchemaBuffer,
    ) -> BatchCoordinator<'a> {
        BatchCoordinator {
            buffer_manager,
            capture_overrides,
            field_mapping_cache,
            computed_column_cache,
            metrics,
            memory_guard,
            pending_schema,
        }
    }

    /// Fields of every `data.dlq_routed` security event, as the log line carries them.
    #[derive(Clone, Default)]
    struct DlqEventCapture {
        events: Arc<std::sync::Mutex<Vec<std::collections::HashMap<String, String>>>>,
    }

    #[derive(Default)]
    struct FieldMap(std::collections::HashMap<String, String>);

    impl tracing::field::Visit for FieldMap {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl tracing::Subscriber for DlqEventCapture {
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
            let mut fields = FieldMap::default();
            event.record(&mut fields);
            if fields.0.get("event_type").map(String::as_str) == Some("data.dlq_routed") {
                self.events.lock().expect("capture lock").push(fields.0);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn dlq_routed_event_names_the_record_location() {
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
            &guard,
            &mut pending,
        );

        let messages = vec![make_kafka_message(
            b"\x00\x00\x02",
            "strimzi.cruisecontrol.metrics",
            3,
            42,
        )];
        let results: Vec<crate::Result<ProcessedMessage>> =
            vec![Err(crate::Error::Json("format rejected".to_string()))];

        let capture = DlqEventCapture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            coord.apply_results(results, &messages);
        });

        let events = capture.events.lock().expect("capture lock");
        assert_eq!(events.len(), 1, "one DLQ'd record, one event: {events:?}");
        let resource = events[0].get("resource").map(String::as_str);
        assert_eq!(
            resource,
            Some("topic=strimzi.cruisecontrol.metrics partition=3 offset=42"),
            "the log line must say which record was dead-lettered: {events:?}"
        );
        let reason = events[0].get("reason").map(String::as_str).unwrap_or("");
        assert!(reason.contains("format rejected"), "{events:?}");
    }

    // ---- BatchOutcome tests ----

    #[test]
    fn batch_outcome_default_is_all_zeros() {
        let outcome = BatchOutcome::default();
        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.pending, 0);
        assert!(outcome.needs_resolution.is_empty());
        assert!(outcome.dead_letters.is_empty());
    }

    // ---- BatchCoordinator tests ----

    #[test]
    fn apply_results_empty_batch() {
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
            &guard,
            &mut pending,
        );

        let messages: Vec<KafkaMessage> = vec![];
        let results: Vec<crate::Result<ProcessedMessage>> = vec![];

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.errors, 0);
        assert!(outcome.dead_letters.is_empty());
    }

    #[test]
    fn apply_results_all_success() {
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
        assert!(outcome.dead_letters.is_empty());
    }

    #[test]
    fn apply_results_all_errors_become_dead_letters() {
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
        assert_eq!(outcome.dead_letters.len(), 2);

        let first = &outcome.dead_letters[0];
        assert_eq!(first.service, "loader");
        assert!(first.reason.contains("parse failed"));
        assert_eq!(first.payload, b"bad1");
        let source = first
            .source
            .as_ref()
            .expect("a dead letter names its source");
        assert_eq!(source.topic.as_deref(), Some("topic"));
        assert_eq!((source.partition, source.offset), (Some(0), Some(10)));

        assert!(outcome.dead_letters[1].reason.contains("missing field"));
    }

    #[test]
    fn apply_results_mixed_success_and_failure() {
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
        let offsets: Vec<Option<i64>> = outcome
            .dead_letters
            .iter()
            .map(|e| e.source.as_ref().and_then(|s| s.offset))
            .collect();
        assert_eq!(offsets, vec![Some(2), Some(4)]);
    }

    #[test]
    fn apply_results_keeps_every_failed_message_however_many_fail() {
        // More failures than a bounded hand-off could hold, and none may drop.
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
            &guard,
            &mut pending,
        );

        let messages: Vec<KafkaMessage> = (0..2_000)
            .map(|n| make_kafka_message(b"e", "topic", 0, n))
            .collect();
        let results: Vec<crate::Result<ProcessedMessage>> = (0..2_000)
            .map(|n| Err(crate::Error::Json(format!("err{n}"))))
            .collect();

        let outcome = coord.apply_results(results, &messages);
        assert_eq!(outcome.errors, 2_000);
        assert_eq!(outcome.dead_letters.len(), 2_000);
    }

    #[test]
    fn apply_results_memory_released_on_error() {
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
        assert_eq!(outcome.dead_letters.len(), 1);
    }

    #[test]
    fn apply_results_more_results_than_messages_ignores_extras() {
        // zip() stops at the shorter iterator — excess results are ignored
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
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 64 * 1024 * 1024,
            ..Default::default()
        });
        let mut pending = crate::pipeline::pending_schema::PendingSchemaBuffer::new(
            crate::pipeline::pending_schema::PendingSchemaConfig {
                max_per_table: 100,
                max_total: 1000,
                max_age: std::time::Duration::from_secs(30),
                on_full: crate::pipeline::pending_schema::OnFull::DeadLetter,
            },
        );

        let outcome = {
            let mut coord = make_coordinator(
                &mut buffer_manager,
                &mut capture_overrides,
                &mut fmc,
                &mut ccc,
                &metrics,
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
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.needs_resolution, vec!["dfe.t1".to_string()]);
        assert_eq!(pending.len(), 1);
        assert!(outcome.dead_letters.is_empty(), "nothing should go to DLQ");
    }

    #[test]
    fn schema_pending_per_table_overflow_goes_to_dlq() {
        let mut buffer_manager = BufferManager::new(&BufferConfig::default());
        let mut capture_overrides = CaptureOverrides::new(&MetadataConfig::default());
        let mut fmc: Option<crate::transform::FieldMappingCache> = None;
        let mut ccc = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let metrics: Option<crate::metrics::Metrics> = None;
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 64 * 1024 * 1024,
            ..Default::default()
        });
        let mut pending = crate::pipeline::pending_schema::PendingSchemaBuffer::new(
            crate::pipeline::pending_schema::PendingSchemaConfig {
                max_per_table: 1, // second message for the table overflows
                max_total: 1000,
                max_age: std::time::Duration::from_secs(30),
                on_full: crate::pipeline::pending_schema::OnFull::DeadLetter,
            },
        );

        let outcome = {
            let mut coord = make_coordinator(
                &mut buffer_manager,
                &mut capture_overrides,
                &mut fmc,
                &mut ccc,
                &metrics,
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
        assert_eq!(outcome.dead_letters.len(), 1, "second overflows to DLQ");
        let entry = &outcome.dead_letters[0];
        assert!(entry.reason.contains("pending_schema"));
        assert_eq!(entry.source.as_ref().and_then(|s| s.offset), Some(1));
    }
}
