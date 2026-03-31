// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Sequential batch coordinator.
//!
//! Applies results from the parallel processing phase to mutable state:
//! buffer push, mark_pending, stats, DLQ routing. Called after
//! [`super::processor::MessageProcessor`] completes and its borrows are released.

use tokio::sync::mpsc;
use tracing::{debug, warn};

use hyperi_rustlib::dlq::{DlqEntry, DlqSource};
use hyperi_rustlib::memory::MemoryGuard;

use crate::buffer::BufferManager;
use crate::kafka::KafkaMessage;
use crate::metrics::Metrics;
use crate::transform::{ComputedColumnCache, FieldMappingCache};

use super::capture::CaptureOverrides;
use super::types::ProcessedMessage;

/// Counters returned from `apply_results` for the orchestrator to update stats.
#[derive(Debug, Default)]
pub struct BatchOutcome {
    pub processed: u64,
    pub errors: u64,
    pub dlq: u64,
}

/// Applies parallel processing results to mutable state.
///
/// Created per-batch in the orchestrator event loop. Holds `&mut` references
/// to caches, buffers, and stats. The borrow checker ensures this is only
/// constructed after the parallel phase completes (processor dropped).
pub struct BatchCoordinator<'a> {
    pub buffer_manager: &'a mut BufferManager,
    pub capture_overrides: &'a mut CaptureOverrides,
    pub field_mapping_cache: &'a mut Option<FieldMappingCache>,
    pub computed_column_cache: &'a mut ComputedColumnCache,
    pub metrics: &'a Option<Metrics>,
    pub dlq_tx: &'a mpsc::Sender<DlqEntry>,
    pub dlq_enabled: bool,
    pub memory_guard: &'a MemoryGuard,
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
        let mut outcome = BatchOutcome::default();

        for (msg, result) in messages.iter().zip(results) {
            match result {
                Ok(processed) => {
                    // Ensure cache entry so mark_pending doesn't re-add this table
                    self.capture_overrides.ensure_cached(&processed.table);
                    self.capture_overrides.mark_pending(&processed.table);
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
                Err(e) => {
                    outcome.errors += 1;
                    if let Some(m) = self.metrics {
                        m.record_dlq();
                    }

                    if self.dlq_enabled {
                        let entry = DlqEntry::new("loader", e.to_string(), msg.payload.clone())
                            .with_source(DlqSource::kafka(
                                msg.topic.to_string(),
                                msg.partition,
                                msg.offset,
                            ));

                        match self.dlq_tx.try_send(entry) {
                            Ok(()) => {
                                outcome.dlq += 1;
                                hyperi_rustlib::logger::security::record_dlq(
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
                                if hyperi_rustlib::logger::log_debounced(&DLQ_FULL_TS, 5000) {
                                    warn!(error = %e, "DLQ channel full, messages dropped (max 1 per 5s)");
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                static DLQ_CLOSED_TS: std::sync::atomic::AtomicU64 =
                                    std::sync::atomic::AtomicU64::new(0);
                                if hyperi_rustlib::logger::log_debounced(&DLQ_CLOSED_TS, 5000) {
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
