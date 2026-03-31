// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Parallel-safe message processor.
//!
//! [`MessageProcessor`] holds only `&` references to immutable dependencies.
//! Its [`process`](MessageProcessor::process) method is pure computation — no
//! mutable state, no I/O, no `.await`. Safe for rayon `par_iter` via
//! [`AdaptiveWorkerPool::process_batch`](hyperi_rustlib::worker::AdaptiveWorkerPool::process_batch).
//!
//! Created per-batch in the orchestrator's event loop. Borrows are released
//! before the sequential phase begins — the borrow checker enforces this.

use std::sync::Arc;

use serde_json::Value;
use tracing::debug;

use crate::buffer::KafkaOffset;
use crate::clickhouse::SharedSchemaCache;
use crate::column_meta::ColumnMetaCache;
use crate::config::Config;
use crate::payload::{FormatDetector, PayloadFormat};
use crate::routing::{RouteResult, Router};
use crate::transform::{ComputedColumnCache, FieldMappingCache, HeaderExtractor, Transformer};

use super::capture::CaptureOverrides;
use super::enrichment::EnrichmentPipeline;
use super::types::ProcessedMessage;

/// Immutable processing context for parallel message processing.
///
/// All fields are `&` references to `Sync` types. Created per-batch,
/// dropped before the sequential phase. The borrow checker enforces
/// that no mutable access to these dependencies occurs during processing.
pub(crate) struct MessageProcessor<'a> {
    pub config: &'a Config,
    pub router: &'a Router,
    pub transformer: &'a Transformer,
    pub extractor: &'a HeaderExtractor,
    pub format_detector: &'a FormatDetector,
    pub json_primary_mode: bool,
    pub enrichment: &'a EnrichmentPipeline,
    pub schema_cache: &'a SharedSchemaCache,
    pub col_meta_cache: &'a ColumnMetaCache,
    pub field_mapping_cache: Option<&'a FieldMappingCache>,
    pub computed_column_cache: &'a ComputedColumnCache,
    pub capture_overrides: &'a CaptureOverrides,
}

impl MessageProcessor<'_> {
    /// Process a single Kafka message through the full pipeline.
    ///
    /// Pure computation — no mutable state, safe for `par_iter`.
    /// Returns `ProcessedMessage` on success or an error for DLQ routing.
    ///
    /// Steps:
    /// 1. Format detection
    /// 2. Parse (sonic-rs JSON or rmp MessagePack)
    /// 3. Route to table
    /// 4. Extract/transform (json_primary or legacy_flatten path)
    /// 5. Apply capture config (pure derivation, no cache mutation)
    /// 6. Apply field mapping (read-only cache lookup)
    /// 7. Evaluate computed columns / CEL expressions (read-only)
    /// 8. Enrichment (GeoIP + reputation + risk, in-memory lookups)
    pub fn process(&self, msg: &crate::kafka::KafkaMessage) -> crate::Result<ProcessedMessage> {
        // Step 1: Check/detect format
        let format = match self.format_detector.check_and_detect(&msg.payload) {
            Ok(fmt) => fmt,
            Err(_expected) => {
                hyperi_rustlib::logger::security::input_validation_failure(
                    "format_check",
                    "payload format mismatch",
                    None,
                );
                return Err(crate::Error::Json("Format mismatch".into()));
            }
        };

        // Step 2: Parse payload to JSON Value
        let value: Value = match format {
            PayloadFormat::Json => sonic_rs::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("JSON parse error: {e}")))?,
            PayloadFormat::MessagePack => rmp_serde::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("MessagePack parse error: {e}")))?,
            PayloadFormat::Unknown => {
                return Err(crate::Error::Json("Unknown format".into()));
            }
        };

        // Step 3: Route to table (db.table)
        let table = match self.router.route_value(&value) {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                debug!(reason = %reason, "Routing to DLQ");
                return Err(crate::Error::Json(format!("DLQ: {reason}")));
            }
        };

        // Step 4: Build the promoted field map.
        //
        // json_primary path: HeaderExtractor SIMD scan + zero-copy _json.
        // Legacy fallback: full flatten + Transformer path.
        let json_primary_schema = if self.json_primary_mode && format == PayloadFormat::Json {
            self.schema_cache.get(&table)
        } else {
            None
        };

        let (mut data, raw_payload) = if let Some(schema) = json_primary_schema {
            let promoted =
                self.extractor
                    .extract(&msg.payload, &table, &schema, self.col_meta_cache);
            let raw: Arc<[u8]> = Arc::from(msg.payload.as_slice());
            (promoted, Some(raw))
        } else {
            // Legacy flatten path
            let common_header = self.config.metadata.enabled;

            let org_id_owned = if common_header {
                self.router
                    .extract_org_id_from_value(&value)
                    .map(std::string::ToString::to_string)
            } else {
                None
            };

            let source_owned = if common_header && self.config.metadata.capture_source {
                Some(self.router.extract_source_from_value(&value).map_or_else(
                    || self.router.derive_source_from_topic(&msg.topic),
                    std::string::ToString::to_string,
                ))
            } else {
                None
            };

            let transform_result = self.transformer.transform_with_raw(
                value,
                org_id_owned.as_deref(),
                source_owned.as_deref(),
            )?;
            let mut d = transform_result.data;

            // Pure derive instead of get_or_default(&mut self)
            let table_capture = self.capture_overrides.derive_config(&table);
            if common_header
                && self.config.metadata.capture_json
                && !table_capture.disable_json
                && let Ok(json_str) = std::str::from_utf8(&msg.payload)
            {
                d.insert("_json".to_string(), Value::String(json_str.to_string()));
            }
            if common_header && table_capture.disable_raw {
                d.remove(self.transformer.raw_output());
            }

            (d, None)
        };

        // Step 5: Apply per-table field mapping (read-only cache lookup)
        if let Some(fm_cache) = self.field_mapping_cache
            && let Some(mapping) = fm_cache.get(&table)
        {
            mapping.apply(&mut data);
        }

        // Step 6: Apply computed columns / CEL expressions (read-only)
        if let Some(computed) = self.computed_column_cache.get(&table) {
            computed.evaluate(&mut data);
        }

        // Step 7: Enrichment (GeoIP + reputation + risk, in-memory)
        self.enrichment.enrich(&mut data);

        // Build offset for commit tracking
        let kafka_offset =
            KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);

        debug!(table = %table, "Message processed");
        Ok(ProcessedMessage {
            table,
            data,
            raw_payload,
            kafka_offset,
        })
    }
}
