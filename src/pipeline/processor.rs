// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Parallel-safe message processor.
//!
//! `MessageProcessor` holds only `&` references to immutable dependencies.
//! Its `process` method is pure computation — no
//! mutable state, no I/O, no `.await`. Safe for rayon `par_iter` via
//! [`AdaptiveWorkerPool::process_batch`](scalo::worker::AdaptiveWorkerPool::process_batch).
//!
//! Created per-batch in the orchestrator's event loop. Borrows are released
//! before the sequential phase begins — the borrow checker enforces this.

use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, error, trace, warn};

use crate::buffer::KafkaOffset;
use crate::clickhouse::SharedSchemaCache;
use crate::column_meta::ColumnMetaCache;
use crate::config::{CaptureMode, Config};
use crate::payload::{LeadingBytes, opens_json_document};
use crate::routing::{RouteResult, Router, TableChoice};
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
    pub json_primary_mode: bool,
    pub enrichment: &'a EnrichmentPipeline,
    pub schema_cache: &'a SharedSchemaCache,
    pub col_meta_cache: &'a ColumnMetaCache,
    pub field_mapping_cache: Option<&'a FieldMappingCache>,
    pub computed_column_cache: &'a ComputedColumnCache,
    pub capture_overrides: &'a CaptureOverrides,
    /// Tables ClickHouse has confirmed absent; their messages re-route to
    /// `default_table` instead of buffering until the pending-schema age cap.
    /// Entries expire, so a table that later appears is resolved again.
    pub absent_tables: &'a super::types::AbsentTables,
    /// Pre-built `db.table` for the routing default.
    pub default_table: &'a str,
}

/// Report a header pass that promoted nothing, at most once a minute.
///
/// The table name goes in the log, never in a metric label: it comes from a
/// payload field with no allowlist, so labelling it would let untrusted input
/// grow the label set without bound.
fn log_header_pass_skipped(table: &str, reason: &str) {
    metrics::counter!("dfe_loader_header_pass_skipped_total").increment(1);
    static SKIPPED_TS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if scalo::logger::log_debounced(&SKIPPED_TS, 60_000) {
        error!(
            table = %table,
            reason = reason,
            "Header pass promoted no columns, rejecting the message (max 1 per 60s)"
        );
    }
}

/// One topic's state for the routing-field warning.
#[derive(Default)]
struct RoutingFieldWarn {
    last_logged_ms: std::sync::atomic::AtomicU64,
    /// Records seen since the last line, so a rate-limited warning still
    /// reports the volume behind it rather than one record a minute.
    records: std::sync::atomic::AtomicU64,
}

/// Report records that named no table on a source's own topic, at most once a
/// minute per topic.
///
/// The topic stays out of the metric labels for the same reason the table does
/// above: on the gRPC transport it arrives in request metadata.
fn log_routing_field_absent(topic: &str, table: &str) {
    metrics::counter!("dfe_loader_routing_field_absent_total").increment(1);

    // Keyed by topic so a second broken source is not hidden by the first; the
    // subscribed topic set bounds the map.
    static WARN: std::sync::LazyLock<
        parking_lot::Mutex<rustc_hash::FxHashMap<String, Arc<RoutingFieldWarn>>>,
    > = std::sync::LazyLock::new(|| parking_lot::Mutex::new(rustc_hash::FxHashMap::default()));

    let slot = Arc::clone(WARN.lock().entry(topic.to_string()).or_default());
    slot.records
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if scalo::logger::log_debounced(&slot.last_logged_ms, 60_000) {
        let records = slot.records.swap(0, std::sync::atomic::Ordering::Relaxed);
        warn!(
            topic = %topic,
            table = %table,
            records = records,
            reason = "no_routing_field",
            "Records named no table on a source topic and were routed by topic name (max 1 per 60s per topic)"
        );
    }
}

/// Whether the payload itself is written to `_raw` as UTF-8 text.
///
/// `RawOnly` always writes it. `Full` writes it only for a payload that is not
/// a JSON document, and only when the record carried no raw field of its own
/// (a receiver capture or an `@renamed` source): `_json` already holds a JSON
/// payload, so a second copy in `_raw` stores it twice. `JsonOnly` and
/// `ExtractedOnly` never write it.
fn captures_payload_as_raw(
    mode: CaptureMode,
    capture_raw: bool,
    has_raw_field: bool,
    payload: &[u8],
) -> bool {
    match mode {
        CaptureMode::RawOnly => true,
        CaptureMode::Full => capture_raw && !has_raw_field && !opens_json_document(payload),
        CaptureMode::JsonOnly | CaptureMode::ExtractedOnly => false,
    }
}

/// Refuse `payload` as not JSON, naming its leading bytes.
#[cold]
#[inline(never)]
fn not_json(payload: &[u8]) -> crate::Error {
    scalo::logger::security::input_validation_failure("format_check", "payload is not JSON", None);
    crate::Error::NotJson {
        leading: LeadingBytes::of(payload),
    }
}

impl MessageProcessor<'_> {
    /// Process a single Kafka message through the full pipeline.
    ///
    /// Pure computation — no mutable state, safe for `par_iter`.
    /// Returns `ProcessedMessage` on success or an error for DLQ routing.
    ///
    /// Steps:
    /// 1. Refuse a payload that does not open a JSON object or array
    /// 2. Parse (sonic-rs)
    /// 3. Route to table
    /// 4. Extract/transform (json_primary or legacy_flatten path)
    /// 5. Apply capture config (pure derivation, no cache mutation)
    /// 6. Apply field mapping (read-only cache lookup)
    /// 7. Evaluate computed columns / CEL expressions (read-only)
    /// 8. Enrichment (GeoIP + reputation + risk, in-memory lookups)
    pub fn process(&self, msg: &crate::kafka::KafkaMessage) -> crate::Result<ProcessedMessage> {
        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                msg_size = msg.payload.len(),
                topic = %msg.topic,
                partition = msg.partition,
                offset = msg.offset,
                "Processing message"
            );
        }

        // Step 1: Refuse anything that is not JSON before the parser sees it
        if !opens_json_document(&msg.payload) {
            return Err(not_json(&msg.payload));
        }

        // Step 2: Parse payload to JSON Value
        let value: Value = sonic_rs::from_slice(&msg.payload).map_err(|e| {
            scalo::logger::security::input_validation_failure(
                "json_parse",
                "invalid JSON payload",
                None,
            );
            // A batch the fan-out could not split reads as trailing
            // characters, so the shape is named instead (#184).
            if crate::payload::has_ndjson_boundary(&msg.payload) {
                return crate::Error::Json(
                    "payload holds more than one JSON record and did not split into whole records"
                        .into(),
                );
            }
            crate::Error::Json(format!("JSON parse error: {e}"))
        })?;

        // An array the fan-out left alone is not a batch of records, and the
        // capture would hand it to a per-row JSON column, so it is named here
        // rather than coming back as an encode error on an empty column (#128).
        if value.is_array() {
            scalo::logger::security::input_validation_failure(
                "payload_shape",
                "top-level JSON array, expected one record object",
                None,
            );
            return Err(crate::Error::Json(
                "payload is a JSON array, not a record object".into(),
            ));
        }

        // Step 3: Route to table (db.table), falling back to the source the
        // topic names when the record carries no routing field (#184).
        let (result, choice) = self.router.route_value_on_topic(&value, &msg.topic);
        let routed = match result {
            RouteResult::Table(t) => {
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(table = %t, "Message routed");
                }
                t
            }
            RouteResult::Dlq(reason) => {
                debug!(reason = %reason, "Routing to DLQ");
                scalo::logger::security::input_validation_failure("routing", &reason, None);
                return Err(crate::Error::Json(format!("DLQ: {reason}")));
            }
        };

        // A source's own topic carrying records that name no table is still a
        // producer defect, and routing by topic is otherwise silent.
        if choice == TableChoice::Topic {
            log_routing_field_absent(&msg.topic, &routed);
        }

        // An unknown source names a table that does not exist, so it lands in the
        // default table rather than the DLQ. The emptiness check keeps the common
        // case free of a hash lookup.
        let (table, fell_back_from) =
            if !self.absent_tables.is_empty() && self.absent_tables.contains(&routed) {
                (self.default_table.to_string(), Some(routed))
            } else {
                (routed, None)
            };

        // Step 4: Build the promoted field map.
        //
        // json_primary path: HeaderExtractor SIMD scan + zero-copy _json.
        // Legacy fallback: full flatten + Transformer path.

        // json_primary: a schema cache miss is NOT a silent transformer
        // fallback. Return SchemaPending so the coordinator buffers the message
        // until the background resolver populates the schema (#36). The extractor
        // is the only path that applies @renamed directives; the transformer path
        // below would drop them, NULLing the renamed columns. legacy_flatten
        // still uses the transformer path.
        let extractor_schema = if self.json_primary_mode {
            match self.schema_cache.get(&table) {
                Some(schema) => Some(schema),
                None => return Err(crate::Error::SchemaPending { table }),
            }
        } else {
            None
        };

        // Resolve capture mode for this table (DDL > per-table config > global).
        let capture_mode = self.capture_overrides.derive_config(&table).mode;

        let (mut data, raw_payload) = if let Some(schema) = extractor_schema {
            // The `topic_name` fallback labels a record that carries no _source
            // of its own, so the topic's source is derived once for the message
            // and every column holding the marker resolves to it (#187).
            let topic_source = self.router.derive_source_from_topic(&msg.topic);
            let promoted = self.extractor.extract(
                &msg.payload,
                &table,
                &schema,
                self.col_meta_cache,
                Some(topic_source.as_str()),
            );

            // A header pass that promoted nothing would land a row of type
            // defaults — no _source, no _tags, _timestamp at epoch zero (#144).
            // Every rejection is logged and counted here, the one site that sees
            // them all; the extractor supplies the reason (#145).
            if let Some(reason) = promoted.empty_reason
                && !schema.columns.is_empty()
            {
                log_header_pass_skipped(&table, reason);
                return Err(crate::Error::Transform(format!(
                    "header pass promoted no columns for {table}"
                )));
            }
            let promoted = promoted.fields;

            // raw_payload carries Kafka bytes for the zero-copy _json splice, so
            // every mode that populates _json passes it.
            // raw_only: _raw set below from payload bytes, no _json splice needed.
            // extracted_only: neither -- no raw payload passed to inserter.
            let raw: Option<Arc<[u8]>> = match capture_mode {
                CaptureMode::Full | CaptureMode::JsonOnly => {
                    Some(Arc::from(msg.payload.as_slice()))
                }
                CaptureMode::RawOnly | CaptureMode::ExtractedOnly => None,
            };
            (promoted, raw)
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

            // Apply capture mode to legacy path
            if common_header {
                match capture_mode {
                    CaptureMode::Full => {
                        // _json: inject full payload as string (legacy path)
                        if let Ok(json_str) = std::str::from_utf8(&msg.payload) {
                            d.insert("_json".to_string(), Value::String(json_str.to_string()));
                        }
                        // _raw: the transformer applies @renamed; the raw
                        // payload capture below fills it when @renamed did not.
                    }
                    CaptureMode::RawOnly => {
                        // _json: not populated
                        // _raw: transformer may have extracted from raw_source_fields,
                        // but we want the full Kafka payload instead — overwrite it
                        d.remove(self.transformer.raw_output());
                    }
                    CaptureMode::JsonOnly => {
                        // _json: inject full payload as string (legacy path)
                        if let Ok(json_str) = std::str::from_utf8(&msg.payload) {
                            d.insert("_json".to_string(), Value::String(json_str.to_string()));
                        }
                        // _raw stays NULL, so drop anything @renamed extracted into it.
                        d.remove(self.transformer.raw_output());
                    }
                    CaptureMode::ExtractedOnly => {
                        // Neither _json nor _raw
                        d.remove(self.transformer.raw_output());
                    }
                }
            }

            (d, None)
        };

        let capture_full_raw = captures_payload_as_raw(
            capture_mode,
            self.config.metadata.capture_raw,
            data.contains_key(self.config.metadata.raw_output.as_str()),
            &msg.payload,
        );
        if capture_full_raw && let Ok(raw_str) = std::str::from_utf8(&msg.payload) {
            data.insert(
                self.config.metadata.raw_output.clone(),
                Value::String(raw_str.to_string()),
            );
        }

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
        let fields_before = data.len();
        self.enrichment.enrich(&mut data);
        if tracing::enabled!(tracing::Level::TRACE) {
            let enrichments_added = data.len().saturating_sub(fields_before);
            if enrichments_added > 0 {
                trace!(
                    table = %table,
                    enrichments_added = enrichments_added,
                    "Message enriched"
                );
            }
        }

        // Build offset for commit tracking
        let kafka_offset =
            KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);

        debug!(table = %table, fields = data.len(), "Message processed");
        Ok(ProcessedMessage {
            table,
            data,
            raw_payload,
            kafka_offset,
            fell_back_from,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::clickhouse::SchemaCache;
    use crate::column_meta::{ColumnDirectivesConfig, ColumnMetaCache};
    use crate::config::{CaptureMode, ComputedColumnsConfig, Config};
    use crate::kafka::KafkaMessage;
    use crate::metrics::counting::counted;
    use crate::pipeline::capture::CaptureOverrides;
    use crate::pipeline::enrichment::EnrichmentPipeline;
    use crate::routing::Router;
    use crate::transform::{ComputedColumnCache, HeaderExtractor, Transformer};

    use super::MessageProcessor;

    /// Helper: build all dependencies from default configs for processor tests.
    struct TestHarness {
        config: Config,
        router: Router,
        transformer: Transformer,
        extractor: HeaderExtractor,
        enrichment: EnrichmentPipeline,
        schema_cache: Arc<SchemaCache>,
        col_meta_cache: ColumnMetaCache,
        computed_column_cache: ComputedColumnCache,
        capture_overrides: CaptureOverrides,
        absent_tables: crate::pipeline::types::AbsentTables,
        default_table: String,
    }

    impl TestHarness {
        fn new() -> Self {
            Self::with_config(Config::default())
        }

        fn with_config(config: Config) -> Self {
            let router = Router::with_metadata(&config.routing, &config.metadata);
            let transformer = Transformer::new(
                &config.timestamp_dq,
                &config.metadata,
                &config.field_sanitization,
            );
            let extractor = HeaderExtractor::new(&config.metadata, &config.routing);
            let enrichment = EnrichmentPipeline {
                ip_fields: config.enrichment.ip_fields.clone(),
                geoip: None,
                reputation: None,
                risk: None,
            };
            let schema_cache = Arc::new(SchemaCache::new(300));
            let col_meta_cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
            let computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
            let capture_overrides = CaptureOverrides::new(&config.metadata);
            let default_table = format!(
                "{}.{}",
                config.routing.default_db, config.routing.default_table
            );

            Self {
                config,
                router,
                transformer,
                extractor,
                enrichment,
                schema_cache,
                col_meta_cache,
                computed_column_cache,
                capture_overrides,
                absent_tables: crate::pipeline::types::AbsentTables::new(
                    std::time::Duration::from_secs(60),
                    16,
                ),
                default_table,
            }
        }

        fn processor(&self) -> MessageProcessor<'_> {
            MessageProcessor {
                config: &self.config,
                router: &self.router,
                transformer: &self.transformer,
                extractor: &self.extractor,
                json_primary_mode: false, // legacy path for unit tests (no schema)
                enrichment: &self.enrichment,
                schema_cache: &self.schema_cache,
                col_meta_cache: &self.col_meta_cache,
                field_mapping_cache: None,
                computed_column_cache: &self.computed_column_cache,
                capture_overrides: &self.capture_overrides,
                absent_tables: &self.absent_tables,
                default_table: &self.default_table,
            }
        }

        fn processor_json_primary(&self) -> MessageProcessor<'_> {
            MessageProcessor {
                config: &self.config,
                router: &self.router,
                transformer: &self.transformer,
                extractor: &self.extractor,
                json_primary_mode: true,
                enrichment: &self.enrichment,
                schema_cache: &self.schema_cache,
                col_meta_cache: &self.col_meta_cache,
                field_mapping_cache: None,
                computed_column_cache: &self.computed_column_cache,
                capture_overrides: &self.capture_overrides,
                absent_tables: &self.absent_tables,
                default_table: &self.default_table,
            }
        }

        fn make_msg(&self, payload: &[u8]) -> KafkaMessage {
            self.make_msg_on("test-events", payload)
        }

        fn make_msg_on(&self, topic: &str, payload: &[u8]) -> KafkaMessage {
            KafkaMessage {
                payload: payload.to_vec(),
                topic: Arc::from(topic),
                partition: 0,
                offset: 1,
                key: None,
                timestamp_ms: Some(1700000000000),
            }
        }
    }

    // ========================================================================
    // Format detection and parsing
    // ========================================================================

    #[test]
    fn process_valid_json_produces_processed_message() {
        let harness = TestHarness::new();
        let proc = harness.processor();
        let payload = serde_json::to_vec(&json!({
            "event_category": "security",
            "action": "login",
            "user": "alice"
        }))
        .expect("serialize");

        let msg = harness.make_msg(&payload);
        let result = proc.process(&msg);

        assert!(
            result.is_ok(),
            "Processing valid JSON failed: {:?}",
            result.err()
        );
        let processed = result.expect("processed");
        assert!(
            !processed.table.is_empty(),
            "Processed message should have a non-empty table"
        );
        assert!(
            processed.data.contains_key("action"),
            "Flattened data should contain 'action' field"
        );
    }

    #[test]
    fn process_invalid_json_returns_error() {
        let harness = TestHarness::new();
        let proc = harness.processor();
        let msg = harness.make_msg(b"{not valid json at all!!!}");

        let result = proc.process(&msg);
        assert!(result.is_err(), "Processing invalid JSON should fail");
        // Use pattern match since ProcessedMessage has no Debug impl
        match result {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("JSON parse error") || err_msg.contains("parse"),
                    "Error should mention JSON parsing, got: {err_msg}"
                );
            }
            Ok(_) => panic!("Invalid JSON must not produce Ok"),
        }
    }

    #[test]
    fn process_empty_payload_returns_error() {
        let harness = TestHarness::new();
        let proc = harness.processor();
        let msg = harness.make_msg(b"");

        let reason = proc
            .process(&msg)
            .err()
            .expect("Empty payload should fail")
            .to_string();
        assert!(
            reason.contains("payload is not JSON") && reason.contains("empty payload"),
            "an empty record must say it was empty, got: {reason}"
        );
    }

    /// A Cruise Control metrics record: a serde version and class id where
    /// JSON wants `{`, followed by bytes that must stay out of the reason.
    const CRUISE_CONTROL_RECORD: &[u8] = &[
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x8c, 0xde, 0xad, 0xbe, 0xef,
    ];

    #[test]
    fn a_binary_record_is_refused_as_not_json_with_its_leading_bytes() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let err = proc
            .process(&harness.make_msg(CRUISE_CONTROL_RECORD))
            .err()
            .expect("a binary record must be rejected");
        assert!(matches!(err, crate::Error::NotJson { .. }), "got {err:?}");
        let reason = err.to_string();

        assert!(
            reason.contains("payload is not JSON"),
            "reason must name the case, got: {reason}"
        );
        assert!(
            reason.contains("00 00 02 00 00 00 01 8c"),
            "reason must carry the first 8 bytes as hex, got: {reason}"
        );
        assert!(
            !reason.contains("de ad"),
            "reason must stop at 8 bytes, got: {reason}"
        );
    }

    /// `{"event_category": "x"}` as a MessagePack map: fixmap(1), fixstr(14)
    /// "event_category", fixstr(1) "x".
    const MESSAGEPACK_RECORD: &[u8] = b"\x81\xaeevent_category\xa1x";

    #[test]
    fn a_messagepack_record_is_dead_lettered_as_not_json() {
        use scalo::memory::{MemoryGuard, MemoryGuardConfig};

        use crate::buffer::BufferManager;
        use crate::pipeline::coordinator::BatchCoordinator;
        use crate::pipeline::pending_schema::{OnFull, PendingSchemaBuffer, PendingSchemaConfig};

        let harness = TestHarness::new();
        let proc = harness.processor();
        let msg = harness.make_msg(MESSAGEPACK_RECORD);

        let err = proc
            .process(&msg)
            .err()
            .expect("a MessagePack record must be refused, never loaded");
        assert!(matches!(err, crate::Error::NotJson { .. }), "got {err:?}");
        let reason = err.to_string();
        assert!(
            reason.contains("payload is not JSON, leading bytes 81 ae 65 76 65 6e 74 5f"),
            "reason must name the refusal and the first 8 bytes, got: {reason}"
        );

        // The refusal takes the path every permanent failure takes: one dead
        // letter with the reason and the original bytes, one error counted,
        // nothing buffered for ClickHouse.
        let guard = MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1_073_741_824,
            ..Default::default()
        });
        let mut buffer_manager = BufferManager::new(&harness.config.buffer);
        let mut capture_overrides = CaptureOverrides::new(&harness.config.metadata);
        let mut field_mapping_cache = None;
        let mut computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let mut pending = PendingSchemaBuffer::new(PendingSchemaConfig {
            max_per_table: 100,
            max_total: 1000,
            max_age: std::time::Duration::from_secs(30),
            on_full: OnFull::DeadLetter,
        });
        let outcome = BatchCoordinator {
            buffer_manager: &mut buffer_manager,
            capture_overrides: &mut capture_overrides,
            field_mapping_cache: &mut field_mapping_cache,
            computed_column_cache: &mut computed_column_cache,
            metrics: &None,
            memory_guard: &guard,
            pending_schema: &mut pending,
        }
        .apply_results(vec![Err(err)], std::slice::from_ref(&msg));

        assert_eq!(outcome.errors, 1, "the refusal is counted");
        assert_eq!(outcome.processed, 0, "the record was loaded");
        assert_eq!(outcome.pending, 0, "the record waits on a schema");
        assert_eq!(outcome.dead_letters.len(), 1);
        assert_eq!(outcome.dead_letters[0].reason, reason);
        assert_eq!(
            outcome.dead_letters[0].payload, MESSAGEPACK_RECORD,
            "the DLQ keeps the bytes that arrived"
        );
        assert_eq!(buffer_manager.stats().pending_rows, 0);

        // No format lock: the next JSON record on the same processor loads.
        let json = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
        assert!(
            proc.process(&harness.make_msg(&json)).is_ok(),
            "a refused record changed how the next JSON record is handled"
        );
    }

    #[test]
    fn process_truncated_json_returns_error() {
        let harness = TestHarness::new();
        let proc = harness.processor();
        let msg = harness.make_msg(b"{\"key\": \"val");

        let result = proc.process(&msg);
        assert!(result.is_err(), "Truncated JSON should fail parsing");
    }

    // ========================================================================
    // capture_mode: verify the ACTION (which columns the processor writes)
    // ALIGNS with the cascade setting -- not merely that the config parsed.
    // Offline: asserts the produced row, no ClickHouse required.
    // ========================================================================

    fn config_with_capture_mode(mode: CaptureMode) -> Config {
        let mut config = Config::default();
        config.metadata.capture_mode = mode;
        config
    }

    fn capture_sample_payload() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "event_category": "security",
            "action": "login",
            "user": "alice"
        }))
        .expect("serialize")
    }

    #[test]
    fn capture_mode_full_action_populates_json() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let proc = harness.processor();
        let processed = proc
            .process(&harness.make_msg(&capture_sample_payload()))
            .expect("processed");
        assert!(
            processed.data.contains_key("_json"),
            "capture_mode=full must populate _json in the written row"
        );
    }

    #[test]
    fn capture_mode_raw_only_action_sets_raw_not_json() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::RawOnly));
        let proc = harness.processor();
        let payload = capture_sample_payload();
        let processed = proc
            .process(&harness.make_msg(&payload))
            .expect("processed");

        assert!(
            !processed.data.contains_key("_json"),
            "capture_mode=raw_only must NOT populate _json"
        );
        let raw = processed
            .data
            .get("_raw")
            .and_then(|v| v.as_str())
            .expect("capture_mode=raw_only must populate _raw");
        assert_eq!(
            raw.as_bytes(),
            payload.as_slice(),
            "_raw must be the full original payload"
        );
    }

    #[test]
    fn capture_mode_json_only_action_sets_json_not_raw() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::JsonOnly));
        let proc = harness.processor();
        let payload = capture_sample_payload();
        let processed = proc
            .process(&harness.make_msg(&payload))
            .expect("processed");

        let json = processed
            .data
            .get("_json")
            .and_then(|v| v.as_str())
            .expect("capture_mode=json_only must populate _json");
        assert_eq!(
            json.as_bytes(),
            payload.as_slice(),
            "_json must be the full original payload"
        );
        assert!(
            !processed.data.contains_key("_raw"),
            "capture_mode=json_only must NOT populate _raw"
        );
    }

    #[test]
    fn capture_mode_extracted_only_action_sets_neither() {
        let harness =
            TestHarness::with_config(config_with_capture_mode(CaptureMode::ExtractedOnly));
        let proc = harness.processor();
        let processed = proc
            .process(&harness.make_msg(&capture_sample_payload()))
            .expect("processed");
        assert!(
            !processed.data.contains_key("_json"),
            "capture_mode=extracted_only must NOT populate _json"
        );
        assert!(
            !processed.data.contains_key("_raw"),
            "capture_mode=extracted_only must NOT populate _raw"
        );
    }

    // ========================================================================
    // routing: verify the routing config drives the ACTUAL landing table,
    // not just that the fields parsed. Offline.
    // ========================================================================

    #[test]
    fn routing_config_drives_actual_table() {
        let mut config = Config::default();
        config.routing.db_fields = vec![]; // shared schema -> default_db
        config.routing.table_fields = vec!["event_category".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "fallback".to_string();
        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let routed = serde_json::to_vec(&json!({"event_category": "auth", "action": "x"}))
            .expect("serialize");
        let processed = proc.process(&harness.make_msg(&routed)).expect("processed");
        assert_eq!(
            processed.table, "dfe.auth",
            "table_fields=event_category must route to dfe.auth"
        );

        let unrouted = serde_json::to_vec(&json!({"action": "x"})).expect("serialize");
        let fallback = proc
            .process(&harness.make_msg(&unrouted))
            .expect("processed");
        assert_eq!(
            fallback.table, "dfe.fallback",
            "missing table field must fall back to default_table"
        );
    }

    // ========================================================================
    // Routing
    // ========================================================================

    #[test]
    fn process_routes_to_table_from_event_category() {
        let mut config = Config::default();
        config.routing.table_fields = vec!["event_category".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "network",
            "src_ip": "10.0.0.1"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(processed.table, "dfe.network");
    }

    #[test]
    fn process_routes_to_default_when_no_table_field() {
        let mut config = Config::default();
        config.routing.table_fields = vec!["event_category".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "unrouted".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"action": "test"})).expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(processed.table, "dfe.unrouted");
    }

    #[test]
    fn process_absent_table_falls_back_to_default_table() {
        let mut config = Config::default();
        config.routing.table_fields = vec!["_source".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let mut harness = TestHarness::with_config(config);
        harness
            .absent_tables
            .insert("dfe.acme_widgets", std::time::Instant::now());
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"_source": "acme_widgets"})).expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(
            processed.table, "dfe.main",
            "an unknown source must land in the default table, not the DLQ"
        );
        assert_eq!(
            processed.fell_back_from.as_deref(),
            Some("dfe.acme_widgets"),
            "the rewrite must be countable, not invisible after the first backlog"
        );
    }

    #[test]
    fn process_unknown_table_not_yet_confirmed_absent_keeps_its_route() {
        // Nothing is in the absent set, so routing is untouched — a ClickHouse
        // outage must never silently redirect a source to the default table.
        let mut config = Config::default();
        config.routing.table_fields = vec!["_source".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"_source": "acme_widgets"})).expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(processed.table, "dfe.acme_widgets");
    }

    /// Routing config matching a deployment: `_source` names the table, the
    /// landing table is `dfe.main`.
    fn config_routing_by_source() -> Config {
        let mut config = Config::default();
        config.routing.table_fields = vec!["_source".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();
        config
    }

    #[test]
    fn a_record_with_no_routing_field_on_a_source_topic_lands_in_that_source_table() {
        // The elastic case: the transform emits ECS, which carries no _source,
        // and `cisco-ios_load` is the output topic of exactly one source (#184).
        let harness = TestHarness::with_config(config_routing_by_source());
        let proc = harness.processor();
        let payload = serde_json::to_vec(&json!({
            "event": {"code": "IPACCESSLOGRP", "action": "deny"},
            "observer": {"vendor": "Cisco"}
        }))
        .expect("serialize");

        let processed = proc
            .process(&harness.make_msg_on("cisco-ios_load", &payload))
            .expect("should succeed");
        assert_eq!(
            processed.table, "dfe.cisco-ios",
            "the topic names the source, so the row lands in that source's table"
        );
    }

    #[test]
    fn a_record_with_no_routing_field_on_a_source_topic_is_counted() {
        let harness = TestHarness::with_config(config_routing_by_source());
        let proc = harness.processor();
        let payload = serde_json::to_vec(&json!({"message": "no source here"})).expect("serialize");

        // Routing by topic covers for the producer, so the count is what says
        // the producer stopped naming the table.
        let hits = counted("dfe_loader_routing_field_absent_total", || {
            let processed = proc
                .process(&harness.make_msg_on("elastic_load", &payload))
                .expect("should succeed");
            assert_eq!(processed.table, "dfe.elastic");
        });
        assert_eq!(hits, 1, "one record, one count");
    }

    #[test]
    fn the_landing_topic_still_falls_back_to_the_landing_table() {
        // Landing records genuinely carry no source, and `main_land` strips to
        // the default table, so the fallback is right there.
        let harness = TestHarness::with_config(config_routing_by_source());
        let proc = harness.processor();
        let unnamed = serde_json::to_vec(&json!({"message": "no source here"})).expect("serialize");

        let hits = counted("dfe_loader_routing_field_absent_total", || {
            let processed = proc
                .process(&harness.make_msg_on("main_land", &unnamed))
                .expect("should succeed");
            assert_eq!(processed.table, "dfe.main");
        });
        assert_eq!(hits, 0, "ordinary landing traffic is not a producer defect");
    }

    #[test]
    fn a_topic_with_no_source_suffix_still_falls_back_to_the_landing_table() {
        // An arbitrary topic name is not `{source}_land` or `{source}_load`, so
        // it names no source to route by.
        let harness = TestHarness::with_config(config_routing_by_source());
        let proc = harness.processor();
        let unnamed = serde_json::to_vec(&json!({"message": "no source here"})).expect("serialize");

        let hits = counted("dfe_loader_routing_field_absent_total", || {
            let processed = proc
                .process(&harness.make_msg_on("dfe.events", &unnamed))
                .expect("should succeed");
            assert_eq!(processed.table, "dfe.main");
        });
        assert_eq!(hits, 0);
    }

    #[test]
    fn a_record_that_names_its_table_is_not_counted() {
        let harness = TestHarness::with_config(config_routing_by_source());
        let proc = harness.processor();
        let named = serde_json::to_vec(&json!({"_source": "elastic"})).expect("serialize");

        let hits = counted("dfe_loader_routing_field_absent_total", || {
            let processed = proc
                .process(&harness.make_msg_on("elastic_load", &named))
                .expect("should succeed");
            assert_eq!(processed.table, "dfe.elastic");
        });
        assert_eq!(hits, 0, "the record named its own table");
    }

    #[test]
    fn process_nested_table_field_via_dot_notation() {
        let mut config = Config::default();
        config.routing.table_fields = vec![
            "tags.event.category".to_string(),
            "event_category".to_string(),
        ];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "tags": {
                "event": {
                    "category": "endpoint"
                }
            },
            "action": "test"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(processed.table, "dfe.endpoint");
    }

    // ========================================================================
    // Capture mode
    // ========================================================================

    #[test]
    fn process_full_capture_includes_json_field() {
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::Full;
        config.routing.table_fields = vec!["type".to_string()];
        config.routing.default_table = "events".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "type": "auth",
            "user": "bob"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert!(
            processed.data.contains_key("_json"),
            "Full capture should inject _json field. Keys: {:?}",
            processed.data.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn process_extracted_only_omits_json_and_raw() {
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::ExtractedOnly;
        config.routing.table_fields = vec!["type".to_string()];
        config.routing.default_table = "events".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "type": "metric",
            "value": 42
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert!(
            !processed.data.contains_key("_json"),
            "ExtractedOnly should not have _json"
        );
        assert!(
            !processed.data.contains_key("_raw"),
            "ExtractedOnly should not have _raw"
        );
    }

    // ========================================================================
    // Kafka offset tracking
    // ========================================================================

    #[test]
    fn process_preserves_kafka_offset() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"event_category": "test"})).expect("serialize");
        let mut msg = harness.make_msg(&payload);
        msg.partition = 7;
        msg.offset = 42;

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(processed.kafka_offset.partition, 7);
        assert_eq!(processed.kafka_offset.offset, 42);
        assert_eq!(&*processed.kafka_offset.topic, "test-events");
    }

    // ========================================================================
    // Complex payloads
    // ========================================================================

    #[test]
    fn process_deeply_nested_json() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "deep",
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": "leaf_value"
                        }
                    }
                }
            },
            "tags": {
                "env": "prod",
                "region": "ap-southeast-2"
            }
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let result = proc.process(&msg);
        assert!(
            result.is_ok(),
            "Deeply nested JSON should process: {:?}",
            result.err()
        );
        let processed = result.expect("processed");
        assert!(
            processed.data.len() > 3,
            "Flattened data should have more than 3 fields, got {}",
            processed.data.len()
        );
    }

    #[test]
    fn process_unicode_field_values() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "i18n",
            "user": "\u{1F600}emoji_user",
            "message": "日本語テスト",
            "path": "C:\\Windows\\System32",
            "newlines": "line1\nline2\ttab"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let result = proc.process(&msg);
        assert!(
            result.is_ok(),
            "Unicode payload should process: {:?}",
            result.err()
        );
        let processed = result.expect("processed");
        assert!(processed.data.contains_key("message"));
    }

    // ========================================================================
    // CaptureMode — RawOnly variant
    // ========================================================================

    #[test]
    fn process_raw_only_capture_writes_full_payload_to_raw() {
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::RawOnly;
        config.metadata.raw_output = "_raw".to_string();
        config.routing.table_fields = vec!["type".to_string()];
        config.routing.default_table = "events".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let original_json = json!({
            "type": "syslog",
            "message": "original payload text",
            "severity": 3
        });
        let payload = serde_json::to_vec(&original_json).expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        // _raw must contain the ENTIRE Kafka payload as UTF-8 string
        assert!(
            processed.data.contains_key("_raw"),
            "RawOnly mode must populate _raw. Keys: {:?}",
            processed.data.keys().collect::<Vec<_>>()
        );
        let raw_value = processed.data["_raw"]
            .as_str()
            .expect("_raw should be string");
        // Payload bytes as UTF-8 string (unchanged)
        assert!(
            raw_value.contains("syslog") && raw_value.contains("original payload text"),
            "_raw should contain original JSON payload: {raw_value}"
        );

        // _json must NOT be present in RawOnly mode
        assert!(
            !processed.data.contains_key("_json"),
            "RawOnly should not populate _json"
        );
    }

    #[test]
    fn process_raw_only_preserves_raw_payload_not_passed_to_inserter() {
        // In the legacy path (no schema cache), RawOnly writes to _raw via UTF-8.
        // raw_payload (Arc<[u8]>) is None for RawOnly/ExtractedOnly in json_primary;
        // in legacy it's also None.
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::RawOnly;

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert!(
            processed.raw_payload.is_none(),
            "RawOnly: raw_payload Arc should be None (legacy path)"
        );
    }

    #[test]
    fn process_full_capture_preserves_raw_and_injects_json() {
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::Full;

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "auth",
            "action": "login"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        // Full mode injects _json (as string in legacy path)
        assert!(
            processed.data.contains_key("_json"),
            "Full mode should populate _json in legacy path"
        );
        let json_val = processed.data["_json"]
            .as_str()
            .expect("_json should be string in legacy path");
        assert!(
            json_val.contains("action") && json_val.contains("login"),
            "_json should contain original JSON: {json_val}"
        );
    }

    // ========================================================================
    // DLQ routing on invalid messages
    // ========================================================================

    #[test]
    fn process_routes_to_dlq_with_routing_rule_failure() {
        // Set up a CEL routing rule that produces a DLQ routing outcome.
        // Simplest DLQ trigger: compat_v2_source with no source/table resolvable.
        // Since Router's DLQ path is complex, we test the error mapping instead.
        // Simpler: test that parse failures (pre-routing) become errors.
        let harness = TestHarness::new();
        let proc = harness.processor();

        // Totally invalid JSON should route to DLQ via error return
        let msg = harness.make_msg(b"\xff\xff\xff not valid anything \xff");
        let result = proc.process(&msg);

        match result {
            Err(e) => {
                let em = format!("{e}");
                assert!(
                    matches!(e, crate::Error::NotJson { .. }) && em.contains("payload is not JSON"),
                    "Error should indicate a format problem for DLQ routing: {em}"
                );
            }
            Ok(_) => panic!("Invalid payload must fail (DLQ-bound)"),
        }
    }

    #[test]
    fn a_top_level_array_is_named_rather_than_encoded() {
        // The reported failure was a ClickHouse encode error naming an empty
        // column and quoting the whole array back (#128). An array the fan-out
        // left alone is not a batch of records, so it is refused by shape.
        let harness = TestHarness::new();
        let proc = harness.processor();

        for payload in [b"[1, 2, 3]".as_slice(), br#"[{"a": 1}]"#.as_slice()] {
            match proc.process(&harness.make_msg(payload)) {
                Err(e) => assert!(
                    format!("{e}").contains("JSON array, not a record object"),
                    "the error must name the shape: {e}"
                ),
                Ok(_) => panic!("an array is not a record and must not be buffered"),
            }
        }
    }

    // ========================================================================
    // Enrichment integration (None enricher — no-op path)
    // ========================================================================

    #[test]
    fn process_with_none_enricher_leaves_data_unenriched() {
        let harness = TestHarness::new(); // enrichment pipeline has all None

        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "net",
            "src_ip": "8.8.8.8",
            "action": "deny"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        // No enrichment components configured → no geo/rep/risk fields
        assert!(
            !processed.data.contains_key("geo_country_code"),
            "No GeoIP configured → no geo fields"
        );
        assert!(
            !processed.data.contains_key("rep_is_vpn"),
            "No reputation configured → no rep fields"
        );
        assert!(
            !processed.data.contains_key("risk_score"),
            "No risk scorer configured → no risk fields"
        );

        // Original fields preserved
        assert_eq!(
            processed.data.get("src_ip").and_then(|v| v.as_str()),
            Some("8.8.8.8")
        );
        assert_eq!(
            processed.data.get("action").and_then(|v| v.as_str()),
            Some("deny")
        );
    }

    #[test]
    fn process_with_reputation_enricher_integrates() {
        // End-to-end: processor → enrichment pipeline injects rep fields
        use crate::enrich::reputation::{ReputationEnricher, ThreatSource, ThreatType};
        use std::net::IpAddr;

        let enricher = ReputationEnricher::new();
        let addr: IpAddr = "192.0.2.77".parse().expect("valid IP");
        enricher.add_ip(addr, ThreatType::Tor, ThreatSource::TorProject);

        let mut config = Config::default();
        config.enrichment.ip_fields = vec!["src_ip".to_string()];

        let harness = {
            let mut h = TestHarness::with_config(config);
            h.enrichment = EnrichmentPipeline {
                ip_fields: vec!["src_ip".to_string()],
                geoip: None,
                reputation: Some(enricher),
                risk: None,
            };
            h
        };
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "net",
            "src_ip": "192.0.2.77"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        // Tor detection was injected by the enrichment pipeline
        assert_eq!(
            processed
                .data
                .get("rep_is_tor")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "Expected rep_is_tor=true. Keys: {:?}",
            processed.data.keys().collect::<Vec<_>>()
        );
    }

    // ========================================================================
    // Field mapping applied — read-only cache path
    // ========================================================================

    #[test]
    fn process_without_field_mapping_cache_works() {
        // field_mapping_cache=None is the common case — test it doesn't panic
        let harness = TestHarness::new();
        let proc = harness.processor();
        // field_mapping_cache=None is the default in the harness

        let payload = serde_json::to_vec(&json!({
            "event_category": "events",
            "original_field": "value"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        // Without a field mapping cache, fields pass through unchanged
        assert_eq!(
            processed
                .data
                .get("original_field")
                .and_then(|v| v.as_str()),
            Some("value")
        );
    }

    // ========================================================================
    // Computed columns (empty config — no-op path)
    // ========================================================================

    #[test]
    fn process_empty_computed_columns_is_noop() {
        // Default ComputedColumnsConfig has no columns — should not modify data
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "metrics",
            "value": 42
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        // With no computed columns configured, original fields untouched.
        // Other transforms may add _org_id, _timestamp_*, etc. (common header).
        assert_eq!(
            processed
                .data
                .get("value")
                .and_then(serde_json::Value::as_i64),
            Some(42)
        );
    }

    // ========================================================================
    // raw_payload preservation across capture modes (legacy path)
    // ========================================================================

    #[test]
    fn process_legacy_path_raw_payload_always_none() {
        // Legacy path (no schema cache) never populates raw_payload Arc.
        // It's only set by the json_primary extractor path.
        for mode in [
            CaptureMode::Full,
            CaptureMode::RawOnly,
            CaptureMode::ExtractedOnly,
        ] {
            let mut config = Config::default();
            config.metadata.enabled = true;
            config.metadata.capture_mode = mode;

            let harness = TestHarness::with_config(config);
            let proc = harness.processor();

            let payload = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
            let msg = harness.make_msg(&payload);

            let processed = proc.process(&msg).expect("should succeed");
            assert!(
                processed.raw_payload.is_none(),
                "Legacy path raw_payload should always be None for mode {mode:?}"
            );
        }
    }

    #[test]
    fn process_extracted_only_mode_omits_both_json_and_raw() {
        let mut config = Config::default();
        config.metadata.enabled = true;
        config.metadata.capture_mode = CaptureMode::ExtractedOnly;

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "metrics",
            "metric_name": "cpu_pct",
            "value": 42.5
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");

        assert!(
            !processed.data.contains_key("_json"),
            "ExtractedOnly must not contain _json. Keys: {:?}",
            processed.data.keys().collect::<Vec<_>>()
        );
        assert!(
            !processed.data.contains_key("_raw"),
            "ExtractedOnly must not contain _raw. Keys: {:?}",
            processed.data.keys().collect::<Vec<_>>()
        );

        // Schema fields (the "extracted") should still be present
        assert!(processed.data.contains_key("metric_name"));
        assert!(processed.data.contains_key("value"));
    }

    // ========================================================================
    // Multiple partitions / offsets — ensure offsets passed through correctly
    // ========================================================================

    #[test]
    fn process_records_correct_offset_across_many_messages() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        for (partition, offset) in [(0i32, 100i64), (1, 200), (5, 999_999), (3, i64::MAX)] {
            let payload = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
            let mut msg = harness.make_msg(&payload);
            msg.partition = partition;
            msg.offset = offset;

            let processed = proc.process(&msg).expect("should succeed");
            assert_eq!(processed.kafka_offset.partition, partition);
            assert_eq!(processed.kafka_offset.offset, offset);
        }
    }

    // ========================================================================
    // Routing: DLQ path
    // ========================================================================

    #[test]
    fn process_malformed_json_goes_to_error() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        // Truncated JSON
        let payload = b"{\"event_category\":";
        let msg = harness.make_msg(payload);
        let result = proc.process(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn process_large_json_payload() {
        // Sanity check: processor doesn't choke on large payloads
        let harness = TestHarness::new();
        let proc = harness.processor();

        let mut big = json!({"event_category": "big_table"});
        // Add a big string field
        big["payload"] = json!("x".repeat(100_000));
        let payload = serde_json::to_vec(&big).expect("serialize");
        let msg = harness.make_msg(&payload);
        let processed = proc.process(&msg).expect("should succeed");
        // Should still produce a valid ProcessedMessage
        assert!(!processed.data.is_empty());
    }

    #[test]
    fn process_json_with_deeply_nested_routing_field() {
        // Router should extract routing field from deep dot path
        let mut config = Config::default();
        config.routing.table_fields = vec!["level1.level2.level3.table_name".into()];
        let harness = TestHarness::with_config(config);
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "level1": {
                "level2": {
                    "level3": {
                        "table_name": "deep_table"
                    }
                }
            }
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        // Table should be routed to deep_table
        assert!(
            processed.table.contains("deep_table"),
            "Got: {}",
            processed.table
        );
    }

    #[test]
    fn process_unicode_payload_preserved() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({
            "event_category": "unicode",
            "message": "Hello, 世界! 🔥",
            "user": "Zoë"
        }))
        .expect("serialize");
        let msg = harness.make_msg(&payload);

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(
            processed.data.get("message").and_then(|v| v.as_str()),
            Some("Hello, 世界! 🔥")
        );
    }

    // ========================================================================
    // Multiple offsets / partitions — ensures correct topic propagation
    // ========================================================================

    #[test]
    fn process_kafka_offset_topic_shared() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
        let mut msg = harness.make_msg(&payload);
        msg.topic = Arc::from("special-topic");

        let processed = proc.process(&msg).expect("should succeed");
        assert_eq!(&*processed.kafka_offset.topic, "special-topic");
    }

    #[test]
    fn process_empty_json_object() {
        // Empty {} object — routes to default, no enrichable fields
        let harness = TestHarness::new();
        let proc = harness.processor();

        let payload = b"{}";
        let msg = harness.make_msg(payload);
        let processed = proc.process(&msg).expect("should succeed");
        // Table should route to default
        assert!(!processed.table.is_empty());
    }

    #[test]
    fn process_string_instead_of_object_still_works() {
        // Non-object JSON payload — the transformer wraps/errors gracefully
        let harness = TestHarness::new();
        let proc = harness.processor();

        // A JSON string
        let payload = b"\"just a string\"";
        let msg = harness.make_msg(payload);
        let result = proc.process(&msg);
        // Should either process (router with default) or error — both are valid
        // depending on the non-object handling in transformer.
        // What matters: no panic.
        let _ = result;
    }

    // ========================================================================
    // json_primary mode: SchemaPending on cache miss (#36)
    // ========================================================================

    #[test]
    fn process_json_primary_cache_miss_returns_schema_pending() {
        let harness = TestHarness::new();
        let proc = harness.processor_json_primary();
        // schema_cache is empty -> any routed table misses.
        let msg = harness.make_msg(br#"{"event_category":"security","action":"login"}"#);
        match proc.process(&msg) {
            Err(crate::Error::SchemaPending { table }) => {
                assert!(
                    !table.is_empty(),
                    "SchemaPending should carry the routed table"
                );
            }
            Ok(_) => panic!("expected SchemaPending, got Ok(ProcessedMessage)"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        }
    }

    #[test]
    fn process_legacy_flatten_does_not_return_schema_pending() {
        // Default harness uses json_primary_mode = false (legacy transformer path).
        let harness = TestHarness::new();
        let proc = harness.processor();
        let msg = harness.make_msg(br#"{"event_category":"security","action":"login"}"#);
        let result = proc.process(&msg);
        assert!(
            result.is_ok(),
            "legacy_flatten must not return SchemaPending: {:?}",
            result.err()
        );
    }

    /// Regression test for #36: a column with an `@renamed` directive must be
    /// populated via the extractor path. The bug was that a schema cache miss
    /// in json_primary mode silently fell through to the transformer (which
    /// ignores `@renamed`), NULLing the renamed column. Now a miss buffers
    /// (`SchemaPending`); once the schema is cached the extractor applies the
    /// rename. This proves the previously-lost data is correct.
    #[test]
    fn json_primary_applies_renamed_after_schema_cached_36() {
        use rustc_hash::FxHashMap;

        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};
        use crate::column_meta::ColumnDirectives;

        let harness = TestHarness::new();
        let msg = harness.make_msg(br#"{"src_field":"hello"}"#);

        // 1) Schema not cached → buffered (SchemaPending), NOT silently
        //    transformed. Discover the routed table from the error.
        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        // 2) Cache a schema whose `dst_field` column is @renamed from src_field.
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "dst_field".to_string(),
            ColumnDirectives {
                renamed: vec!["src_field".to_string()],
                ..Default::default()
            },
        );
        harness.col_meta_cache.apply_ddl(&table, ddl);

        let (db, tbl) = table.split_once('.').expect("db.table");
        let schema = TableSchema {
            database: db.to_string(),
            table: tbl.to_string(),
            columns: vec![ColumnInfo {
                name: "dst_field".to_string(),
                type_name: "String".to_string(),
                parsed_type: ParsedType::parse("String"),
                position: 0,
                default_kind: String::new(),
                default_expression: String::new(),
                comment: String::new(),
                is_in_primary_key: false,
                is_in_sorting_key: false,
            }],
            comment: String::new(),
        };
        harness.schema_cache.insert(table.clone(), schema);

        // 3) Reprocess: extractor path applies @renamed → dst_field populated
        //    from src_field. Before #36's fix this column would have been NULL.
        let processed = harness
            .processor_json_primary()
            .process(&msg)
            .expect("extractor path should succeed once schema is cached");
        assert_eq!(
            processed.data.get("dst_field"),
            Some(&serde_json::Value::String("hello".to_string())),
            "@renamed must map src_field → dst_field"
        );
        assert!(
            !processed.data.contains_key("src_field"),
            "source field should be renamed away, not left at top level"
        );
    }

    /// Regression test for #187: the topic the message arrived on has to reach
    /// the extractor, or the `topic_name` fallback dfe-schemas ships on every
    /// timeseries table resolves nowhere and `_source` lands NULL.
    #[test]
    fn json_primary_labels_source_from_the_message_topic_187() {
        use rustc_hash::FxHashMap;

        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};
        use crate::column_meta::parse_directives;

        let harness = TestHarness::new();
        let msg = harness.make_msg_on("cisco-ios_load", br#"{"message":"ecs, no source"}"#);

        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        // Verbatim from dfe-schemas common-header/timeseries.yaml.
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "_source".to_string(),
            parse_directives(
                "@source: first(_source) | topic_name - Data source label (e.g. beats, syslog, crowdstrike-edr)",
            ),
        );
        harness.col_meta_cache.apply_ddl(&table, ddl);

        let (db, tbl) = table.split_once('.').expect("db.table");
        let schema = TableSchema {
            database: db.to_string(),
            table: tbl.to_string(),
            columns: vec![ColumnInfo {
                name: "_source".to_string(),
                type_name: "LowCardinality(String)".to_string(),
                parsed_type: ParsedType::parse("LowCardinality(String)"),
                position: 0,
                default_kind: String::new(),
                default_expression: String::new(),
                comment: String::new(),
                is_in_primary_key: false,
                is_in_sorting_key: false,
            }],
            comment: String::new(),
        };
        harness.schema_cache.insert(table.clone(), schema);

        let processed = harness
            .processor_json_primary()
            .process(&msg)
            .expect("extractor path should succeed once schema is cached");
        assert_eq!(
            processed.data.get("_source"),
            Some(&serde_json::Value::String("cisco-ios".to_string())),
            "the _load suffix is stripped and the topic's source labels the row"
        );
    }

    // ========================================================================
    // capture_mode=Full: `_json` holds a JSON payload, and `_raw` only a raw
    // field the record carried itself, so the payload is never stored twice.
    // ========================================================================

    #[test]
    fn capture_mode_full_stores_a_json_payload_in_json_alone() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let proc = harness.processor();
        let payload = capture_sample_payload();
        let processed = proc
            .process(&harness.make_msg(&payload))
            .expect("processed");

        let json = processed
            .data
            .get("_json")
            .and_then(|v| v.as_str())
            .expect("Full mode must populate _json");
        assert_eq!(json.as_bytes(), payload.as_slice());
        assert!(
            !processed.data.contains_key("_raw"),
            "a JSON payload landed in _raw as well as _json: {:?}",
            processed.data.get("_raw")
        );
    }

    #[test]
    fn full_writes_the_payload_to_raw_only_when_it_is_not_json() {
        let syslog = b"<134>Sep 29 10:00:00 edge-01 sshd[42]: Accepted publickey";
        let json = br#"{"event_category":"security"}"#;
        let full = |has_raw_field, payload: &[u8]| {
            super::captures_payload_as_raw(CaptureMode::Full, true, has_raw_field, payload)
        };

        assert!(full(false, syslog), "a text payload has nowhere else to go");
        assert!(!full(false, json), "_json already holds a JSON payload");
        assert!(
            !full(false, b"  \n[1, 2]"),
            "a JSON array behind whitespace is still JSON"
        );
        assert!(
            !full(true, syslog),
            "a raw field the record carried is never clobbered"
        );
        assert!(
            !super::captures_payload_as_raw(CaptureMode::Full, false, false, syslog),
            "capture_raw=false turns the capture off"
        );
    }

    #[test]
    fn only_raw_only_writes_a_json_payload_to_raw() {
        let json = br#"{"event_category":"security"}"#;
        for (mode, writes) in [
            (CaptureMode::RawOnly, true),
            (CaptureMode::Full, false),
            (CaptureMode::JsonOnly, false),
            (CaptureMode::ExtractedOnly, false),
        ] {
            assert_eq!(
                super::captures_payload_as_raw(mode, true, false, json),
                writes,
                "{mode:?}"
            );
        }
    }

    #[test]
    fn capture_mode_full_does_not_clobber_renamed_raw() {
        // A logoriginal field is @renamed → _raw by the transformer; the raw
        // payload capture must not overwrite it.
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let proc = harness.processor();
        let payload = serde_json::to_vec(&json!({
            "event_category": "security",
            "logoriginal": "the original syslog line"
        }))
        .expect("serialize");
        let processed = proc
            .process(&harness.make_msg(&payload))
            .expect("processed");

        assert_eq!(
            processed.data.get("_raw").and_then(|v| v.as_str()),
            Some("the original syslog line"),
            "an @renamed _raw must survive — payload capture must not clobber it"
        );
    }

    #[test]
    fn capture_mode_full_capture_raw_disabled_leaves_raw_absent() {
        let mut config = config_with_capture_mode(CaptureMode::Full);
        config.metadata.capture_raw = false;
        let harness = TestHarness::with_config(config);
        let proc = harness.processor();
        let processed = proc
            .process(&harness.make_msg(&capture_sample_payload()))
            .expect("processed");

        assert!(
            !processed.data.contains_key("_raw"),
            "capture_raw=false must leave _raw absent even in Full mode"
        );
    }

    /// Process `payload` on the json_primary path, the default, against a
    /// cached schema of `action` and `_raw` columns.
    fn process_json_primary_with_raw_column(
        harness: &TestHarness,
        payload: &[u8],
    ) -> super::ProcessedMessage {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let msg = harness.make_msg(payload);

        // Schema miss buffers first — discover the routed table.
        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        let column = |name: &str, position| ColumnInfo {
            name: name.to_string(),
            type_name: "String".to_string(),
            parsed_type: ParsedType::parse("String"),
            position,
            default_kind: String::new(),
            default_expression: String::new(),
            comment: String::new(),
            is_in_primary_key: false,
            is_in_sorting_key: false,
        };
        let (db, tbl) = table.split_once('.').expect("db.table");
        harness.schema_cache.insert(
            table.clone(),
            TableSchema {
                database: db.to_string(),
                table: tbl.to_string(),
                columns: vec![column("action", 0), column("_raw", 1)],
                comment: String::new(),
            },
        );

        harness
            .processor_json_primary()
            .process(&msg)
            .expect("extractor path should succeed once schema is cached")
    }

    #[test]
    fn json_primary_full_stores_a_json_payload_in_json_alone() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let payload = br#"{"event_category":"security","action":"login"}"#;

        let processed = process_json_primary_with_raw_column(&harness, payload);

        assert_eq!(
            processed.raw_payload.as_deref(),
            Some(payload.as_slice()),
            "_json is spliced from the payload bytes"
        );
        assert!(
            !processed.data.contains_key("_raw"),
            "a JSON payload landed in _raw as well as _json: {:?}",
            processed.data.get("_raw")
        );
    }

    #[test]
    fn json_primary_full_keeps_the_raw_line_a_receiver_captured() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let line = "<134>Sep 29 10:00:00 edge-01 sshd[42]: Accepted publickey";
        let payload = serde_json::to_vec(&json!({
            "event_category": "security",
            "action": "login",
            "_raw": line
        }))
        .expect("serialize");

        let processed = process_json_primary_with_raw_column(&harness, &payload);

        assert_eq!(
            processed.data.get("_raw").and_then(|v| v.as_str()),
            Some(line),
            "the source's own raw line is what _raw holds"
        );
    }

    #[test]
    fn json_primary_raw_only_still_writes_the_whole_payload_to_raw() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::RawOnly));
        let payload = br#"{"event_category":"security","action":"login"}"#;

        let processed = process_json_primary_with_raw_column(&harness, payload);

        assert_eq!(
            processed.data.get("_raw").and_then(|v| v.as_str()),
            std::str::from_utf8(payload).ok()
        );
        assert!(processed.raw_payload.is_none(), "raw_only splices no _json");
    }

    /// Regression for #144: a header pass that promotes nothing must reject the
    /// message. It used to fall through and land a row of type defaults — no
    /// `_source`, no `_tags`, `_timestamp` at epoch zero — which reads as data
    /// while carrying none.
    #[test]
    fn json_primary_rejects_a_message_whose_header_pass_promoted_nothing_144() {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let harness = TestHarness::new();
        // Parses as JSON, carries no routing field, and matches no column, so
        // the extractor has nothing to promote from it.
        let msg = harness.make_msg(br#"{"a":1,"b":2}"#);

        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        let (db, tbl) = table.split_once('.').expect("db.table");
        harness.schema_cache.insert(
            table.clone(),
            TableSchema {
                database: db.to_string(),
                table: tbl.to_string(),
                columns: vec![ColumnInfo {
                    name: "message".to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                }],
                comment: String::new(),
            },
        );

        match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::Transform(reason)) => assert!(
                reason.contains("promoted no columns"),
                "the rejection must name the cause, got: {reason}"
            ),
            Ok(_) => panic!("a header-less row must not be handed on for insert"),
            Err(e) => panic!("expected a Transform rejection, got Err({e:?})"),
        }
    }

    /// The silent half of #144: a payload that parses, IS an object, and simply
    /// matches no column. The extractor never spoke about this one, so the DLQ
    /// rejection carried no log and no counter.
    #[test]
    fn json_primary_rejects_a_valid_object_that_matches_no_column() {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let harness = TestHarness::new();
        let msg = harness.make_msg(br#"{"event_category":"security","other":1}"#);

        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        // Only `message`, which the payload does not carry.
        let (db, tbl) = table.split_once('.').expect("db.table");
        harness.schema_cache.insert(
            table.clone(),
            TableSchema {
                database: db.to_string(),
                table: tbl.to_string(),
                columns: vec![ColumnInfo {
                    name: "message".to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                }],
                comment: String::new(),
            },
        );

        match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::Transform(reason)) => assert!(
                reason.contains("promoted no columns"),
                "the rejection must name the cause, got: {reason}"
            ),
            Ok(_) => panic!("a row of type defaults must not be handed on for insert"),
            Err(e) => panic!("expected a Transform rejection, got Err({e:?})"),
        }
    }

    // ========================================================================
    // #182 — a changed column COMMENT must reach a table that keeps taking
    // traffic, because rewriting the COMMENT is how a column directive ships.
    // Both probe fields ride in every payload at distinguishable values, so the
    // landed value names which directive is in force.
    // ========================================================================

    const PROBE_OLD_COMMENT: &str =
        "@source: probe_old_field - Promoted from _json.probe.promote.value";
    const PROBE_NEW_COMMENT: &str =
        "@source: probe_alt_field - Promoted from _json.probe.promote.value";
    const PROBE_PAYLOAD: &[u8] =
        br#"{"probe_old_field":"old-cf4c862c128","probe_alt_field":"new-9f1c33b0d41","in_json":"copy-d024c547a4"}"#;

    /// `system.columns` as the resolver reads it: column name -> COMMENT.
    fn probe_schema(table: &str, columns: &[(&str, &str)]) -> crate::clickhouse::TableSchema {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let (db, tbl) = table.split_once('.').expect("db.table");
        TableSchema {
            database: db.to_string(),
            table: tbl.to_string(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, (name, comment))| ColumnInfo {
                    name: (*name).to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: i as u64 + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: (*comment).to_string(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                })
                .collect(),
            comment: String::new(),
        }
    }

    /// Apply a resolver result the way the orchestrator's resolution arm does:
    /// the parsed column directives first, then the schema.
    fn apply_resolution(harness: &TestHarness, table: &str, columns: &[(&str, &str)]) {
        use crate::column_meta::parse_directives;

        let directives = columns
            .iter()
            .map(|(name, comment)| ((*name).to_string(), parse_directives(comment)))
            .collect();
        harness.col_meta_cache.apply_ddl(table, directives);
        harness
            .schema_cache
            .insert(table.to_string(), probe_schema(table, columns));
    }

    fn landed_probe_value(harness: &TestHarness, msg: &KafkaMessage) -> Option<String> {
        harness
            .processor_json_primary()
            .process(msg)
            .expect("the extractor path must succeed once the schema is cached")
            .data
            .get("probe_promote_value")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
    }

    #[tokio::test(start_paused = true)]
    async fn ddl_comment_change_reaches_a_hot_table_182() {
        use std::time::Duration;

        use tokio::sync::mpsc;

        let harness = TestHarness::new();
        let msg = harness.make_msg(PROBE_PAYLOAD);

        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        let warm = &[("probe_promote_value", PROBE_OLD_COMMENT), ("in_json", "")];
        apply_resolution(&harness, &table, warm);

        // Control: the pre-ALTER directive is live, so only the COMMENT changes
        // from here.
        assert_eq!(
            landed_probe_value(&harness, &msg).as_deref(),
            Some("old-cf4c862c128"),
            "the pre-ALTER directive must be in force before the ALTER"
        );

        // ALTER TABLE ... COMMENT COLUMN probe_promote_value '@source: probe_alt_field ...'
        let altered = &[("probe_promote_value", PROBE_NEW_COMMENT), ("in_json", "")];

        // The table keeps taking traffic, so it never misses the cache. Only the
        // refresh pass can re-request it.
        let (tx, mut rx) = mpsc::channel(8);
        let _handle = harness.schema_cache.start_background_refresh(tx);
        // Let the loop arm its first sleep before the clock moves under it.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(305)).await;

        let requested = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the refresh pass must re-request a hot table (#182)")
            .expect("the resolver channel must stay open");
        assert_eq!(requested, table);

        apply_resolution(&harness, &requested, altered);

        assert_eq!(
            landed_probe_value(&harness, &msg).as_deref(),
            Some("new-9f1c33b0d41"),
            "the changed COMMENT must be in force without a restart (#182)"
        );

        harness.schema_cache.shutdown();
    }

    /// A column added to a hot table is the same defect: its COMMENT carries the
    /// directive, and a refresh that fetched only the column set left the new
    /// column present and empty.
    #[tokio::test(start_paused = true)]
    async fn new_column_on_a_hot_table_gets_its_directive_182() {
        use std::time::Duration;

        use tokio::sync::mpsc;

        let harness = TestHarness::new();
        let msg = harness.make_msg(PROBE_PAYLOAD);

        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        apply_resolution(&harness, &table, &[("in_json", "")]);
        assert_eq!(
            landed_probe_value(&harness, &msg),
            None,
            "the column does not exist yet"
        );

        // ALTER TABLE ... ADD COLUMN probe_promote_value String COMMENT '@source: ...'
        let added = &[("in_json", ""), ("probe_promote_value", PROBE_NEW_COMMENT)];

        // Fetching the column set alone is what the refresh used to do: the
        // column arrives, its directive does not, and the column lands empty.
        harness
            .schema_cache
            .insert(table.clone(), probe_schema(&table, added));
        assert_eq!(
            landed_probe_value(&harness, &msg),
            None,
            "a schema-only refresh leaves the new column with no directive"
        );

        let (tx, mut rx) = mpsc::channel(8);
        let _handle = harness.schema_cache.start_background_refresh(tx);
        // Let the loop arm its first sleep before the clock moves under it.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(305)).await;

        let requested = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the refresh pass must re-request a hot table (#182)")
            .expect("the resolver channel must stay open");
        apply_resolution(&harness, &requested, added);

        assert_eq!(
            landed_probe_value(&harness, &msg).as_deref(),
            Some("new-9f1c33b0d41"),
            "a column added to a hot table must get its directive (#182)"
        );

        harness.schema_cache.shutdown();
    }
}
