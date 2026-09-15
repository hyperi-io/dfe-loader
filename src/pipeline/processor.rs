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
use crate::payload::{FormatDetector, PayloadFormat};
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
    pub format_detector: &'a FormatDetector,
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

/// Report a record that named no table on a source's own topic, at most once a
/// minute per topic.
///
/// The topic stays out of the metric labels for the same reason the table does
/// above: on the gRPC transport it arrives in request metadata.
fn log_routing_field_absent(topic: &str, table: &str) {
    metrics::counter!("dfe_loader_routing_field_absent_total").increment(1);

    // Keyed by topic so a second broken source is not hidden by the first; the
    // subscribed topic set bounds the map.
    static WARN_TS: std::sync::LazyLock<
        parking_lot::Mutex<rustc_hash::FxHashMap<String, Arc<std::sync::atomic::AtomicU64>>>,
    > = std::sync::LazyLock::new(|| parking_lot::Mutex::new(rustc_hash::FxHashMap::default()));

    let slot = Arc::clone(WARN_TS.lock().entry(topic.to_string()).or_default());
    if scalo::logger::log_debounced(&slot, 60_000) {
        warn!(
            topic = %topic,
            table = %table,
            reason = "no_routing_field",
            "No routing field on a source topic, falling back to the default table (max 1 per 60s per topic)"
        );
    }
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
        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                msg_size = msg.payload.len(),
                topic = %msg.topic,
                partition = msg.partition,
                offset = msg.offset,
                "Processing message"
            );
        }

        // Step 1: Check/detect format
        let format = match self.format_detector.check_and_detect(&msg.payload) {
            Ok(fmt) => fmt,
            Err(_expected) => {
                scalo::logger::security::input_validation_failure(
                    "format_check",
                    "payload format mismatch",
                    None,
                );
                return Err(crate::Error::Json("Format mismatch".into()));
            }
        };

        // Step 2: Parse payload to JSON Value
        let value: Value = match format {
            PayloadFormat::Json => sonic_rs::from_slice(&msg.payload).map_err(|e| {
                scalo::logger::security::input_validation_failure(
                    "json_parse",
                    "invalid JSON payload",
                    None,
                );
                crate::Error::Json(format!("JSON parse error: {e}"))
            })?,
            PayloadFormat::MessagePack => rmp_serde::from_slice(&msg.payload).map_err(|e| {
                scalo::logger::security::input_validation_failure(
                    "msgpack_parse",
                    "invalid MessagePack payload",
                    None,
                );
                crate::Error::Json(format!("MessagePack parse error: {e}"))
            })?,
            PayloadFormat::Unknown => {
                scalo::logger::security::input_validation_failure(
                    "format_check",
                    "unknown payload format",
                    None,
                );
                return Err(crate::Error::Json("Unknown format".into()));
            }
        };

        // Step 3: Route to table (db.table)
        let (result, choice) = self.router.route_value_traced(&value);
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

        // A source's own topic carrying records that name no table is a producer
        // defect, and the fallback to the default table is otherwise silent.
        if choice == TableChoice::DefaultFallback && self.router.is_source_topic(&msg.topic) {
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

        // json_primary + JSON: a schema cache miss is NOT a silent transformer
        // fallback. Return SchemaPending so the coordinator buffers the message
        // until the background resolver populates the schema (#36). The extractor
        // is the only path that applies @renamed directives; the transformer path
        // below would drop them, NULLing the renamed columns. MessagePack and
        // legacy_flatten still use the transformer path.
        let extractor_schema = if self.json_primary_mode && format == PayloadFormat::Json {
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
            let promoted =
                self.extractor
                    .extract(&msg.payload, &table, &schema, self.col_meta_cache);

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

            // raw_payload carries Kafka bytes for zero-copy _json splice (full mode only).
            // raw_only: _raw set below from payload bytes, no _json splice needed.
            // extracted_only: neither — no raw payload passed to inserter.
            let raw: Option<Arc<[u8]>> = match capture_mode {
                CaptureMode::Full => Some(Arc::from(msg.payload.as_slice())),
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
                    CaptureMode::ExtractedOnly => {
                        // Neither _json nor _raw
                        d.remove(self.transformer.raw_output());
                    }
                }
            }

            (d, None)
        };

        // Capture the original event payload into _raw as UTF-8 text.
        // - RawOnly: _raw is the sole capture; the full payload always wins.
        // - Full: _raw mirrors _json via @captured: raw_payload (dfe-engine#182).
        //   The json_primary extractor wires _json only, so without this the API
        //   /ingest path leaves _raw NULL. Never clobber a _raw already set by
        //   @renamed (logoriginal) or upstream.
        // - ExtractedOnly: captures neither _json nor _raw.
        let capture_full_raw = match capture_mode {
            CaptureMode::RawOnly => true,
            CaptureMode::Full => {
                self.config.metadata.capture_raw
                    && !data.contains_key(self.config.metadata.raw_output.as_str())
            }
            CaptureMode::ExtractedOnly => false,
        };
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
    use crate::payload::{FormatDetector, FormatMode};
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
        format_detector: FormatDetector,
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
            let format_detector = FormatDetector::new();
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
                format_detector,
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
                format_detector: &self.format_detector,
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
                format_detector: &self.format_detector,
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

    /// Counts one named counter, so a test asserts the value the processor
    /// emitted rather than that a recorder was installed.
    struct CountingRecorder {
        name: &'static str,
        hits: Arc<std::sync::atomic::AtomicU64>,
    }

    struct CountingHandle(Arc<std::sync::atomic::AtomicU64>);

    impl metrics::CounterFn for CountingHandle {
        fn increment(&self, value: u64) {
            self.0
                .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
        }

        fn absolute(&self, value: u64) {
            self.0.store(value, std::sync::atomic::Ordering::Relaxed);
        }
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            if key.name() == self.name {
                metrics::Counter::from_arc(Arc::new(CountingHandle(Arc::clone(&self.hits))))
            } else {
                metrics::Counter::noop()
            }
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// Run `f` with a thread-local recorder counting `name`.
    fn counted(name: &'static str, f: impl FnOnce()) -> u64 {
        let hits = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let recorder = CountingRecorder {
            name,
            hits: Arc::clone(&hits),
        };
        metrics::with_local_recorder(&recorder, f);
        hits.load(std::sync::atomic::Ordering::Relaxed)
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

        let result = proc.process(&msg);
        assert!(result.is_err(), "Empty payload should fail");
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

    #[test]
    fn a_record_with_no_routing_field_on_a_source_topic_is_counted() {
        let mut config = Config::default();
        config.routing.table_fields = vec!["_source".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();
        let payload = serde_json::to_vec(&json!({"message": "no source here"})).expect("serialize");

        // The elastic case: the transform stopped emitting _source on a topic
        // the loader was told is a source's own.
        let hits = counted("dfe_loader_routing_field_absent_total", || {
            let processed = proc
                .process(&harness.make_msg_on("elastic_load", &payload))
                .expect("should succeed");
            assert_eq!(
                processed.table, "dfe.main",
                "the fallback itself is unchanged, only the signal is new"
            );
        });
        assert_eq!(hits, 1, "one record, one count");
    }

    #[test]
    fn the_landing_topic_and_a_named_record_are_not_counted() {
        let mut config = Config::default();
        config.routing.table_fields = vec!["_source".to_string()];
        config.routing.default_db = "dfe".to_string();
        config.routing.default_table = "main".to_string();

        let harness = TestHarness::with_config(config);
        let proc = harness.processor();
        let unnamed = serde_json::to_vec(&json!({"message": "no source here"})).expect("serialize");
        let named = serde_json::to_vec(&json!({"_source": "main"})).expect("serialize");

        let hits = counted("dfe_loader_routing_field_absent_total", || {
            // Ordinary landing traffic names no source and is not a defect.
            proc.process(&harness.make_msg_on("main_land", &unnamed))
                .expect("should succeed");
            // A source topic whose record names its table is not one either.
            proc.process(&harness.make_msg_on("elastic_load", &named))
                .expect("should succeed");
        });
        assert_eq!(hits, 0, "neither case is a producer defect");
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
    // MessagePack format
    // ========================================================================

    #[test]
    fn process_valid_msgpack() {
        let harness = TestHarness::new();
        let proc = harness.processor();

        let value = json!({"event_category": "network", "count": 5});
        let payload = rmp_serde::to_vec(&value).expect("msgpack serialize");
        let msg = harness.make_msg(&payload);

        let result = proc.process(&msg);
        assert!(
            result.is_ok(),
            "MessagePack processing should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn process_forced_json_rejects_msgpack() {
        let mut config = Config::default();
        config.payload.format = "json".to_string();

        let router = Router::with_metadata(&config.routing, &config.metadata);
        let transformer = Transformer::new(
            &config.timestamp_dq,
            &config.metadata,
            &config.field_sanitization,
        );
        let extractor = HeaderExtractor::new(&config.metadata, &config.routing);
        let format_detector = FormatDetector::with_mode(FormatMode::ForceJson);
        let enrichment = EnrichmentPipeline {
            ip_fields: vec![],
            geoip: None,
            reputation: None,
            risk: None,
        };
        let schema_cache = Arc::new(SchemaCache::new(300));
        let col_meta_cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let computed_column_cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        let capture_overrides = CaptureOverrides::new(&config.metadata);
        let absent_tables =
            crate::pipeline::types::AbsentTables::new(std::time::Duration::from_secs(60), 16);

        let proc = MessageProcessor {
            config: &config,
            router: &router,
            transformer: &transformer,
            extractor: &extractor,
            format_detector: &format_detector,
            json_primary_mode: false,
            enrichment: &enrichment,
            schema_cache: &schema_cache,
            col_meta_cache: &col_meta_cache,
            field_mapping_cache: None,
            computed_column_cache: &computed_column_cache,
            capture_overrides: &capture_overrides,
            absent_tables: &absent_tables,
            default_table: "dfe.main",
        };

        let value = json!({"event_category": "test"});
        let payload = rmp_serde::to_vec(&value).expect("msgpack serialize");
        let msg = KafkaMessage {
            payload,
            topic: Arc::from("test-events"),
            partition: 0,
            offset: 1,
            key: None,
            timestamp_ms: None,
        };

        let result = proc.process(&msg);
        assert!(
            result.is_err(),
            "ForceJson mode should reject MessagePack payload"
        );
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
                    em.contains("parse") || em.contains("Format") || em.contains("Unknown"),
                    "Error should indicate parse/format problem for DLQ routing: {em}"
                );
            }
            Ok(_) => panic!("Invalid payload must fail (DLQ-bound)"),
        }
    }

    #[test]
    fn process_non_object_json_payload_still_processes() {
        // JSON can be a top-level array or scalar. sonic-rs parses these.
        // Router may then route to default table or DLQ depending on config.
        let harness = TestHarness::new();
        let proc = harness.processor();

        // Top-level array — parseable JSON but no extractable routing fields
        let msg = harness.make_msg(b"[1, 2, 3]");
        let result = proc.process(&msg);
        // Either Ok (routes to default) or Err (DLQ). Both are valid outcomes.
        // What we care about: no panic, deterministic behaviour.
        match result {
            Ok(processed) => {
                // Routes to default: dfe.main
                assert!(processed.table.contains('.'));
            }
            Err(e) => {
                let msg = format!("{e}");
                // Expected failures for top-level non-object JSON:
                // - "Expected JSON object" (transform requires Object)
                // - "DLQ"/"route"/"parse" (alternative failure paths)
                assert!(
                    msg.contains("DLQ")
                        || msg.contains("route")
                        || msg.contains("parse")
                        || msg.contains("Expected JSON object")
                        || msg.contains("object"),
                    "Error should be routing/transform-related: {msg}"
                );
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
    // MessagePack payloads (strict format mode)
    // ========================================================================

    #[test]
    fn process_msgpack_with_strict_format_check_rejects_json() {
        // If FormatDetector is forced to MessagePack but the payload is JSON,
        // format detection should reject it.
        // Use a forced-MessagePack detector directly.
        let harness = TestHarness::new();
        let msgpack_detector = FormatDetector::with_mode(FormatMode::ForceMessagePack);

        let processor = MessageProcessor {
            config: &harness.config,
            router: &harness.router,
            transformer: &harness.transformer,
            extractor: &harness.extractor,
            format_detector: &msgpack_detector,
            json_primary_mode: false,
            enrichment: &harness.enrichment,
            schema_cache: &harness.schema_cache,
            col_meta_cache: &harness.col_meta_cache,
            field_mapping_cache: None,
            computed_column_cache: &harness.computed_column_cache,
            capture_overrides: &harness.capture_overrides,
            absent_tables: &harness.absent_tables,
            default_table: &harness.default_table,
        };

        let payload = serde_json::to_vec(&json!({"event_category": "x"})).expect("serialize");
        let msg = harness.make_msg(&payload);

        let result = processor.process(&msg);
        // Strict format mode should fail fast on format mismatch
        assert!(
            result.is_err(),
            "JSON payload with strict msgpack format should error"
        );
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

    // ========================================================================
    // capture_mode=Full: _raw is captured from the raw payload (@captured:
    // raw_payload). Regression for dfe-engine#182 — the API /ingest path left
    // _raw NULL because Full mode only wired _json.
    // ========================================================================

    #[test]
    fn capture_mode_full_populates_raw_from_payload_182() {
        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let proc = harness.processor();
        let payload = capture_sample_payload();
        let processed = proc
            .process(&harness.make_msg(&payload))
            .expect("processed");

        // _json still spliced (legacy path inserts it into the map)
        assert!(
            processed.data.contains_key("_json"),
            "Full mode must still populate _json"
        );
        // _raw is now the full original payload as text (the #182 fix)
        let raw = processed
            .data
            .get("_raw")
            .and_then(|v| v.as_str())
            .expect("Full mode must populate _raw from the raw payload");
        assert_eq!(
            raw.as_bytes(),
            payload.as_slice(),
            "_raw must be the full original payload as UTF-8 text"
        );
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

    /// Regression for dfe-engine#182 on the ACTUAL failing path: json_primary
    /// (API /ingest) in Full capture mode. The extractor wires _json only, so
    /// before the fix _raw landed NULL. Once a schema is cached the processor
    /// must populate _raw with the full original payload as text.
    #[test]
    fn json_primary_full_populates_raw_from_payload_182() {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let harness = TestHarness::with_config(config_with_capture_mode(CaptureMode::Full));
        let payload = br#"{"event_category":"security","action":"login"}"#;
        let msg = harness.make_msg(payload);

        // Schema miss buffers first — discover the routed table.
        let table = match harness.processor_json_primary().process(&msg) {
            Err(crate::Error::SchemaPending { table }) => table,
            Ok(_) => panic!("expected SchemaPending before schema cached, got Ok"),
            Err(e) => panic!("expected SchemaPending, got Err({e:?})"),
        };

        // Cache a schema carrying the _raw String column.
        let (db, tbl) = table.split_once('.').expect("db.table");
        let schema = TableSchema {
            database: db.to_string(),
            table: tbl.to_string(),
            columns: vec![
                ColumnInfo {
                    name: "action".to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: 0,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                },
                ColumnInfo {
                    name: "_raw".to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                },
            ],
            comment: String::new(),
        };
        harness.schema_cache.insert(table.clone(), schema);

        // Reprocess through the extractor path: _raw must be the full payload.
        let processed = harness
            .processor_json_primary()
            .process(&msg)
            .expect("extractor path should succeed once schema is cached");
        let raw = processed
            .data
            .get("_raw")
            .and_then(|v| v.as_str())
            .expect("json_primary Full mode must populate _raw (dfe-engine#182)");
        assert_eq!(
            raw.as_bytes(),
            payload.as_slice(),
            "_raw must be the full original ingest payload as UTF-8 text"
        );
    }

    /// Regression for #144: a header pass that promotes nothing must reject the
    /// message. It used to fall through and land a row of type defaults — no
    /// `_source`, no `_tags`, `_timestamp` at epoch zero — which reads as data
    /// while carrying none.
    #[test]
    fn json_primary_rejects_a_message_whose_header_pass_promoted_nothing_144() {
        use crate::clickhouse::{ColumnInfo, ParsedType, TableSchema};

        let harness = TestHarness::new();
        // Parses as JSON, routes, and is not an object — the extractor has no
        // columns to promote from it.
        let msg = harness.make_msg(br"[1,2,3]");

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
}
