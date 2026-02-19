// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Main pipeline coordinator
//!
//! Orchestrates the Transport → Transform → Buffer → ClickHouse pipeline.
//!
//! Uses the hyperi-rustlib Transport abstraction for message sources (Kafka/Zenoh/Memory).
//! Processes messages in batches for efficiency.
//!
//! Uses Arrow batching: accumulates multiple messages, converts to
//! columnar Arrow RecordBatch, then pushes to buffer for ClickHouse insert.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::buffer::{BufferManager, FlushBatch, KafkaOffset};
use crate::clickhouse::{ArrowClickHouseClient, Inserter, InserterConfig};
use crate::config::{Config, MetadataConfig, TableCaptureConfig};
use crate::kafka::{DlqMessage, DlqProducer, KafkaMessage, TransportBackend};
use crate::metrics::Metrics;
use crate::payload::{FormatDetector, FormatMode, PayloadFormat};
use crate::pipeline::AutoInitializer;
use crate::routing::{RouteResult, Router};
use crate::schema::TableTags;
use crate::transform::Transformer;
use crate::transform::{FieldMappingCache, MappingBuilder};
use crate::Result;

/// Pipeline statistics
#[derive(Debug, Default, Clone)]
pub struct PipelineStats {
    pub messages_received: u64,
    pub messages_processed: u64,
    pub messages_dlq: u64,
    pub batches_flushed: u64,
    pub rows_inserted: u64,
    pub errors: u64,
}

/// Per-table capture override resolution cache.
///
/// Resolves `_json` and `_raw` disable flags from two sources:
/// 1. Config lists (`disable_json_tables`, `disable_raw_tables`) — applied immediately
/// 2. DDL comment tags (`@no_capture_json`, `@no_capture_raw`) — applied after async fetch
///
/// DDL tags take precedence over config lists.
struct CaptureOverrides {
    /// Resolved per-table configs (cached after first lookup)
    configs: FxHashMap<String, TableCaptureConfig>,
    /// Tables with _json disabled (from config, O(1) lookup)
    disable_json_tables: FxHashSet<String>,
    /// Tables with _raw disabled (from config, O(1) lookup)
    disable_raw_tables: FxHashSet<String>,
    /// Tables needing async DDL tag resolution
    pending_tables: Vec<String>,
}

impl CaptureOverrides {
    /// Create from metadata config.
    fn new(metadata_config: &MetadataConfig) -> Self {
        Self {
            configs: FxHashMap::default(),
            disable_json_tables: metadata_config
                .disable_json_tables
                .iter()
                .cloned()
                .collect(),
            disable_raw_tables: metadata_config.disable_raw_tables.iter().cloned().collect(),
            pending_tables: Vec::new(),
        }
    }

    /// Get or create capture config for a table (config-list-based defaults).
    fn get_or_default(&mut self, table: &str) -> &TableCaptureConfig {
        if !self.configs.contains_key(table) {
            let config = TableCaptureConfig {
                disable_json: self.disable_json_tables.contains(table),
                disable_raw: self.disable_raw_tables.contains(table),
            };
            self.configs.insert(table.to_string(), config);
        }
        &self.configs[table]
    }

    /// Mark a table for async DDL tag resolution (first time seen).
    fn mark_pending(&mut self, table: &str) {
        if !self.configs.contains_key(table) {
            self.pending_tables.push(table.to_string());
        }
    }

    /// Take pending tables for async resolution.
    fn take_pending(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_tables)
    }

    /// Update capture config from DDL table comment tags.
    ///
    /// DDL tags override config list settings (only to disable, not re-enable).
    fn update_from_comment(&mut self, table: &str, comment: &str) {
        if comment.is_empty() {
            return;
        }

        let tags = TableTags::from_comment(comment);

        let entry = self
            .configs
            .entry(table.to_string())
            .or_insert_with(|| TableCaptureConfig {
                disable_json: self.disable_json_tables.contains(table),
                disable_raw: self.disable_raw_tables.contains(table),
            });

        // DDL tags override config (only to disable)
        if tags
            .get("no_capture_json")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            entry.disable_json = true;
        }
        if tags
            .get("no_capture_raw")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            entry.disable_raw = true;
        }
    }
}

/// Orchestrates the Kafka → ClickHouse pipeline
pub struct Orchestrator {
    config: Config,
    shutdown: CancellationToken,
    stats: PipelineStats,
    metrics: Option<Metrics>,
}

impl Orchestrator {
    /// Create a new orchestrator with config
    pub fn new(config: Config) -> Self {
        Self {
            config,
            shutdown: CancellationToken::new(),
            stats: PipelineStats::default(),
            metrics: None,
        }
    }

    /// Create a new orchestrator with config and metrics
    pub fn with_metrics(config: Config, metrics: Metrics) -> Self {
        Self {
            config,
            shutdown: CancellationToken::new(),
            stats: PipelineStats::default(),
            metrics: Some(metrics),
        }
    }

    /// Get the shutdown token for external shutdown requests
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Run the pipeline until shutdown
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting pipeline orchestrator");

        // Run auto-initialization (creates topics, database, table if configured)
        let initializer = AutoInitializer::new(&self.config);
        initializer.run().await?;

        // Initialize transport backend (Kafka, Zenoh, etc. based on config)
        let transport = TransportBackend::from_config(&self.config).await?;
        info!(transport = transport.name(), "Transport initialized");

        // Create Arrow client for native protocol inserts and schema queries
        // Convert from dfe-loader config::ClickHouseConfig to clickhouse::ClickHouseConfig
        let ch_config: crate::clickhouse::ClickHouseConfig = (&self.config.clickhouse).into();
        let arrow_client = Arc::new(ArrowClickHouseClient::new(&ch_config).await?);

        // Inserter uses Arrow-only path (gets its own Arc clone)
        let inserter = Inserter::new(Arc::clone(&arrow_client), InserterConfig::default());

        // DLQ producer (optional - only if enabled in config)
        let dlq_config = &self.config.routing.dlq;
        let dlq_producer: Option<Arc<DlqProducer>> = if dlq_config.enabled {
            match DlqProducer::new(&self.config.kafka, dlq_config) {
                Ok(producer) => {
                    info!(suffix = %dlq_config.topic_suffix, "DLQ producer enabled");
                    Some(Arc::new(producer))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create DLQ producer, DLQ disabled");
                    None
                }
            }
        } else {
            debug!("DLQ producer disabled by config");
            None
        };

        let router = Router::new(&self.config.routing);
        // Use with_routing to enable routing field removal
        let transformer = Transformer::with_routing(
            &self.config.timestamp_dq,
            &self.config.metadata,
            &self.config.field_sanitization,
            &self.config.routing,
        );

        // Determine format mode from config
        let format_mode =
            FormatMode::parse(&self.config.payload.format).unwrap_or(FormatMode::Auto);
        let format_detector = FormatDetector::with_mode(format_mode);

        let mut buffer_manager = BufferManager::new(&self.config.buffer);

        // Per-table capture overrides (_json/_raw disable via config + DDL tags)
        let mut capture_overrides = CaptureOverrides::new(&self.config.metadata);

        // Per-table field mapping (rename/copy source fields to destination names)
        let mut field_mapping_cache: Option<FieldMappingCache> =
            if self.config.field_mapping.enabled {
                match MappingBuilder::from_config(&self.config.field_mapping) {
                    Ok(builder) => {
                        info!(
                            builtin = %self.config.field_mapping.builtin,
                            files = self.config.field_mapping.files.len(),
                            base_rules = builder.base_rule_count(),
                            "Field mapping enabled"
                        );
                        Some(FieldMappingCache::new(builder))
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to initialize field mapping, continuing without");
                        None
                    }
                }
            } else {
                None
            };

        // Flush interval timer
        let mut flush_interval = interval(Duration::from_secs(self.config.buffer.flush_age_secs));

        // Batch size for transport.recv() - process multiple messages per iteration
        const RECV_BATCH_SIZE: usize = 100;

        info!(
            format_mode = ?format_mode,
            flush_rows = self.config.buffer.flush_rows,
            flush_bytes = self.config.buffer.flush_bytes,
            flush_secs = self.config.buffer.flush_age_secs,
            recv_batch_size = RECV_BATCH_SIZE,
            "Pipeline running"
        );

        loop {
            tokio::select! {
                biased; // Prioritize shutdown check

                _ = self.shutdown.cancelled() => {
                    info!("Shutdown requested, flushing remaining buffers");
                    break;
                }

                _ = flush_interval.tick() => {
                    // Check for buffers ready to flush
                    match buffer_manager.get_ready_for_flush() {
                        Ok(batches) if !batches.is_empty() => {
                            self.flush_batches_transport(&inserter, &transport, batches).await;
                        }
                        Ok(_) => {} // No batches ready
                        Err(e) => {
                            warn!(error = %e, "Failed to get flush batches");
                        }
                    }
                }

                // Receive batch of messages from transport
                // Zero-copy: payload is moved (not copied), topic is Arc<str> clone (refcount only)
                messages = transport.recv(RECV_BATCH_SIZE) => {
                    match messages {
                        Ok(batch) if !batch.is_empty() => {
                            // Process batch of messages
                            for kafka_msg in batch {
                                self.stats.messages_received += 1;
                                if let Some(ref m) = self.metrics {
                                    m.record_received();
                                }

                                // Hot path: process_message uses sonic-rs directly on payload bytes
                                // No intermediate copies - payload bytes go straight to parser
                                match self.process_message(
                                    &kafka_msg,
                                    &format_detector,
                                    &router,
                                    &transformer,
                                    &mut buffer_manager,
                                    &mut capture_overrides,
                                    &mut field_mapping_cache,
                                ) {
                                    Ok(table) => {
                                        self.stats.messages_processed += 1;
                                        if let Some(ref m) = self.metrics {
                                            m.record_processed(&table);
                                        }
                                    }
                                    Err(e) => {
                                        self.stats.messages_dlq += 1;
                                        if let Some(ref m) = self.metrics {
                                            m.record_dlq();
                                        }

                                        // Send to DLQ if producer is available
                                        if let Some(ref dlq) = dlq_producer {
                                            // Clone Arc for the spawned task
                                            let dlq_clone: Arc<DlqProducer> = Arc::clone(dlq);
                                            let reason_owned = e.to_string();
                                            let payload_owned = kafka_msg.payload.clone();
                                            let topic_owned = kafka_msg.topic.to_string();
                                            let partition = kafka_msg.partition;
                                            let offset = kafka_msg.offset;
                                            let key_owned = kafka_msg.key.clone();

                                            // Fire-and-forget DLQ send (don't block pipeline)
                                            tokio::spawn(async move {
                                                let msg = DlqMessage {
                                                    payload: &payload_owned,
                                                    reason: &reason_owned,
                                                    destination: None,
                                                    original_topic: &topic_owned,
                                                    original_partition: partition,
                                                    original_offset: offset,
                                                    key: key_owned.as_deref(),
                                                };
                                                if let Err(dlq_err) = dlq_clone.send(msg).await {
                                                    error!(error = %dlq_err, "Failed to send message to DLQ");
                                                }
                                            });
                                            debug!(error = %e, "Message queued for DLQ");
                                        } else {
                                            warn!(error = %e, "Message processing failed, DLQ disabled");
                                        }
                                    }
                                }
                            }

                            // Update buffer stats (once per batch, not per message)
                            if let Some(ref m) = self.metrics {
                                let stats = buffer_manager.stats();
                                m.update_buffer_stats(
                                    stats.pending_rows,
                                    stats.pending_bytes,
                                    stats.pending_chunks,
                                );
                            }

                            // Resolve pending DDL capture tags for newly seen tables
                            // This is async but runs once per new table, not per message
                            let pending = capture_overrides.take_pending();
                            for table in pending {
                                match arrow_client.fetch_table_comment(&table).await {
                                    Ok(comment) if !comment.is_empty() => {
                                        capture_overrides.update_from_comment(&table, &comment);
                                        debug!(table = %table, "Resolved DDL capture tags");
                                    }
                                    Ok(_) => {} // No comment, config defaults apply
                                    Err(e) => {
                                        debug!(table = %table, error = %e, "Failed to fetch table comment for capture tags");
                                    }
                                }
                            }

                            // Resolve pending field mapping for newly seen tables
                            if let Some(ref mut fm_cache) = field_mapping_cache {
                                let fm_pending = fm_cache.take_pending();
                                for table in fm_pending {
                                    // Fetch schema and column comments for this table
                                    let schema_result = arrow_client.fetch_table_schema(&table).await;
                                    let comments_result = arrow_client.fetch_column_comments(&table).await;

                                    match (schema_result, comments_result) {
                                        (Ok(schema), Ok(comments)) => {
                                            fm_cache.build_and_cache(&table, &schema, &comments);
                                            debug!(table = %table, "Resolved field mapping");
                                        }
                                        (Ok(schema), Err(e)) => {
                                            debug!(table = %table, error = %e, "Column comments unavailable, using base rules only");
                                            fm_cache.build_and_cache_no_comments(&table, &schema);
                                        }
                                        (Err(e), _) => {
                                            debug!(table = %table, error = %e, "Schema unavailable for field mapping");
                                        }
                                    }
                                }
                            }

                            // Check for immediate flush (fast path: skip if nothing ready)
                            if buffer_manager.should_flush() {
                                match buffer_manager.get_ready_for_flush() {
                                    Ok(batches) if !batches.is_empty() => {
                                        self.flush_batches_transport(&inserter, &transport, batches).await;
                                    }
                                    Ok(_) => {} // No batches ready
                                    Err(e) => {
                                        warn!(error = %e, "Failed to get flush batches");
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // Empty batch - no messages available, continue
                        }
                        Err(e) => {
                            // Check if transport is closed
                            if !transport.is_healthy() {
                                warn!("Transport closed");
                                break;
                            }
                            error!(error = %e, "Transport recv error");
                            // Brief pause before retry
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        // Final flush
        match buffer_manager.flush_all() {
            Ok(final_batches) if !final_batches.is_empty() => {
                info!(batches = final_batches.len(), "Flushing remaining buffers");
                self.flush_batches_transport(&inserter, &transport, final_batches)
                    .await;
            }
            Ok(_) => {} // No batches to flush
            Err(e) => {
                error!(error = %e, "Failed to flush remaining buffers");
            }
        }

        // Close transport
        if let Err(e) = transport.close().await {
            warn!(error = %e, "Error closing transport");
        }

        info!(
            messages_received = self.stats.messages_received,
            messages_processed = self.stats.messages_processed,
            messages_dlq = self.stats.messages_dlq,
            batches_flushed = self.stats.batches_flushed,
            rows_inserted = self.stats.rows_inserted,
            "Pipeline stopped"
        );

        Ok(())
    }

    /// Process a single message through the pipeline
    /// Returns the table name on success for metrics tracking
    #[allow(clippy::too_many_arguments)]
    fn process_message(
        &self,
        msg: &KafkaMessage,
        format_detector: &FormatDetector,
        router: &Router,
        transformer: &Transformer,
        buffer_manager: &mut BufferManager,
        capture_overrides: &mut CaptureOverrides,
        field_mapping_cache: &mut Option<FieldMappingCache>,
    ) -> Result<String> {
        // Step 1: Check/detect format
        let format = match format_detector.check_and_detect(&msg.payload) {
            Ok(fmt) => fmt,
            Err(_expected) => {
                return Err(crate::Error::Json("Format mismatch".into()));
            }
        };

        // Step 2: Parse payload to JSON Value
        let value: Value = match format {
            PayloadFormat::Json => sonic_rs::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("JSON parse error: {}", e)))?,
            PayloadFormat::MessagePack => rmp_serde::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("MessagePack parse error: {}", e)))?,
            PayloadFormat::Unknown => {
                return Err(crate::Error::Json("Unknown format".into()));
            }
        };

        // Step 3: Route to table (db.table)
        // Use route_value() to avoid re-parsing the JSON we just parsed
        let route_result = router.route_value(&value);
        let table = match route_result {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                debug!(reason = %reason, "Routing to DLQ");
                return Err(crate::Error::Json(format!("DLQ: {}", reason)));
            }
        };

        // Step 3.5: Extract org_id for _org_id field (Common Header v2 - RLS)
        // Clone the str to avoid borrowing value (which we need to move into transform)
        let org_id_owned = router
            .extract_org_id_from_value(&value)
            .map(|s| s.to_string());

        // Step 4: Transform (flatten, timestamp validation, _raw rename, routing field removal)
        // Note: _json is NOT injected here — it's built from raw bytes in ArrowBatchBuilder sidecar
        let transform_result = transformer.transform_with_raw(value, org_id_owned.as_deref())?;

        // Step 4.5: Apply per-table capture overrides
        // Mark table for async DDL tag resolution if first time seen
        capture_overrides.mark_pending(&table);
        let mut data = transform_result.data;

        // Determine raw_payload for _json sidecar:
        // Pass Some(bytes) to enable _json, None to suppress.
        // Checks: global capture_json config AND per-table override.
        let table_capture = capture_overrides.get_or_default(&table);
        let raw_payload: Option<&[u8]> =
            if self.config.metadata.capture_json && !table_capture.disable_json {
                Some(&msg.payload)
            } else {
                None
            };

        // Remove _raw if disabled for this table (config list or DDL tags)
        if table_capture.disable_raw {
            data.remove(transformer.raw_output());
        }

        // Step 4.7: Apply per-table field mapping (rename/copy source fields)
        if let Some(ref mut fm_cache) = field_mapping_cache {
            fm_cache.mark_pending(&table);
            if let Some(mapping) = fm_cache.get(&table) {
                mapping.apply(&mut data);
            }
        }

        // Step 5: Push to per-table buffer with raw payload sidecar for _json
        // Each table has its own ArrowBatchBuilder for schema uniformity.
        // The data stays as JSON Map until the batch is ready, then converts to Arrow.
        // _json is built from raw_payload directly in ArrowBatchBuilder (zero-copy).
        let kafka_offset = KafkaOffset::with_shared_topic(
            msg.topic.clone(), // Arc::clone is cheap (just increments refcount)
            msg.partition,
            msg.offset,
        );

        buffer_manager.push(&table, data, Some(kafka_offset), raw_payload);

        debug!(table = %table, "Message buffered for Arrow batch");
        Ok(table)
    }

    /// Flush Arrow batches to ClickHouse and commit Kafka offsets via transport on success
    async fn flush_batches_transport(
        &mut self,
        inserter: &Inserter,
        transport: &TransportBackend,
        batches: Vec<FlushBatch>,
    ) {
        use std::time::Instant;

        let batch_count = batches.len();
        let total_rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();

        debug!(
            batches = batch_count,
            rows = total_rows,
            "Flushing Arrow batches"
        );

        // Pre-calculate total offset count for efficient allocation
        let total_offsets: usize = batches.iter().map(|b| b.offsets.len()).sum();

        // Collect offsets for commit after successful insert (takes ownership, avoids cloning)
        let mut all_offsets: Vec<KafkaOffset> = Vec::with_capacity(total_offsets);
        let batches_for_insert: Vec<FlushBatch> = batches
            .into_iter()
            .map(|mut b| {
                all_offsets.append(&mut b.offsets);
                b
            })
            .collect();

        let start = Instant::now();
        let results = inserter.insert_batches(batches_for_insert).await;
        let latency = start.elapsed().as_secs_f64();

        let mut all_success = true;
        let mut success_count = 0;

        for result in results {
            match result {
                Ok(count) => {
                    self.stats.rows_inserted += count as u64;
                    success_count += count;
                    if let Some(ref m) = self.metrics {
                        m.record_flush(count, latency);
                    }
                }
                Err(e) => {
                    error!(error = %e, "Arrow batch insert failed");
                    self.stats.errors += 1;
                    all_success = false;
                    if let Some(ref m) = self.metrics {
                        m.record_error();
                    }
                }
            }
        }

        // Commit Kafka offsets via transport ONLY if all inserts succeeded
        // This ensures at-least-once delivery - if any insert fails,
        // the messages will be re-delivered on restart
        if all_success && !all_offsets.is_empty() {
            match transport.commit(&all_offsets).await {
                Ok(()) => {
                    debug!(
                        offsets = all_offsets.len(),
                        rows = success_count,
                        "Kafka offsets committed after successful insert"
                    );
                    if let Some(ref m) = self.metrics {
                        m.record_offsets_committed(all_offsets.len());
                    }
                }
                Err(e) => {
                    // Log but don't fail - offsets will be re-committed on next successful batch
                    // or messages will be re-delivered (at-least-once semantics)
                    error!(error = %e, "Failed to commit Kafka offsets");
                }
            }
        } else if !all_success {
            warn!(
                failed_inserts = batch_count,
                "Skipping offset commit due to insert failures - messages will be re-delivered"
            );
        }

        self.stats.batches_flushed += batch_count as u64;
    }

    /// Get current statistics
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_stats_default() {
        let stats = PipelineStats::default();
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.rows_inserted, 0);
    }

    #[test]
    fn test_orchestrator_creation() {
        let config = Config::default();
        let orchestrator = Orchestrator::new(config);
        assert_eq!(orchestrator.stats().messages_received, 0);
    }

    #[test]
    fn test_capture_overrides_default() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // Default: nothing disabled
        let config = overrides.get_or_default("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }

    #[test]
    fn test_capture_overrides_config_disable() {
        let mut metadata_config = MetadataConfig::default();
        metadata_config
            .disable_json_tables
            .push("common.metrics".to_string());
        metadata_config
            .disable_raw_tables
            .push("common.health".to_string());

        let mut overrides = CaptureOverrides::new(&metadata_config);

        // common.metrics: json disabled, raw enabled
        let config = overrides.get_or_default("common.metrics");
        assert!(config.disable_json);
        assert!(!config.disable_raw);

        // common.health: json enabled, raw disabled
        let config = overrides.get_or_default("common.health");
        assert!(!config.disable_json);
        assert!(config.disable_raw);

        // common.events: neither disabled
        let config = overrides.get_or_default("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }

    #[test]
    fn test_capture_overrides_ddl_tag_precedence() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // Initialize default config first
        let _ = overrides.get_or_default("common.events");

        // DDL tag overrides even when config didn't disable
        overrides.update_from_comment(
            "common.events",
            "@schema_source: core | @no_capture_json: true",
        );

        let config = overrides.get_or_default("common.events");
        assert!(config.disable_json); // Overridden by DDL tag
        assert!(!config.disable_raw); // Not in DDL tags
    }

    #[test]
    fn test_capture_overrides_pending_tables() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // First time: should be marked pending
        overrides.mark_pending("common.events");
        overrides.mark_pending("common.metrics");

        let pending = overrides.take_pending();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"common.events".to_string()));

        // Resolve configs for these tables (simulates what get_or_default does)
        let _ = overrides.get_or_default("common.events");
        let _ = overrides.get_or_default("common.metrics");

        // Now they have configs — should not be pending again
        overrides.mark_pending("common.events");
        let pending = overrides.take_pending();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_capture_overrides_empty_comment() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // Empty comment should be a no-op
        overrides.update_from_comment("common.events", "");
        let config = overrides.get_or_default("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }
}
