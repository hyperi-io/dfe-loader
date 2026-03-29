// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Main pipeline coordinator.
//!
//! Orchestrates the Transport → Transform → Buffer → `ClickHouse` pipeline.
//!
//! Uses the hyperi-rustlib Transport abstraction for message sources (Kafka/Memory).
//! Processes messages in batches for efficiency.
//!
//! Accumulates rows as `Map<String, Value>` per table, then flushes via
//! `JSONEachRow` HTTP inserts to `ClickHouse`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use rustc_hash::{FxHashMap, FxHashSet};

use hyperi_rustlib::ScalingPressure;
use hyperi_rustlib::dlq::{Dlq, DlqEntry, DlqSource};
use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};

use crate::Result;
use crate::buffer::{BufferManager, FlushBatch, KafkaOffset};
use crate::clickhouse::{
    ClickHouseQueryClient, Inserter, InserterConfig, SchemaCache, SharedSchemaCache,
};
use crate::column_meta::{ColumnMetaCache, parse_directives};
use crate::config::{Config, MetadataConfig, SharedConfig, TableCaptureConfig};
use crate::enrich::geoip::GeoIpEnricher;
use crate::enrich::reputation::{ReputationEnricher, ThreatSource, ThreatType};
use crate::enrich::risk::{RiskInput, RiskPreset, RiskScorer};
use crate::kafka::{
    KafkaMessage, TopicResolver, TransportAdapter, TransportBackend, resolver_from_config,
};
use crate::metrics::Metrics;
use crate::payload::{FormatDetector, FormatMode, PayloadFormat};
use crate::routing::{RouteResult, Router};
use crate::schema::TableTags;
use crate::transform::Transformer;
use crate::transform::{ComputedColumnCache, FieldMappingCache, HeaderExtractor, MappingBuilder};

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

/// Active enrichment pipeline (`GeoIP` + reputation + risk scoring)
///
/// All components are optional — each is only active if configured and
/// initialised successfully. Failures are non-fatal (logged as warnings).
struct EnrichmentPipeline {
    /// IP field names to check in event data (first match wins)
    ip_fields: Vec<String>,
    /// `GeoIP` lookup (city + ASN)
    geoip: Option<GeoIpEnricher>,
    /// IP reputation lookup (VPN, Tor, proxy, botnet, ...)
    reputation: Option<ReputationEnricher>,
    /// Risk scorer (weighted composite of geo + reputation data)
    risk: Option<RiskScorer>,
}

impl EnrichmentPipeline {
    /// Initialise enrichment pipeline from config
    ///
    /// All failures are non-fatal — the pipeline continues with whatever
    /// components were successfully initialised.
    async fn init(config: &Config) -> Self {
        // GeoIP enricher (async — may download MMDB files)
        let geoip = if config.geoip.enabled {
            let enricher = GeoIpEnricher::from_config(&config.geoip).await;
            if enricher.is_available() {
                info!(provider = ?config.geoip.provider, "GeoIP enrichment enabled");
                Some(enricher)
            } else {
                warn!(provider = ?config.geoip.provider, "GeoIP enabled but no databases loaded");
                None
            }
        } else {
            None
        };

        // Reputation enricher (sync — loads local blocklist files)
        let reputation = if config.enrichment.reputation.enabled {
            let enricher = ReputationEnricher::new()
                .with_cache_capacity(config.enrichment.reputation.cache_capacity);

            let mut loaded = 0usize;
            for path in &config.enrichment.reputation.blocklist_files {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        enricher.load_plain_list(&content, ThreatType::None, ThreatSource::Custom);
                        loaded += 1;
                        debug!(path = %path, "Loaded reputation blocklist");
                    }
                    Err(e) => {
                        warn!(path = %path, error = %e, "Failed to load reputation blocklist");
                    }
                }
            }

            if enricher.is_available() {
                info!(blocklists = loaded, "Reputation enrichment enabled");
                Some(enricher)
            } else if !config.enrichment.reputation.blocklist_files.is_empty() {
                warn!("Reputation enabled but no blocklists loaded");
                None
            } else {
                // Enabled with no files configured — allow it (user may add IPs programmatically)
                info!("Reputation enrichment enabled (no blocklist files configured)");
                Some(enricher)
            }
        } else {
            None
        };

        // Risk scorer (sync — just config, no I/O)
        let risk = if config.enrichment.risk_scoring.enabled {
            let preset = match config.enrichment.risk_scoring.preset.as_str() {
                "us_enterprise" => RiskPreset::UsEnterprise,
                "eu_enterprise" => RiskPreset::EuEnterprise,
                "apac_enterprise" => RiskPreset::ApacEnterprise,
                "high_security" => RiskPreset::HighSecurity,
                _ => RiskPreset::Global,
            };
            info!(preset = %config.enrichment.risk_scoring.preset, "Risk scoring enabled");
            Some(RiskScorer::from_preset(preset))
        } else {
            None
        };

        Self {
            ip_fields: config.enrichment.ip_fields.clone(),
            geoip,
            reputation,
            risk,
        }
    }

    /// Returns true if any enrichment is active
    fn is_active(&self) -> bool {
        self.geoip.is_some() || self.reputation.is_some() || self.risk.is_some()
    }
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
        if tags.get("no_capture_json").is_some_and(|v| v == "true") {
            entry.disable_json = true;
        }
        if tags.get("no_capture_raw").is_some_and(|v| v == "true") {
            entry.disable_raw = true;
        }
    }
}

/// Result of async per-table schema resolution.
///
/// Fetched off the event loop by a background resolver task.
/// Applied to all per-table caches when received via `result_rx`.
struct TableResolutionResult {
    /// Destination table (db.table)
    table: String,
    /// Table-level COMMENT string (for DDL capture tags)
    comment: String,
    /// Full schema from system.columns (for field mapping + `SharedSchemaCache`)
    schema: Option<crate::clickhouse::TableSchema>,
    /// Parsed per-column directives (skip/default/renamed/computed/coerce)
    column_directives: FxHashMap<String, crate::column_meta::ColumnDirectives>,
}

/// Orchestrates the Kafka → `ClickHouse` pipeline
pub struct Orchestrator {
    config: Config,
    shared_config: Option<SharedConfig>,
    shutdown: CancellationToken,
    stats: PipelineStats,
    metrics: Option<Metrics>,
    scaling: Option<Arc<ScalingPressure>>,
    memory_guard: Arc<MemoryGuard>,
}

impl Orchestrator {
    /// Create a new orchestrator with config
    pub fn new(config: Config) -> Self {
        let memory_guard = Arc::new(MemoryGuard::new(memory_guard_config(&config)));
        Self {
            config,
            shared_config: None,
            shutdown: CancellationToken::new(),
            stats: PipelineStats::default(),
            metrics: None,
            scaling: None,
            memory_guard,
        }
    }

    /// Create a new orchestrator with config and metrics
    pub fn with_metrics(config: Config, metrics: Metrics) -> Self {
        let memory_guard = Arc::new(MemoryGuard::new(memory_guard_config(&config)));
        Self {
            config,
            shared_config: None,
            shutdown: CancellationToken::new(),
            stats: PipelineStats::default(),
            metrics: Some(metrics),
            scaling: None,
            memory_guard,
        }
    }

    /// Set shared config for hot-reload support
    pub fn with_shared_config(mut self, shared: SharedConfig) -> Self {
        self.shared_config = Some(shared);
        self
    }

    /// Set scaling pressure for KEDA autoscaling
    pub fn with_scaling(mut self, scaling: Arc<ScalingPressure>) -> Self {
        self.scaling = Some(scaling);
        self
    }

    /// Get the shutdown token for external shutdown requests
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Run the pipeline until shutdown
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting pipeline orchestrator");

        // Initialize transport backend (Kafka based on config)
        let mut transport = TransportBackend::from_config(&self.config).await?;
        info!(transport = transport.name(), "Transport initialized");

        // Validate ClickHouse config (transport/port mismatch, native not yet supported)
        let ch_config: crate::clickhouse::ClickHouseConfig = (&self.config.clickhouse).into();
        match ch_config.validate() {
            Err(e) => return Err(crate::Error::Config(e)),
            Ok(warnings) => {
                for w in warnings {
                    warn!("{}", w);
                }
            }
        }

        // Create HTTP client for DDL/schema queries
        let http_client = Arc::new(
            ClickHouseQueryClient::new(&ch_config)
                .map_err(|e| crate::Error::ClickHouse(e.to_string()))?,
        );

        // Build unified client for inserts -- same transport as the query client.
        // RowBinary (DynamicInsert) works on both HTTP and native.
        // JSONEachRow (InsertFormatted) is HTTP-only and will error on native.
        let ch_client = {
            use crate::clickhouse::Transport;
            let host = ch_config
                .primary_endpoint()
                .unwrap_or_else(|| "localhost:8123".to_string());
            match ch_config.transport {
                Transport::Http => {
                    let scheme = if ch_config.tls { "https" } else { "http" };
                    clickhouse::UnifiedClient::http()
                        .with_url(format!("{scheme}://{host}"))
                        .with_user(&ch_config.username)
                        .with_password(&ch_config.password)
                        .with_database(&ch_config.database)
                        .build()
                }
                Transport::Native => {
                    let mut builder = clickhouse::UnifiedClient::native()
                        .with_addr(&*host)
                        .with_user(&ch_config.username)
                        .with_password(&ch_config.password)
                        .with_database(&ch_config.database)
                        .with_lz4();
                    if ch_config.tls {
                        let hostname = host.split(':').next().unwrap_or(&host);
                        builder = builder.with_tls(hostname);
                    }
                    builder.build()
                }
            }
        };

        let insert_format = ch_config.insert_format;
        info!(format = %insert_format, "Insert format configured");

        // Inserter dispatches based on insert_format — single client handles all inserts
        let inserter = Inserter::new(
            Arc::clone(&http_client),
            ch_client,
            InserterConfig::default(),
        )
        .with_insert_format(insert_format);

        // DLQ (unified rustlib module — cascade: Kafka primary, file fallback)
        let dlq_config = self.config.routing.dlq.to_rustlib_config();
        let transport_kafka_config = TransportAdapter::convert_config(&self.config.kafka);
        let dlq: Option<Arc<Dlq>> = if dlq_config.enabled {
            match Dlq::with_kafka(&dlq_config, "loader", &transport_kafka_config) {
                Ok(d) => {
                    info!(mode = ?dlq_config.mode, "DLQ enabled");
                    Some(Arc::new(d))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create DLQ, disabled");
                    None
                }
            }
        } else {
            debug!("DLQ disabled by config");
            None
        };

        // Bounded DLQ channel — avoids unbounded tokio::spawn per failed message.
        // Hot path uses try_send() (non-blocking, drops on full).
        // Background task drains the channel and forwards to the actual DLQ backend.
        const DLQ_CHANNEL_CAPACITY: usize = 1_000;
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<DlqEntry>(DLQ_CHANNEL_CAPACITY);
        if let Some(ref dlq_arc) = dlq {
            let dlq_bg = Arc::clone(dlq_arc);
            let shutdown_bg = self.shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown_bg.cancelled() => {
                            // Drain remaining entries before exiting
                            while let Ok(entry) = dlq_rx.try_recv() {
                                if let Err(e) = dlq_bg.send(entry).await {
                                    error!(error = %e, "DLQ send failed during shutdown drain");
                                }
                            }
                            break;
                        }
                        Some(entry) = dlq_rx.recv() => {
                            if let Err(e) = dlq_bg.send(entry).await {
                                error!(error = %e, "DLQ send failed");
                            }
                        }
                    }
                }
            });
        }

        // Schema cache — shared with background resolver and (after Change A) HeaderExtractor.
        // Background resolver populates it; orchestrator reads it in process_message.
        let schema_cache: SharedSchemaCache =
            Arc::new(SchemaCache::new(self.config.schema.cache_ttl_secs));

        // Column directive cache — unified framework for skip/default/renamed/computed/coerce.
        // Config layer is fixed at construction; DDL layer populated by background resolver.
        let col_meta_cache = Arc::new(ColumnMetaCache::new(self.config.column_directives.clone()));

        // Background schema resolver — moves all schema fetching off the event loop.
        //
        // Channel pair:
        //   resolve_tx  → send table names that need resolution (from take_pending())
        //   result_rx   → receive TableResolutionResult back (applied in select!)
        //
        // Capacity 256: enough to buffer a burst of new tables without backpressure on hot path.
        const SCHEMA_RESOLVE_CAPACITY: usize = 256;
        let (resolve_tx, mut resolve_rx) = mpsc::channel::<String>(SCHEMA_RESOLVE_CAPACITY);
        let (schema_result_tx, mut schema_result_rx) =
            mpsc::channel::<TableResolutionResult>(SCHEMA_RESOLVE_CAPACITY);
        {
            let resolver_client = Arc::clone(&http_client);
            let result_tx = schema_result_tx;
            let shutdown_resolver = self.shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown_resolver.cancelled() => break,
                        Some(table) = resolve_rx.recv() => {
                            let client = Arc::clone(&resolver_client);
                            let tx = result_tx.clone();
                            // Each table resolved concurrently — never blocks the resolver loop.
                            tokio::spawn(async move {
                                let (comment_res, schema_res, comments_res) = tokio::join!(
                                    client.fetch_table_comment(&table),
                                    client.fetch_table_schema(&table),
                                    client.fetch_column_comments(&table),
                                );
                                let column_directives = comments_res
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|(col, comment)| (col, parse_directives(&comment)))
                                    .collect();
                                let _ = tx.send(TableResolutionResult {
                                    table,
                                    comment: comment_res.unwrap_or_default(),
                                    schema: schema_res.ok(),
                                    column_directives,
                                }).await;
                            });
                        }
                    }
                }
            });
        }

        let mut router = Router::new(&self.config.routing);
        let mut transformer = Transformer::with_routing(
            &self.config.timestamp_dq,
            &self.config.metadata,
            &self.config.field_sanitization,
            &self.config.routing,
        );

        // Pipeline mode gate: json_primary (default) or legacy_flatten.
        // json_primary uses HeaderExtractor + zero-copy _json splice; legacy_flatten
        // uses the existing full-flatten Transformer path (unchanged).
        let json_primary_mode = self.config.payload.pipeline_mode != "legacy_flatten";

        let extractor = HeaderExtractor::new(&self.config.metadata, &self.config.routing);

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

        // Per-table computed columns (CEL expressions producing column values)
        let mut computed_column_cache =
            ComputedColumnCache::new(self.config.computed_columns.clone());

        // Enrichment pipeline (GeoIP + reputation + risk scoring)
        // Initialised here once — not rebuilt on hot-reload (databases are stable)
        let enrichment = EnrichmentPipeline::init(&self.config).await;
        if enrichment.is_active() {
            info!(
                geoip = enrichment.geoip.is_some(),
                reputation = enrichment.reputation.is_some(),
                risk = enrichment.risk.is_some(),
                ip_fields = ?self.config.enrichment.ip_fields,
                "Enrichment pipeline active"
            );
        }

        // Flush interval timer
        let mut flush_interval = interval(Duration::from_secs(self.config.buffer.flush_age_secs));

        // Topic refresh: active only for Kafka transport in auto-discovery mode
        // (topics.is_empty() = auto-discover). topic_refresh_secs=0 disables refresh.
        let topic_refresh_enabled = matches!(transport, TransportBackend::Kafka(_))
            && self.config.kafka.topics.is_empty()
            && self.config.kafka.topic_refresh_secs > 0;

        let topic_resolver: Option<TopicResolver> = if topic_refresh_enabled {
            match resolver_from_config(&self.config.kafka) {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(error = %e, "Failed to create topic resolver, refresh disabled");
                    None
                }
            }
        } else {
            None
        };

        // Track the currently active topic set for change detection.
        // Initialised empty; first refresh will populate it.
        let mut current_topics: Vec<String> = vec![];

        let refresh_secs = self.config.kafka.topic_refresh_secs.max(1);
        let mut topic_refresh_interval = interval(Duration::from_secs(refresh_secs));
        // Consume the immediate tick so first refresh fires after `refresh_secs`.
        topic_refresh_interval.tick().await;

        // Hot-reload: subscribe to config changes if SharedConfig is available
        let mut config_rx = self
            .shared_config
            .as_ref()
            .map(hyperi_rustlib::SharedConfig::subscribe);

        // Batch size for transport.recv() - process multiple messages per iteration
        const RECV_BATCH_SIZE: usize = 100;

        info!(
            format_mode = ?format_mode,
            flush_rows = self.config.buffer.flush_rows,
            flush_bytes = self.config.buffer.flush_bytes,
            flush_secs = self.config.buffer.flush_age_secs,
            recv_batch_size = RECV_BATCH_SIZE,
            hot_reload = self.shared_config.is_some(),
            memory_limit_bytes = self.memory_guard.limit_bytes(),
            memory_pressure_threshold = self.config.memory.pressure_threshold,
            "Pipeline running"
        );

        loop {
            tokio::select! {
                biased; // Prioritize shutdown check

                () = self.shutdown.cancelled() => {
                    info!("Shutdown requested, flushing remaining buffers");
                    break;
                }

                // Hot-reload: rebuild mutable components on config change
                Ok(()) = async {
                    match config_rx.as_mut() {
                        Some(rx) => rx.changed().await.map_err(|_| ()),
                        None => std::future::pending::<std::result::Result<(), ()>>().await,
                    }
                } => {
                    if let Some(ref shared) = self.shared_config {
                        let new_config = shared.read().clone();
                        let version = shared.version();

                        // Warn about restart-required changes (silently ignored otherwise)
                        warn_restart_required(&self.config, &new_config);

                        info!(version = version, "Config reloaded, applying safe changes");

                        // --- Hot-reloaded: takes effect on next batch ---
                        router = Router::new(&new_config.routing);
                        transformer = Transformer::with_routing(
                            &new_config.timestamp_dq,
                            &new_config.metadata,
                            &new_config.field_sanitization,
                            &new_config.routing,
                        );
                        buffer_manager.update_config(&new_config.buffer);
                        capture_overrides = CaptureOverrides::new(&new_config.metadata);

                        if new_config.buffer.flush_age_secs != self.config.buffer.flush_age_secs {
                            flush_interval = interval(Duration::from_secs(
                                new_config.buffer.flush_age_secs,
                            ));
                        }

                        // Store new config (for process_message to reference)
                        self.config = new_config;

                        // Update config registry (enables /config endpoint to reflect changes)
                        self.config.register_sections();

                        hyperi_rustlib::logger::security::config_changed(
                            "config_reload",
                            "system",
                            &format!("pipeline config reloaded (version {version})"),
                        );
                        info!(version = version, "Config hot-reload complete");
                    }
                }

                _ = topic_refresh_interval.tick(), if topic_resolver.is_some() => {
                    if let Some(ref resolver) = topic_resolver {
                        match resolver.resolve() {
                            Ok(mut new_topics) => {
                                new_topics.sort_unstable();
                                let mut sorted_current = current_topics.clone();
                                sorted_current.sort_unstable();

                                if new_topics != sorted_current {
                                    info!(
                                        old = ?sorted_current,
                                        new = ?new_topics,
                                        "Topic list changed — recreating transport"
                                    );

                                    // Flush before recreating transport
                                    let batches = buffer_manager.flush_all();
                                    if !batches.is_empty() {
                                        self.flush_batches_transport(&inserter, &transport, batches).await;
                                    }

                                    if let Err(e) = transport.close().await {
                                        warn!(error = %e, "Error closing transport during topic refresh");
                                    }

                                    match TransportBackend::from_config(&self.config).await {
                                        Ok(new_transport) => {
                                            transport = new_transport;
                                            current_topics = new_topics;
                                            info!("Transport recreated with updated topic list");
                                        }
                                        Err(e) => {
                                            error!(error = %e, "Failed to recreate transport after topic change — keeping old");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Topic refresh failed, retaining current topics");
                            }
                        }
                    }
                }

                _ = flush_interval.tick() => {
                    // Update memory metrics and scaling pressure from MemoryGuard
                    {
                        let used = self.memory_guard.current_bytes();
                        let limit = self.memory_guard.limit_bytes();
                        if let Some(ref m) = self.metrics {
                            m.set_memory_usage(used, limit);
                        }
                        if let Some(ref scaling) = self.scaling {
                            scaling.set_memory(used, limit);
                            if limit > 0 {
                                scaling.set_component("memory", used as f64 / limit as f64);
                            }
                        }
                    }

                    // Check for buffers ready to flush
                    let batches = buffer_manager.get_ready_for_flush();
                    if !batches.is_empty() {
                        self.flush_batches_transport(&inserter, &transport, batches).await;
                    }
                }

                // Apply schema resolution results from background resolver.
                //
                // Receives TableResolutionResult and applies to all three per-table caches:
                //   - CaptureOverrides: DDL comment tags (_json/_raw disable)
                //   - FieldMappingCache: rename/copy rules from schema + column comments
                //   - ComputedColumnCache: CEL expressions from column comments
                //   - SchemaCache: full schema for HeaderExtractor (Change A)
                Some(result) = schema_result_rx.recv() => {
                    let table = &result.table;

                    // Apply DDL capture tags (only if comment is non-empty)
                    if !result.comment.is_empty() {
                        capture_overrides.update_from_comment(table, &result.comment);
                        debug!(table = %table, "Applied DDL capture tags from background resolver");
                    }

                    // Populate ColumnMetaCache DDL layer — must happen before field mapping
                    // and computed column caches, which read from it.
                    col_meta_cache.apply_ddl(table, result.column_directives);

                    // Apply field mapping (needs schema; uses ColumnMetaCache for rename rules)
                    if let Some(ref mut fm) = field_mapping_cache
                        && let Some(ref schema) = result.schema {
                            fm.build_and_cache(table, schema, &col_meta_cache);
                            debug!(table = %table, "Applied field mapping from background resolver");
                        }

                    // Apply computed columns (uses ColumnMetaCache for CEL expressions)
                    computed_column_cache.build_and_cache(table, &col_meta_cache);

                    // Populate SchemaCache for HeaderExtractor (Change A)
                    if let Some(schema) = result.schema {
                        schema_cache.insert(table.clone(), schema);
                    }
                }

                // Receive batch of messages from transport
                // Zero-copy: payload is moved (not copied), topic is Arc<str> clone (refcount only)
                messages = transport.recv(RECV_BATCH_SIZE) => {
                    // Memory pressure gate (Pattern B): skip processing when under pressure.
                    // Messages stay in Kafka (not committed) — consumer lag rises, KEDA scales.
                    if self.memory_guard.under_pressure() {
                        static PRESSURE_TS: AtomicU64 = AtomicU64::new(0);
                        if hyperi_rustlib::logger::log_debounced(&PRESSURE_TS, 5000) {
                            let current = self.memory_guard.current_bytes();
                            let limit = self.memory_guard.limit_bytes();
                            warn!(
                                current_bytes = current,
                                limit_bytes = limit,
                                ratio = format_args!("{:.1}%", if limit > 0 { current as f64 / limit as f64 * 100.0 } else { 0.0 }),
                                "Memory pressure HIGH — pausing consumption (max 1 per 5s)"
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    match messages {
                        Ok(batch) if !batch.is_empty() => {
                            // Process batch of messages
                            for kafka_msg in batch {
                                // Track memory for backpressure
                                self.memory_guard.add_bytes(kafka_msg.payload.len() as u64);

                                self.stats.messages_received += 1;
                                if let Some(ref m) = self.metrics {
                                    m.record_received();
                                }

                                // Hot path: process_message uses sonic-rs directly on payload bytes
                                // No intermediate copies - payload bytes go straight to parser
                                match self.process_message(
                                    &kafka_msg,
                                    json_primary_mode,
                                    &format_detector,
                                    &router,
                                    &transformer,
                                    &extractor,
                                    &schema_cache,
                                    &col_meta_cache,
                                    &enrichment,
                                    &mut buffer_manager,
                                    &mut capture_overrides,
                                    &mut field_mapping_cache,
                                    &mut computed_column_cache,
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

                                        // Send to DLQ if available (bounded channel, non-blocking)
                                        if dlq.is_some() {
                                            let entry = DlqEntry::new(
                                                "loader",
                                                e.to_string(),
                                                kafka_msg.payload.clone(),
                                            )
                                            .with_source(DlqSource::kafka(
                                                kafka_msg.topic.to_string(),
                                                kafka_msg.partition,
                                                kafka_msg.offset,
                                            ));

                                            match dlq_tx.try_send(entry) {
                                                Ok(()) => {
                                                    hyperi_rustlib::logger::security::record_dlq(
                                                        "processing",
                                                        &e.to_string(),
                                                        Some(&format!(
                                                            "topic: {}, partition: {}, offset: {}",
                                                            kafka_msg.topic,
                                                            kafka_msg.partition,
                                                            kafka_msg.offset
                                                        )),
                                                    );
                                                    debug!(error = %e, "Message queued for DLQ");
                                                }
                                                Err(mpsc::error::TrySendError::Full(_)) => {
                                                    static DLQ_FULL_TS: AtomicU64 = AtomicU64::new(0);
                                                    if hyperi_rustlib::logger::log_debounced(&DLQ_FULL_TS, 5000) {
                                                        warn!(error = %e, "DLQ channel full, messages dropped (max 1 per 5s)");
                                                    }
                                                }
                                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                                    static DLQ_CLOSED_TS: AtomicU64 = AtomicU64::new(0);
                                                    if hyperi_rustlib::logger::log_debounced(&DLQ_CLOSED_TS, 5000) {
                                                        warn!(error = %e, "DLQ channel closed (max 1 per 5s)");
                                                    }
                                                }
                                            }
                                        } else {
                                            warn!(error = %e, "Message processing failed, DLQ disabled");
                                        }
                                    }
                                }
                            }

                            // Update buffer stats (once per batch, not per message)
                            let buf_stats = buffer_manager.stats();
                            if let Some(ref m) = self.metrics {
                                m.update_buffer_stats(
                                    buf_stats.pending_rows,
                                    buf_stats.pending_bytes,
                                    buf_stats.pending_chunks,
                                );

                                // Per-table buffer depth for monitoring individual table backlog
                                for (table, rows, bytes) in buffer_manager.per_table_stats() {
                                    m.update_per_table_buffer(table, rows, bytes);
                                }

                                // EPS gauge (events per second from counter delta)
                                m.update_eps();

                                // ClickHouse connection pool stats (native transport only)
                                m.update_pool_stats(inserter.pool_stats());

                                // Per-table circuit breaker state
                                if let Some(cb) = inserter.circuit_breaker() {
                                    for (table, state) in cb.per_table_states() {
                                        m.update_circuit_breaker_state(&table, state);
                                    }
                                }
                            }

                            // Update scaling pressure components
                            if let Some(ref scaling) = self.scaling {
                                scaling.set_component("buffer_depth", buf_stats.pending_rows as f64);
                                scaling.set_component("errors", self.stats.errors as f64);
                            }

                            // Dispatch pending schema resolution tasks off the event loop.
                            //
                            // Collect tables needing resolution from all three caches,
                            // deduplicate (a new table appears in all three), then send
                            // each unique table to the background resolver via resolve_tx.
                            // Results arrive in schema_result_rx (handled in select! arm below).
                            {
                                let mut seen = FxHashSet::default();
                                for table in capture_overrides.take_pending() {
                                    if seen.insert(table.clone())
                                        && resolve_tx.try_send(table).is_err() {
                                            debug!("Schema resolve channel full, will retry next tick");
                                        }
                                }
                                if let Some(ref mut fm) = field_mapping_cache {
                                    for table in fm.take_pending() {
                                        if seen.insert(table.clone())
                                            && resolve_tx.try_send(table).is_err() {
                                                debug!("Schema resolve channel full, will retry next tick");
                                            }
                                    }
                                }
                                for table in computed_column_cache.take_pending() {
                                    if seen.insert(table.clone())
                                        && resolve_tx.try_send(table).is_err() {
                                            debug!("Schema resolve channel full, will retry next tick");
                                        }
                                }
                            }

                            // Single-pass flush check: get_ready_for_flush() returns empty if
                            // nothing is ready — no separate should_flush() guard needed
                            let batches = buffer_manager.get_ready_for_flush();
                            if !batches.is_empty() {
                                self.flush_batches_transport(&inserter, &transport, batches).await;
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
        let final_batches = buffer_manager.flush_all();
        if !final_batches.is_empty() {
            info!(batches = final_batches.len(), "Flushing remaining buffers");
            self.flush_batches_transport(&inserter, &transport, final_batches)
                .await;
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

    /// Process a single message through the pipeline.
    ///
    /// Returns the table name on success for metrics tracking.
    ///
    /// Two code paths:
    /// - `json_primary`: schema-guided SIMD extraction (`HeaderExtractor`) + zero-copy `_json`.
    ///   When schema is not yet resolved, silently falls back to the legacy path.
    /// - `legacy_flatten`: existing full-flatten + Transformer path (unchanged).
    #[allow(clippy::too_many_arguments)]
    fn process_message(
        &self,
        msg: &KafkaMessage,
        json_primary_mode: bool,
        format_detector: &FormatDetector,
        router: &Router,
        transformer: &Transformer,
        extractor: &HeaderExtractor,
        schema_cache: &SharedSchemaCache,
        col_meta_cache: &ColumnMetaCache,
        enrichment: &EnrichmentPipeline,
        buffer_manager: &mut BufferManager,
        capture_overrides: &mut CaptureOverrides,
        field_mapping_cache: &mut Option<FieldMappingCache>,
        computed_column_cache: &mut ComputedColumnCache,
    ) -> Result<String> {
        // Step 1: Check/detect format
        let format = match format_detector.check_and_detect(&msg.payload) {
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

        // Step 2: Parse payload to JSON Value (needed for routing in both modes)
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
        let route_result = router.route_value(&value);
        let table = match route_result {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                debug!(reason = %reason, "Routing to DLQ");
                return Err(crate::Error::Json(format!("DLQ: {reason}")));
            }
        };

        // Mark table for async schema/DDL resolution (both paths).
        capture_overrides.mark_pending(&table);
        if let Some(fm_cache) = field_mapping_cache.as_mut() {
            fm_cache.mark_pending(&table);
        }
        computed_column_cache.mark_pending(&table);

        // Step 4: Build the promoted field map and optionally keep raw bytes for zero-copy _json.
        //
        // json_primary path (when schema is known and payload is JSON):
        //   HeaderExtractor does one SIMD scan per schema column. Raw bytes are kept as
        //   Arc<[u8]> for zero-copy _json splice at serialisation time.
        //
        // Legacy fallback (schema not yet resolved, MessagePack input, or legacy_flatten mode):
        //   Full flatten + Transformer path. _json is injected inline as a UTF-8 string copy.
        let json_primary_schema = if json_primary_mode && format == PayloadFormat::Json {
            schema_cache.get(&table)
        } else {
            None
        };

        let (mut data, raw_payload) = if let Some(schema) = json_primary_schema {
            let promoted = extractor.extract(&msg.payload, &table, &schema, col_meta_cache);
            let raw: Arc<[u8]> = Arc::from(msg.payload.as_slice());
            (promoted, Some(raw))
        } else {
            // Legacy flatten path: full DOM → flatten → transform → inline _json
            let common_header = self.config.metadata.enabled;

            let org_id_owned = if common_header {
                router
                    .extract_org_id_from_value(&value)
                    .map(std::string::ToString::to_string)
            } else {
                None
            };

            let source_owned = if common_header && self.config.metadata.capture_source {
                Some(router.extract_source_from_value(&value).map_or_else(
                    || router.derive_source_from_topic(&msg.topic),
                    std::string::ToString::to_string,
                ))
            } else {
                None
            };

            let transform_result = transformer.transform_with_raw(
                value,
                org_id_owned.as_deref(),
                source_owned.as_deref(),
            )?;
            let mut d = transform_result.data;

            let table_capture = capture_overrides.get_or_default(&table);
            if common_header
                && self.config.metadata.capture_json
                && !table_capture.disable_json
                && let Ok(json_str) = std::str::from_utf8(&msg.payload)
            {
                d.insert("_json".to_string(), Value::String(json_str.to_string()));
            }
            if common_header && table_capture.disable_raw {
                d.remove(transformer.raw_output());
            }

            (d, None)
        };

        // Step 4.7: Apply per-table field mapping (rename/copy source fields)
        if let Some(fm_cache) = field_mapping_cache
            && let Some(mapping) = fm_cache.get(&table)
        {
            mapping.apply(&mut data);
        }

        // Step 4.8: Apply computed columns (CEL expressions producing column values)
        if let Some(computed) = computed_column_cache.get(&table) {
            computed.evaluate(&mut data);
        }

        // Step 4.9: IP enrichment (GeoIP + reputation + risk scoring)
        if enrichment.is_active()
            && let Some(ip) = extract_enrich_ip(&data, &enrichment.ip_fields)
        {
            let geo_result = enrichment.geoip.as_ref().and_then(|g| g.lookup(&ip));
            let rep_result = enrichment.reputation.as_ref().and_then(|r| r.lookup(&ip));

            if let Some(ref geo) = geo_result {
                inject_geo(&mut data, geo);
            }
            if let Some(ref rep) = rep_result {
                inject_reputation(&mut data, rep);
            }
            if let Some(ref scorer) = enrichment.risk
                && (geo_result.is_some() || rep_result.is_some())
            {
                let input = RiskInput::from_enrichment(geo_result.as_ref(), rep_result.as_ref());
                let output = scorer.score(&input);
                inject_risk(&mut data, &output);
            }
        }

        // Step 5: Push to per-table buffer.
        // raw_payload is Some for json_primary path — _json is spliced at serialisation.
        // raw_payload is None for legacy path — _json already injected inline above.
        let kafka_offset =
            KafkaOffset::with_shared_topic(msg.topic.clone(), msg.partition, msg.offset);

        buffer_manager.push(&table, data, Some(kafka_offset), raw_payload);

        debug!(table = %table, "Message buffered");
        Ok(table)
    }

    /// Flush batches to `ClickHouse` and commit Kafka offsets via transport on success
    async fn flush_batches_transport(
        &mut self,
        inserter: &Inserter,
        transport: &TransportBackend,
        batches: Vec<FlushBatch>,
    ) {
        use std::time::Instant;

        let batch_count = batches.len();
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();

        debug!(batches = batch_count, rows = total_rows, "Flushing batches");

        // Extract per-batch offsets and byte sizes — enables independent commit and memory release.
        // A failure in Table A must not block offset commit for Table B (correctness fix).
        let mut per_batch_offsets: Vec<Vec<KafkaOffset>> = Vec::with_capacity(batches.len());
        let mut per_batch_bytes: Vec<u64> = Vec::with_capacity(batches.len());
        let batches_for_insert: Vec<FlushBatch> = batches
            .into_iter()
            .map(|mut b| {
                per_batch_offsets.push(std::mem::take(&mut b.offsets));
                // Track original payload bytes for memory guard release
                let batch_bytes: u64 = b.raw_payloads.iter().map(|p| p.len() as u64).sum();
                per_batch_bytes.push(batch_bytes);
                b
            })
            .collect();

        let start = Instant::now();
        let results = inserter.insert_batches(batches_for_insert).await;
        let latency = start.elapsed().as_secs_f64();

        // Update scaling pressure with insert latency
        if let Some(ref scaling) = self.scaling {
            scaling.set_component("insert_latency", latency);
        }

        // Commit offsets independently per batch — Table A success/failure is isolated
        for ((result, offsets), batch_bytes) in results
            .into_iter()
            .zip(per_batch_offsets.into_iter())
            .zip(per_batch_bytes.into_iter())
        {
            // Release tracked memory regardless of insert outcome.
            // Success: data is in ClickHouse, memory freed.
            // Failure: offsets withheld, Kafka re-delivers — we'll re-track on re-consume.
            self.memory_guard.release(batch_bytes);

            match result {
                Ok(count) => {
                    self.stats.rows_inserted += count as u64;
                    if let Some(ref m) = self.metrics {
                        m.record_flush(count, latency);
                        m.record_insert_quantities(batch_bytes, 1);
                    }
                    if !offsets.is_empty() {
                        match transport.commit(&offsets).await {
                            Ok(()) => {
                                debug!(
                                    offsets = offsets.len(),
                                    rows = count,
                                    "Kafka offsets committed"
                                );
                                if let Some(ref m) = self.metrics {
                                    m.record_offsets_committed(offsets.len());
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to commit Kafka offsets");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Batch insert failed — offsets withheld, messages will re-deliver");
                    self.stats.errors += 1;
                    if let Some(ref m) = self.metrics {
                        m.record_error();
                    }
                }
            }
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

/// Build a `MemoryGuardConfig` from the loader's `MemoryConfig`.
///
/// Maps dfe-loader config fields to the rustlib `MemoryGuardConfig`.
/// Falls back to `DFE_LOADER_MEMORY_*` env vars when config values are default.
fn memory_guard_config(config: &Config) -> MemoryGuardConfig {
    let mem = &config.memory;
    if mem.limit_bytes > 0 || (mem.pressure_threshold - 0.8).abs() > f64::EPSILON {
        // Explicit config — use directly
        MemoryGuardConfig {
            limit_bytes: mem.limit_bytes as u64,
            pressure_threshold: mem.pressure_threshold,
            ..MemoryGuardConfig::default()
        }
    } else {
        // Default config — let env vars override (K8s ConfigMap pattern)
        MemoryGuardConfig::from_env("DFE_LOADER")
    }
}

// =============================================================================
// Enrichment helpers (module-private)
// =============================================================================

/// Extract the first IP string found in `data` by checking `ip_fields` in order.
///
/// Returns `Some(ip_string)` for the first field that exists and is a non-empty
/// string value. Returns `None` if no IP field is found.
fn extract_enrich_ip(
    data: &serde_json::Map<String, Value>,
    ip_fields: &[String],
) -> Option<String> {
    for field in ip_fields {
        if let Some(Value::String(s)) = data.get(field.as_str())
            && !s.is_empty()
        {
            return Some(s.clone());
        }
    }
    None
}

/// Inject `GeoIP` result fields into event data as `geo_*` prefixed fields.
///
/// Only non-None fields are injected. If a `geo_*` field already exists in
/// the data, it is preserved (not overwritten) to allow source-provided values.
fn inject_geo(
    data: &mut serde_json::Map<String, Value>,
    result: &crate::enrich::geoip::GeoIpResult,
) {
    macro_rules! insert_if_absent {
        ($key:expr, $val:expr) => {
            if !data.contains_key($key) {
                data.insert($key.to_string(), $val);
            }
        };
    }
    if let Some(ref v) = result.continent_code {
        insert_if_absent!("geo_continent_code", Value::String(v.clone()));
    }
    if let Some(ref v) = result.country_code {
        insert_if_absent!("geo_country_code", Value::String(v.clone()));
    }
    if let Some(ref v) = result.country_name {
        insert_if_absent!("geo_country", Value::String(v.clone()));
    }
    if let Some(ref v) = result.city {
        insert_if_absent!("geo_city", Value::String(v.clone()));
    }
    if let Some(v) = result.latitude {
        insert_if_absent!("geo_latitude", serde_json::json!(v));
    }
    if let Some(v) = result.longitude {
        insert_if_absent!("geo_longitude", serde_json::json!(v));
    }
    if let Some(ref v) = result.timezone {
        insert_if_absent!("geo_timezone", Value::String(v.clone()));
    }
    if let Some(ref v) = result.subdivision {
        insert_if_absent!("geo_region", Value::String(v.clone()));
    }
    if let Some(ref v) = result.subdivision_code {
        insert_if_absent!("geo_region_code", Value::String(v.clone()));
    }
    if let Some(v) = result.asn {
        insert_if_absent!("geo_asn", serde_json::json!(v));
    }
    if let Some(ref v) = result.asn_org {
        insert_if_absent!("geo_asn_org", Value::String(v.clone()));
    }
    insert_if_absent!("geo_is_private", Value::Bool(result.is_private));
}

/// Inject reputation result fields as `rep_*` prefixed fields.
fn inject_reputation(
    data: &mut serde_json::Map<String, Value>,
    result: &crate::enrich::reputation::ReputationResult,
) {
    macro_rules! insert_if_absent {
        ($key:expr, $val:expr) => {
            if !data.contains_key($key) {
                data.insert($key.to_string(), $val);
            }
        };
    }
    insert_if_absent!("rep_is_vpn", Value::Bool(result.is_vpn));
    insert_if_absent!("rep_is_proxy", Value::Bool(result.is_proxy));
    insert_if_absent!("rep_is_tor", Value::Bool(result.is_tor));
    insert_if_absent!("rep_is_relay", Value::Bool(result.is_relay));
    insert_if_absent!("rep_is_datacenter", Value::Bool(result.is_datacenter));
    insert_if_absent!("rep_is_botnet", Value::Bool(result.is_botnet));
    insert_if_absent!("rep_is_spam", Value::Bool(result.is_spam));
    insert_if_absent!("rep_is_scanner", Value::Bool(result.is_scanner));
    insert_if_absent!("rep_is_malicious", Value::Bool(result.is_malicious));
    insert_if_absent!(
        "rep_threat_type",
        Value::String(format!("{:?}", result.threat_type).to_lowercase())
    );
    if result.abuse_score > 0 {
        insert_if_absent!("rep_abuse_score", serde_json::json!(result.abuse_score));
    }
}

/// Inject risk scoring output as `risk_*` prefixed fields.
fn inject_risk(
    data: &mut serde_json::Map<String, Value>,
    output: &crate::enrich::risk::RiskOutput,
) {
    if !data.contains_key("risk_score") {
        data.insert(
            "risk_score".to_string(),
            serde_json::json!(output.risk_score),
        );
    }
    if !data.contains_key("risk_level") {
        data.insert(
            "risk_level".to_string(),
            Value::String(output.risk_level.as_str().to_string()),
        );
    }
    if !output.risk_factors.is_empty() && !data.contains_key("risk_factors") {
        let factors: Vec<Value> = output
            .risk_factors
            .iter()
            .map(|s| Value::String(s.to_string()))
            .collect();
        data.insert("risk_factors".to_string(), Value::Array(factors));
    }
}

// =============================================================================
// Hot-Reload Safety: restart-required change detection
// =============================================================================
//
// Hot-reloaded (takes effect on next batch):
//   routing.*             — router rebuilt on reload
//   timestamp_dq.*        — transformer rebuilt on reload
//   metadata.*            — transformer + capture overrides rebuilt on reload
//   field_sanitization.*  — transformer rebuilt on reload
//   buffer.flush_rows / flush_bytes / flush_age_secs — buffer thresholds updated
//   coercion.*            — coercer config (referenced per-batch)
//   enrichment.ip_fields  — which fields to enrich
//   field_mapping.*       — field mapping overrides
//
// Requires pod restart (connections/state established at startup):
//   kafka.*               — Kafka consumer created at startup
//   grpc.*                — gRPC server binds at startup
//   transport             — transport type bound at startup
//   clickhouse.*          — HTTP client + clickhouse::Client created at startup
//   payload.format        — format detection set at startup
//   metrics.*             — HTTP metrics server binds at startup
//   logging.*             — tracing subscriber installed at startup
//   scaling.* / keda.*    — scaling pressure built at startup
//   hot_reload.*          — watcher config set at startup
//   schema.*              — schema cache created at startup
//   geoip.*               — MMDB readers opened at startup
//   computed_columns.*    — computed column cache built at startup
//   column_directives.*   — column directive cache built at startup

/// Log warnings for config fields that changed but require a pod restart.
///
/// These fields are bound to connections or state created at startup. Changing
/// them via hot-reload has no effect — the old values remain active until the
/// pod is restarted.
fn warn_restart_required(old: &Config, new: &Config) {
    if old.transport != new.transport {
        warn!("transport changed — requires restart to take effect");
    }
    if old.kafka != new.kafka {
        warn!("kafka config changed — requires restart to take effect");
    }
    if old.grpc != new.grpc {
        warn!("grpc config changed — requires restart to take effect");
    }
    if old.clickhouse != new.clickhouse {
        warn!("clickhouse config changed — requires restart to take effect");
    }
    if old.payload != new.payload {
        warn!("payload config changed — requires restart to take effect");
    }
    if old.metrics != new.metrics {
        warn!("metrics config changed — requires restart to take effect");
    }
    if old.logging != new.logging {
        warn!("logging config changed — requires restart to take effect");
    }
    if old.scaling != new.scaling {
        warn!("scaling config changed — requires restart to take effect");
    }
    if old.keda != new.keda {
        warn!("keda config changed — requires restart to take effect");
    }
    if old.hot_reload != new.hot_reload {
        warn!("hot_reload config changed — requires restart to take effect");
    }
    if old.schema != new.schema {
        warn!("schema config changed — requires restart to take effect");
    }
    if old.geoip != new.geoip {
        warn!("geoip config changed — requires restart to take effect");
    }
    if old.computed_columns != new.computed_columns {
        warn!("computed_columns config changed — requires restart to take effect");
    }
    if old.column_directives != new.column_directives {
        warn!("column_directives config changed — requires restart to take effect");
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

    #[test]
    fn test_orchestrator_with_shared_config() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());

        let orchestrator = Orchestrator::new(config).with_shared_config(shared.clone());
        assert!(orchestrator.shared_config.is_some());
        assert_eq!(shared.version(), 0);
    }

    #[tokio::test]
    async fn test_orchestrator_shared_config_update() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());
        let mut rx = shared.subscribe();

        // Simulate config update
        let mut new_config = Config::default();
        new_config.buffer.flush_rows = 99999;
        shared.update(new_config);

        rx.changed().await.unwrap();
        let updated = shared.read();
        assert_eq!(updated.buffer.flush_rows, 99999);
        assert_eq!(shared.version(), 1);
    }
}
