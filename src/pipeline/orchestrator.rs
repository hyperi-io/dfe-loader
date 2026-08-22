// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Main pipeline coordinator.
//!
//! Orchestrates the Transport → Transform → Buffer → `ClickHouse` pipeline.
//!
//! Uses the scalo Transport abstraction for message sources (Kafka/Memory).
//! Processes messages in batches for efficiency.
//!
//! Accumulates rows as `Map<String, Value>` per table, then flushes via
//! `JSONEachRow` HTTP inserts to `ClickHouse`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use rustc_hash::FxHashSet;

use scalo::ScalingPressure;
use scalo::SelfRegulationGovernor;
use scalo::dlq::{Dlq, DlqEntry};
use scalo::memory::{MemoryGuard, MemoryGuardConfig};

use crate::Result;
use crate::buffer::{BufferManager, FlushBatch, KafkaOffset};
use crate::clickhouse::{
    ClickHouseQueryClient, Inserter, InserterConfig, SchemaCache, SharedSchemaCache,
};
use crate::column_meta::{ColumnMetaCache, parse_directives};
use crate::config::{Config, SharedConfig};
use crate::kafka::{TransportAdapter, TransportBackend};
use crate::metrics::Metrics;
use crate::payload::{FormatDetector, FormatMode};
use crate::routing::Router;
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

// EnrichmentPipeline moved to super::enrichment (enrichment.rs)
// CaptureOverrides moved to super::capture (capture.rs)
// TableResolutionResult moved to super::types (types.rs)

use super::capture::CaptureOverrides;
use super::enrichment::EnrichmentPipeline;
use super::types::TableResolutionResult;

/// Orchestrates the Kafka → `ClickHouse` pipeline
pub struct Orchestrator {
    config: Config,
    shared_config: Option<SharedConfig>,
    shutdown: CancellationToken,
    stats: PipelineStats,
    metrics: Option<Metrics>,
    scaling: Option<Arc<ScalingPressure>>,
    /// Cgroup-aware memory guard. In production this is the runtime's shared
    /// guard (the SAME one feeding the self-regulation governor and the worker
    /// pool), set via [`with_memory_guard`](Self::with_memory_guard); a
    /// stand-alone guard is built only as a test/default fallback. Accounting
    /// here (`add_bytes` on recv, `release` after flush) drives the inbound
    /// pause-partitions brake.
    memory_guard: Arc<MemoryGuard>,
    /// Self-regulation governor (default-on). When `Some`, its Kafka
    /// pause-partitions gate is attached to the receive transport so inbound
    /// intake brakes under memory pressure. `None` when self-regulation is
    /// disabled (`self_regulation.enabled = false`).
    governor: Option<SelfRegulationGovernor>,
    worker_pool: Option<Arc<scalo::worker::AdaptiveWorkerPool>>,
    batch_engine: Option<Arc<scalo::worker::BatchEngine>>,
    /// Lightweight ClickHouse sink-health latch driving `set_circuit_open`.
    /// Set when a whole flush cycle fails with zero successful inserts (sink
    /// unreachable); cleared the moment any insert succeeds. This is the real,
    /// observable "sink dead" signal — the loader's per-table CircuitBreaker is
    /// not wired into the inserter, so we derive the gate from insert outcomes.
    sink_circuit_open: bool,
}

impl Orchestrator {
    /// Create a new orchestrator with config.
    ///
    /// Builds a stand-alone memory guard as a test/default fallback. In
    /// production the runtime's shared guard is injected via
    /// [`with_memory_guard`](Self::with_memory_guard).
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
            governor: None,
            worker_pool: None,
            batch_engine: None,
            sink_circuit_open: false,
        }
    }

    /// Create a new orchestrator with config and metrics.
    ///
    /// Builds a stand-alone memory guard as a test/default fallback. In
    /// production the runtime's shared guard is injected via
    /// [`with_memory_guard`](Self::with_memory_guard).
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
            governor: None,
            worker_pool: None,
            batch_engine: None,
            sink_circuit_open: false,
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

    /// Set the adaptive worker pool for parallel message processing.
    pub fn with_worker_pool(mut self, pool: Arc<scalo::worker::AdaptiveWorkerPool>) -> Self {
        self.worker_pool = Some(pool);
        self
    }

    /// Set the batch processing engine (SIMD parse, pre-route, parallel transform).
    pub fn with_batch_engine(mut self, engine: Arc<scalo::worker::BatchEngine>) -> Self {
        self.batch_engine = Some(engine);
        self
    }

    /// Inject the runtime's shared cgroup-aware memory guard.
    ///
    /// This replaces the stand-alone fallback guard so the orchestrator accounts
    /// in-flight bytes on the SAME guard that feeds the self-regulation governor
    /// and the worker pool — without it the inbound brake would read a guard the
    /// pipeline never touches.
    pub fn with_memory_guard(mut self, guard: Arc<MemoryGuard>) -> Self {
        self.memory_guard = guard;
        self
    }

    /// Set the self-regulation governor (default-on).
    ///
    /// When `Some`, its Kafka pause-partitions inbound gate is attached to the
    /// receive transport in [`run`](Self::run). `None` disables self-regulation
    /// (byte-identical to the pre-governor data path).
    pub fn with_governor(mut self, governor: Option<SelfRegulationGovernor>) -> Self {
        self.governor = governor;
        self
    }

    /// Access the memory guard (for worker pool integration in main.rs).
    pub fn memory_guard(&self) -> &Arc<MemoryGuard> {
        &self.memory_guard
    }

    /// Get the shutdown token for external shutdown requests
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Run the pipeline until shutdown
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting pipeline orchestrator");

        // Initialize transport backend (Kafka based on config). When a
        // self-regulation governor is present, its Kafka pause-partitions inbound
        // gate is attached to the receiver so intake brakes under memory pressure
        // (the gate is evaluated automatically inside recv). The outbound
        // ClickHouse insert drain is NEVER gated — gating the sink would deadlock.
        let transport = TransportBackend::from_config(&self.config, self.governor.as_ref()).await?;
        info!(
            transport = transport.name(),
            governed = self.governor.is_some(),
            "Transport initialized"
        );

        // Validate ClickHouse config (transport/port mismatch, JSONEachRow+native)
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

        // Build the insert client -- same transport as the query client.
        // RowBinary (DynamicInsert) dispatches HTTP/TCP via insert_native_with_columns;
        // JSONEachRow (InsertFormatted) is HTTP-only.
        let ch_client = crate::clickhouse::client_http::build_client(&ch_config)
            .map_err(|e| crate::Error::ClickHouse(e.to_string()))?;

        let insert_format = ch_config.insert_format;
        info!(format = %insert_format, "Insert format configured");

        // Inserter dispatches based on insert_format — single client handles all inserts.
        // Schema cache is wired below (after creation) for drift-error invalidation.
        let mut inserter = Inserter::new(
            Arc::clone(&http_client),
            ch_client,
            InserterConfig::default(),
        )
        .with_insert_format(insert_format);

        // DLQ (unified scalo module — cascade: Kafka primary, file fallback)
        let dlq_config = self.config.routing.dlq.to_scalo_config();
        let transport_kafka_config = TransportAdapter::convert_config(&self.config.kafka);
        let dlq: Option<Arc<Dlq>> = if dlq_config.enabled {
            match Dlq::spawn(
                &dlq_config,
                "loader",
                Some(&transport_kafka_config),
                self.shutdown.clone(),
            ) {
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

        // Background refresh — proactively re-fetches schemas before TTL expiry.
        // Without this, expired schemas cause the extractor path to fall back to
        // the transformer path, silently dropping @renamed directive mappings (#25).
        let _schema_refresh_handle =
            schema_cache.start_background_refresh(Arc::clone(&http_client));

        // Wire schema cache into inserter for drift-error invalidation (fixes #20).
        // RowBinary inserts that fail with data errors (e.g. "Cannot parse JSON",
        // "type mismatch") now invalidate the schema cache and retry with fresh schema.
        inserter = inserter.with_schema_cache(Arc::clone(&schema_cache));

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

        // Pending-schema buffer — holds messages whose table schema is not yet cached.
        // Drained when the background resolver populates the schema (later task).
        let mut pending_schema_buffer = super::pending_schema::PendingSchemaBuffer::new(
            super::pending_schema::PendingSchemaConfig::from_schema_config(&self.config.schema),
        );

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

        // Hot-reload: subscribe to config changes if SharedConfig is available
        let mut config_rx = self
            .shared_config
            .as_ref()
            .map(scalo::SharedConfig::subscribe);

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

        // Pre-warm the schema cache with bounded-backoff retry so a brief
        // ClickHouse outage at startup recovers before the first message —
        // failed tables otherwise fall to the silent-loss transformer path (#36).
        // Tables still failing after the budget fall back to per-message
        // queue-and-retry. Comments are collected and applied to capture
        // overrides after the loop (capture_overrides is &mut and cannot be
        // borrowed inside the warm closure's future).
        {
            let pre_warm_budget = Duration::from_secs(self.config.schema.pre_warm_retry_secs);
            let tables = collect_pre_warm_tables(&self.config.routing);
            let comments_cell: Arc<std::sync::Mutex<Vec<(String, String)>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let report = {
                let http = &http_client;
                let sc = &schema_cache;
                let cm = &col_meta_cache;
                let comments = Arc::clone(&comments_cell);
                pre_warm_with_retry(
                    move |t| {
                        let comments = Arc::clone(&comments);
                        warm_tables_once(t, http, sc, cm, comments)
                    },
                    tables,
                    pre_warm_budget,
                    &self.shutdown,
                )
                .await
            };
            let collected = Arc::try_unwrap(comments_cell)
                .unwrap_or_else(|a| std::sync::Mutex::new(a.lock().unwrap().clone()))
                .into_inner()
                .unwrap_or_default();
            for (table, comment) in collected {
                capture_overrides.update_from_comment(&table, &comment);
            }
            info!(
                succeeded = report.succeeded.len(),
                failed = report.failed.len(),
                rounds = report.rounds,
                "Schema cache pre-warm complete"
            );
            if !report.failed.is_empty() {
                warn!(
                    tables = ?report.failed,
                    "Pre-warm gave up on some tables — will retry per-message"
                );
            }
            if let Some(ref m) = self.metrics {
                m.update_schema_prewarm_failed_tables(report.failed.len());
                // Rounds beyond the first are retries.
                for _ in 1..report.rounds {
                    m.record_schema_prewarm_retry();
                }
            }
        }

        loop {
            tokio::select! {
                biased; // Prioritize shutdown check

                () = self.shutdown.cancelled() => {
                    // Drain the pending-schema buffer to DLQ — these messages
                    // never received a schema and cannot be processed (#36).
                    let drained = pending_schema_buffer.drain_all();
                    let drained_n = drained.len() as u64;
                    for (msg, reason) in drained {
                        route_pending_to_dlq(
                            &dlq_tx,
                            dlq.is_some(),
                            &self.memory_guard,
                            &self.metrics,
                            msg,
                            &reason,
                        );
                    }
                    if drained_n > 0 {
                        self.stats.messages_dlq += drained_n;
                        warn!(count = drained_n, "Drained pending-schema buffer to DLQ on shutdown");
                    }
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

                        scalo::logger::security::config_changed(
                            "config_reload",
                            "system",
                            &format!("pipeline config reloaded (version {version})"),
                        );
                        info!(version = version, "Config hot-reload complete");
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

                    // Push the per-pod Kafka inbound scaling signal into the
                    // unified ScalingPressure engine (scalo 2.10 collapsed the old
                    // separate scaling-signal cell into ONE ScalingPressure served
                    // to KEDA at /scaling/pressure). assigned_lag() sums lag over
                    // THIS pod's ASSIGNED partitions (scale-invariant). gRPC has no
                    // broker lag -> None -> the kafka_lag term stays 0 (CPU-only).
                    // The flush tick (every flush_age_secs, default 5s) is fresher
                    // than the engine's evaluation tick. The circuit-open gate is
                    // pushed from the flush path on every insert outcome.
                    if let Some(ref scaling) = self.scaling
                        && let Some(lag) = transport.assigned_lag()
                    {
                        scaling.set_component("kafka_lag", lag as f64);
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

                // Receive batch of messages from transport.
                // Zero-copy: payload Bytes is moved, topic is an Arc<str> clone
                // (refcount only).
                //
                // Inbound memory backpressure is handled by the self-regulation
                // governor, NOT a hand-rolled pause loop here: the Kafka
                // pause-partitions gate (attached to the receive transport) pauses
                // the consumer's ASSIGNED partitions under pressure — the member
                // stays in the group (no rebalance), consumer lag rises, KEDA
                // scales up. The gate is evaluated automatically inside recv, so
                // recv simply returns an empty batch while paused. We never gate
                // the outbound ClickHouse drain — gating the sink would deadlock.
                received = transport.recv(RECV_BATCH_SIZE) => {
                    // Surface any inbound-filter DLQ entries (no silent drop). The
                    // loader configures no inbound scalo filters, so this is
                    // normally empty, but the contract is honoured regardless.
                    let messages = match received {
                        Ok(batch) => {
                            for entry in batch.dlq_entries {
                                if dlq.is_some() {
                                    let dlq_entry = DlqEntry::new(
                                        "loader",
                                        entry.reason,
                                        entry.payload,
                                    );
                                    if dlq_tx.try_send(dlq_entry).is_ok() {
                                        self.stats.messages_dlq += 1;
                                    }
                                    if let Some(ref m) = self.metrics {
                                        m.record_dlq();
                                    }
                                }
                            }
                            Ok(batch.messages)
                        }
                        Err(e) => Err(e),
                    };

                    // --- Pending-schema buffer maintenance (#36) ---
                    // Expire stale / globally-evicted entries to DLQ, then
                    // re-request resolution for tables still stuck (e.g. a failed
                    // fetch during a transient ClickHouse outage). Runs every recv
                    // tick regardless of whether a batch arrived.
                    //
                    // Note: with inbound backpressure now handled at the transport
                    // (the governor's pause-partitions gate), there is no
                    // memory-pressure `continue` short-circuiting this block — recv
                    // simply returns an empty batch every poll while partitions are
                    // paused. Maintenance therefore keeps running under sustained
                    // pressure: stuck tables still re-request and aged entries still
                    // expire, while no new payload bytes are admitted. The global
                    // cap still bounds the buffer via eviction on enqueue.
                    let now_pending = std::time::Instant::now();
                    {
                        let expired = pending_schema_buffer.expire(now_pending);
                        let expired_n = expired.len() as u64;
                        for (msg, reason) in expired {
                            route_pending_to_dlq(
                                &dlq_tx,
                                dlq.is_some(),
                                &self.memory_guard,
                                &self.metrics,
                                msg,
                                &reason,
                            );
                        }
                        if expired_n > 0 {
                            self.stats.messages_dlq += expired_n;
                            warn!(count = expired_n, "Expired pending-schema messages to DLQ");
                        }
                        for table in pending_schema_buffer
                            .tables_needing_rerequest(now_pending, PENDING_REREQUEST_INTERVAL)
                        {
                            let _ = resolve_tx.try_send(table);
                        }
                    }

                    // Merge messages whose schema just resolved (re-processed via
                    // the extractor path) ahead of the freshly received batch.
                    // Their memory was counted on first receipt and is NOT
                    // re-counted. take_ready only runs on a transport Ok, so
                    // resolved messages are never dropped on a transport error.
                    let combined: crate::Result<Vec<crate::kafka::KafkaMessage>> = match messages {
                        Ok(mut fresh) => {
                            for msg in &fresh {
                                self.memory_guard.add_bytes(msg.payload.len() as u64);
                            }
                            self.stats.messages_received += fresh.len() as u64;
                            if let Some(ref m) = self.metrics {
                                for _ in 0..fresh.len() {
                                    m.record_received();
                                }
                            }
                            let ready = pending_schema_buffer.take_ready(&schema_cache);
                            if ready.is_empty() {
                                Ok(fresh)
                            } else {
                                let mut merged = Vec::with_capacity(ready.len() + fresh.len());
                                merged.extend(ready);
                                merged.append(&mut fresh);
                                Ok(merged)
                            }
                        }
                        Err(e) => Err(e),
                    };

                    match combined {
                        Ok(batch) if !batch.is_empty() => {
                            debug!(
                                batch_size = batch.len(),
                                "Batch received from transport"
                            );

                            let batch_start = std::time::Instant::now();

                            // === PRE-ROUTE PHASE (engine SIMD filter) ===
                            // When batch engine is available, use SIMD pre-route to
                            // skip full parse for messages that will be filtered/DLQ'd.
                            // Messages that pass pre-route are processed by MessageProcessor.
                            let pre_route_filtered = if let Some(ref engine) = self.batch_engine
                                && let Some(ref field) = engine.config().routing_field
                            {
                                use scalo::worker::engine::pre_route::{
                                    PreRouteOutcome, apply_filters, extract_routing_field,
                                    filters_from_config,
                                };
                                let filters = filters_from_config(&engine.config().pre_route_filters);
                                let mut pass_indices: Vec<usize> = Vec::with_capacity(batch.len());
                                let mut filtered_count: u64 = 0;
                                let mut dlq_entries: Vec<(usize, String)> = Vec::new();

                                for (idx, msg) in batch.iter().enumerate() {
                                    let extraction = extract_routing_field(&msg.payload, field);
                                    let outcome = apply_filters(&extraction, &filters);
                                    match outcome {
                                        PreRouteOutcome::Continue => pass_indices.push(idx),
                                        PreRouteOutcome::Filtered => {
                                            filtered_count += 1;
                                            // Release memory for filtered messages
                                            self.memory_guard.release(msg.payload.len() as u64);
                                        }
                                        PreRouteOutcome::Dlq(reason) => {
                                            dlq_entries.push((idx, reason));
                                        }
                                    }
                                }

                                if filtered_count > 0 {
                                    debug!(filtered = filtered_count, "Pre-route filtered messages (SIMD)");
                                    self.stats.messages_processed += filtered_count;
                                }

                                // Send DLQ entries for pre-route failures
                                for (idx, reason) in &dlq_entries {
                                    let msg = &batch[*idx];
                                    if dlq.is_some() {
                                        let entry = DlqEntry::new("loader", reason.clone(), msg.payload.clone())
                                            .with_source(scalo::dlq::DlqSource::kafka(
                                                &*msg.topic,
                                                msg.partition,
                                                msg.offset,
                                            ));
                                        if dlq_tx.try_send(entry).is_ok() {
                                            self.stats.messages_dlq += 1;
                                            scalo::logger::security::record_dlq(
                                                "pre_route",
                                                reason,
                                                Some(&format!(
                                                    "topic: {}, partition: {}, offset: {}",
                                                    msg.topic, msg.partition, msg.offset
                                                )),
                                            );
                                        }
                                    }
                                    self.memory_guard.release(msg.payload.len() as u64);
                                    if let Some(ref m) = self.metrics {
                                        m.record_dlq();
                                    }
                                }

                                Some(pass_indices)
                            } else {
                                None // No pre-route — process all messages
                            };

                            // === PARALLEL PHASE ===
                            // Create immutable processor (borrows caches as &)
                            let processor = super::processor::MessageProcessor {
                                config: &self.config,
                                router: &router,
                                transformer: &transformer,
                                extractor: &extractor,
                                format_detector: &format_detector,
                                json_primary_mode,
                                enrichment: &enrichment,
                                schema_cache: &schema_cache,
                                col_meta_cache: &col_meta_cache,
                                field_mapping_cache: field_mapping_cache.as_ref(),
                                computed_column_cache: &computed_column_cache,
                                capture_overrides: &capture_overrides,
                            };

                            // Process batch — engine pool preferred, then worker pool,
                            // then sequential. Pre-route indices filter which messages
                            // get processed (filtered/DLQ'd messages are skipped).
                            let pool = self.batch_engine.as_ref().map(|e| e.pool())
                                .or(self.worker_pool.as_ref());

                            let results: Vec<crate::Result<super::types::ProcessedMessage>> =
                                match (&pre_route_filtered, pool) {
                                    // Pre-route active + pool: process only passing messages in parallel
                                    (Some(indices), Some(pool)) => {
                                        let msgs_to_process: Vec<&crate::kafka::KafkaMessage> =
                                            indices.iter().map(|&i| &batch[i]).collect();
                                        pool.process_batch(&msgs_to_process, |msg| processor.process(msg))
                                    }
                                    // Pre-route active, no pool: process only passing messages sequentially
                                    (Some(indices), None) => {
                                        indices.iter().map(|&i| processor.process(&batch[i])).collect()
                                    }
                                    // No pre-route + pool: process all in parallel (original path)
                                    (None, Some(pool)) => {
                                        pool.process_batch(&batch, |msg| processor.process(msg))
                                    }
                                    // No pre-route, no pool: sequential fallback
                                    (None, None) => {
                                        batch.iter().map(|msg| processor.process(msg)).collect()
                                    }
                                };
                            // Processor dropped here — immutable borrows released

                            // === SEQUENTIAL PHASE ===
                            let mut coordinator = super::coordinator::BatchCoordinator {
                                buffer_manager: &mut buffer_manager,
                                capture_overrides: &mut capture_overrides,
                                field_mapping_cache: &mut field_mapping_cache,
                                computed_column_cache: &mut computed_column_cache,
                                metrics: &self.metrics,
                                dlq_tx: &dlq_tx,
                                dlq_enabled: dlq.is_some(),
                                memory_guard: &self.memory_guard,
                                pending_schema: &mut pending_schema_buffer,
                            };
                            // When pre-route is active, results only contain passing
                            // messages — build the matching message slice.
                            let outcome = if let Some(ref indices) = pre_route_filtered {
                                let filtered_batch: Vec<&crate::kafka::KafkaMessage> =
                                    indices.iter().map(|&i| &batch[i]).collect();
                                coordinator.apply_results_refs(results, &filtered_batch)
                            } else {
                                coordinator.apply_results(results, &batch)
                            };

                            let batch_elapsed = batch_start.elapsed();
                            debug!(
                                batch_size = batch.len(),
                                duration_ms = batch_elapsed.as_millis(),
                                processed = outcome.processed,
                                errors = outcome.errors,
                                pending_schema = pending_schema_buffer.len(),
                                "Batch processed"
                            );

                            // Trace per-table routing distribution for this batch
                            if tracing::enabled!(tracing::Level::TRACE) {
                                let buf_stats_tables = buffer_manager.per_table_stats();
                                let tables: Vec<&str> =
                                    buf_stats_tables.iter().map(|(t, _, _)| *t).collect();
                                trace!(
                                    total = batch.len(),
                                    routed_tables = tables.len(),
                                    tables = ?tables,
                                    "Batch routing complete"
                                );
                            }

                            // Update stats from coordinator outcome
                            self.stats.messages_processed += outcome.processed;
                            self.stats.messages_dlq += outcome.errors;
                            // stats.errors tracks ClickHouse insert failures only (in flush_batches_transport),
                            // NOT per-message processing failures. Do not increment here.

                            // Update buffer stats (once per batch, not per message)
                            let buf_stats = buffer_manager.stats();
                            if let Some(ref m) = self.metrics {
                                m.update_buffer_stats(
                                    buf_stats.pending_rows,
                                    buf_stats.pending_bytes,
                                    buf_stats.pending_chunks,
                                );

                                // Messages held awaiting schema resolution (#36)
                                m.update_pending_schema_messages(pending_schema_buffer.len());

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
                                // Tables newly buffered for schema resolution (#36) —
                                // kick off their first resolution request.
                                for table in outcome.needs_resolution {
                                    if seen.insert(table.clone())
                                        && resolve_tx.try_send(table).is_err() {
                                            debug!("Schema resolve channel full, will retry next tick");
                                        }
                                }
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

        // Stop schema cache background refresh task.
        schema_cache.shutdown();

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

    // process_message moved to super::processor::MessageProcessor

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

        // Inserter in-flight / queue depth (2.8.10 audit): the number of insert
        // tasks the inserter runs concurrently this cycle (bounded by its
        // semaphore). A high steady value means inserts are the bottleneck.
        if let Some(ref m) = self.metrics {
            m.set_inserter_inflight(batch_count as u64);
        }

        let start = Instant::now();
        let results = inserter.insert_batches(batches_for_insert).await;
        let latency = start.elapsed().as_secs_f64();

        // Drain the in-flight gauge once the concurrent inserts complete.
        if let Some(ref m) = self.metrics {
            m.set_inserter_inflight(0);
        }

        // Update scaling pressure with insert latency
        if let Some(ref scaling) = self.scaling {
            scaling.set_component("insert_latency", latency);
        }

        // Track per-cycle insert outcomes to derive the ClickHouse sink
        // circuit-open scaling gate (sink dead == whole cycle failed).
        let mut cycle_ok = 0usize;
        let mut cycle_err = 0usize;

        // Commit offsets independently per batch — Table A success/failure is isolated
        for ((result, offsets), batch_bytes) in results
            .into_iter()
            .zip(per_batch_offsets)
            .zip(per_batch_bytes)
        {
            // Release tracked memory regardless of insert outcome.
            // Success: data is in ClickHouse, memory freed.
            // Failure: offsets withheld, Kafka re-delivers — we'll re-track on re-consume.
            self.memory_guard.release(batch_bytes);

            match result {
                Ok(count) => {
                    cycle_ok += 1;
                    self.stats.rows_inserted += count as u64;
                    if let Some(ref m) = self.metrics {
                        m.record_flush(count, latency);
                        m.record_insert_quantities(batch_bytes, 1);
                        // ClickHouse flush-size (bytes) distribution (2.8.10 audit).
                        m.record_flush_bytes(batch_bytes);
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
                    cycle_err += 1;
                    error!(error = %e, "Batch insert failed — offsets withheld, messages will re-deliver");
                    self.stats.errors += 1;
                    if let Some(ref m) = self.metrics {
                        m.record_error();
                        // ClickHouse-specific terminal insert error (2.8.10 audit).
                        m.record_clickhouse_insert_error();
                    }
                }
            }
        }

        // Refresh the ClickHouse rows-per-second gauge once per flush cycle.
        if let Some(ref m) = self.metrics {
            m.update_clickhouse_rows_per_sec();
        }

        // Drive the ClickHouse sink circuit-open gate on the unified
        // ScalingPressure engine. The sink is "dead" when a whole flush cycle
        // failed with zero successes; it recovers the moment any insert succeeds.
        // The engine zeroes the composite while circuit_open is true (more pods
        // can't relieve a dead sink) — exactly the right behaviour for a
        // non-scalo outbound.
        if cycle_ok > 0 {
            self.sink_circuit_open = false;
        } else if cycle_err > 0 {
            self.sink_circuit_open = true;
        }
        if let Some(ref scaling) = self.scaling {
            scaling.set_circuit_open(self.sink_circuit_open);
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
/// Maps dfe-loader config fields to the scalo `MemoryGuardConfig`.
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

// Enrichment helpers moved to super::enrichment (enrichment.rs)

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

/// Report from `pre_warm_with_retry`.
#[derive(Debug, Default)]
pub(crate) struct PreWarmReport {
    pub succeeded: Vec<String>,
    pub failed: Vec<String>,
    pub rounds: usize,
}

/// Collect the config-known tables to pre-warm (default table + CEL rule
/// targets + source_to_table targets), deduplicated.
fn collect_pre_warm_tables(routing: &crate::config::RoutingConfig) -> Vec<String> {
    let default_db = &routing.default_db;
    let mut tables: Vec<String> = vec![format!("{default_db}.{}", routing.default_table)];
    for rule in &routing.rules {
        let db = rule.db.as_deref().unwrap_or(default_db);
        tables.push(format!("{db}.{}", rule.target));
    }
    for table in routing.source_to_table.values() {
        tables.push(format!("{default_db}.{table}"));
    }
    tables.sort();
    tables.dedup();
    tables
}

/// Bounded-backoff retry over still-failing tables.
///
/// `warm` is invoked per round with the tables still needing a schema and
/// returns per-table success. Backoff between rounds: 500ms, 1s, 2s, 4s, 8s,
/// then 8s capped. Selects on `shutdown` so it never delays process exit.
/// A `budget` of 0 means a single attempt with no retry.
pub(crate) async fn pre_warm_with_retry<F, Fut>(
    mut warm: F,
    initial_tables: Vec<String>,
    budget: Duration,
    shutdown: &tokio_util::sync::CancellationToken,
) -> PreWarmReport
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = Vec<(String, bool)>>,
{
    use std::time::Instant;

    let deadline = Instant::now() + budget;
    let mut report = PreWarmReport::default();
    let mut to_warm = initial_tables;
    let backoff_steps = [500u64, 1000, 2000, 4000, 8000];
    let mut backoff_idx: usize = 0;

    loop {
        if shutdown.is_cancelled() {
            report.failed.extend(to_warm);
            return report;
        }
        report.rounds += 1;
        let results = warm(to_warm.clone()).await;

        let mut still_failing = Vec::new();
        for (table, ok) in results {
            if ok {
                if !report.succeeded.contains(&table) {
                    report.succeeded.push(table);
                }
            } else {
                still_failing.push(table);
            }
        }

        if still_failing.is_empty() {
            return report;
        }
        if Instant::now() >= deadline {
            report.failed = still_failing;
            return report;
        }

        let sleep_ms = backoff_steps[backoff_idx.min(backoff_steps.len() - 1)];
        backoff_idx = backoff_idx.saturating_add(1);
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                report.failed = still_failing;
                return report;
            }
            () = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
        }
        to_warm = still_failing;
    }
}

/// Production single-pass warm: fetch schema + column comments + table comment
/// for each table, populate the schema cache and column-meta cache, and collect
/// non-empty table comments into `comments_out` for the caller to apply to
/// `CaptureOverrides` afterwards. Returns per-table success.
///
/// All borrowed parameters are SHARED references (the caches use interior
/// mutability via `Arc`), so the returned future borrows the caller's scope —
/// NOT a closure environment — which keeps `pre_warm_with_retry`'s closure
/// bound satisfiable.
///
/// `comments_out` uses `Arc<Mutex<_>>` rather than `RefCell` so that the
/// future is `Send` when the calling async task requires it.
async fn warm_tables_once(
    tables: Vec<String>,
    http_client: &Arc<ClickHouseQueryClient>,
    schema_cache: &SharedSchemaCache,
    col_meta_cache: &Arc<ColumnMetaCache>,
    comments_out: Arc<std::sync::Mutex<Vec<(String, String)>>>,
) -> Vec<(String, bool)> {
    use futures::future::join_all;

    let results: Vec<_> = join_all(tables.into_iter().map(|table| {
        let client = Arc::clone(http_client);
        async move {
            let (schema_res, comments_res, comment_res) = tokio::join!(
                client.fetch_table_schema(&table),
                client.fetch_column_comments(&table),
                client.fetch_table_comment(&table),
            );
            (table, schema_res, comments_res, comment_res)
        }
    }))
    .await;

    let mut out = Vec::with_capacity(results.len());
    for (table, schema_res, comments_res, comment_res) in results {
        match schema_res {
            Ok(schema) => {
                schema_cache.insert(table.clone(), schema);
                if let Ok(comments) = comments_res {
                    let directives = comments
                        .into_iter()
                        .map(|(col, comment)| (col, parse_directives(&comment)))
                        .collect();
                    col_meta_cache.apply_ddl(&table, directives);
                }
                if let Ok(comment) = comment_res
                    && !comment.is_empty()
                    && let Ok(mut guard) = comments_out.lock()
                {
                    guard.push((table.clone(), comment));
                }
                debug!(table = %table, "Pre-warmed schema cache");
                out.push((table, true));
            }
            Err(e) => {
                // Name the fault: a swallowed fetch error here once hid a
                // wrong-protocol config behind a bare failed-count (#115).
                warn!(table = %table, error = %e, "Schema pre-warm fetch failed");
                out.push((table, false));
            }
        }
    }
    out
}

/// Interval between resolution re-requests for tables still stuck in the
/// pending-schema buffer (e.g. resolution failed due to a transient CH outage).
const PENDING_REREQUEST_INTERVAL: Duration = Duration::from_secs(2);

/// Human-readable DLQ reason string for an expired / evicted / shutdown-drained
/// pending-schema message.
fn format_pending_reason(reason: &super::pending_schema::ExpireReason) -> String {
    use super::pending_schema::ExpireReason;
    match reason {
        ExpireReason::AgeExceeded { age_ms, table } => {
            format!("schema_pending_timeout table={table} age_ms={age_ms}")
        }
        ExpireReason::GlobalCapEviction { table } => {
            format!("pending_schema_global_overflow table={table}")
        }
        ExpireReason::Shutdown { table } => format!("schema_pending_shutdown table={table}"),
    }
}

/// Security-event category label for a pending-schema DLQ reason.
fn pending_reason_label(reason: &super::pending_schema::ExpireReason) -> &'static str {
    use super::pending_schema::ExpireReason;
    match reason {
        ExpireReason::AgeExceeded { .. } => "schema_pending_timeout",
        ExpireReason::GlobalCapEviction { .. } => "pending_schema_global_overflow",
        ExpireReason::Shutdown { .. } => "schema_pending_shutdown",
    }
}

/// Route a pending-schema message to the DLQ (with a security event) and
/// release its tracked memory. Used by the per-tick expire sweep and the
/// shutdown drain — these messages never received a schema, so the loss is
/// surfaced (DLQ + security event), never silent (#36).
fn route_pending_to_dlq(
    dlq_tx: &mpsc::Sender<DlqEntry>,
    dlq_enabled: bool,
    memory_guard: &MemoryGuard,
    metrics: &Option<Metrics>,
    msg: crate::kafka::KafkaMessage,
    reason: &super::pending_schema::ExpireReason,
) {
    let reason_str = format_pending_reason(reason);
    if dlq_enabled {
        let entry = DlqEntry::new("loader", reason_str.clone(), msg.payload.clone()).with_source(
            scalo::dlq::DlqSource::kafka(&*msg.topic, msg.partition, msg.offset),
        );
        let _ = dlq_tx.try_send(entry);
    }
    scalo::logger::security::record_dlq(pending_reason_label(reason), &reason_str, None);
    // Surface in Prometheus too — both the generic DLQ counter and the
    // pending-schema-specific counter — so dashboards see this loss class (#36).
    if let Some(m) = metrics {
        m.record_dlq();
        m.record_pending_schema_expired();
    }
    memory_guard.release(msg.payload.len() as u64);
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

    // CaptureOverrides tests moved to capture.rs

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

    #[tokio::test]
    async fn pre_warm_retry_succeeds_after_failures() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = tokio_util::sync::CancellationToken::new();
        let calls_inner = Arc::clone(&calls);
        let report = pre_warm_with_retry(
            move |tables: Vec<String>| {
                let calls = Arc::clone(&calls_inner);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    // fail rounds 0 and 1, succeed from round 2 (3rd call)
                    tables.into_iter().map(|t| (t, n >= 2)).collect()
                }
            },
            vec!["dfe.late".to_string()],
            Duration::from_secs(10),
            &shutdown,
        )
        .await;
        assert_eq!(report.succeeded, vec!["dfe.late".to_string()]);
        assert!(report.failed.is_empty());
        assert!(report.rounds >= 3);
    }

    #[tokio::test]
    async fn pre_warm_retry_gives_up_at_budget() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let report = pre_warm_with_retry(
            |tables: Vec<String>| async move { tables.into_iter().map(|t| (t, false)).collect() },
            vec!["dfe.never".to_string()],
            Duration::from_millis(800),
            &shutdown,
        )
        .await;
        assert!(report.succeeded.is_empty());
        assert_eq!(report.failed, vec!["dfe.never".to_string()]);
    }

    #[tokio::test]
    async fn pre_warm_retry_cancels_on_shutdown() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            shutdown_clone.cancel();
        });
        let t0 = std::time::Instant::now();
        let report = pre_warm_with_retry(
            |tables: Vec<String>| async move { tables.into_iter().map(|t| (t, false)).collect() },
            vec!["dfe.x".to_string()],
            Duration::from_secs(60),
            &shutdown,
        )
        .await;
        assert!(
            t0.elapsed() < Duration::from_secs(3),
            "should cancel quickly"
        );
        assert!(!report.failed.is_empty());
    }
}
