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
    BatchDisposition, ClickHouseError, ClickHouseQueryClient, FailedRow, Inserter, InserterConfig,
    SchemaCache, SharedSchemaCache,
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
use super::types::{SchemaResolution, TableResolutionResult};

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
        let transport_kafka_config =
            TransportAdapter::convert_config(&self.config.kafka, &self.config.routing.dlq);
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

        // A DLQ whose backends all failed to build still spawns Ok: scalo hands
        // back a DISABLED Dlq, whose send() counts a drop and returns Ok. So
        // is_some() answers "was a DLQ asked for", not "will it keep anything",
        // and reading it to gate an offset commit throws the rows away and
        // commits over the top. Read what the DLQ can actually do instead --
        // a read-only rootfs on a gRPC-transport pod reaches this by config
        // alone (dlq.enabled with neither the file nor the Kafka backend).
        let dlq_enabled = dlq_accepts(dlq.as_ref());
        if dlq.is_some() && !dlq_enabled {
            error!(
                "DLQ is configured but no backend started — nothing can be DLQ'd, \
                 so permanently rejected rows will withhold their offsets"
            );
        }

        // Bounded DLQ channel — avoids unbounded tokio::spawn per failed message.
        // Background task drains the channel and forwards to the actual DLQ backend.
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
                                // Keep the reason: the event loop can only fall
                                // back to the default table safely if it can tell
                                // an absent table from an unreachable ClickHouse.
                                let schema = match schema_res {
                                    Ok(s) => SchemaResolution::Resolved(s),
                                    Err(ClickHouseError::TableNotFound(_)) => {
                                        SchemaResolution::TableNotFound
                                    }
                                    Err(e) => {
                                        debug!(table = %table, error = %e, "Schema fetch failed, will retry");
                                        SchemaResolution::Unavailable
                                    }
                                };
                                let _ = tx.send(TableResolutionResult {
                                    table,
                                    comment: comment_res.unwrap_or_default(),
                                    schema,
                                    column_directives,
                                }).await;
                            });
                        }
                    }
                }
            });
        }

        // Background refresh — expiring tables go back through the resolver, so a
        // table that keeps taking traffic has its column COMMENT directives
        // re-read and not only its column set (#182). Without a refresh at all an
        // expired schema drops the extractor path and its @renamed mappings (#25).
        let _schema_refresh_handle = schema_cache.start_background_refresh(resolve_tx.clone());

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

        // Tables ClickHouse has confirmed absent. The processor re-routes their
        // messages to the default table; a failed fetch never lands here.
        // Entries expire so a table created after its source started producing
        // begins receiving its own data without a pod restart.
        let mut absent_tables =
            super::types::AbsentTables::new(ABSENT_TABLE_TTL, ABSENT_TABLE_CAPACITY);
        let default_table = format!(
            "{}.{}",
            self.config.routing.default_db, self.config.routing.default_table
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
                            dlq_enabled,
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
                        // Read after the take, so it names only what stayed behind.
                        let still_buffered = buffer_manager.lowest_pending_offsets();
                        self.flush_batches_transport(&inserter, &transport, dlq.as_ref(), batches, still_buffered).await;
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

                    // Apply computed columns (uses ColumnMetaCache for CEL expressions)
                    computed_column_cache.build_and_cache(table, &col_meta_cache);

                    match result.schema {
                        SchemaResolution::Resolved(schema) => {
                            // The table exists now, so stop diverting its events.
                            absent_tables.remove(table);
                            // Apply field mapping (needs schema; uses ColumnMetaCache for rename rules)
                            if let Some(ref mut fm) = field_mapping_cache {
                                fm.build_and_cache(table, &schema, &col_meta_cache);
                                debug!(table = %table, "Applied field mapping from background resolver");
                            }
                            // Populate SchemaCache for HeaderExtractor (Change A)
                            schema_cache.insert(table.clone(), schema);
                        }
                        SchemaResolution::TableNotFound => {
                            // An unknown source is a routing miss, so its events
                            // fall back to the default table rather than ageing
                            // out to the DLQ.
                            let marked = absent_tables.insert(table, std::time::Instant::now());
                            // Only the move into absence is news: the entry is
                            // re-resolved every TTL, so warning on the state
                            // costs a line a minute per dead source name (#129).
                            match marked {
                                super::types::AbsentOutcome::Recorded => warn!(
                                    table = %table,
                                    default_table = %default_table,
                                    "Destination table does not exist, falling back to the default table"
                                ),
                                super::types::AbsentOutcome::Refreshed => {}
                                super::types::AbsentOutcome::Rejected => {
                                    static ABSENT_FULL_TS: std::sync::atomic::AtomicU64 =
                                        std::sync::atomic::AtomicU64::new(0);
                                    if scalo::logger::log_debounced(&ABSENT_FULL_TS, 60_000) {
                                        warn!(
                                            table = %table,
                                            tracked = absent_tables.len(),
                                            "Absent-table set is full, check routing for unbounded table names (max 1 per 60s)"
                                        );
                                    }
                                }
                            }
                            let n = pending_schema_buffer.len_per_table(table);
                            if n > 0 {
                                static ABSENT_TABLE_TS: std::sync::atomic::AtomicU64 =
                                    std::sync::atomic::AtomicU64::new(0);
                                if scalo::logger::log_debounced(&ABSENT_TABLE_TS, 60_000) {
                                    warn!(
                                        table = %table,
                                        pending = n,
                                        "Re-routing pending messages for an unknown table (max 1 per 60s)"
                                    );
                                }
                            }
                            // Only a table the capped set actually took gets a
                            // metric label. The routed name comes from a
                            // payload field with no allowlist, so counting a
                            // rejected one would let untrusted input grow the
                            // label set past ABSENT_TABLE_CAPACITY -- the very
                            // thing the cap exists to stop.
                            if let Some(ref m) = self.metrics
                                && marked != super::types::AbsentOutcome::Rejected
                            {
                                m.record_unknown_table_fallback_n(table, n as u64);
                            }
                        }
                        SchemaResolution::Unavailable => {
                            // Keep buffering: the per-tick re-request loop retries
                            // until ClickHouse answers, or the age cap DLQs.
                            debug!(table = %table, "Schema unresolved, still buffering");
                        }
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
                                if dlq_enabled {
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
                                dlq_enabled,
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
                        // Re-resolve tables whose "does not exist" answer has
                        // aged out. Their messages buffer again until the
                        // answer arrives, so a table created in the meantime
                        // starts receiving its own data.
                        for table in absent_tables.expired(now_pending) {
                            debug!(table = %table, "Absent-table entry expired, re-resolving");
                            let _ = resolve_tx.try_send(table);
                        }
                    }

                    // Merge messages whose schema just resolved (re-processed via
                    // the extractor path) ahead of the freshly received batch.
                    // Their memory was counted on first receipt and is NOT
                    // re-counted. take_ready only runs on a transport Ok, so
                    // resolved messages are never dropped on a transport error.
                    let combined: crate::Result<Vec<crate::kafka::KafkaMessage>> = match messages {
                        Ok(fresh) => {
                            // Every stage below takes one message to be one
                            // record, so a batched message is split first
                            // (#128, #184). Splitting ahead of the guard keeps
                            // the bytes admitted equal to the bytes later
                            // released.
                            let fan = fan_out_batched_records(fresh);
                            if let Some(ref m) = self.metrics {
                                if fan.arrays > 0 {
                                    m.record_batched_array_fanout(fan.arrays, fan.array_records);
                                }
                                if fan.ndjson > 0 {
                                    m.record_batched_ndjson_fanout(fan.ndjson, fan.ndjson_records);
                                }
                            }
                            let mut fresh = fan.messages;
                            for msg in &fresh {
                                self.memory_guard.add_bytes(msg.payload.len() as u64);
                            }
                            self.stats.messages_received += fresh.len() as u64;
                            if let Some(ref m) = self.metrics {
                                for _ in 0..fresh.len() {
                                    m.record_received();
                                }
                            }
                            let ready = pending_schema_buffer
                                .take_ready(&schema_cache, &absent_tables);
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
                                    if dlq_enabled {
                                        let entry = DlqEntry::new("loader", reason.clone(), msg.payload.clone())
                                            .with_source(scalo::dlq::DlqSource::kafka(
                                                &*msg.topic,
                                                msg.partition,
                                                msg.offset,
                                            ));
                                        if dlq_tx.try_send(entry).is_ok() {
                                            self.stats.messages_dlq += 1;
                                            super::coordinator::record_dlq_routed(
                                                "pre_route",
                                                reason,
                                                &msg.location(),
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
                                absent_tables: &absent_tables,
                                default_table: &default_table,
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
                                dlq_enabled,
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
                                // Read after the take, so it names only what stayed behind.
                                let still_buffered = buffer_manager.lowest_pending_offsets();
                                self.flush_batches_transport(&inserter, &transport, dlq.as_ref(), batches, still_buffered).await;
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

        // Final flush: `flush_all` empties every buffer, so nothing is left
        // behind to hold the watermark down.
        let final_batches = buffer_manager.flush_all();
        if !final_batches.is_empty() {
            info!(batches = final_batches.len(), "Flushing remaining buffers");
            self.flush_batches_transport(
                &inserter,
                &transport,
                dlq.as_ref(),
                final_batches,
                Vec::new(),
            )
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

    /// Flush batches to `ClickHouse`, then commit ONE Kafka watermark for the
    /// whole cycle.
    ///
    /// A batch is per TABLE; a Kafka commit is per PARTITION. Committing batch
    /// by batch therefore buries whatever another table withheld on the same
    /// partition -- see [`committable_offsets`], which is where that decision
    /// now lives.
    ///
    /// `still_buffered` is the caller's post-take residual from
    /// `BufferManager::lowest_pending_offsets`: rows held in a buffer that was
    /// not ready this cycle, and therefore not yet written anywhere durable.
    async fn flush_batches_transport(
        &mut self,
        inserter: &Inserter,
        transport: &TransportBackend,
        dlq: Option<&Arc<Dlq>>,
        batches: Vec<FlushBatch>,
        still_buffered: Vec<KafkaOffset>,
    ) {
        use std::time::Instant;

        let batch_count = batches.len();
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();

        debug!(batches = batch_count, rows = total_rows, "Flushing batches");

        // Extract per-batch offsets and byte sizes — enables independent commit and memory release.
        // A failure in Table A must not block offset commit for Table B (correctness fix).
        let mut per_batch_offsets: Vec<Vec<KafkaOffset>> = Vec::with_capacity(batches.len());
        let mut per_batch_bytes: Vec<u64> = Vec::with_capacity(batches.len());
        let mut per_batch_tables: Vec<String> = Vec::with_capacity(batches.len());
        let mut per_batch_payloads: Vec<Vec<Arc<[u8]>>> = Vec::with_capacity(batches.len());
        let batches_for_insert: Vec<FlushBatch> = batches
            .into_iter()
            .map(|mut b| {
                per_batch_offsets.push(std::mem::take(&mut b.offsets));
                // Track original payload bytes for memory guard release
                let batch_bytes: u64 = b.raw_payloads.iter().map(|p| p.len() as u64).sum();
                per_batch_bytes.push(batch_bytes);
                per_batch_tables.push(b.table.to_string());
                // Refcount clones only, so a permanently rejected batch can still
                // reach the DLQ without copying any payload.
                per_batch_payloads.push(b.raw_payloads.clone());
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
        // Salvage is on by default: one unencodable row costs one row, not the
        // whole batch it happened to share a flush with.
        let results = inserter
            .insert_batches_with_salvage(batches_for_insert)
            .await;
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

        // Offsets are sorted into three piles across the WHOLE cycle and
        // resolved once at the end. Nothing commits inside the loop.
        let mut committable: Vec<KafkaOffset> = Vec::new();
        // Seeded with the rows still sitting in a buffer: a commit-after-process
        // watermark may not pass an offset whose only copy is in memory.
        let mut withheld: Vec<KafkaOffset> = still_buffered;
        // Rows the DLQ has ACCEPTED but not yet proven durable. They join
        // `committable` only once the flush barrier below returns.
        let mut dlq_pending: Vec<KafkaOffset> = Vec::new();

        // ONE DLQ budget for the whole cycle, not one per batch. The batch loop
        // is sequential and the select! loop is blocked for its duration, so a
        // per-batch deadline multiplies by the number of rejecting tables --
        // and scalo warns that backing off recv past max.poll.interval.ms
        // (default 300s) evicts the pod from the consumer group.
        let dlq_deadline = tokio::time::Instant::now() + DLQ_ROUTE_DEADLINE;

        for (((((_, result), offsets), batch_bytes), table), payloads) in results
            .into_iter()
            .zip(per_batch_offsets)
            .zip(per_batch_bytes)
            .zip(per_batch_tables)
            .zip(per_batch_payloads)
        {
            // Release tracked memory regardless of insert outcome.
            // Success: data is in ClickHouse, memory freed.
            // Failure: offsets withheld, Kafka re-delivers — we'll re-track on re-consume.
            self.memory_guard.release(batch_bytes);

            if result.inserted > 0 {
                self.stats.rows_inserted += result.inserted as u64;
                if let Some(ref m) = self.metrics {
                    m.record_flush(result.inserted, latency);
                    m.record_insert_quantities(batch_bytes, 1);
                    // ClickHouse flush-size (bytes) distribution (2.8.10 audit).
                    m.record_flush_bytes(batch_bytes);
                }
            }

            match result.disposition {
                BatchDisposition::Retry(reason) => {
                    cycle_err += 1;
                    self.stats.errors += 1;
                    error!(
                        table = %table,
                        rows = payloads.len(),
                        error = %reason,
                        "Batch insert failed — offsets withheld, messages will re-deliver"
                    );
                    if let Some(ref m) = self.metrics {
                        m.record_error();
                        // ClickHouse-specific terminal insert error (2.8.10 audit).
                        m.record_clickhouse_insert_error();
                    }
                    withheld.extend(offsets);
                }
                BatchDisposition::Settled if result.failed.is_empty() => {
                    cycle_ok += 1;
                    committable.extend(offsets);
                }
                BatchDisposition::Settled => {
                    // The sink answered, so the circuit gate must not read this
                    // cycle as a dead sink.
                    cycle_ok += 1;
                    self.stats.errors += 1;
                    if let Some(ref m) = self.metrics {
                        m.record_error();
                        m.record_clickhouse_insert_error();
                    }
                    let rejected = result.failed.len();
                    let delivery = route_rejected_rows_to_dlq(
                        dlq,
                        &self.metrics,
                        &table,
                        &payloads,
                        &result.failed,
                        dlq_deadline,
                    )
                    .await;
                    self.stats.messages_dlq += delivery.delivered;

                    // Committing is what breaks the livelock, but only once
                    // every rejected row is somewhere other than Kafka — and
                    // "accepted" is not yet "written", so these offsets wait
                    // for the flush barrier below.
                    if delivery.complete {
                        error!(
                            table = %table,
                            inserted = result.inserted,
                            rejected,
                            dlq = delivery.delivered,
                            "Rows permanently rejected — DLQ took the batch"
                        );
                        dlq_pending.extend(offsets);
                    } else {
                        error!(
                            table = %table,
                            inserted = result.inserted,
                            rejected,
                            dlq = delivery.delivered,
                            "DLQ took only part of a rejected batch — offsets withheld"
                        );
                        withheld.extend(offsets);
                    }
                }
            }
        }

        // The DLQ accepting an entry means QUEUED, not written: it sits in
        // scalo's bounded sink until the drain reaches a backend. Committing on
        // the acceptance alone loses those rows to a pod that dies in between,
        // so the barrier runs before anything the DLQ took is committed. The
        // same cycle budget bounds it — a wedged backend must not hold the
        // consumer past max.poll.interval.ms.
        if !dlq_pending.is_empty() {
            let flushed = match dlq {
                Some(d) => match tokio::time::timeout_at(dlq_deadline, d.flush()).await {
                    Ok(Ok(())) => true,
                    Ok(Err(e)) => {
                        error!(error = %e, "DLQ flush failed — offsets withheld");
                        false
                    }
                    Err(_elapsed) => {
                        error!("DLQ flush did not complete within the deadline — offsets withheld");
                        false
                    }
                },
                // Unreachable: a complete delivery requires an enabled DLQ.
                None => false,
            };
            if flushed {
                committable.append(&mut dlq_pending);
            } else {
                withheld.append(&mut dlq_pending);
            }
        }

        // ONE commit for the cycle, bounded by the lowest withheld offset on
        // each partition. Doing it per batch buries a sibling table's withheld
        // offset, and `get_ready_for_flush` iterates an FxHashMap, so batch
        // order is not even deterministic.
        let to_commit = committable_offsets(committable, &withheld);
        commit_offsets(transport, &self.metrics, &to_commit).await;

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

/// How long a "table does not exist" answer is trusted before the destination
/// is resolved again. Bounds how long a source keeps landing in the default
/// table after its own table is created.
const ABSENT_TABLE_TTL: Duration = Duration::from_secs(60);

/// Upper bound on tables tracked as absent. The routed table name comes from a
/// payload field with no allowlist, so this is what stops untrusted input
/// growing both the set and the per-table metric label set.
const ABSENT_TABLE_CAPACITY: usize = 1024;

/// Total budget for handing a whole flush CYCLE's rejected rows to the DLQ,
/// including the durability barrier at the end of it.
///
/// Backpressure is the point, but this cannot be a per-batch budget: batches
/// are routed sequentially with the `select!` loop blocked, so ten rejecting
/// tables behind a wedged DLQ would multiply it by ten. scalo documents that
/// backing off `recv` past `max.poll.interval.ms` (default 300s) evicts the pod
/// from the consumer group, which costs far more than the rows it was waiting
/// on -- those are still in Kafka.
const DLQ_ROUTE_DEADLINE: Duration = Duration::from_secs(30);

/// Depth of the channel between the hot path and the DLQ backend task.
///
/// Carries the fire-and-forget losses only -- pre-route rejects, inbound-filter
/// rejects, and pending-schema expiries. Permanently rejected ROWS bypass it and
/// talk to the DLQ directly, because an entry parked in here is invisible to
/// `Dlq::flush` and their offsets commit on that barrier.
const DLQ_CHANNEL_CAPACITY: usize = 1_000;

/// Which of a flush cycle's offsets may actually be committed.
///
/// Kafka's commit is a per-PARTITION watermark, not a per-message ack: scalo's
/// `build_commit_tpl` takes the highest offset per partition and SETS it as
/// `highest + 1`. A flush batch, though, is per TABLE, and one partition feeds
/// many tables through `source_to_table`. So committing batch by batch buries
/// whatever a sibling table withheld:
///
/// ```text
/// partition 0: offset 100 -> dfe.foo, 101 -> dfe.bar, 102 -> dfe.foo
/// batch dfe.foo [100, 102] succeeds -> commits partition 0 at 103
/// batch dfe.bar [101]      withheld -> unreachable, in neither CH nor the DLQ
/// ```
///
/// The watermark is therefore computed once for the whole cycle: on each
/// partition, everything at or above the LOWEST withheld offset is dropped from
/// the commit, so the stored watermark can never pass a row that still needs to
/// come back. Rows above it re-deliver as duplicates, which is what at-least-
/// once buys.
///
/// This also settles a second hazard: `BufferManager::get_ready_for_flush`
/// iterates an `FxHashMap`, so batch order is nondeterministic and a low-offset
/// batch committing after a high one used to rewind the watermark. One commit
/// per cycle cannot rewind.
///
/// `withheld` carries two kinds of offset: rows this cycle could not place
/// (insert failed, or the DLQ would not take them) and rows still held in a
/// buffer that was not ready to flush.
/// Both bound the watermark, because a row whose only copy is in memory is lost
/// to a crash just as surely as one Kafka must re-deliver.
///
/// `withheld` may name partitions absent from `committable` and vice versa;
/// both are handled.
fn committable_offsets(
    committable: Vec<KafkaOffset>,
    withheld: &[KafkaOffset],
) -> Vec<KafkaOffset> {
    if withheld.is_empty() {
        return committable;
    }

    let mut floor: rustc_hash::FxHashMap<(&str, i32), i64> = rustc_hash::FxHashMap::default();
    for off in withheld {
        floor
            .entry((&*off.topic, off.partition))
            .and_modify(|lowest| {
                if off.offset < *lowest {
                    *lowest = off.offset;
                }
            })
            .or_insert(off.offset);
    }

    committable
        .into_iter()
        .filter(|off| {
            floor
                .get(&(&*off.topic, off.partition))
                .is_none_or(|lowest| off.offset < *lowest)
        })
        .collect()
}

/// Commit a batch's Kafka offsets.
async fn commit_offsets(
    transport: &TransportBackend,
    metrics: &Option<Metrics>,
    offsets: &[KafkaOffset],
) {
    if offsets.is_empty() {
        return;
    }
    match transport.commit(offsets).await {
        Ok(()) => {
            // gRPC has no broker-side offsets, so its no-op commit must not tick
            // a kafka-named counter on a broker-less deployment (#125).
            if transport.commits_offsets() {
                debug!(offsets = offsets.len(), "Kafka offsets committed");
                if let Some(m) = metrics {
                    m.record_offsets_committed(offsets.len());
                }
            }
        }
        Err(e) => error!(error = %e, "Failed to commit Kafka offsets"),
    }
}

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

/// How much of a permanently rejected batch the DLQ actually took.
#[derive(Debug, Default, PartialEq, Eq)]
struct DlqDelivery {
    /// Entries the DLQ channel accepted.
    delivered: u64,
    /// Whether every rejected row now exists somewhere other than Kafka.
    /// Only then may the offsets be committed.
    complete: bool,
}

/// Whether the DLQ will actually keep what it is handed.
///
/// `Dlq::spawn` returns `Ok(Dlq::disabled())` when the config asks for a DLQ
/// but no backend builds -- a read-only rootfs on a gRPC-transport pod reaches
/// that by config alone. A disabled `Dlq` counts a drop and returns `Ok` from
/// `send`, so `is_some()` answers "was one asked for", never "will it keep
/// anything". Only the latter may gate an offset commit.
fn dlq_accepts(dlq: Option<&Arc<Dlq>>) -> bool {
    dlq.is_some_and(|d| d.is_enabled())
}

/// Route permanently rejected rows to the DLQ, one entry each.
///
/// Talks to the DLQ DIRECTLY rather than through the orchestrator's forwarding
/// channel. That channel exists so the fire-and-forget paths never block the
/// event loop, but it puts a queue in front of the one `Dlq::flush` barriers --
/// an entry still sitting in it is invisible to the flush, so the caller would
/// be committing on a barrier that never covered it. This path already awaits
/// with backpressure, so the hop bought nothing and cost the durability proof.
///
/// `expiry` is the cycle-wide deadline: backpressure is the point, but a wedged
/// DLQ backend must not hold the consumer past `max.poll.interval.ms`. Any
/// shortfall is reported instead, and the caller withholds the offsets so Kafka
/// re-delivers.
///
/// With no DLQ, or one whose backends never started, nothing is delivered and
/// nothing may be committed: a livelock keeps the events in Kafka, which a
/// shredder does not.
async fn route_rejected_rows_to_dlq(
    dlq: Option<&Arc<Dlq>>,
    metrics: &Option<Metrics>,
    table: &str,
    payloads: &[Arc<[u8]>],
    failed: &[FailedRow],
    expiry: tokio::time::Instant,
) -> DlqDelivery {
    if failed.is_empty() {
        return DlqDelivery {
            delivered: 0,
            complete: true,
        };
    }

    if let Some(m) = metrics {
        for _ in failed {
            m.record_permanent_reject(table);
        }
    }

    let summary = format!(
        "clickhouse_permanent_reject table={table}: {}",
        failed[0].reason
    );
    super::coordinator::record_dlq_routed(
        "clickhouse_permanent_reject",
        &summary,
        &format!("table={table}"),
    );

    let Some(dlq) = dlq.filter(|d| d.is_enabled()) else {
        error!(
            table = %table,
            rows = failed.len(),
            "Rows permanently rejected with no working DLQ — offsets withheld"
        );
        return DlqDelivery::default();
    };

    let mut delivered = 0u64;

    for row in failed {
        let Some(payload) = rejected_payload(payloads, row) else {
            // Nothing at all to hand over: the offset is the only remaining
            // copy, so it stays put. Unreachable in practice -- the inserter
            // serialises the promoted row whenever the raw slot is empty.
            error!(
                table = %table,
                row_index = row.row_index,
                delivered,
                rejected = failed.len(),
                "Rejected row has no payload to DLQ — offsets withheld"
            );
            return DlqDelivery {
                delivered,
                complete: false,
            };
        };
        let mut entry = DlqEntry::new(
            "loader",
            format!("clickhouse_permanent_reject table={table}: {}", row.reason),
            payload,
        );
        if let Some(offset) = &row.offset {
            entry = entry.with_source(scalo::dlq::DlqSource::kafka(
                &*offset.topic,
                offset.partition,
                offset.offset,
            ));
        }
        match tokio::time::timeout_at(expiry, dlq.send(entry)).await {
            Ok(Ok(())) => {
                delivered += 1;
                if let Some(m) = metrics {
                    m.record_dlq();
                }
            }
            Ok(Err(e)) => {
                error!(
                    table = %table,
                    delivered,
                    rejected = failed.len(),
                    error = %e,
                    "DLQ stopped accepting mid-batch — offsets withheld"
                );
                return DlqDelivery {
                    delivered,
                    complete: false,
                };
            }
            Err(_elapsed) => {
                error!(
                    table = %table,
                    delivered,
                    rejected = failed.len(),
                    "DLQ did not drain within the cycle deadline — offsets withheld"
                );
                return DlqDelivery {
                    delivered,
                    complete: false,
                };
            }
        }
    }

    DlqDelivery {
        delivered,
        complete: true,
    }
}

/// The bytes to hand the DLQ for one rejected row.
///
/// `raw_payloads[i]` is empty for every capture mode that keeps no raw bytes --
/// `raw_only` (which this repo's own guidance recommends for high-cardinality
/// tables), `extracted_only`, the whole `legacy_flatten` path, and any
/// `MessagePack` payload. DLQ'ing an empty entry for those and committing the
/// offset drops the event from `ClickHouse`, the DLQ and Kafka at once, so the
/// inserter attaches the serialised promoted row, which still carries the
/// payload as `_raw` or `_json`.
///
/// `None` means there is genuinely nothing to write, and the caller withholds.
fn rejected_payload(payloads: &[Arc<[u8]>], row: &FailedRow) -> Option<Vec<u8>> {
    if let Some(raw) = payloads.get(row.row_index).filter(|p| !p.is_empty()) {
        return Some(raw.to_vec());
    }
    row.row_json.clone().filter(|b| !b.is_empty())
}

/// The wire shape a producer used to put several records in one message.
enum BatchShape {
    /// A top-level JSON array of objects, as dfe-receiver forwarded (#128).
    Array,
    /// Newline-separated JSON objects, as dfe-transform-elastic emits (#184).
    Ndjson,
}

/// What [`fan_out_batched_records`] made of a received batch.
#[derive(Default)]
struct FanOut {
    messages: Vec<crate::kafka::KafkaMessage>,
    /// Received messages that carried a batch of records as a JSON array.
    arrays: u64,
    /// Records those arrays expanded into.
    array_records: u64,
    /// Received messages that carried newline-separated JSON records.
    ndjson: u64,
    /// Records those messages expanded into.
    ndjson_records: u64,
}

/// Expand every message carrying several records into one message per record.
///
/// Any producer can batch, in either wire shape, so the split belongs at the
/// consumer rather than at each producer in turn (#128, #184). Each element
/// inherits its source message's topic, partition and offset, so a batch still
/// commits as one unit and the memory guard counts the elements it will later
/// release.
fn fan_out_batched_records(batch: Vec<crate::kafka::KafkaMessage>) -> FanOut {
    if !batch.iter().any(|m| {
        crate::payload::opens_json_array(&m.payload)
            || crate::payload::has_ndjson_boundary(&m.payload)
    }) {
        return FanOut {
            messages: batch,
            ..FanOut::default()
        };
    }

    let mut fan = FanOut {
        messages: Vec::with_capacity(batch.len()),
        ..FanOut::default()
    };
    for msg in batch {
        let split = crate::payload::split_json_array(&msg.payload)
            .map(|e| (BatchShape::Array, e))
            .or_else(|| {
                crate::payload::split_ndjson(&msg.payload).map(|e| (BatchShape::Ndjson, e))
            });
        let Some((shape, elements)) = split else {
            fan.messages.push(msg);
            continue;
        };

        let (shape_name, messages_seen, records_seen) = match shape {
            BatchShape::Array => ("json_array", &mut fan.arrays, &mut fan.array_records),
            BatchShape::Ndjson => ("ndjson", &mut fan.ndjson, &mut fan.ndjson_records),
        };
        *messages_seen += 1;
        *records_seen += elements.len() as u64;

        static FANNED_TS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        if scalo::logger::log_debounced(&FANNED_TS, 60_000) {
            info!(
                topic = %msg.topic,
                shape = shape_name,
                records = elements.len(),
                "Split a batched message into one record per element (max 1 per 60s)"
            );
        }

        fan.messages.extend(
            elements
                .into_iter()
                .map(|payload| crate::kafka::KafkaMessage {
                    payload,
                    topic: Arc::clone(&msg.topic),
                    partition: msg.partition,
                    offset: msg.offset,
                    key: msg.key.clone(),
                    timestamp_ms: msg.timestamp_ms,
                }),
        );
    }

    fan
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
    super::coordinator::record_dlq_routed(
        pending_reason_label(reason),
        &reason_str,
        &msg.location(),
    );
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

    // ---- batched-array fan-out (#128) ----

    fn msg_at(offset: i64, payload: &[u8]) -> crate::kafka::KafkaMessage {
        crate::kafka::KafkaMessage {
            payload: payload.to_vec(),
            topic: Arc::from("dfe.events"),
            partition: 3,
            offset,
            key: Some(b"k".to_vec()),
            timestamp_ms: Some(1_700_000_000_000),
        }
    }

    #[test]
    fn a_batched_array_becomes_one_message_per_record() {
        let fan = fan_out_batched_records(vec![msg_at(
            7,
            br#"[{"_source": "syslog", "n": 1}, {"_source": "syslog", "n": 2}]"#,
        )]);

        assert_eq!(fan.arrays, 1);
        assert_eq!(fan.array_records, 2);
        assert_eq!(fan.messages.len(), 2);
        assert_eq!(fan.messages[0].payload, br#"{"_source": "syslog", "n": 1}"#);
        assert_eq!(fan.messages[1].payload, br#"{"_source": "syslog", "n": 2}"#);
        for m in &fan.messages {
            assert_eq!(&*m.topic, "dfe.events");
            assert_eq!(m.partition, 3);
            assert_eq!(m.offset, 7, "a batch commits as one unit");
            assert_eq!(m.key.as_deref(), Some(b"k".as_slice()));
            assert_eq!(m.timestamp_ms, Some(1_700_000_000_000));
        }
    }

    #[test]
    fn an_unbatched_batch_is_returned_untouched() {
        let batch = vec![msg_at(1, br#"{"a": 1}"#), msg_at(2, br#"{"b": 2}"#)];
        let fan = fan_out_batched_records(batch);

        assert_eq!(fan.arrays, 0);
        assert_eq!(fan.array_records, 0);
        assert_eq!(fan.ndjson, 0);
        assert_eq!(fan.ndjson_records, 0);
        assert_eq!(fan.messages.len(), 2);
        assert_eq!(fan.messages[0].payload, br#"{"a": 1}"#);
        assert_eq!(fan.messages[1].payload, br#"{"b": 2}"#);
    }

    #[test]
    fn a_mixed_batch_splits_only_the_batches_and_keeps_order() {
        let batch = vec![
            msg_at(1, br#"{"a": 1}"#),
            msg_at(2, br#"[{"b": 2}, {"c": 3}]"#),
            msg_at(3, br"[1, 2, 3]"),
        ];
        let fan = fan_out_batched_records(batch);

        assert_eq!(fan.arrays, 1);
        assert_eq!(fan.array_records, 2);
        let payloads: Vec<&[u8]> = fan.messages.iter().map(|m| m.payload.as_slice()).collect();
        assert_eq!(
            payloads,
            vec![
                br#"{"a": 1}"#.as_slice(),
                br#"{"b": 2}"#.as_slice(),
                br#"{"c": 3}"#.as_slice(),
                br"[1, 2, 3]".as_slice(),
            ],
            "a scalar array is not a batch and keeps its place"
        );
        assert_eq!(fan.messages[3].offset, 3);
    }

    #[test]
    fn the_split_bytes_equal_what_the_memory_guard_will_release() {
        // The guard is fed after the split, so the two must agree or the
        // accounting drifts every batch.
        let fan = fan_out_batched_records(vec![msg_at(9, br#"[{"a": 1}, {"b": 2}]"#)]);
        let admitted: usize = fan.messages.iter().map(|m| m.payload.len()).sum();
        assert_eq!(admitted, br#"{"a": 1}"#.len() + br#"{"b": 2}"#.len());
    }

    // ---- newline-separated records (#184) ----

    #[test]
    fn newline_separated_records_become_one_message_each() {
        // The `cisco-ios_load` message: five records in one, which the parser
        // refused whole, so all five were lost with the DLQ disabled.
        let batch = br#"{"n": 1}
{"n": 2}
{"n": 3}
{"n": 4}
{"n": 5}
"#;
        let fan = fan_out_batched_records(vec![msg_at(11, batch)]);

        assert_eq!(fan.ndjson, 1);
        assert_eq!(fan.ndjson_records, 5);
        assert_eq!(
            fan.messages.len(),
            5,
            "every record, not the first and not zero"
        );
        assert_eq!(fan.arrays, 0, "ndjson is counted apart from an array");
        for (i, m) in fan.messages.iter().enumerate() {
            let value = crate::payload::parse_payload(&m.payload).expect("record parses alone");
            assert_eq!(value["n"], i + 1);
            assert_eq!(&*m.topic, "dfe.events");
            assert_eq!(m.partition, 3);
            assert_eq!(m.offset, 11, "a batch commits as one unit");
        }
    }

    #[test]
    fn the_two_wire_shapes_are_counted_apart() {
        let batch = vec![
            msg_at(1, br#"[{"a": 1}, {"b": 2}]"#),
            msg_at(2, b"{\"c\": 3}\n{\"d\": 4}\n{\"e\": 5}"),
        ];
        let fan = fan_out_batched_records(batch);

        assert_eq!((fan.arrays, fan.array_records), (1, 2));
        assert_eq!((fan.ndjson, fan.ndjson_records), (1, 3));
        assert_eq!(fan.messages.len(), 5);
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

    fn rejected_rows(n: usize, reason: &str) -> Vec<FailedRow> {
        (0..n)
            .map(|i| FailedRow {
                row_index: i,
                offset: Some(KafkaOffset {
                    topic: Arc::from("dfe-events"),
                    partition: 0,
                    offset: 10 + i as i64,
                }),
                reason: reason.to_string(),
                row_json: None,
            })
            .collect()
    }

    fn offset(topic: &str, partition: i32, offset: i64) -> KafkaOffset {
        KafkaOffset {
            topic: Arc::from(topic),
            partition,
            offset,
        }
    }

    /// A per-test spool directory for the file-backed DLQ.
    fn dlq_dir(name: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "dfe-loader-dlq-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// A REAL file-backed DLQ. No broker, no mock: the entries land as NDJSON,
    /// which is what makes the `flush` durability barrier checkable.
    fn file_dlq(dir: &std::path::Path) -> Arc<Dlq> {
        let config = scalo::dlq::DlqConfig {
            enabled: true,
            mode: scalo::dlq::DlqMode::FileOnly,
            file: scalo::dlq::FileDlqConfig {
                enabled: true,
                path: dir.to_path_buf(),
                compress_rotated: false,
                ..scalo::dlq::FileDlqConfig::default()
            },
            ..scalo::dlq::DlqConfig::default()
        };
        Arc::new(
            Dlq::spawn(&config, "loader", None, CancellationToken::new()).expect("spawn file DLQ"),
        )
    }

    /// Every NDJSON line the file backend has written under `dir`.
    fn spooled_lines(dir: &std::path::Path) -> Vec<String> {
        fn walk(dir: &std::path::Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if let Ok(body) = std::fs::read_to_string(&path) {
                    out.extend(body.lines().filter(|l| !l.is_empty()).map(str::to_string));
                }
            }
        }
        let mut out = Vec::new();
        walk(dir, &mut out);
        out
    }

    fn in_30s() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(30)
    }

    #[tokio::test]
    async fn a_batch_larger_than_the_dlq_channel_loses_nothing() {
        // buffer.flush_rows defaults to 20_000, far past any queue in the
        // chain, so a non-blocking send would discard most of a rejected batch
        // and the offsets would commit over the top of it.
        let dir = dlq_dir("bigbatch");
        let dlq = file_dlq(&dir);

        let rows = 20_000;
        let payloads: Vec<Arc<[u8]>> = (0..rows)
            .map(|i| Arc::from(format!("{{\"n\":{i}}}").into_bytes().as_slice()))
            .collect();
        let failed = rejected_rows(rows, "server error code 117");

        let delivery =
            route_rejected_rows_to_dlq(Some(&dlq), &None, "dfe.main", &payloads, &failed, in_30s())
                .await;
        dlq.flush().await.expect("durability barrier");

        assert_eq!(delivery.delivered, rows as u64);
        assert!(
            delivery.complete,
            "the offsets may only commit when complete"
        );
        let spooled = spooled_lines(&dir);
        assert_eq!(
            spooled.len(),
            rows,
            "every rejected row must be durably written before the offsets commit"
        );
        assert!(spooled[0].contains("clickhouse_permanent_reject"));
        assert!(spooled[0].contains("dfe.main"));
        assert!(spooled[0].contains("code 117"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_dlq_that_stops_accepting_withholds_the_offsets() {
        // The drain has exited, so the send fails part-way. Reporting the
        // shortfall is what keeps the events in Kafka.
        let dir = dlq_dir("closed");
        let dlq = file_dlq(&dir);
        dlq.shutdown().await.expect("stop the drain");

        let payloads: Vec<Arc<[u8]>> = (0..10).map(|_| Arc::from(&b"{}"[..])).collect();
        let failed = rejected_rows(10, "code 117");

        let delivery =
            route_rejected_rows_to_dlq(Some(&dlq), &None, "dfe.main", &payloads, &failed, in_30s())
                .await;

        assert!(!delivery.complete, "a partial delivery must not commit");
        assert!(delivery.delivered < 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_wedged_dlq_gives_up_at_the_cycle_deadline() {
        // The budget is per flush CYCLE, so the caller hands in an absolute
        // instant. One already past means no row may be committed on.
        let dir = dlq_dir("deadline");
        let dlq = file_dlq(&dir);
        dlq.shutdown().await.expect("stop the drain");

        let payloads: Vec<Arc<[u8]>> = vec![Arc::from(&b"{}"[..])];
        let delivery = route_rejected_rows_to_dlq(
            Some(&dlq),
            &None,
            "dfe.main",
            &payloads,
            &rejected_rows(1, "code 117"),
            tokio::time::Instant::now(),
        )
        .await;

        assert!(!delivery.complete);
        assert_eq!(delivery.delivered, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn no_dlq_configured_never_commits_a_permanent_rejection() {
        // With no DLQ the only copy left is in Kafka, so the offsets stay put.
        let payloads: Vec<Arc<[u8]>> = vec![Arc::from(&b"{}"[..])];
        let failed = rejected_rows(1, "code 117");

        let delivery =
            route_rejected_rows_to_dlq(None, &None, "dfe.main", &payloads, &failed, in_30s()).await;

        assert_eq!(delivery, DlqDelivery::default());
        assert!(!delivery.complete);
    }

    #[tokio::test]
    async fn a_degraded_dlq_is_not_a_working_one() {
        // scalo's Dlq::spawn returns Ok(disabled) when the config enables a DLQ
        // but no backend builds -- a read-only rootfs on a gRPC-transport pod
        // gets here by config alone. A disabled Dlq counts a drop and returns
        // Ok from send(), so reading is_some() commits the offsets for rows it
        // just threw away.
        let config = scalo::dlq::DlqConfig {
            enabled: true,
            mode: scalo::dlq::DlqMode::Cascade,
            file: scalo::dlq::FileDlqConfig {
                enabled: false,
                ..scalo::dlq::FileDlqConfig::default()
            },
            kafka: scalo::dlq::KafkaDlqConfig {
                enabled: false,
                ..scalo::dlq::KafkaDlqConfig::default()
            },
            ..scalo::dlq::DlqConfig::default()
        };
        let degraded = Arc::new(
            Dlq::spawn(&config, "loader", None, CancellationToken::new())
                .expect("spawn returns Ok"),
        );

        assert!(
            !degraded.is_enabled(),
            "no backend built, so nothing is kept"
        );
        assert!(
            !dlq_accepts(Some(&degraded)),
            "is_some() is true here — only is_enabled() may gate a commit"
        );

        let payloads: Vec<Arc<[u8]>> = vec![Arc::from(&b"{\"n\":1}"[..])];
        let delivery = route_rejected_rows_to_dlq(
            Some(&degraded),
            &None,
            "dfe.main",
            &payloads,
            &rejected_rows(1, "code 117"),
            in_30s(),
        )
        .await;

        assert!(
            !delivery.complete,
            "a DLQ that keeps nothing must not release the offsets"
        );
        assert_eq!(delivery.delivered, 0);
        assert_eq!(degraded.dropped(), 0, "nothing was handed to it to drop");
    }

    #[tokio::test]
    async fn rejected_rows_carry_their_own_reason_and_kafka_source() {
        let dir = dlq_dir("source");
        let dlq = file_dlq(&dir);
        let payloads: Vec<Arc<[u8]>> = vec![
            Arc::from(&b"{\"tags\":[\"a\"]}"[..]),
            Arc::from(&b"{\"tags\":[\"b\"]}"[..]),
        ];
        // Salvage isolated row 1 only; row 0 landed.
        let failed = vec![FailedRow {
            row_index: 1,
            offset: Some(KafkaOffset {
                topic: Arc::from("dfe-events"),
                partition: 3,
                offset: 77,
            }),
            reason: "RowBinary encode: unsupported value".to_string(),
            row_json: None,
        }];

        let delivery =
            route_rejected_rows_to_dlq(Some(&dlq), &None, "dfe.main", &payloads, &failed, in_30s())
                .await;
        dlq.flush().await.expect("durability barrier");

        assert_eq!(delivery.delivered, 1);
        assert!(delivery.complete);
        let spooled = spooled_lines(&dir);
        assert_eq!(spooled.len(), 1, "only the isolated row is DLQ'd");
        assert!(spooled[0].contains("unsupported value"));
        assert!(spooled[0].contains("dfe-events"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_rejected_row_with_no_raw_payload_is_never_dlqd_empty() {
        // raw_only keeps no raw bytes -- and this repo's own guidance
        // recommends it for high-cardinality tables. DLQ'ing an empty entry and
        // committing loses the event from ClickHouse, the DLQ and Kafka at
        // once, so the inserter attaches the promoted row instead.
        let dir = dlq_dir("norawpayload");
        let dlq = file_dlq(&dir);
        let payloads: Vec<Arc<[u8]>> = vec![Arc::from(&[][..])];
        let failed = vec![FailedRow {
            row_index: 0,
            offset: Some(offset("dfe-events", 0, 10)),
            reason: "code 117".to_string(),
            row_json: Some(br#"{"_raw":"{\"user\":\"kaz\"}"}"#.to_vec()),
        }];

        let delivery =
            route_rejected_rows_to_dlq(Some(&dlq), &None, "dfe.main", &payloads, &failed, in_30s())
                .await;
        dlq.flush().await.expect("durability barrier");

        assert_eq!(delivery.delivered, 1);
        assert!(delivery.complete);
        let spooled = spooled_lines(&dir);
        assert_eq!(spooled.len(), 1);
        // The file backend base64s the payload, so an empty one spools as "".
        let entry: serde_json::Value = serde_json::from_str(&spooled[0]).expect("NDJSON entry");
        assert!(
            !entry["payload"].as_str().expect("payload field").is_empty(),
            "the payload must survive, not just the reason: {}",
            spooled[0]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_empty_raw_slot_falls_back_to_the_promoted_row() {
        // raw_payloads[i] is empty under raw_only, extracted_only, the whole
        // legacy_flatten path and any MessagePack payload. The promoted row
        // still holds the bytes as _raw or _json.
        let row_bytes = br#"{"_raw":"{\"user\":\"kaz\"}"}"#.to_vec();
        let failed = FailedRow {
            row_index: 0,
            offset: None,
            reason: "code 117".to_string(),
            row_json: Some(row_bytes.clone()),
        };
        let empty: Vec<Arc<[u8]>> = vec![Arc::from(&[][..])];
        assert_eq!(rejected_payload(&empty, &failed), Some(row_bytes));

        // A raw payload that IS present still wins — no re-serialisation.
        let raw: Vec<Arc<[u8]>> = vec![Arc::from(&b"{\"user\":\"kaz\"}"[..])];
        assert_eq!(
            rejected_payload(&raw, &failed),
            Some(b"{\"user\":\"kaz\"}".to_vec())
        );

        // Nothing at all: the caller must withhold rather than DLQ air.
        let nothing = FailedRow {
            row_index: 0,
            offset: None,
            reason: "code 117".to_string(),
            row_json: None,
        };
        assert_eq!(rejected_payload(&empty, &nothing), None);
    }

    #[tokio::test]
    async fn a_rejected_row_with_nothing_at_all_withholds_rather_than_dlqing_air() {
        // Both slots empty means the offset is the only copy left. Committing
        // an empty DLQ entry over the top of it is the loss this guards.
        let dir = dlq_dir("nothing");
        let dlq = file_dlq(&dir);
        let payloads: Vec<Arc<[u8]>> = vec![Arc::from(&[][..])];

        let delivery = route_rejected_rows_to_dlq(
            Some(&dlq),
            &None,
            "dfe.main",
            &payloads,
            &rejected_rows(1, "boom"),
            in_30s(),
        )
        .await;

        assert_eq!(delivery.delivered, 0);
        assert!(!delivery.complete, "nothing was kept, so nothing commits");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_batch_with_nothing_rejected_is_committable() {
        let delivery =
            route_rejected_rows_to_dlq(None, &None, "dfe.main", &[], &[], in_30s()).await;
        assert!(delivery.complete);
        assert_eq!(delivery.delivered, 0);
    }

    // ========================================================================
    // Cycle-wide commit watermark
    // ========================================================================

    #[tokio::test]
    async fn a_withheld_offset_is_never_buried_by_a_committed_one() {
        // The default DFE shape: one topic, one partition, two tables via
        // source_to_table. scalo's build_commit_tpl SETS the partition to the
        // highest offset + 1, so committing dfe.foo's batch on its own stores
        // 103 and offset 101 becomes unreachable -- not in ClickHouse, not in
        // the DLQ, and the consumer resumes past it.
        let foo = vec![offset("dfe-events", 0, 100), offset("dfe-events", 0, 102)];
        let bar = vec![offset("dfe-events", 0, 101)];

        let to_commit = committable_offsets(foo, &bar);

        let highest = to_commit.iter().map(|o| o.offset).max();
        assert_eq!(
            highest,
            Some(100),
            "the watermark must stop below the withheld offset, not at 102"
        );
        assert!(
            to_commit.iter().all(|o| o.offset < 101),
            "nothing at or above the withheld offset may commit"
        );
    }

    #[test]
    fn the_watermark_does_not_depend_on_batch_order() {
        // get_ready_for_flush iterates an FxHashMap, so batch order is
        // nondeterministic. One watermark per cycle cannot rewind.
        let withheld = vec![offset("t", 0, 55)];
        let ascending = committable_offsets(
            vec![offset("t", 0, 50), offset("t", 0, 60), offset("t", 0, 54)],
            &withheld,
        );
        let descending = committable_offsets(
            vec![offset("t", 0, 60), offset("t", 0, 54), offset("t", 0, 50)],
            &withheld,
        );
        let mut a: Vec<i64> = ascending.iter().map(|o| o.offset).collect();
        let mut b: Vec<i64> = descending.iter().map(|o| o.offset).collect();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, vec![50, 54]);
        assert_eq!(a, b);
    }

    #[test]
    fn a_withhold_on_one_partition_does_not_hold_another() {
        // The watermark is per partition, so a wedged table on partition 0 must
        // not stall partition 1's progress.
        let committable = vec![
            offset("dfe-events", 0, 100),
            offset("dfe-events", 1, 500),
            offset("other", 0, 900),
        ];
        let withheld = vec![offset("dfe-events", 0, 99)];

        let to_commit = committable_offsets(committable, &withheld);
        let mut got: Vec<(i32, i64)> = to_commit
            .iter()
            .map(|o| (o.partition, o.offset))
            .collect::<Vec<_>>();
        got.sort_unstable();
        assert_eq!(got, vec![(0, 900), (1, 500)]);
    }

    #[test]
    fn nothing_withheld_commits_everything() {
        let committable = vec![offset("t", 0, 1), offset("t", 1, 2)];
        assert_eq!(committable_offsets(committable, &[]).len(), 2);
    }

    #[test]
    fn a_topic_only_named_in_the_withheld_pile_blocks_nothing_else() {
        let to_commit = committable_offsets(
            vec![offset("a", 0, 10)],
            &[offset("b", 0, 1), offset("a", 0, 11)],
        );
        assert_eq!(to_commit.len(), 1);
        assert_eq!(to_commit[0].offset, 10);
    }

    // ========================================================================
    // Offsets still held in a buffer
    // ========================================================================

    /// Buffer manager whose thresholds only a 5-row table can trip, so a
    /// sibling table is left buffered exactly as it is in the running pipeline.
    fn staged_buffers() -> BufferManager {
        let mut buffer = Config::default().buffer;
        buffer.flush_rows = 5;
        buffer.flush_age_secs = 3600;
        BufferManager::new(&buffer)
    }

    fn buffered_row(m: &mut BufferManager, table: &str, partition: i32, off: i64) {
        m.push(
            table,
            serde_json::json!({"id": off}).as_object().unwrap().clone(),
            Some(offset("dfe-events", partition, off)),
            None,
        );
    }

    #[test]
    fn a_buffered_offset_holds_the_watermark_below_it() {
        // Partition 0 feeds dfe.foo at 100 and 102 and dfe.bar at 101.
        // Only dfe.foo trips the row threshold, so 101 exists nowhere but
        // memory and a crash would lose it.
        let mut buffers = staged_buffers();
        buffered_row(&mut buffers, "dfe.foo", 0, 100);
        buffered_row(&mut buffers, "dfe.bar", 0, 101);
        buffered_row(&mut buffers, "dfe.foo", 0, 102);
        for off in 103..106 {
            buffered_row(&mut buffers, "dfe.foo", 0, off);
        }

        let flushed = buffers.get_ready_for_flush();
        assert_eq!(flushed.len(), 1, "only dfe.foo reached 5 rows");
        let committable: Vec<KafkaOffset> = flushed.into_iter().flat_map(|b| b.offsets).collect();

        let to_commit = committable_offsets(committable, &buffers.lowest_pending_offsets());

        assert_eq!(
            to_commit.iter().map(|o| o.offset).max(),
            Some(100),
            "the watermark must stop below the offset still in the bar buffer"
        );
    }

    #[test]
    fn an_empty_buffer_leaves_the_watermark_where_it_was() {
        // Nothing buffered means nothing to hold back: the flushed batch
        // commits in full, exactly as before this floor existed.
        let mut buffers = staged_buffers();
        for off in 100..105 {
            buffered_row(&mut buffers, "dfe.foo", 0, off);
        }

        let flushed = buffers.get_ready_for_flush();
        let committable: Vec<KafkaOffset> = flushed.into_iter().flat_map(|b| b.offsets).collect();
        assert_eq!(committable.len(), 5);

        let still_buffered = buffers.lowest_pending_offsets();
        assert!(still_buffered.is_empty());

        let to_commit = committable_offsets(committable, &still_buffered);
        assert_eq!(to_commit.iter().map(|o| o.offset).max(), Some(104));
    }

    #[test]
    fn two_tables_buffering_different_partitions_hold_only_their_own() {
        // dfe.bar holds partition 1 only, so partition 0 must still advance.
        let mut buffers = staged_buffers();
        for off in 200..205 {
            buffered_row(&mut buffers, "dfe.foo", 0, off);
        }
        buffered_row(&mut buffers, "dfe.bar", 1, 7);

        let flushed = buffers.get_ready_for_flush();
        let committable: Vec<KafkaOffset> = flushed.into_iter().flat_map(|b| b.offsets).collect();

        let to_commit = committable_offsets(committable, &buffers.lowest_pending_offsets());
        let mut got: Vec<(i32, i64)> = to_commit.iter().map(|o| (o.partition, o.offset)).collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![(0, 200), (0, 201), (0, 202), (0, 203), (0, 204)],
            "partition 0 commits in full; partition 1 never entered the batch"
        );
    }

    #[test]
    fn permanent_reject_is_a_distinct_error_from_a_transient_one() {
        // The flush path branches on this: permanent DLQs and commits,
        // transient withholds offsets so Kafka re-delivers.
        let permanent = crate::Error::ClickHousePermanent("code 117".into());
        let transient = crate::Error::ClickHouse("connection reset".into());
        assert!(matches!(permanent, crate::Error::ClickHousePermanent(_)));
        assert!(!matches!(transient, crate::Error::ClickHousePermanent(_)));
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
