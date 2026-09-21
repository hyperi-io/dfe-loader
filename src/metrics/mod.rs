// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Metrics via scalo `MetricsManager` + DFE metric groups.
//!
//! Three layers:
//! 1. `ServiceMetrics` — platform `dfe_*` metrics (records, transport, scaling)
//! 2. Metric groups — standardised `dfe_loader_*` metrics (app, buffer, consumer, sink, CB)
//! 3. Loader-specific — per-table gauges, salvage, routing metrics
//!
//! Legacy `loader_*` names are dual-emitted alongside new `dfe_loader_*` names.
//! Remove legacy names after dashboard migration.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use metrics::{Counter, Gauge, Histogram};

use scalo::ScalingPressure;
use scalo::metrics::groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, CircuitBreakerMetrics, ConsumerMetrics,
    EnrichmentMetrics, SchemaCacheMetrics, SinkMetrics,
};
use scalo::metrics::{MetricsManager, ServiceMetrics, TransportKind};

/// Application metrics backed by scalo `MetricsManager`.
///
/// Registers metrics at three layers:
/// - `dfe_*` platform metrics via `ServiceMetrics`
/// - `dfe_loader_*` standardised metrics via metric groups
/// - `loader_*` legacy metrics (dual-emit, remove after dashboard migration)
#[derive(Clone)]
pub struct Metrics {
    dfe: Arc<ServiceMetrics>,

    // Standardised metric groups (dfe_loader_* namespace)
    pub app: AppMetrics,
    pub buffer: BufferMetrics,
    pub consumer: ConsumerMetrics,
    pub sink: SinkMetrics,
    pub circuit_breaker: CircuitBreakerMetrics,
    pub backpressure: BackpressureMetrics,
    pub enrichment: EnrichmentMetrics,
    pub schema_cache: SchemaCacheMetrics,

    // ClickHouse connection pool gauges (native transport only)
    pub pool_max: Gauge,
    pub pool_active: Gauge,
    pub pool_idle: Gauge,
    pub pool_waiting: Gauge,

    // Insert byte/transaction counters (from fork commit callbacks)
    pub insert_bytes: Counter,
    pub insert_transactions: Counter,

    // EPS (events per second) — live gauge for top/debugging
    pub eps: Gauge,
    eps_counter: Arc<AtomicU64>,
    eps_last_update: Arc<parking_lot::Mutex<Instant>>,
    eps_last_count: Arc<AtomicU64>,

    // ClickHouse sink domain metrics (scaling-signal gap audit, scalo 2.8.10).
    // The engine can only correlate signals that EXIST — these surface the
    // ClickHouse sink's load (latency, errors, throughput, backlog, batch size)
    // so they can drive a domain scaling pressure or KEDA Prometheus trigger.
    /// Insert wall-clock latency distribution (seconds).
    pub ch_insert_duration: Histogram,
    /// Rows inserted per flush — count distribution (flush size).
    pub ch_flush_rows: Histogram,
    /// Uncompressed bytes per flush — byte distribution (flush size).
    pub ch_flush_bytes: Histogram,
    /// In-flight concurrent insert tasks (inserter queue/concurrency depth).
    pub ch_inserter_inflight: Gauge,
    /// Insert errors (terminal, after retry/salvage).
    pub ch_insert_errors: Counter,
    /// Rows inserted per second — live gauge (sink throughput).
    pub ch_rows_per_sec: Gauge,
    ch_rows_counter: Arc<AtomicU64>,
    ch_rows_last_update: Arc<parking_lot::Mutex<Instant>>,
    ch_rows_last_count: Arc<AtomicU64>,

    // Schema-resolution metrics. Registered through the manager rather than
    // emitted straight from the macro, so the manifest carries them (#158).
    /// Messages DLQ'd because a pending-schema buffer hit a cap.
    pub pending_schema_overflow: Counter,
    /// Pending-schema messages DLQ'd by the expire sweep.
    pub pending_schema_expired: Counter,
    /// Messages currently held awaiting schema resolution.
    pub pending_schema_messages: Gauge,
    /// Pre-warm retry rounds beyond the first.
    pub schema_prewarm_retries: Counter,
    /// Tables still failing pre-warm after the retry budget.
    pub schema_prewarm_failed_tables: Gauge,

    /// Received messages carrying a batch of records as a JSON array.
    pub batched_array_messages: Counter,
    /// Records those batched arrays were split into.
    pub batched_array_records: Counter,
    /// Received messages carrying newline-separated JSON records.
    pub batched_ndjson_messages: Counter,
    /// Records those messages were split into.
    pub batched_ndjson_records: Counter,

    // Legacy loader_* metrics (dual-emit — remove after dashboard migration)
    pub messages_received: Counter,
    pub messages_processed: Counter,
    pub messages_dlq: Counter,
    pub batches_flushed: Counter,
    pub rows_inserted: Counter,
    pub insert_errors: Counter,
    pub offsets_committed: Counter,
    pub buffer_rows: Gauge,
    pub buffer_bytes: Gauge,
    pub buffer_tables: Gauge,
    pub insert_latency: Histogram,
    pub memory_used: Gauge,
}

impl Metrics {
    /// Create metrics using a pre-built `MetricsManager`.
    ///
    /// The manager must already have installed the global recorder
    /// (via `MetricsManager::new` or `MetricsManager::with_config`).
    pub fn new(manager: &MetricsManager) -> Self {
        let dfe = Arc::new(ServiceMetrics::register(manager));

        // Emitted from the processor, which holds no Metrics handle; registered
        // here so the catalogue names it (#158).
        let _ = manager.counter(
            "dfe_loader_header_pass_skipped_total",
            "Messages rejected because the header pass promoted no columns",
        );
        let _ = manager.counter(
            "dfe_loader_routing_field_absent_total",
            "Records on a source topic that named no table and fell back to the default",
        );

        Self {
            dfe,

            // Standardised groups (dfe_loader_* namespace)
            app: AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), ""),
            buffer: BufferMetrics::new(manager),
            consumer: ConsumerMetrics::new(manager),
            sink: SinkMetrics::new(manager),
            circuit_breaker: CircuitBreakerMetrics::new(manager),
            backpressure: BackpressureMetrics::new(manager),
            enrichment: EnrichmentMetrics::new(manager),
            schema_cache: SchemaCacheMetrics::new(manager),

            // Legacy metrics (dual-emit)
            messages_received: manager.counter(
                "messages_received_total",
                "Total messages received from Kafka",
            ),
            messages_processed: manager.counter(
                "messages_processed_total",
                "Total messages successfully processed",
            ),
            messages_dlq: manager.counter("messages_dlq_total", "Total messages sent to DLQ"),
            batches_flushed: manager.counter(
                "batches_flushed_total",
                "Total batches flushed to ClickHouse",
            ),
            rows_inserted: manager
                .counter("rows_inserted_total", "Total rows inserted to ClickHouse"),
            insert_errors: manager.counter("insert_errors_total", "Total insert errors"),
            offsets_committed: manager.counter(
                "kafka_offsets_committed_total",
                "Total Kafka offsets committed after successful insert",
            ),
            buffer_rows: manager.gauge("buffer_rows", "Current rows buffered"),
            buffer_bytes: manager.gauge("buffer_bytes", "Current bytes buffered"),
            buffer_tables: manager.gauge("buffer_tables", "Number of active table buffers"),
            insert_latency: manager
                .histogram("insert_latency_seconds", "Insert batch latency in seconds"),
            memory_used: manager.gauge("memory_bytes", "Estimated memory used by loader"),

            // ClickHouse connection pool (native transport only)
            pool_max: manager.gauge("clickhouse_pool_max", "Max connections in pool"),
            pool_active: manager.gauge("clickhouse_pool_active", "Active connections in pool"),
            pool_idle: manager.gauge("clickhouse_pool_idle", "Idle connections in pool"),
            pool_waiting: manager
                .gauge("clickhouse_pool_waiting", "Tasks waiting for a connection"),

            // Insert byte/transaction counters (from fork commit callbacks)
            insert_bytes: manager.counter(
                "insert_bytes_total",
                "Total uncompressed bytes inserted to ClickHouse",
            ),
            insert_transactions: manager.counter(
                "insert_transactions_total",
                "Total INSERT statements committed",
            ),

            // EPS (events per second) — updated periodically from counter delta
            eps: manager.gauge("events_per_second", "Current events processed per second"),
            eps_counter: Arc::new(AtomicU64::new(0)),
            eps_last_update: Arc::new(parking_lot::Mutex::new(Instant::now())),
            eps_last_count: Arc::new(AtomicU64::new(0)),

            // ClickHouse sink domain metrics (scaling-signal gap audit, 2.8.10)
            ch_insert_duration: manager.histogram(
                "clickhouse_insert_duration_seconds",
                "ClickHouse insert batch latency",
            ),
            // scalo 2.10 dropped histogram_count / histogram_with_unit: the
            // remaining histogram constructors register the manifest unit as
            // seconds. These two are a count (rows) and bytes distribution; the
            // emitted metric is unchanged, only the manifest unit metadata.
            ch_flush_rows: manager.histogram(
                "clickhouse_flush_rows",
                "Rows per flush to ClickHouse (flush size)",
            ),
            ch_flush_bytes: manager.histogram(
                "clickhouse_flush_bytes",
                "Uncompressed bytes per flush to ClickHouse (flush size)",
            ),
            ch_inserter_inflight: manager.gauge(
                "clickhouse_inserter_inflight",
                "In-flight concurrent ClickHouse insert tasks (inserter queue depth)",
            ),
            ch_insert_errors: manager.counter(
                "clickhouse_insert_errors_total",
                "Terminal ClickHouse insert errors (after retry/salvage)",
            ),
            ch_rows_per_sec: manager.gauge(
                "clickhouse_rows_per_second",
                "Rows inserted to ClickHouse per second (sink throughput)",
            ),
            ch_rows_counter: Arc::new(AtomicU64::new(0)),
            ch_rows_last_update: Arc::new(parking_lot::Mutex::new(Instant::now())),
            ch_rows_last_count: Arc::new(AtomicU64::new(0)),

            // Schema resolution (#36 buffer, #158 catalogue)
            pending_schema_overflow: manager.counter(
                "dfe_loader_pending_schema_overflow_total",
                "Messages DLQ'd because a pending-schema buffer hit its cap",
            ),
            pending_schema_expired: manager.counter(
                "dfe_loader_pending_schema_expired_total",
                "Pending-schema messages DLQ'd by the expire sweep",
            ),
            pending_schema_messages: manager.gauge(
                "dfe_loader_pending_schema_messages",
                "Messages held awaiting schema resolution",
            ),
            schema_prewarm_retries: manager.counter(
                "dfe_loader_schema_prewarm_retries_total",
                "Schema pre-warm retry rounds beyond the first",
            ),
            schema_prewarm_failed_tables: manager.gauge(
                "dfe_loader_schema_prewarm_failed_tables",
                "Tables still failing schema pre-warm after the retry budget",
            ),

            // Batched-array fan-out (#128)
            batched_array_messages: manager.counter(
                "dfe_loader_batched_array_messages_total",
                "Received messages carrying a batch of records as a JSON array",
            ),
            batched_array_records: manager.counter(
                "dfe_loader_batched_array_records_total",
                "Records split out of batched-array messages",
            ),

            // Newline-separated batch fan-out (#184)
            batched_ndjson_messages: manager.counter(
                "dfe_loader_batched_ndjson_messages_total",
                "Received messages carrying newline-separated JSON records",
            ),
            batched_ndjson_records: manager.counter(
                "dfe_loader_batched_ndjson_records_total",
                "Records split out of newline-separated messages",
            ),
        }
    }

    /// Record batched arrays split into one record per element.
    pub fn record_batched_array_fanout(&self, arrays: u64, records: u64) {
        self.batched_array_messages.increment(arrays);
        self.batched_array_records.increment(records);
    }

    /// Record newline-separated batches split into one record per line.
    ///
    /// Counted apart from the array fan-out so an operator reads which producer
    /// is batching, not merely that one is.
    pub fn record_batched_ndjson_fanout(&self, messages: u64, records: u64) {
        self.batched_ndjson_messages.increment(messages);
        self.batched_ndjson_records.increment(records);
    }

    /// Record a message received.
    pub fn record_received(&self) {
        self.messages_received.increment(1);
        self.app.record_received(1);
        self.dfe.records_received(1);
        self.eps_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message processed for a table.
    pub fn record_processed(&self, table: &str) {
        self.messages_processed.increment(1);
        self.app.record_processed(1);
        metrics::counter!("loader_messages_by_table_total", "table" => table.to_string())
            .increment(1);
        self.dfe.records_delivered(1);
    }

    /// Record a message sent to DLQ.
    pub fn record_dlq(&self) {
        self.messages_dlq.increment(1);
        self.app.record_error(1);
        self.dfe.records_dlq(1);
    }

    /// Record a batch flush.
    pub fn record_flush(&self, rows: usize, latency_secs: f64) {
        self.batches_flushed.increment(1);
        self.rows_inserted.increment(rows as u64);
        self.insert_latency.record(latency_secs);
        self.sink.record_duration("clickhouse", latency_secs);
        self.dfe.transport_sent(TransportKind::Http, rows as u64);
        self.dfe.transport_send_duration("clickhouse", latency_secs);

        // ClickHouse sink domain metrics (2.8.10 scaling-signal gap audit):
        // latency distribution, flush-size (rows), and throughput accounting.
        self.ch_insert_duration.record(latency_secs);
        self.ch_flush_rows.record(rows as f64);
        self.ch_rows_counter
            .fetch_add(rows as u64, Ordering::Relaxed);
    }

    /// Record the uncompressed byte size of a flushed batch (flush-size distribution).
    pub fn record_flush_bytes(&self, bytes: u64) {
        self.ch_flush_bytes.record(bytes as f64);
    }

    /// Record a terminal ClickHouse insert error (after retry/salvage exhausted).
    pub fn record_clickhouse_insert_error(&self) {
        self.ch_insert_errors.increment(1);
    }

    /// Set the in-flight concurrent insert count (inserter queue/concurrency depth).
    pub fn set_inserter_inflight(&self, inflight: u64) {
        self.ch_inserter_inflight.set(inflight as f64);
    }

    /// Record a batch flush for a specific table.
    pub fn record_flush_table(&self, table: &str, rows: usize, latency_secs: f64) {
        self.batches_flushed.increment(1);
        self.rows_inserted.increment(rows as u64);
        self.insert_latency.record(latency_secs);
        metrics::histogram!(
            "loader_insert_latency_by_table_seconds",
            "table" => table.to_string()
        )
        .record(latency_secs);
        self.sink.record_duration("clickhouse", latency_secs);
        self.dfe.transport_sent(TransportKind::Http, rows as u64);
        self.dfe.transport_send_duration("clickhouse", latency_secs);
    }

    /// Record an insert error.
    pub fn record_error(&self) {
        self.insert_errors.increment(1);
        self.sink.record_error("clickhouse");
        self.dfe.transport_send_errors(TransportKind::Http, 1);
    }

    /// Record a max_dynamic_paths limit hit on a JSON column.
    pub fn record_max_dynamic_paths_exceeded(&self, table: &str) {
        metrics::counter!(
            "loader_json_max_paths_exceeded_total",
            "table" => table.to_string()
        )
        .increment(1);
    }

    /// Record a message DLQ'd because its pending-schema buffer hit a per-table
    /// or global cap (#36).
    pub fn record_pending_schema_overflow(&self) {
        self.pending_schema_overflow.increment(1);
    }

    /// Record a pending-schema message DLQ'd by the expire sweep (aged out,
    /// globally evicted, or drained on shutdown) (#36).
    pub fn record_pending_schema_expired(&self) {
        self.pending_schema_expired.increment(1);
    }

    /// Record a row DLQ'd because ClickHouse rejected it deterministically and
    /// retrying it would wedge the partition.
    pub fn record_permanent_reject(&self, table: &str) {
        metrics::counter!(
            "dfe_loader_permanent_reject_total",
            "table" => table.to_string()
        )
        .increment(1);
    }

    /// Record `n` messages re-routed to the default table because `ClickHouse`
    /// confirmed their destination table does not exist.
    ///
    /// Counted in bulk per table per batch: steady-state fallback traffic is
    /// the part that matters, and a per-row call would allocate the label on
    /// every message.
    pub fn record_unknown_table_fallback_n(&self, table: &str, n: u64) {
        if n == 0 {
            return;
        }
        metrics::counter!(
            "dfe_loader_unknown_table_fallback_total",
            "table" => table.to_string()
        )
        .increment(n);
    }

    /// Update the gauge of messages currently held in the pending-schema buffer.
    pub fn update_pending_schema_messages(&self, n: usize) {
        self.pending_schema_messages.set(n as f64);
    }

    /// Record a pre-warm retry round (counted once per round beyond the first).
    pub fn record_schema_prewarm_retry(&self) {
        self.schema_prewarm_retries.increment(1);
    }

    /// Set the gauge of tables still failing pre-warm after the retry budget.
    pub fn update_schema_prewarm_failed_tables(&self, n: usize) {
        self.schema_prewarm_failed_tables.set(n as f64);
    }

    /// Update aggregate buffer stats.
    pub fn update_buffer_stats(&self, rows: usize, bytes: usize, tables: usize) {
        self.buffer_rows.set(rows as f64);
        self.buffer_bytes.set(bytes as f64);
        self.buffer_tables.set(tables as f64);
        self.buffer.set_buffer(bytes, rows);
    }

    /// Update per-table buffer depth.
    pub fn update_per_table_buffer(&self, table: &str, rows: usize, bytes: usize) {
        metrics::gauge!("loader_buffer_rows_by_table", "table" => table.to_string())
            .set(rows as f64);
        metrics::gauge!("loader_buffer_bytes_by_table", "table" => table.to_string())
            .set(bytes as f64);
    }

    /// Update per-table circuit breaker state.
    pub fn update_circuit_breaker_state(&self, table: &str, state: u8) {
        metrics::gauge!("loader_circuit_breaker_state", "table" => table.to_string())
            .set(f64::from(state));
        self.circuit_breaker.set_state(table, state);
    }

    /// Record Kafka offsets committed after successful insert.
    pub fn record_offsets_committed(&self, count: usize) {
        self.offsets_committed.increment(count as u64);
        self.consumer.record_offsets_committed(count as u64);
    }

    /// Update pipeline readiness.
    pub fn set_pipeline_ready(&self, ready: bool) {
        self.dfe.pipeline_ready(ready);
    }

    /// Update scaling pressure.
    pub fn set_scaling_pressure(&self, pressure: f64) {
        self.dfe.scaling_pressure(pressure);
    }

    /// Update memory usage from `MemoryGuard`.
    pub fn set_memory_usage(&self, current_bytes: u64, limit_bytes: u64) {
        self.memory_used.set(current_bytes as f64);
        self.app.set_memory(current_bytes, limit_bytes);
    }

    /// Update EPS gauge from counter delta.
    ///
    /// Call periodically (e.g., every 5s alongside buffer stats).
    /// Calculates events/second from the delta since last call.
    pub fn update_eps(&self) {
        let current = self.eps_counter.load(Ordering::Relaxed);
        let previous = self.eps_last_count.swap(current, Ordering::Relaxed);
        let mut last = self.eps_last_update.lock();
        let elapsed = last.elapsed().as_secs_f64();
        *last = Instant::now();

        if elapsed > 0.0 {
            let delta = current.saturating_sub(previous);
            self.eps.set(delta as f64 / elapsed);
        }
    }

    /// Update the ClickHouse rows-per-second gauge from the inserted-rows counter
    /// delta. Call periodically (e.g. every flush tick alongside `update_eps`).
    pub fn update_clickhouse_rows_per_sec(&self) {
        let current = self.ch_rows_counter.load(Ordering::Relaxed);
        let previous = self.ch_rows_last_count.swap(current, Ordering::Relaxed);
        let mut last = self.ch_rows_last_update.lock();
        let elapsed = last.elapsed().as_secs_f64();
        *last = Instant::now();

        if elapsed > 0.0 {
            let delta = current.saturating_sub(previous);
            self.ch_rows_per_sec.set(delta as f64 / elapsed);
        }
    }

    /// Update ClickHouse connection pool gauges from `PoolStats`.
    ///
    /// Call periodically (e.g., every 5s). No-op if `stats` is `None`
    /// (HTTP transport has no managed pool).
    pub fn update_pool_stats(&self, stats: Option<crate::clickhouse::PoolStats>) {
        if let Some(s) = stats {
            self.pool_max.set(s.max_size as f64);
            self.pool_active.set(s.size as f64);
            self.pool_idle.set(s.available as f64);
            self.pool_waiting.set(s.waiting as f64);
        }
    }

    /// Record insert bytes and transactions from a commit callback.
    pub fn record_insert_quantities(&self, bytes: u64, transactions: u64) {
        self.insert_bytes.increment(bytes);
        self.insert_transactions.increment(transactions);
        self.app.record_bytes_written(bytes);
    }
}

/// Shared server state for the pipeline.
///
/// Wraps readiness tracking. The metrics HTTP server is managed by
/// `MetricsManager::start_server()` — no bespoke server needed.
pub struct ServerState {
    pub metrics: Metrics,
    pub ready: std::sync::atomic::AtomicBool,
    pub kafka_connected: std::sync::atomic::AtomicBool,
    pub clickhouse_connected: std::sync::atomic::AtomicBool,
    pub scaling: Arc<ScalingPressure>,
}

impl ServerState {
    pub fn new(metrics: Metrics, scaling: Arc<ScalingPressure>) -> Self {
        Self {
            metrics,
            ready: std::sync::atomic::AtomicBool::new(false),
            kafka_connected: std::sync::atomic::AtomicBool::new(false),
            clickhouse_connected: std::sync::atomic::AtomicBool::new(false),
            scaling,
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready
            .store(ready, std::sync::atomic::Ordering::Release);
    }

    pub fn set_kafka_connected(&self, connected: bool) {
        self.kafka_connected
            .store(connected, std::sync::atomic::Ordering::Release);
    }

    pub fn set_clickhouse_connected(&self, connected: bool) {
        self.clickhouse_connected
            .store(connected, std::sync::atomic::Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use scalo::ScalingPressure;
    use scalo::metrics::MetricsManager;
    use scalo::scaling::ScalingPressureConfig;

    use super::{Metrics, ServerState};

    /// Global MetricsManager — recorder can only be installed once per process.
    fn test_manager() -> &'static MetricsManager {
        static MANAGER: OnceLock<MetricsManager> = OnceLock::new();
        MANAGER.get_or_init(|| MetricsManager::new("loader_unit"))
    }

    fn test_metrics() -> Metrics {
        Metrics::new(test_manager())
    }

    fn test_scaling() -> Arc<ScalingPressure> {
        Arc::new(ScalingPressure::new(
            ScalingPressureConfig::default(),
            vec![],
        ))
    }

    // ---- metric catalogue (what `metrics-manifest` prints) ----

    #[test]
    fn building_the_metrics_fills_the_manifest_catalogue() {
        // Same manager config the metrics-manifest subcommand builds, and the
        // registry is per-manager, so this reads only what this test registered.
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let manifest_before = manager.registry().manifest();
        assert!(
            manifest_before.metrics.is_empty(),
            "a fresh manager starts with no catalogue"
        );

        let _metrics = Metrics::new(&manager);
        let names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|d| d.name)
            .collect();

        for expected in [
            "kafka_offsets_committed_total",
            "consumer_partitions_assigned",
            "dfe_loader_pending_schema_overflow_total",
            "dfe_loader_pending_schema_expired_total",
            "dfe_loader_pending_schema_messages",
            "dfe_loader_schema_prewarm_retries_total",
            "dfe_loader_schema_prewarm_failed_tables",
            "dfe_loader_header_pass_skipped_total",
            "dfe_loader_routing_field_absent_total",
            "dfe_loader_batched_array_messages_total",
            "dfe_loader_batched_array_records_total",
            "dfe_loader_batched_ndjson_messages_total",
            "dfe_loader_batched_ndjson_records_total",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "{expected} is missing from the catalogue: {names:?}"
            );
        }
    }

    // ---- pending-schema / pre-warm metrics ----

    #[test]
    fn pending_schema_and_prewarm_metrics_do_not_panic() {
        let m = test_metrics();
        m.record_pending_schema_overflow();
        m.record_pending_schema_expired();
        m.update_pending_schema_messages(42);
        m.record_schema_prewarm_retry();
        m.update_schema_prewarm_failed_tables(3);
    }

    #[test]
    fn batched_fanout_metrics_do_not_panic() {
        let m = test_metrics();
        m.record_batched_array_fanout(2, 25);
        m.record_batched_ndjson_fanout(3, 15);
    }

    // ---- ServerState tests ----

    #[test]
    fn server_state_starts_not_ready() {
        let state = ServerState::new(test_metrics(), test_scaling());
        assert!(!state.is_ready());
    }

    #[test]
    fn server_state_set_ready_true_then_false() {
        let state = ServerState::new(test_metrics(), test_scaling());
        state.set_ready(true);
        assert!(state.is_ready());
        state.set_ready(false);
        assert!(!state.is_ready());
    }

    #[test]
    fn server_state_kafka_connection_lifecycle() {
        let state = ServerState::new(test_metrics(), test_scaling());
        // Starts disconnected
        assert!(
            !state
                .kafka_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
        // Connect
        state.set_kafka_connected(true);
        assert!(
            state
                .kafka_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
        // Disconnect
        state.set_kafka_connected(false);
        assert!(
            !state
                .kafka_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }

    #[test]
    fn server_state_clickhouse_connection_lifecycle() {
        let state = ServerState::new(test_metrics(), test_scaling());
        assert!(
            !state
                .clickhouse_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
        state.set_clickhouse_connected(true);
        assert!(
            state
                .clickhouse_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
        state.set_clickhouse_connected(false);
        assert!(
            !state
                .clickhouse_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }

    #[test]
    fn server_state_independent_flags() {
        // Setting one flag must not affect others
        let state = ServerState::new(test_metrics(), test_scaling());
        state.set_ready(true);
        state.set_kafka_connected(false);
        state.set_clickhouse_connected(false);
        assert!(state.is_ready());
        assert!(
            !state
                .kafka_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );

        state.set_kafka_connected(true);
        state.set_ready(false);
        assert!(!state.is_ready());
        assert!(
            state
                .kafka_connected
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }

    // ---- Metrics record methods (exercise all paths, no panics) ----

    #[test]
    fn metrics_record_received_increments() {
        let m = test_metrics();
        // Should not panic — exercises counter + eps_counter
        m.record_received();
        m.record_received();
        m.record_received();
    }

    #[test]
    fn metrics_record_processed_with_various_tables() {
        let m = test_metrics();
        m.record_processed("dfe.events");
        m.record_processed("dfe.metrics");
        m.record_processed(""); // empty table name — edge case
    }

    #[test]
    fn metrics_record_dlq() {
        let m = test_metrics();
        m.record_dlq();
    }

    #[test]
    fn metrics_record_flush_zero_rows() {
        let m = test_metrics();
        m.record_flush(0, 0.0);
    }

    #[test]
    fn metrics_record_flush_large_batch() {
        let m = test_metrics();
        m.record_flush(1_000_000, 2.5);
    }

    #[test]
    fn metrics_record_flush_table() {
        let m = test_metrics();
        m.record_flush_table("dfe.events", 500, 0.042);
        m.record_flush_table("dfe.events", 0, 0.0); // zero rows
    }

    #[test]
    fn metrics_record_error() {
        let m = test_metrics();
        m.record_error();
    }

    #[test]
    fn metrics_record_max_dynamic_paths_exceeded() {
        let m = test_metrics();
        m.record_max_dynamic_paths_exceeded("dfe.events");
        m.record_max_dynamic_paths_exceeded(""); // empty table
    }

    #[test]
    fn metrics_update_buffer_stats_zero() {
        let m = test_metrics();
        m.update_buffer_stats(0, 0, 0);
    }

    #[test]
    fn metrics_update_buffer_stats_nonzero() {
        let m = test_metrics();
        m.update_buffer_stats(50_000, 10_485_760, 12);
    }

    #[test]
    fn metrics_update_per_table_buffer() {
        let m = test_metrics();
        m.update_per_table_buffer("dfe.events", 1000, 65536);
        m.update_per_table_buffer("dfe.events", 0, 0); // reset
    }

    #[test]
    fn metrics_update_circuit_breaker_state() {
        let m = test_metrics();
        m.update_circuit_breaker_state("dfe.events", 0); // closed
        m.update_circuit_breaker_state("dfe.events", 1); // open
        m.update_circuit_breaker_state("dfe.events", 2); // half-open
    }

    #[test]
    fn metrics_record_offsets_committed_zero_and_many() {
        let m = test_metrics();
        m.record_offsets_committed(0);
        m.record_offsets_committed(1);
        m.record_offsets_committed(10_000);
    }

    #[test]
    fn metrics_set_pipeline_ready_toggle() {
        let m = test_metrics();
        m.set_pipeline_ready(true);
        m.set_pipeline_ready(false);
    }

    #[test]
    fn metrics_set_scaling_pressure_boundaries() {
        let m = test_metrics();
        m.set_scaling_pressure(0.0);
        m.set_scaling_pressure(0.5);
        m.set_scaling_pressure(1.0);
    }

    #[test]
    fn metrics_set_memory_usage() {
        let m = test_metrics();
        m.set_memory_usage(0, 0); // edge: both zero
        m.set_memory_usage(1_073_741_824, 4_294_967_296); // 1GB / 4GB
    }

    #[test]
    fn metrics_update_eps_immediate_call() {
        let m = test_metrics();
        // First call with zero elapsed — should not divide by zero
        m.update_eps();
    }

    #[test]
    fn metrics_update_eps_after_receiving() {
        let m = test_metrics();
        m.record_received();
        m.record_received();
        m.record_received();
        // Small delay to get non-zero elapsed
        std::thread::sleep(std::time::Duration::from_millis(10));
        m.update_eps();
        // EPS gauge should be set (we can't read the value, but it must not panic)
    }

    #[test]
    fn metrics_update_pool_stats_none_is_noop() {
        let m = test_metrics();
        m.update_pool_stats(None);
    }

    #[test]
    fn metrics_update_pool_stats_some() {
        let m = test_metrics();
        m.update_pool_stats(Some(crate::clickhouse::PoolStats {
            max_size: 10,
            size: 5,
            available: 3,
            waiting: 2,
        }));
    }

    #[test]
    fn metrics_record_insert_quantities_zero() {
        let m = test_metrics();
        m.record_insert_quantities(0, 0);
    }

    #[test]
    fn metrics_record_insert_quantities_large() {
        let m = test_metrics();
        m.record_insert_quantities(10_737_418_240, 5000);
    }
}
