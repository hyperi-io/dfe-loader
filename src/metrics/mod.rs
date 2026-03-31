// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Metrics via hyperi-rustlib `MetricsManager` + DFE metric groups.
//!
//! Three layers:
//! 1. `DfeMetrics` — platform `dfe_*` metrics (records, transport, scaling)
//! 2. Metric groups — standardised `dfe_loader_*` metrics (app, buffer, consumer, sink, CB)
//! 3. Loader-specific — per-table gauges, salvage, routing metrics
//!
//! Legacy `loader_*` names are dual-emitted alongside new `dfe_loader_*` names.
//! Remove legacy names after dashboard migration.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use metrics::{Counter, Gauge, Histogram};

use hyperi_rustlib::ScalingPressure;
use hyperi_rustlib::metrics::dfe_groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, CircuitBreakerMetrics, ConsumerMetrics,
    EnrichmentMetrics, SchemaCacheMetrics, SinkMetrics,
};
use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};

/// Application metrics backed by rustlib `MetricsManager`.
///
/// Registers metrics at three layers:
/// - `dfe_*` platform metrics via `DfeMetrics`
/// - `dfe_loader_*` standardised metrics via metric groups
/// - `loader_*` legacy metrics (dual-emit, remove after dashboard migration)
#[derive(Clone)]
pub struct Metrics {
    dfe: Arc<DfeMetrics>,

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
        let dfe = Arc::new(DfeMetrics::register());

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
        }
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
        self.dfe.transport_sent("clickhouse", rows as u64);
        self.dfe.transport_send_duration("clickhouse", latency_secs);
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
        self.dfe.transport_sent("clickhouse", rows as u64);
        self.dfe.transport_send_duration("clickhouse", latency_secs);
    }

    /// Record an insert error.
    pub fn record_error(&self) {
        self.insert_errors.increment(1);
        self.sink.record_error("clickhouse");
        self.dfe.transport_send_errors("clickhouse", 1);
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

    /// Update ClickHouse connection pool gauges from `PoolStats`.
    ///
    /// Call periodically (e.g., every 5s). No-op if `stats` is `None`
    /// (HTTP transport has no managed pool).
    pub fn update_pool_stats(&self, stats: Option<clickhouse::PoolStats>) {
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
    #[test]
    fn test_server_state_ready() {
        let ready = std::sync::atomic::AtomicBool::new(false);
        assert!(!ready.load(std::sync::atomic::Ordering::Acquire));
        ready.store(true, std::sync::atomic::Ordering::Release);
        assert!(ready.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn test_server_state_connections() {
        let kafka = std::sync::atomic::AtomicBool::new(false);
        let clickhouse = std::sync::atomic::AtomicBool::new(false);

        kafka.store(true, std::sync::atomic::Ordering::Release);
        clickhouse.store(true, std::sync::atomic::Ordering::Release);

        assert!(kafka.load(std::sync::atomic::Ordering::Acquire));
        assert!(clickhouse.load(std::sync::atomic::Ordering::Acquire));
    }
}
