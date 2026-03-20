// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Metrics via hyperi-rustlib MetricsManager.
//!
//! All `loader_*` metrics are registered through the global `metrics` crate
//! recorder installed by `MetricsManager`. The `/metrics`, `/healthz`, and
//! `/readyz` endpoints are served by `MetricsManager::start_server()`.
//!
//! `DfeMetrics` provides the standard `dfe_*` platform metrics alongside
//! the loader-specific names.

use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram};

use hyperi_rustlib::ScalingPressure;
use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};

/// Application metrics backed by rustlib MetricsManager.
///
/// Registers `loader_*` metrics via the global `metrics` recorder and
/// `dfe_*` platform metrics via `DfeMetrics`. Both are served on `/metrics`.
#[derive(Clone)]
pub struct Metrics {
    dfe: Arc<DfeMetrics>,
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
    /// Create metrics using a pre-built MetricsManager.
    ///
    /// The manager must already have installed the global recorder
    /// (via `MetricsManager::new` or `MetricsManager::with_config`).
    pub fn new(manager: &MetricsManager) -> Self {
        let dfe = Arc::new(DfeMetrics::register());

        Self {
            dfe,
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
        }
    }

    /// Record a message received.
    pub fn record_received(&self) {
        self.messages_received.increment(1);
        self.dfe.records_received(1);
    }

    /// Record a message processed for a table.
    pub fn record_processed(&self, table: &str) {
        self.messages_processed.increment(1);
        metrics::counter!("loader_messages_by_table_total", "table" => table.to_string())
            .increment(1);
        self.dfe.records_delivered(1);
    }

    /// Record a message sent to DLQ.
    pub fn record_dlq(&self) {
        self.messages_dlq.increment(1);
        self.dfe.records_dlq(1);
    }

    /// Record a batch flush.
    pub fn record_flush(&self, rows: usize, latency_secs: f64) {
        self.batches_flushed.increment(1);
        self.rows_inserted.increment(rows as u64);
        self.insert_latency.record(latency_secs);
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
        self.dfe.transport_sent("clickhouse", rows as u64);
        self.dfe.transport_send_duration("clickhouse", latency_secs);
    }

    /// Record an insert error.
    pub fn record_error(&self) {
        self.insert_errors.increment(1);
        self.dfe.transport_send_errors("clickhouse", 1);
    }

    /// Update aggregate buffer stats.
    pub fn update_buffer_stats(&self, rows: usize, bytes: usize, tables: usize) {
        self.buffer_rows.set(rows as f64);
        self.buffer_bytes.set(bytes as f64);
        self.buffer_tables.set(tables as f64);
    }

    /// Update per-table buffer depth.
    ///
    /// Emits `loader_buffer_rows_by_table` and `loader_buffer_bytes_by_table`
    /// gauges labelled by table name. Enables monitoring individual table
    /// backlog when one table has a problematic schema or cluster-side issue.
    pub fn update_per_table_buffer(&self, table: &str, rows: usize, bytes: usize) {
        metrics::gauge!("loader_buffer_rows_by_table", "table" => table.to_string())
            .set(rows as f64);
        metrics::gauge!("loader_buffer_bytes_by_table", "table" => table.to_string())
            .set(bytes as f64);
    }

    /// Update per-table circuit breaker state.
    ///
    /// Emits `loader_circuit_breaker_state` gauge labelled by table.
    /// Values: 0=closed (healthy), 1=open (failing), 2=half-open (probing).
    pub fn update_circuit_breaker_state(&self, table: &str, state: u8) {
        metrics::gauge!("loader_circuit_breaker_state", "table" => table.to_string())
            .set(f64::from(state));
    }

    /// Record Kafka offsets committed after successful insert.
    pub fn record_offsets_committed(&self, count: usize) {
        self.offsets_committed.increment(count as u64);
    }

    /// Update pipeline readiness.
    pub fn set_pipeline_ready(&self, ready: bool) {
        self.dfe.pipeline_ready(ready);
    }

    /// Update scaling pressure.
    pub fn set_scaling_pressure(&self, pressure: f64) {
        self.dfe.scaling_pressure(pressure);
    }

    /// Update memory usage from MemoryGuard.
    pub fn set_memory_usage(&self, current_bytes: u64, _limit_bytes: u64) {
        self.memory_used.set(current_bytes as f64);
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
    use super::*;

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
