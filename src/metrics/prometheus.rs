// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics implementation

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Opts, Registry};

/// Application metrics with Prometheus instrumentation
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    pub messages_received: Counter,
    pub messages_processed: Counter,
    pub messages_dlq: Counter,
    pub messages_by_table: CounterVec,
    pub batches_flushed: Counter,
    pub rows_inserted: Counter,
    pub insert_errors: Counter,
    pub offsets_committed: Counter,
    pub buffer_rows: Gauge,
    pub buffer_bytes: Gauge,
    pub buffer_tables: Gauge,
    pub insert_latency: Histogram,
    pub insert_latency_by_table: HistogramVec,
    pub memory_used: Gauge,
    pub kafka_lag: GaugeVec,
}

impl Metrics {
    /// Create and register metrics with default registry
    pub fn new() -> Self {
        Self::with_registry(Registry::new())
    }

    /// Create and register metrics with custom registry
    pub fn with_registry(registry: Registry) -> Self {
        // Message counters
        let messages_received = Counter::with_opts(Opts::new(
            "loader_messages_received_total",
            "Total messages received from Kafka",
        ))
        .unwrap();

        let messages_processed = Counter::with_opts(Opts::new(
            "loader_messages_processed_total",
            "Total messages successfully processed",
        ))
        .unwrap();

        let messages_dlq = Counter::with_opts(Opts::new(
            "loader_messages_dlq_total",
            "Total messages sent to DLQ",
        ))
        .unwrap();

        let messages_by_table = CounterVec::new(
            Opts::new(
                "loader_messages_by_table_total",
                "Messages processed by destination table",
            ),
            &["table"],
        )
        .unwrap();

        // Batch counters
        let batches_flushed = Counter::with_opts(Opts::new(
            "loader_batches_flushed_total",
            "Total batches flushed to ClickHouse",
        ))
        .unwrap();

        let rows_inserted = Counter::with_opts(Opts::new(
            "loader_rows_inserted_total",
            "Total rows inserted to ClickHouse",
        ))
        .unwrap();

        let insert_errors = Counter::with_opts(Opts::new(
            "loader_insert_errors_total",
            "Total insert errors",
        ))
        .unwrap();

        let offsets_committed = Counter::with_opts(Opts::new(
            "loader_kafka_offsets_committed_total",
            "Total Kafka offsets committed after successful insert",
        ))
        .unwrap();

        // Buffer gauges
        let buffer_rows =
            Gauge::with_opts(Opts::new("loader_buffer_rows", "Current rows buffered")).unwrap();

        let buffer_bytes =
            Gauge::with_opts(Opts::new("loader_buffer_bytes", "Current bytes buffered")).unwrap();

        let buffer_tables = Gauge::with_opts(Opts::new(
            "loader_buffer_tables",
            "Number of active table buffers",
        ))
        .unwrap();

        // Latency histograms
        let insert_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "loader_insert_latency_seconds",
                "Insert batch latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .unwrap();

        let insert_latency_by_table = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "loader_insert_latency_by_table_seconds",
                "Insert latency by table",
            )
            .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["table"],
        )
        .unwrap();

        // Memory gauge
        let memory_used = Gauge::with_opts(Opts::new(
            "loader_memory_bytes",
            "Estimated memory used by loader",
        ))
        .unwrap();

        // Kafka lag gauge
        let kafka_lag = GaugeVec::new(
            Opts::new("loader_kafka_lag", "Kafka consumer lag by partition"),
            &["topic", "partition"],
        )
        .unwrap();

        // Register all metrics
        registry
            .register(Box::new(messages_received.clone()))
            .unwrap();
        registry
            .register(Box::new(messages_processed.clone()))
            .unwrap();
        registry.register(Box::new(messages_dlq.clone())).unwrap();
        registry
            .register(Box::new(messages_by_table.clone()))
            .unwrap();
        registry
            .register(Box::new(batches_flushed.clone()))
            .unwrap();
        registry.register(Box::new(rows_inserted.clone())).unwrap();
        registry.register(Box::new(insert_errors.clone())).unwrap();
        registry
            .register(Box::new(offsets_committed.clone()))
            .unwrap();
        registry.register(Box::new(buffer_rows.clone())).unwrap();
        registry.register(Box::new(buffer_bytes.clone())).unwrap();
        registry.register(Box::new(buffer_tables.clone())).unwrap();
        registry.register(Box::new(insert_latency.clone())).unwrap();
        registry
            .register(Box::new(insert_latency_by_table.clone()))
            .unwrap();
        registry.register(Box::new(memory_used.clone())).unwrap();
        registry.register(Box::new(kafka_lag.clone())).unwrap();

        Self {
            registry,
            messages_received,
            messages_processed,
            messages_dlq,
            messages_by_table,
            batches_flushed,
            rows_inserted,
            insert_errors,
            offsets_committed,
            buffer_rows,
            buffer_bytes,
            buffer_tables,
            insert_latency,
            insert_latency_by_table,
            memory_used,
            kafka_lag,
        }
    }

    /// Get the Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Gather metrics as Prometheus text format
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Record a message received
    pub fn record_received(&self) {
        self.messages_received.inc();
    }

    /// Record a message processed for a table
    pub fn record_processed(&self, table: &str) {
        self.messages_processed.inc();
        self.messages_by_table.with_label_values(&[table]).inc();
    }

    /// Record a message sent to DLQ
    pub fn record_dlq(&self) {
        self.messages_dlq.inc();
    }

    /// Record a batch flush
    pub fn record_flush(&self, rows: usize, latency_secs: f64) {
        self.batches_flushed.inc();
        self.rows_inserted.inc_by(rows as f64);
        self.insert_latency.observe(latency_secs);
    }

    /// Record a batch flush for a specific table
    pub fn record_flush_table(&self, table: &str, rows: usize, latency_secs: f64) {
        self.batches_flushed.inc();
        self.rows_inserted.inc_by(rows as f64);
        self.insert_latency.observe(latency_secs);
        self.insert_latency_by_table
            .with_label_values(&[table])
            .observe(latency_secs);
    }

    /// Record an insert error
    pub fn record_error(&self) {
        self.insert_errors.inc();
    }

    /// Update buffer stats
    pub fn update_buffer_stats(&self, rows: usize, bytes: usize, tables: usize) {
        self.buffer_rows.set(rows as f64);
        self.buffer_bytes.set(bytes as f64);
        self.buffer_tables.set(tables as f64);
    }

    /// Record Kafka offsets committed after successful insert
    pub fn record_offsets_committed(&self, count: usize) {
        self.offsets_committed.inc_by(count as f64);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        assert_eq!(metrics.messages_received.get(), 0.0);
    }

    #[test]
    fn test_metrics_increment() {
        let metrics = Metrics::new();
        metrics.record_received();
        metrics.record_received();
        assert_eq!(metrics.messages_received.get(), 2.0);
    }

    #[test]
    fn test_metrics_gather() {
        let metrics = Metrics::new();
        metrics.record_received();
        let output = metrics.gather();
        assert!(output.contains("loader_messages_received_total"));
    }

    #[test]
    fn test_metrics_by_table() {
        let metrics = Metrics::new();
        metrics.record_processed("events_auth");
        metrics.record_processed("events_auth");
        metrics.record_processed("events_api");

        let output = metrics.gather();
        assert!(output.contains("events_auth"));
        assert!(output.contains("events_api"));
    }
}
