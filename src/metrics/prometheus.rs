//! Prometheus metrics implementation

use prometheus::{Counter, Gauge, Histogram, Registry};

/// Application metrics
pub struct Metrics {
    pub messages_received: Counter,
    pub messages_inserted: Counter,
    pub messages_failed: Counter,
    pub buffer_rows: Gauge,
    pub buffer_bytes: Gauge,
    pub insert_latency: Histogram,
    pub memory_used: Gauge,
}

impl Metrics {
    /// Create and register metrics
    pub fn new(registry: &Registry) -> Self {
        let messages_received =
            Counter::new("loader_messages_received_total", "Total messages received from Kafka")
                .unwrap();
        let messages_inserted =
            Counter::new("loader_messages_inserted_total", "Total messages inserted to ClickHouse")
                .unwrap();
        let messages_failed =
            Counter::new("loader_messages_failed_total", "Total messages failed").unwrap();
        let buffer_rows = Gauge::new("loader_buffer_rows", "Current rows in buffers").unwrap();
        let buffer_bytes = Gauge::new("loader_buffer_bytes", "Current bytes in buffers").unwrap();
        let insert_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new("loader_insert_latency_seconds", "Insert latency")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )
        .unwrap();
        let memory_used = Gauge::new("loader_memory_bytes", "Memory used by loader").unwrap();

        registry.register(Box::new(messages_received.clone())).unwrap();
        registry.register(Box::new(messages_inserted.clone())).unwrap();
        registry.register(Box::new(messages_failed.clone())).unwrap();
        registry.register(Box::new(buffer_rows.clone())).unwrap();
        registry.register(Box::new(buffer_bytes.clone())).unwrap();
        registry.register(Box::new(insert_latency.clone())).unwrap();
        registry.register(Box::new(memory_used.clone())).unwrap();

        Self {
            messages_received,
            messages_inserted,
            messages_failed,
            buffer_rows,
            buffer_bytes,
            insert_latency,
            memory_used,
        }
    }
}
