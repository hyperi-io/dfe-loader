//! Main pipeline coordinator
//!
//! Orchestrates the Kafka → Transform → Buffer → ClickHouse pipeline.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::buffer::{BufferManager, FlushBatch};
use crate::clickhouse::{ClickHouseClient, Inserter, InserterConfig};
use crate::config::Config;
use crate::kafka::{Consumer, KafkaMessage};
use crate::metrics::Metrics;
use crate::payload::{FormatDetector, FormatMode, PayloadFormat};
use crate::routing::{RouteResult, Router};
use crate::transform::Transformer;
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

        // Initialize components
        let consumer = Consumer::new(&self.config.kafka)?;
        consumer.subscribe()?;

        let clickhouse = Arc::new(ClickHouseClient::new(&self.config.clickhouse).await?);
        let inserter = Inserter::new(clickhouse.clone(), InserterConfig::default());

        let router = Router::new(&self.config.routing);
        let transformer = Transformer::new(
            &self.config.timestamp_dq,
            &self.config.metadata,
            &self.config.field_sanitization,
        );

        // Determine format mode from config
        let format_mode = FormatMode::from_str(&self.config.payload.format)
            .unwrap_or(FormatMode::Auto);
        let format_detector = FormatDetector::with_mode(format_mode);

        let mut buffer_manager = BufferManager::new(&self.config.buffer);

        // Message channel
        let (tx, mut rx) = mpsc::channel::<KafkaMessage>(1000);

        // Spawn consumer task
        let consumer_shutdown = self.shutdown.clone();
        let consumer_handle = tokio::spawn(async move {
            consumer.run(tx, consumer_shutdown).await
        });

        // Flush interval timer
        let mut flush_interval = interval(Duration::from_secs(self.config.buffer.flush_age_secs));

        info!(
            format_mode = ?format_mode,
            flush_rows = self.config.buffer.flush_rows,
            flush_bytes = self.config.buffer.flush_bytes,
            flush_secs = self.config.buffer.flush_age_secs,
            "Pipeline running"
        );

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("Shutdown requested, flushing remaining buffers");
                    break;
                }

                _ = flush_interval.tick() => {
                    // Check for buffers ready to flush
                    let batches = buffer_manager.get_ready_for_flush();
                    if !batches.is_empty() {
                        self.flush_batches(&inserter, batches).await;
                    }
                }

                msg = rx.recv() => {
                    match msg {
                        Some(kafka_msg) => {
                            self.stats.messages_received += 1;
                            if let Some(ref m) = self.metrics {
                                m.record_received();
                            }

                            match self.process_message(
                                &kafka_msg,
                                &format_detector,
                                &router,
                                &transformer,
                                &mut buffer_manager,
                            ) {
                                Ok(table) => {
                                    self.stats.messages_processed += 1;
                                    if let Some(ref m) = self.metrics {
                                        m.record_processed(&table);
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Message processing failed, sending to DLQ");
                                    self.stats.messages_dlq += 1;
                                    if let Some(ref m) = self.metrics {
                                        m.record_dlq();
                                    }
                                    // TODO: Send to DLQ
                                }
                            }

                            // Update buffer stats
                            if let Some(ref m) = self.metrics {
                                let (rows, bytes, tables) = buffer_manager.stats();
                                m.update_buffer_stats(rows, bytes, tables);
                            }

                            // Check for immediate flush
                            let batches = buffer_manager.get_ready_for_flush();
                            if !batches.is_empty() {
                                self.flush_batches(&inserter, batches).await;
                            }
                        }
                        None => {
                            warn!("Message channel closed");
                            break;
                        }
                    }
                }
            }
        }

        // Final flush
        let final_batches = buffer_manager.flush_all();
        if !final_batches.is_empty() {
            info!(batches = final_batches.len(), "Flushing remaining buffers");
            self.flush_batches(&inserter, final_batches).await;
        }

        // Wait for consumer to stop
        let _ = consumer_handle.await;

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
    fn process_message(
        &self,
        msg: &KafkaMessage,
        format_detector: &FormatDetector,
        router: &Router,
        transformer: &Transformer,
        buffer_manager: &mut BufferManager,
    ) -> Result<String> {
        // Step 1: Check/detect format
        let format = match format_detector.check_and_detect(&msg.payload) {
            Ok(fmt) => fmt,
            Err(_expected) => {
                return Err(crate::Error::Json("Format mismatch".into()));
            }
        };

        // Step 2: Parse payload
        let value: Value = match format {
            PayloadFormat::Json => sonic_rs::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("JSON parse error: {}", e)))?,
            PayloadFormat::MessagePack => rmp_serde::from_slice(&msg.payload)
                .map_err(|e| crate::Error::Json(format!("MessagePack parse error: {}", e)))?,
            PayloadFormat::Unknown => {
                return Err(crate::Error::Json("Unknown format".into()));
            }
        };

        // Step 3: Route to table
        let route_result = router.route(&msg.payload);
        let table = match route_result {
            RouteResult::Table(t) => t,
            RouteResult::Dlq(reason) => {
                debug!(reason = %reason, "Routing to DLQ");
                return Err(crate::Error::Json(format!("DLQ: {}", reason)));
            }
        };

        // Step 4: Transform
        let transform_result = transformer.transform(value)?;

        // Step 5: Buffer for batch insert
        buffer_manager.push(&table, transform_result.data)?;

        debug!(table = %table, "Message buffered");
        Ok(table)
    }

    /// Flush batches to ClickHouse
    async fn flush_batches(&mut self, inserter: &Inserter, batches: Vec<FlushBatch>) {
        use std::time::Instant;

        let batch_count = batches.len();
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();

        debug!(batches = batch_count, rows = total_rows, "Flushing batches");

        let start = Instant::now();
        let results = inserter.insert_batches(batches).await;
        let latency = start.elapsed().as_secs_f64();

        for result in results {
            match result {
                Ok(count) => {
                    self.stats.rows_inserted += count as u64;
                    if let Some(ref m) = self.metrics {
                        m.record_flush(count, latency);
                    }
                }
                Err(e) => {
                    error!(error = %e, "Batch insert failed");
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
}
