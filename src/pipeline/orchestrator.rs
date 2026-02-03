//! Main pipeline coordinator
//!
//! Orchestrates the Transport → Transform → Buffer → ClickHouse pipeline.
//!
//! Uses the hs-rustlib Transport abstraction for message sources (Kafka/Zenoh/Memory).
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

use crate::buffer::{BufferManager, FlushBatch, KafkaOffset};
use crate::clickhouse::{ArrowClickHouseClient, Inserter, InserterConfig};
use crate::config::Config;
use crate::kafka::{DlqMessage, DlqProducer, KafkaMessage, TransportAdapter};
use crate::metrics::Metrics;
use crate::payload::{FormatDetector, FormatMode, PayloadFormat};
use crate::pipeline::AutoInitializer;
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

        // Run auto-initialization (creates topics, database, table if configured)
        let initializer = AutoInitializer::new(&self.config);
        initializer.run().await?;

        // Initialize transport adapter (wraps hs-rustlib KafkaTransport)
        let transport = TransportAdapter::new(&self.config.kafka).await?;
        info!(transport = transport.name(), "Transport initialized");

        // Create Arrow client for native protocol inserts and schema queries
        // Convert from dfe-loader config::ClickHouseConfig to clickhouse::ClickHouseConfig
        let ch_config: crate::clickhouse::ClickHouseConfig = (&self.config.clickhouse).into();
        let arrow_client = Arc::new(ArrowClickHouseClient::new(&ch_config).await?);

        // Inserter uses Arrow-only path
        let inserter = Inserter::new(arrow_client, InserterConfig::default());

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

        // Step 4: Transform (flatten, timestamp validation, _tags extraction, logjson capture, routing field removal)
        // Pass raw payload for logjson capture and org_id for _org_id field (Common Header v2)
        let transform_result =
            transformer.transform_with_raw(value, &msg.payload, org_id_owned.as_deref())?;

        // Step 5: Push to per-table buffer
        // Each table has its own ArrowBatchBuilder for schema uniformity.
        // The data stays as JSON Map until the batch is ready, then converts to Arrow.
        // Use with_shared_topic to share the Arc<str> without cloning the string.
        let kafka_offset = KafkaOffset::with_shared_topic(
            msg.topic.clone(), // Arc::clone is cheap (just increments refcount)
            msg.partition,
            msg.offset,
        );

        buffer_manager.push(&table, transform_result.data, Some(kafka_offset));

        debug!(table = %table, "Message buffered for Arrow batch");
        Ok(table)
    }

    /// Flush Arrow batches to ClickHouse and commit Kafka offsets via transport on success
    async fn flush_batches_transport(
        &mut self,
        inserter: &Inserter,
        transport: &TransportAdapter,
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
}
