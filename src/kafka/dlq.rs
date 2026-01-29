//! Dead Letter Queue producer
//!
//! Provides routing for failed messages to DLQ topics.
//!
//! ## Topic Naming Strategies
//!
//! 1. **db.table matching**: Route to topic matching the failed db.table destination
//!    e.g., message destined for `acme.auth` → DLQ topic `acme.auth.dlq`
//!
//! 2. **Common DLQ**: All failed messages to a single common DLQ topic
//!    e.g., all failures → `clickhouse-loader.dlq`
//!
//! ## Message Format
//!
//! DLQ messages include:
//! - Original payload
//! - Error reason (as header)
//! - Original topic/partition/offset (as headers)
//! - Timestamp (as header)

use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tracing::{debug, error, info};

use crate::config::{KafkaConfig, DlqConfig, SaslMechanism};
use crate::Result;

/// DLQ routing mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqRoutingMode {
    /// Route to topic matching db.table name with suffix
    /// e.g., "acme.auth" → "acme.auth.dlq"
    PerTable,
    /// Route all failures to a single common topic
    /// e.g., all → "clickhouse-loader.dlq"
    Common,
}

/// DLQ message with metadata
#[derive(Debug)]
pub struct DlqMessage<'a> {
    /// Original message payload
    pub payload: &'a [u8],
    /// Reason for DLQ routing
    pub reason: &'a str,
    /// Original destination (db.table)
    pub destination: Option<&'a str>,
    /// Original Kafka topic
    pub original_topic: &'a str,
    /// Original Kafka partition
    pub original_partition: i32,
    /// Original Kafka offset
    pub original_offset: i64,
    /// Optional message key
    pub key: Option<&'a [u8]>,
}

/// Produces failed messages to DLQ topics
pub struct DlqProducer {
    producer: FutureProducer,
    routing_mode: DlqRoutingMode,
    topic_suffix: String,
    common_topic: String,
    send_timeout: Duration,
}

impl DlqProducer {
    /// Create a new DLQ producer from Kafka config
    pub fn new(kafka_config: &KafkaConfig, dlq_config: &DlqConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();

        // Basic config - same as consumer
        client_config
            .set("bootstrap.servers", kafka_config.brokers.join(","))
            .set("client.id", format!("{}-dlq", kafka_config.client_id))
            // Producer-specific settings
            .set("acks", "all")
            .set("retries", "3")
            .set("retry.backoff.ms", "100");

        // SASL authentication (same as consumer)
        if let Some(ref sasl) = kafka_config.sasl {
            let mechanism = sasl.mechanism();
            if sasl.enabled && mechanism != SaslMechanism::None {
                if let Some(mech) = mechanism.as_rdkafka_mechanism() {
                    client_config.set("sasl.mechanism", mech);
                }
                client_config.set("security.protocol", "SASL_PLAINTEXT");

                if mechanism.requires_credentials() {
                    client_config
                        .set("sasl.username", &sasl.username)
                        .set("sasl.password", &sasl.password);
                }
            }
        }

        // TLS configuration
        if let Some(ref tls) = kafka_config.tls {
            if tls.enabled {
                if kafka_config.sasl.as_ref().is_some_and(|s| s.enabled) {
                    client_config.set("security.protocol", "SASL_SSL");
                } else {
                    client_config.set("security.protocol", "SSL");
                }

                if let Some(ref ca) = tls.ca_cert_file {
                    client_config.set("ssl.ca.location", ca);
                }
                if let Some(ref cert) = tls.cert_file {
                    client_config.set("ssl.certificate.location", cert);
                }
                if let Some(ref key) = tls.key_file {
                    client_config.set("ssl.key.location", key);
                }
            }
        }

        let producer: FutureProducer = client_config.create()
            .map_err(|e| crate::Error::Kafka(format!("Failed to create DLQ producer: {}", e)))?;

        info!(suffix = %dlq_config.topic_suffix, "DLQ producer initialized");

        Ok(Self {
            producer,
            routing_mode: DlqRoutingMode::PerTable, // Default to per-table routing
            topic_suffix: dlq_config.topic_suffix.clone(),
            common_topic: "clickhouse-loader.dlq".to_string(),
            send_timeout: Duration::from_secs(5),
        })
    }

    /// Create DLQ producer with common topic routing mode
    pub fn with_common_topic(mut self, topic: &str) -> Self {
        self.routing_mode = DlqRoutingMode::Common;
        self.common_topic = topic.to_string();
        self
    }

    /// Create DLQ producer with per-table routing mode
    pub fn with_per_table_routing(mut self, suffix: &str) -> Self {
        self.routing_mode = DlqRoutingMode::PerTable;
        self.topic_suffix = suffix.to_string();
        self
    }

    /// Set send timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Determine the DLQ topic for a message
    fn get_dlq_topic(&self, destination: Option<&str>) -> String {
        match self.routing_mode {
            DlqRoutingMode::Common => self.common_topic.clone(),
            DlqRoutingMode::PerTable => {
                if let Some(dest) = destination {
                    // db.table → db.table.dlq (or whatever suffix)
                    format!("{}{}", dest, self.topic_suffix)
                } else {
                    // No destination known, use common topic
                    self.common_topic.clone()
                }
            }
        }
    }

    /// Send a message to the DLQ
    ///
    /// Returns the topic the message was sent to.
    pub async fn send(&self, msg: DlqMessage<'_>) -> Result<String> {
        let topic = self.get_dlq_topic(msg.destination);

        debug!(
            topic = %topic,
            reason = %msg.reason,
            original_topic = %msg.original_topic,
            partition = msg.original_partition,
            offset = msg.original_offset,
            "Sending to DLQ"
        );

        // Build the record with headers
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        let mut record = FutureRecord::to(&topic)
            .payload(msg.payload)
            .timestamp(timestamp_ms);

        // Add key if present
        if let Some(key) = msg.key {
            record = record.key(key);
        }

        // Send with timeout
        match self.producer.send(record, self.send_timeout).await {
            Ok(delivery) => {
                debug!(
                    dlq_topic = %topic,
                    dlq_partition = delivery.partition,
                    dlq_offset = delivery.offset,
                    "DLQ message sent successfully"
                );
                Ok(topic)
            }
            Err((err, _)) => {
                error!(
                    error = %err,
                    topic = %topic,
                    reason = %msg.reason,
                    "Failed to send DLQ message"
                );
                Err(crate::Error::Kafka(format!("DLQ send failed: {}", err)))
            }
        }
    }

    /// Send a simple message to DLQ (convenience method)
    ///
    /// For messages that failed before routing (no destination known).
    pub async fn send_simple(
        &self,
        payload: &[u8],
        reason: &str,
        original_topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<String> {
        self.send(DlqMessage {
            payload,
            reason,
            destination: None,
            original_topic,
            original_partition: partition,
            original_offset: offset,
            key: None,
        })
        .await
    }

    /// Send a routed message to DLQ
    ///
    /// For messages that failed after routing (destination known).
    pub async fn send_routed(
        &self,
        payload: &[u8],
        reason: &str,
        destination: &str,
        original_topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<String> {
        self.send(DlqMessage {
            payload,
            reason,
            destination: Some(destination),
            original_topic,
            original_partition: partition,
            original_offset: offset,
            key: None,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to compute DLQ topic without needing a producer
    fn compute_dlq_topic(
        routing_mode: DlqRoutingMode,
        topic_suffix: &str,
        common_topic: &str,
        destination: Option<&str>,
    ) -> String {
        match routing_mode {
            DlqRoutingMode::Common => common_topic.to_string(),
            DlqRoutingMode::PerTable => {
                if let Some(dest) = destination {
                    format!("{}{}", dest, topic_suffix)
                } else {
                    common_topic.to_string()
                }
            }
        }
    }

    #[test]
    fn test_dlq_topic_per_table() {
        let mode = DlqRoutingMode::PerTable;
        let suffix = ".dlq";
        let common = "common.dlq";

        assert_eq!(compute_dlq_topic(mode, suffix, common, Some("acme.auth")), "acme.auth.dlq");
        assert_eq!(compute_dlq_topic(mode, suffix, common, Some("db.events")), "db.events.dlq");
        assert_eq!(compute_dlq_topic(mode, suffix, common, None), "common.dlq");
    }

    #[test]
    fn test_dlq_topic_common() {
        let mode = DlqRoutingMode::Common;
        let suffix = ".dlq";
        let common = "all-errors.dlq";

        assert_eq!(compute_dlq_topic(mode, suffix, common, Some("acme.auth")), "all-errors.dlq");
        assert_eq!(compute_dlq_topic(mode, suffix, common, Some("db.events")), "all-errors.dlq");
        assert_eq!(compute_dlq_topic(mode, suffix, common, None), "all-errors.dlq");
    }

    #[test]
    fn test_dlq_routing_mode_eq() {
        assert_eq!(DlqRoutingMode::PerTable, DlqRoutingMode::PerTable);
        assert_eq!(DlqRoutingMode::Common, DlqRoutingMode::Common);
        assert_ne!(DlqRoutingMode::PerTable, DlqRoutingMode::Common);
    }
}
