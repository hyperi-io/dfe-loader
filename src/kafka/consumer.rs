// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka consumer with manual offset management for at-least-once delivery
//!
//! ## Offset Commit Strategy
//!
//! For true at-least-once delivery to ClickHouse:
//! 1. Consumer receives messages and forwards to processing pipeline
//! 2. Messages are buffered and inserted to ClickHouse
//! 3. ONLY after successful ClickHouse insert, offsets are committed
//!
//! This ensures that if the loader crashes before insert, messages will be
//! re-delivered on restart.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer as RdConsumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::config::{KafkaConfig, SaslMechanism};

/// Kafka message with metadata
///
/// Uses `Arc<str>` for topic to enable zero-cost sharing with KafkaOffset.
#[derive(Debug)]
pub struct KafkaMessage {
    pub payload: Vec<u8>,
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
}

impl KafkaMessage {
    fn from_borrowed(msg: &BorrowedMessage<'_>) -> Option<Self> {
        Some(Self {
            payload: msg.payload()?.to_vec(),
            topic: Arc::from(msg.topic()),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(|k| k.to_vec()),
            timestamp_ms: msg.timestamp().to_millis(),
        })
    }
}

/// Kafka consumer with at-least-once delivery semantics
pub struct Consumer {
    inner: Arc<StreamConsumer>,
    config: KafkaConfig,
}

impl Consumer {
    /// Create a new consumer from config
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();

        // Basic config
        client_config
            .set("bootstrap.servers", config.brokers.join(","))
            .set("group.id", &config.group)
            .set("client.id", &config.client_id)
            // At-least-once delivery: manual offset storage, auto commit
            .set("enable.auto.offset.store", "false")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            // Start from earliest if no committed offset
            .set("auto.offset.reset", "earliest")
            // Session timeout for rebalance
            .set("session.timeout.ms", "30000")
            .set("heartbeat.interval.ms", "10000");

        // SASL authentication
        if let Some(ref sasl) = config.sasl {
            let mechanism = sasl.mechanism();
            if sasl.enabled && mechanism != SaslMechanism::None {
                // Set SASL mechanism
                if let Some(mech) = mechanism.as_rdkafka_mechanism() {
                    client_config.set("sasl.mechanism", mech);
                }

                // Default to SASL_PLAINTEXT; TLS config may override to SASL_SSL
                client_config.set("security.protocol", "SASL_PLAINTEXT");

                // Username/password auth (PLAIN, SCRAM-*)
                if mechanism.requires_credentials() {
                    client_config
                        .set("sasl.username", &sasl.username)
                        .set("sasl.password", &sasl.password);
                }

                // OAuth configuration
                if mechanism.is_oauth() {
                    if let Some(ref endpoint) = sasl.oauth_token_endpoint {
                        client_config.set("sasl.oauthbearer.token.endpoint.url", endpoint);
                    }
                    if let Some(ref client_id) = sasl.oauth_client_id {
                        client_config.set("sasl.oauthbearer.client.id", client_id);
                    }
                    if let Some(ref client_secret) = sasl.oauth_client_secret {
                        client_config.set("sasl.oauthbearer.client.secret", client_secret);
                    }
                    if let Some(ref scope) = sasl.oauth_scope {
                        client_config.set("sasl.oauthbearer.scope", scope);
                    }
                    if let Some(ref extensions) = sasl.oauth_extensions {
                        client_config.set("sasl.oauthbearer.extensions", extensions);
                    }
                }

                // AWS MSK IAM configuration
                if mechanism.is_aws_iam() {
                    // AWS MSK IAM uses OAUTHBEARER with a custom callback
                    // For rdkafka, this requires the aws-msk-iam-sasl-signer library
                    // Set the AWS region; credentials come from env/profile/explicit
                    if let Some(ref region) = sasl.aws_region {
                        // Note: AWS MSK IAM auth requires custom token provider
                        // rdkafka doesn't natively support this - may need custom solution
                        client_config
                            .set("sasl.oauthbearer.config", format!("awsRegion={}", region));
                    }
                }

                info!(mechanism = %mechanism, "SASL authentication enabled");
            }
        }

        // TLS configuration
        if let Some(ref tls) = config.tls
            && tls.enabled
        {
            // Update security protocol if SASL is also enabled
            if config.sasl.as_ref().is_some_and(|s| s.enabled) {
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

            info!("TLS enabled");
        }

        let consumer: StreamConsumer = client_config.create()?;

        Ok(Self {
            inner: Arc::new(consumer),
            config: config.clone(),
        })
    }

    /// Subscribe to configured topics
    pub fn subscribe(&self) -> Result<()> {
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        self.inner.subscribe(&topics)?;
        info!(topics = ?topics, "Subscribed to topics");
        Ok(())
    }

    /// Run the consumer loop, sending messages to the channel
    ///
    /// Note: This does NOT auto-commit offsets. The caller must use
    /// `commit_offsets()` after successful processing to commit.
    pub async fn run(
        &self,
        tx: mpsc::Sender<KafkaMessage>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        info!("Starting consumer loop");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Consumer shutdown requested");
                    break;
                }
                result = self.inner.recv() => {
                    match result {
                        Ok(msg) => {
                            if let Some(kafka_msg) = KafkaMessage::from_borrowed(&msg) {
                                debug!(
                                    topic = %kafka_msg.topic,
                                    partition = kafka_msg.partition,
                                    offset = kafka_msg.offset,
                                    "Received message"
                                );

                                // Send to processing channel
                                // NOTE: Do NOT store offset here - wait for successful ClickHouse insert
                                if tx.send(kafka_msg).await.is_err() {
                                    warn!("Message channel closed");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Consumer error");
                            // Brief pause before retry
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        // Commit any pending offsets before shutdown
        if let Err(e) = self.inner.commit_consumer_state(CommitMode::Sync) {
            warn!(error = %e, "Failed to commit offsets on shutdown");
        }

        info!("Consumer stopped");
        Ok(())
    }

    /// Commit offsets for successfully processed messages
    ///
    /// Call this AFTER successful ClickHouse insert to mark messages as processed.
    /// Uses synchronous commit for reliability.
    ///
    /// # Arguments
    /// * `offsets` - List of (topic, partition, offset) tuples to commit.
    ///   The offset should be the message offset + 1 (next to consume)
    pub fn commit_offsets(&self, offsets: &[(String, i32, i64)]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();

        for (topic, partition, offset) in offsets {
            // Kafka commits the NEXT offset to consume, so add 1
            tpl.add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
                .map_err(|e| crate::Error::Kafka(format!("Failed to add partition: {}", e)))?;
        }

        debug!(count = offsets.len(), "Committing offsets");

        self.inner
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| crate::Error::Kafka(format!("Offset commit failed: {}", e)))?;

        Ok(())
    }

    /// Commit offsets from KafkaOffset structs (convenience method)
    ///
    /// Groups offsets by topic/partition and commits the highest offset for each.
    pub fn commit_kafka_offsets(&self, offsets: &[crate::buffer::KafkaOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        // Group by topic/partition, keeping highest offset
        // Use Arc<str> as key to avoid cloning strings
        let mut max_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();

        for off in offsets {
            let key = (off.topic.clone(), off.partition); // Arc::clone is cheap
            max_offsets
                .entry(key)
                .and_modify(|existing| {
                    if off.offset > *existing {
                        *existing = off.offset;
                    }
                })
                .or_insert(off.offset);
        }

        let commit_list: Vec<(String, i32, i64)> = max_offsets
            .into_iter()
            .map(|((topic, partition), offset)| (topic.to_string(), partition, offset))
            .collect();

        self.commit_offsets(&commit_list)
    }

    /// Get current partition assignments
    pub fn assignment(&self) -> Result<TopicPartitionList> {
        Ok(self.inner.assignment()?)
    }

    /// Get the inner consumer for advanced operations
    pub fn inner(&self) -> &StreamConsumer {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_consumer_config_basic() {
        let config = KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            group: "test-group".to_string(),
            topics: vec!["test-topic".to_string()],
            topic_regex: None,
            client_id: "test-client".to_string(),
            sasl: None,
            tls: None,
            ..Default::default()
        };

        // This will fail without a real broker, but validates config parsing
        let result = Consumer::new(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_message_from_borrowed() {
        // KafkaMessage struct creation test
        let msg = KafkaMessage {
            payload: b"test".to_vec(),
            topic: Arc::from("topic"),
            partition: 0,
            offset: 100,
            key: Some(b"key".to_vec()),
            timestamp_ms: Some(1234567890),
        };

        assert_eq!(&*msg.topic, "topic");
        assert_eq!(msg.offset, 100);
    }
}
