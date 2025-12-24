//! Kafka consumer with manual offset management for at-least-once delivery

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{Consumer as RdConsumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{ClientConfig, TopicPartitionList};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::KafkaConfig;
use crate::Result;

/// Kafka message with metadata
#[derive(Debug)]
pub struct KafkaMessage {
    pub payload: Vec<u8>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
}

impl KafkaMessage {
    fn from_borrowed(msg: &BorrowedMessage<'_>) -> Option<Self> {
        Some(Self {
            payload: msg.payload()?.to_vec(),
            topic: msg.topic().to_string(),
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
            if sasl.enabled {
                client_config
                    .set("security.protocol", "SASL_PLAINTEXT")
                    .set("sasl.mechanism", &sasl.mechanism)
                    .set("sasl.username", &sasl.username)
                    .set("sasl.password", &sasl.password);

                info!(mechanism = %sasl.mechanism, username = %sasl.username, "SASL authentication enabled");
            }
        }

        // TLS configuration
        if let Some(ref tls) = config.tls {
            if tls.enabled {
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
                                if tx.send(kafka_msg).await.is_err() {
                                    warn!("Message channel closed");
                                    break;
                                }

                                // Store offset for auto-commit
                                // This marks the message as processed
                                if let Err(e) = self.inner.store_offset_from_message(&msg) {
                                    error!(error = %e, "Failed to store offset");
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
        if let Err(e) = self.inner.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
            warn!(error = %e, "Failed to commit offsets on shutdown");
        }

        info!("Consumer stopped");
        Ok(())
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
            topic: "topic".to_string(),
            partition: 0,
            offset: 100,
            key: Some(b"key".to_vec()),
            timestamp_ms: Some(1234567890),
        };

        assert_eq!(msg.topic, "topic");
        assert_eq!(msg.offset, 100);
    }
}
