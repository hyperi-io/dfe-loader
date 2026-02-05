// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Transport adapter for hs-rustlib transport abstraction.
//!
//! This module provides a thin adapter layer between the hs-rustlib Transport trait
//! and the local KafkaMessage/KafkaOffset types. The adapter is designed for
//! zero-overhead conversion in the hot path.
//!
//! ## Performance Strategy
//!
//! - Uses hs-rustlib `KafkaTransport` for receive/commit operations
//! - Converts `Message<KafkaToken>` → local `KafkaMessage` with Arc<str> sharing
//! - Local sonic-rs parsing is preserved (NOT using hs-rustlib payload utilities)
//! - KafkaToken → KafkaOffset is a simple field copy (same structure)

use hs_rustlib::transport::{
    KafkaConfig as TransportKafkaConfig, KafkaToken, KafkaTransport, Transport, TransportError,
};

use crate::buffer::KafkaOffset;
use crate::config::KafkaConfig;
use crate::Result;

use super::KafkaMessage;

/// Adapter that wraps hs-rustlib KafkaTransport for local use.
///
/// Provides the same interface as the old Consumer but uses the transport abstraction
/// underneath. This allows swapping to Zenoh or Memory transports for dev/test.
pub struct TransportAdapter {
    transport: KafkaTransport,
}

impl TransportAdapter {
    /// Create a new transport adapter from local KafkaConfig.
    ///
    /// Converts local config to hs-rustlib TransportKafkaConfig.
    pub async fn new(config: &KafkaConfig) -> Result<Self> {
        let transport_config = Self::convert_config(config);
        let transport = KafkaTransport::new(&transport_config)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Transport error: {e}")))?;

        Ok(Self { transport })
    }

    /// Convert local KafkaConfig to hs-rustlib TransportKafkaConfig.
    fn convert_config(config: &KafkaConfig) -> TransportKafkaConfig {
        let mut transport_config = TransportKafkaConfig {
            brokers: config.brokers.clone(),
            group: config.group.clone(),
            client_id: config.client_id.clone(),
            topics: config.topics.clone(),
            ..Default::default()
        };

        // SASL configuration
        if let Some(ref sasl) = config.sasl {
            if sasl.enabled {
                // Set mechanism
                transport_config.sasl_mechanism =
                    sasl.mechanism().as_rdkafka_mechanism().map(String::from);
                transport_config.sasl_username = Some(sasl.username.clone());
                transport_config.sasl_password = Some(sasl.password.clone());

                // Set security protocol based on TLS
                if config.tls.as_ref().is_some_and(|t| t.enabled) {
                    transport_config.security_protocol = "sasl_ssl".to_string();
                } else {
                    transport_config.security_protocol = "sasl_plaintext".to_string();
                }
            }
        }

        // TLS configuration
        if let Some(ref tls) = config.tls {
            if tls.enabled {
                if transport_config.security_protocol == "plaintext" {
                    transport_config.security_protocol = "ssl".to_string();
                }
                transport_config.ssl_ca_location = tls.ca_cert_file.clone();
                transport_config.ssl_certificate_location = tls.cert_file.clone();
                transport_config.ssl_key_location = tls.key_file.clone();
                transport_config.ssl_skip_verify = tls.skip_verify;
            }
        }

        transport_config
    }

    /// Receive up to `max` messages from the transport.
    ///
    /// Converts transport `Message<KafkaToken>` to local `KafkaMessage`.
    /// This is the hot path - Arc<str> is shared, payload is moved (no copy).
    pub async fn recv(&self, max: usize) -> Result<Vec<KafkaMessage>> {
        let messages = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Recv error: {e}")))?;

        Ok(messages
            .into_iter()
            .map(|msg| KafkaMessage {
                payload: msg.payload,
                topic: msg.token.topic.clone(), // Arc<str> clone is cheap
                partition: msg.token.partition,
                offset: msg.token.offset,
                key: None, // Transport doesn't preserve key - OK for our use case
                timestamp_ms: msg.timestamp_ms,
            })
            .collect())
    }

    /// Commit offsets for processed messages.
    ///
    /// Converts local KafkaOffset to KafkaToken for commit.
    pub async fn commit(&self, offsets: &[KafkaOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let tokens: Vec<KafkaToken> = offsets
            .iter()
            .map(|off| KafkaToken::new(off.topic.clone(), off.partition, off.offset))
            .collect();

        self.transport
            .commit(&tokens)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Commit error: {e}")))?;

        Ok(())
    }

    /// Close the transport.
    pub async fn close(&self) -> Result<()> {
        self.transport
            .close()
            .await
            .map_err(|e| crate::Error::Kafka(format!("Close error: {e}")))?;
        Ok(())
    }

    /// Check if transport is healthy.
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Get transport name.
    pub fn name(&self) -> &'static str {
        self.transport.name()
    }
}

/// Convert transport error to local error.
impl From<TransportError> for crate::Error {
    fn from(e: TransportError) -> Self {
        crate::Error::Kafka(e.to_string())
    }
}

// ============================================================================
// MemoryTransportAdapter - For unit testing without Kafka
// ============================================================================

#[cfg(feature = "transport-memory")]
pub use memory_adapter::MemoryTransportAdapter;

#[cfg(feature = "transport-memory")]
mod memory_adapter {
    use hs_rustlib::transport::{MemoryConfig, MemoryTransport, Transport};
    use std::sync::Arc;

    use crate::buffer::KafkaOffset;
    use crate::Result;

    use super::super::KafkaMessage;

    /// Adapter that wraps hs-rustlib MemoryTransport for local testing.
    ///
    /// Same interface as TransportAdapter but uses in-memory channels.
    /// Perfect for unit tests - no Kafka required.
    pub struct MemoryTransportAdapter {
        transport: Arc<MemoryTransport>,
        topic: Arc<str>,
    }

    impl MemoryTransportAdapter {
        /// Create a new memory transport adapter.
        #[must_use]
        pub fn new(topic: &str) -> Self {
            let config = MemoryConfig {
                buffer_size: 10_000,
                recv_timeout_ms: 100, // 100ms timeout for tests
            };
            Self {
                transport: Arc::new(MemoryTransport::new(&config)),
                topic: Arc::from(topic),
            }
        }

        /// Create with custom config.
        #[must_use]
        pub fn with_config(topic: &str, config: &MemoryConfig) -> Self {
            Self {
                transport: Arc::new(MemoryTransport::new(config)),
                topic: Arc::from(topic),
            }
        }

        /// Inject a message for testing.
        ///
        /// This is the main way to simulate incoming messages in tests.
        pub async fn inject(&self, payload: Vec<u8>) -> Result<()> {
            self.transport
                .inject(None, payload)
                .await
                .map_err(|e| crate::Error::Kafka(format!("Inject error: {e}")))
        }

        /// Inject a message with a key.
        pub async fn inject_with_key(&self, key: &str, payload: Vec<u8>) -> Result<()> {
            self.transport
                .inject(Some(key), payload)
                .await
                .map_err(|e| crate::Error::Kafka(format!("Inject error: {e}")))
        }

        /// Receive up to `max` messages.
        ///
        /// Converts to local KafkaMessage type for compatibility with pipeline.
        pub async fn recv(&self, max: usize) -> Result<Vec<KafkaMessage>> {
            let messages = self
                .transport
                .recv(max)
                .await
                .map_err(|e| crate::Error::Kafka(format!("Recv error: {e}")))?;

            Ok(messages
                .into_iter()
                .map(|msg| KafkaMessage {
                    payload: msg.payload,
                    topic: self.topic.clone(), // All messages use the configured topic
                    partition: 0,              // Memory transport doesn't have partitions
                    offset: msg.token.seq as i64,
                    key: msg.key.map(|k| k.to_string().into_bytes()),
                    timestamp_ms: msg.timestamp_ms,
                })
                .collect())
        }

        /// Commit offsets (no-op for memory, but tracks for verification).
        pub async fn commit(&self, _offsets: &[KafkaOffset]) -> Result<()> {
            // Memory transport tracks commits internally
            // We could call transport.commit() but it's a no-op
            Ok(())
        }

        /// Close the transport.
        pub async fn close(&self) -> Result<()> {
            self.transport
                .close()
                .await
                .map_err(|e| crate::Error::Kafka(format!("Close error: {e}")))
        }

        /// Check if transport is healthy.
        pub fn is_healthy(&self) -> bool {
            self.transport.is_healthy()
        }

        /// Get transport name.
        pub fn name(&self) -> &'static str {
            self.transport.name()
        }

        /// Get the underlying transport for advanced testing.
        pub fn inner(&self) -> &MemoryTransport {
            &self.transport
        }
    }
}
