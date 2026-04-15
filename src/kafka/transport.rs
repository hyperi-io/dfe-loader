// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Transport adapters for hyperi-rustlib transport abstraction.
//!
//! This module provides thin adapter layers between the hyperi-rustlib Transport trait
//! and the local KafkaMessage/KafkaOffset types. Each adapter converts transport-specific
//! messages into the common pipeline types.
//!
//! ## Supported Transports
//!
//! - **Kafka** (always compiled): Production transport with at-least-once delivery
//! - **gRPC** (always compiled): Receives Push RPCs from dfe-receiver (server mode)
//! - **Memory** (feature `transport-memory`): In-process channels for unit tests
//!
//! ## `TransportBackend`
//!
//! The `TransportBackend` enum provides a unified interface over all compiled transports.
//! The orchestrator uses this to be transport-agnostic.

use crate::Result;
use crate::buffer::KafkaOffset;
use crate::config::KafkaConfig;
use hyperi_rustlib::transport::{
    GrpcConfig as TransportGrpcConfig, GrpcTransport, KafkaConfig as TransportKafkaConfig,
    KafkaToken, KafkaTransport, TransportBase, TransportError, TransportReceiver,
};
use tracing::{debug, trace};

use super::KafkaMessage;

/// Adapter that wraps hyperi-rustlib `KafkaTransport` for local use.
///
/// Provides the same interface as the old Consumer but uses the transport abstraction
/// underneath. This allows swapping to Memory transports for dev/test.
pub struct TransportAdapter {
    transport: KafkaTransport,
}

impl TransportAdapter {
    /// Create a new transport adapter from local `KafkaConfig`.
    ///
    /// Rustlib's `KafkaTransport::new()` handles auto-discovery when
    /// `config.topics` is empty — no app-side resolver needed.
    pub async fn new(config: &KafkaConfig) -> Result<Self> {
        let transport_config = Self::convert_config(config);

        let transport = KafkaTransport::new(&transport_config)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Transport error: {e}")))?;

        Ok(Self { transport })
    }

    /// Convert local `KafkaConfig` to hyperi-rustlib `TransportKafkaConfig`.
    pub fn convert_config(config: &KafkaConfig) -> TransportKafkaConfig {
        let mut transport_config = TransportKafkaConfig {
            brokers: config.brokers.clone(),
            group: config.group.clone(),
            client_id: config.client_id.clone(),
            topics: config.topics.clone(),
            auto_discover: config.topics.is_empty(),
            librdkafka_overrides: config.librdkafka_overrides.clone(),
            ..Default::default()
        };

        // Map topic_regex to rustlib's topic_include filter
        if let Some(ref regex) = config.topic_regex {
            transport_config.topic_include = vec![regex.clone()];
        }

        // SASL configuration
        if let Some(ref sasl) = config.sasl
            && sasl.enabled
        {
            // Set mechanism
            transport_config.sasl_mechanism =
                sasl.mechanism().as_rdkafka_mechanism().map(String::from);
            transport_config.sasl_username = Some(sasl.username.clone());
            #[allow(clippy::useless_conversion)] // String→SensitiveString via Into
            {
                transport_config.sasl_password = Some(sasl.password.expose().to_string().into());
            }

            // Set security protocol based on TLS
            if config.tls.as_ref().is_some_and(|t| t.enabled) {
                transport_config.security_protocol = "sasl_ssl".to_string();
            } else {
                transport_config.security_protocol = "sasl_plaintext".to_string();
            }
        }

        // TLS configuration
        if let Some(ref tls) = config.tls
            && tls.enabled
        {
            if transport_config.security_protocol == "plaintext" {
                transport_config.security_protocol = "ssl".to_string();
            }
            transport_config.ssl_ca_location = tls.ca_cert_file.clone();
            transport_config.ssl_certificate_location = tls.cert_file.clone();
            transport_config.ssl_key_location = tls.key_file.clone();
            transport_config.ssl_skip_verify = tls.skip_verify;
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

        let converted: Vec<KafkaMessage> = messages
            .into_iter()
            .map(|msg| {
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        topic = %msg.token.topic,
                        partition = msg.token.partition,
                        offset = msg.token.offset,
                        payload_bytes = msg.payload.len(),
                        "Message received"
                    );
                }
                KafkaMessage {
                    payload: msg.payload,
                    topic: msg.token.topic.clone(), // Arc<str> clone is cheap
                    partition: msg.token.partition,
                    offset: msg.token.offset,
                    key: None, // Transport doesn't preserve key - OK for our use case
                    timestamp_ms: msg.timestamp_ms,
                }
            })
            .collect();

        if !converted.is_empty() {
            // Collect unique topics for the batch debug log
            let mut topics: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for msg in &converted {
                topics.insert(&msg.topic);
            }
            debug!(
                count = converted.len(),
                topics = ?topics.into_iter().collect::<Vec<_>>(),
                "Kafka batch received"
            );
        }

        Ok(converted)
    }

    /// Commit offsets for processed messages.
    ///
    /// Converts local `KafkaOffset` to `KafkaToken` for commit.
    pub async fn commit(&self, offsets: &[KafkaOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let tokens: Vec<KafkaToken> = offsets
            .iter()
            .map(|off| KafkaToken::new(off.topic.clone(), off.partition, off.offset))
            .collect();

        debug!(count = tokens.len(), "Committing Kafka offsets");

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
// GrpcTransportAdapter - Receives Push RPCs from dfe-receiver
// ============================================================================

/// Adapter that wraps hyperi-rustlib `GrpcTransport` for receiving mode.
///
/// dfe-loader acts as a gRPC server: remote senders (e.g. dfe-receiver) call
/// the `Push` RPC to deliver messages. The adapter converts those into the
/// common `KafkaMessage` type used by the rest of the pipeline.
///
/// gRPC has no broker-side persistence, so commit is always a no-op.
pub struct GrpcTransportAdapter {
    transport: GrpcTransport,
    /// Fallback topic when the sender doesn't set one in gRPC metadata.
    default_topic: std::sync::Arc<str>,
}

impl GrpcTransportAdapter {
    /// Create a new gRPC transport adapter in server (receive) mode.
    ///
    /// Requires `config.listen` to be set. The adapter starts a tonic gRPC
    /// server that accepts incoming Push RPCs.
    pub async fn new(config: &crate::config::GrpcConfig) -> Result<Self> {
        let transport_config = TransportGrpcConfig {
            listen: config.listen.clone(),
            endpoint: None, // receive-only
            recv_buffer_size: config.recv_buffer_size,
            recv_timeout_ms: config.recv_timeout_ms,
            max_message_size: config.max_message_size,
            compression: config.compression,
            ..Default::default()
        };

        let transport = GrpcTransport::new(&transport_config)
            .await
            .map_err(|e| crate::Error::Kafka(format!("gRPC transport error: {e}")))?;

        Ok(Self {
            transport,
            default_topic: std::sync::Arc::from(config.default_topic.as_str()),
        })
    }

    /// Receive up to `max` messages from incoming gRPC Push RPCs.
    ///
    /// The sender sets the topic via gRPC metadata field "topic". If absent,
    /// `default_topic` is used as the routing key.
    pub async fn recv(&self, max: usize) -> Result<Vec<KafkaMessage>> {
        let messages = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("gRPC recv error: {e}")))?;

        let default = self.default_topic.clone();
        Ok(messages
            .into_iter()
            .map(|msg| KafkaMessage {
                payload: msg.payload,
                // Use sender-provided topic from metadata, or fall back to default.
                topic: msg.key.unwrap_or_else(|| default.clone()),
                partition: 0, // gRPC has no partition concept
                offset: msg.token.seq as i64,
                key: None,
                timestamp_ms: msg.timestamp_ms,
            })
            .collect())
    }

    /// Commit (no-op — gRPC ACK is the Push RPC response itself).
    pub async fn commit(&self, _offsets: &[crate::buffer::KafkaOffset]) -> Result<()> {
        Ok(())
    }

    /// Close the gRPC server.
    pub async fn close(&self) -> Result<()> {
        self.transport
            .close()
            .await
            .map_err(|e| crate::Error::Kafka(format!("gRPC close error: {e}")))
    }

    /// Check if the transport is healthy.
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Get transport name.
    pub fn name(&self) -> &'static str {
        self.transport.name()
    }
}

// ============================================================================
// MemoryTransportAdapter - For unit testing without Kafka
// ============================================================================

#[cfg(feature = "transport-memory")]
pub use memory_adapter::MemoryTransportAdapter;

#[cfg(feature = "transport-memory")]
mod memory_adapter {
    use hyperi_rustlib::transport::{
        MemoryConfig, MemoryTransport, TransportBase, TransportReceiver,
    };
    use std::sync::Arc;

    use crate::Result;
    use crate::buffer::KafkaOffset;

    use super::super::KafkaMessage;

    /// Adapter that wraps hyperi-rustlib `MemoryTransport` for local testing.
    ///
    /// Same interface as `TransportAdapter` but uses in-memory channels.
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
                ..Default::default()
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
        /// Converts to local `KafkaMessage` type for compatibility with pipeline.
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

// ============================================================================
// TransportBackend - Unified dispatch over all compiled transports
// ============================================================================

/// Unified transport backend that dispatches to the configured transport.
///
/// The orchestrator uses this instead of a concrete adapter type, making the
/// pipeline transport-agnostic at runtime.
pub enum TransportBackend {
    /// Kafka transport (production, default)
    Kafka(TransportAdapter),
    /// gRPC transport — dfe-loader acts as server receiving Push RPCs
    Grpc(GrpcTransportAdapter),
}

impl TransportBackend {
    /// Create a transport backend from the application config.
    ///
    /// Selects transport type based on `config.transport`:
    /// - `"grpc"` → gRPC server mode (receives from dfe-receiver)
    /// - anything else → Kafka (default)
    pub async fn from_config(config: &crate::config::Config) -> Result<Self> {
        if config.transport == "grpc" {
            let adapter = GrpcTransportAdapter::new(&config.grpc).await?;
            Ok(Self::Grpc(adapter))
        } else {
            let adapter = TransportAdapter::new(&config.kafka).await?;
            Ok(Self::Kafka(adapter))
        }
    }

    /// Receive up to `max` messages.
    pub async fn recv(&self, max: usize) -> Result<Vec<super::KafkaMessage>> {
        match self {
            Self::Kafka(a) => a.recv(max).await,
            Self::Grpc(a) => a.recv(max).await,
        }
    }

    /// Commit offsets (no-op for gRPC).
    pub async fn commit(&self, offsets: &[crate::buffer::KafkaOffset]) -> Result<()> {
        match self {
            Self::Kafka(a) => a.commit(offsets).await,
            Self::Grpc(a) => a.commit(offsets).await,
        }
    }

    /// Close the transport.
    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Kafka(a) => a.close().await,
            Self::Grpc(a) => a.close().await,
        }
    }

    /// Check if transport is healthy.
    pub fn is_healthy(&self) -> bool {
        match self {
            Self::Kafka(a) => a.is_healthy(),
            Self::Grpc(a) => a.is_healthy(),
        }
    }

    /// Get transport name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Kafka(a) => a.name(),
            Self::Grpc(a) => a.name(),
        }
    }
}
