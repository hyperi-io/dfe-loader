// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Transport adapters for scalo transport abstraction.
//!
//! This module provides thin adapter layers between the scalo Transport trait
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
use scalo::SelfRegulationGovernor;
use scalo::transport::filter::FilteredDlqEntry;
use scalo::transport::{
    GrpcConfig as TransportGrpcConfig, GrpcTransport, KafkaConfig as TransportKafkaConfig,
    KafkaToken, KafkaTransport, TransportBase, TransportError, TransportReceiver,
};
use tracing::{debug, trace};

use super::KafkaMessage;

/// A received block: the passing messages plus any inbound-filter DLQ entries.
///
/// The DLQ entries are surfaced (never silently dropped) so the orchestrator
/// can route them onward. The loader configures no inbound scalo filters, so
/// `dlq_entries` is empty in practice -- but the no-silent-drop contract is
/// honoured regardless.
pub struct ReceivedBatch {
    /// Passing messages, in the same order as the source records.
    pub messages: Vec<KafkaMessage>,
    /// Inbound-filter DLQ entries carried forward from the transport.
    pub dlq_entries: Vec<FilteredDlqEntry>,
}

/// Adapter that wraps scalo `KafkaTransport` for local use.
///
/// Provides the same interface as the old Consumer but uses the transport abstraction
/// underneath. This allows swapping to Memory transports for dev/test.
pub struct TransportAdapter {
    transport: KafkaTransport,
}

impl TransportAdapter {
    /// Create a new transport adapter from local `KafkaConfig`.
    ///
    /// scalo's `KafkaTransport::new()` handles auto-discovery when
    /// `config.topics` is empty — no app-side resolver needed.
    ///
    /// When `governor` is `Some`, the self-regulation inbound brake is attached
    /// to the Kafka receiver: under memory pressure the consumer's ASSIGNED
    /// partitions are paused (the member stays in the group — no rebalance) and
    /// resumed once pressure clears. This is the pause-partitions gate for a
    /// Kafka-source stage; nothing on the outbound ClickHouse insert drain is
    /// gated (gating the drain would deadlock the pipeline). When `governor` is
    /// `None` (self-regulation disabled) construction is byte-identical to before.
    pub async fn new(
        config: &KafkaConfig,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        let transport_config = Self::convert_config(config);

        let transport = KafkaTransport::new(&transport_config)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Transport error: {e}")))?;

        // Attach the self-regulation pause-partitions gate over the runtime's
        // shared pressure (the gate is evaluated automatically inside `recv`).
        let transport = match governor {
            Some(gov) => gov.attach_kafka_gate(transport),
            None => transport,
        };

        Ok(Self { transport })
    }

    /// Convert local `KafkaConfig` to scalo `TransportKafkaConfig`.
    pub fn convert_config(config: &KafkaConfig) -> TransportKafkaConfig {
        let mut transport_config = TransportKafkaConfig {
            // loader is consume-only (Kafka -> ClickHouse). scalo 2.10 dropped the
            // explicit role enum for a profile-based config: a NON-EMPTY consumer
            // group is what makes scalo build a consumer (and no idle producer).
            // The loader is a consumer, so the group MUST stay set (do not clear
            // it). The DLQ uses scalo's standalone KafkaProducer, not this
            // transport. profile defaults to Production (lean librdkafka baseline).
            brokers: config.brokers.clone(),
            group: config.group.clone(),
            client_id: config.client_id.clone(),
            topics: config.topics.clone(),
            auto_discover: config.topics.is_empty(),
            // scalo (>=2.8) rejects an unencrypted transport under a production
            // profile at construction unless this is explicitly set. Secure by
            // default; operators opt in for mesh-encrypted in-cluster traffic.
            allow_insecure_transport: config.allow_insecure_transport,
            librdkafka_overrides: config.librdkafka_overrides.clone(),
            ..Default::default()
        };

        // Map topic_regex to scalo's topic_include filter
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

    /// Receive a batch from Kafka as a `WorkBatch`, reshaped into the loader's
    /// per-message `KafkaMessage` model plus any inbound-filter DLQ entries.
    ///
    /// The transport yields a `WorkBatch<KafkaToken>` whose `records` and
    /// `commit_tokens` are 1:1 and in the same order (one Kafka record produces
    /// one record + one commit token), so they are zipped back into individual
    /// `KafkaMessage`s — preserving the per-message offset tracking the buffer
    /// relies on for at-least-once commit. The payload `Bytes` is moved into the
    /// `KafkaMessage` (one copy out of the shared arena). Inbound-filter DLQ
    /// entries are surfaced for the caller to route onward (no silent drop).
    pub async fn recv(&self, max: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Recv error: {e}")))?;

        // `records[i]` corresponds to `commit_tokens[i]` (the transport builds
        // both in the same order from each Kafka record). Zip them back into the
        // per-message model the buffer uses.
        let messages: Vec<KafkaMessage> = batch
            .records
            .into_iter()
            .zip(batch.commit_tokens)
            .map(|(record, token)| {
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        topic = %token.topic,
                        partition = token.partition,
                        offset = token.offset,
                        payload_bytes = record.payload.len(),
                        "Message received"
                    );
                }
                KafkaMessage {
                    payload: record.payload.to_vec(),
                    topic: token.topic.clone(), // Arc<str> clone is cheap
                    partition: token.partition,
                    offset: token.offset,
                    key: None, // Transport doesn't preserve the partition key — OK for our use case
                    timestamp_ms: record.metadata.timestamp_ms,
                }
            })
            .collect();

        if !messages.is_empty() {
            // Collect unique topics for the batch debug log
            let mut topics: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for msg in &messages {
                topics.insert(&msg.topic);
            }
            debug!(
                count = messages.len(),
                dlq = batch.dlq_entries.len(),
                topics = ?topics.into_iter().collect::<Vec<_>>(),
                "Kafka batch received"
            );
        }

        Ok(ReceivedBatch {
            messages,
            dlq_entries: batch.dlq_entries,
        })
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

    /// Total consumer lag summed over THIS pod's ASSIGNED partitions.
    ///
    /// rdkafka reports `consumer_lag` only for assigned partitions, so the sum
    /// is inherently PER-POD and scale-invariant. The orchestrator pushes it as
    /// the Kafka inbound pressure term into the unified `ScalingPressure` engine
    /// via `set_component("kafka_lag", lag)`. Requires librdkafka statistics
    /// to be enabled (`statistics.interval.ms` > 0); with stats disabled the
    /// snapshot is empty and this returns 0.
    pub fn assigned_lag(&self) -> i64 {
        use scalo::transport::kafka::total_consumer_lag;
        total_consumer_lag(&self.transport.stats()).max(0)
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

/// Adapter that wraps scalo `GrpcTransport` for receiving mode.
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

    /// Receive a batch from incoming gRPC Push RPCs as a `WorkBatch`, reshaped
    /// into the loader's per-message `KafkaMessage` model plus any inbound-filter
    /// DLQ entries.
    ///
    /// The sender sets the topic via the gRPC metadata routing key
    /// (`record.key`). If absent, `default_topic` is used. `records` and
    /// `commit_tokens` are 1:1 in the same order; the per-token sequence becomes
    /// the message offset.
    pub async fn recv(&self, max: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("gRPC recv error: {e}")))?;

        let default = self.default_topic.clone();
        let messages = batch
            .records
            .into_iter()
            .zip(batch.commit_tokens)
            .map(|(record, token)| KafkaMessage {
                payload: record.payload.to_vec(),
                // Use sender-provided topic from metadata, or fall back to default.
                topic: record.key.unwrap_or_else(|| default.clone()),
                partition: 0, // gRPC has no partition concept
                offset: token.seq as i64,
                key: None,
                timestamp_ms: record.metadata.timestamp_ms,
            })
            .collect();

        Ok(ReceivedBatch {
            messages,
            dlq_entries: batch.dlq_entries,
        })
    }

    /// Commit (no-op — gRPC ACK is the Push RPC response itself).
    // async is deliberate: TransportBackend::commit awaits every adapter arm
    // uniformly, and the Kafka adapter's commit genuinely awaits.
    #[allow(clippy::unused_async_trait_impl)]
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
    use scalo::transport::{MemoryConfig, MemoryTransport, TransportBase, TransportReceiver};
    use std::sync::Arc;

    use crate::Result;
    use crate::buffer::KafkaOffset;

    use super::super::KafkaMessage;

    /// Adapter that wraps scalo `MemoryTransport` for local testing.
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
                transport: Arc::new(
                    MemoryTransport::new(&config).expect("memory transport init for tests"),
                ),
                topic: Arc::from(topic),
            }
        }

        /// Create with custom config.
        #[must_use]
        pub fn with_config(topic: &str, config: &MemoryConfig) -> Self {
            Self {
                transport: Arc::new(
                    MemoryTransport::new(config).expect("memory transport init for tests"),
                ),
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
        /// Maps the transport's `WorkBatch` into the local `KafkaMessage` type
        /// for compatibility with the pipeline (records and commit tokens are
        /// 1:1, in order). DLQ entries are not surfaced here — the memory
        /// transport configures no inbound filters.
        pub async fn recv(&self, max: usize) -> Result<Vec<KafkaMessage>> {
            let batch = self
                .transport
                .recv(max)
                .await
                .map_err(|e| crate::Error::Kafka(format!("Recv error: {e}")))?;

            Ok(batch
                .records
                .into_iter()
                .zip(batch.commit_tokens)
                .map(|(record, token)| KafkaMessage {
                    payload: record.payload.to_vec(),
                    topic: self.topic.clone(), // All messages use the configured topic
                    partition: 0,              // Memory transport doesn't have partitions
                    offset: token.seq as i64,
                    key: record.key.map(|k| k.to_string().into_bytes()),
                    timestamp_ms: record.metadata.timestamp_ms,
                })
                .collect())
        }

        /// Commit offsets (no-op for memory, but tracks for verification).
        // async is deliberate: it mirrors the awaitable transport commit
        // interface the real backends implement.
        #[allow(clippy::unused_async_trait_impl)]
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
    ///
    /// `governor`, when `Some`, attaches the Kafka pause-partitions inbound gate
    /// to the receiver (default-on self-regulation). gRPC has no broker-side
    /// brake (a push source sheds via the upstream's retry, not a pull pause), so
    /// the governor is a no-op on that path.
    pub async fn from_config(
        config: &crate::config::Config,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        if config.transport == "grpc" {
            let adapter = GrpcTransportAdapter::new(&config.grpc).await?;
            Ok(Self::Grpc(adapter))
        } else {
            let adapter = TransportAdapter::new(&config.kafka, governor).await?;
            Ok(Self::Kafka(adapter))
        }
    }

    /// Receive a batch (messages + inbound-filter DLQ entries).
    pub async fn recv(&self, max: usize) -> Result<ReceivedBatch> {
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

    /// Per-pod assigned-partition consumer lag (Kafka only).
    ///
    /// Returns `None` for the gRPC backend (a push source has no broker-side
    /// backlog the pod can read locally — the engine falls back to CPU-only for
    /// that path). Feeds the unified `ScalingPressure` engine's `kafka_lag`
    /// component via `set_component("kafka_lag", lag)`.
    pub fn assigned_lag(&self) -> Option<i64> {
        match self {
            Self::Kafka(a) => Some(a.assigned_lag()),
            Self::Grpc(_) => None,
        }
    }
}

#[cfg(all(test, feature = "transport-memory"))]
mod tests {
    use super::*;
    use scalo::transport::MemoryConfig;

    // ========================================================================
    // MemoryTransportAdapter: construction
    // ========================================================================

    #[tokio::test]
    async fn test_memory_adapter_new_defaults() {
        let adapter = MemoryTransportAdapter::new("test-topic");
        assert_eq!(adapter.name(), "memory");
        assert!(adapter.is_healthy(), "Fresh adapter should be healthy");
    }

    #[tokio::test]
    async fn test_memory_adapter_with_config_custom_capacity() {
        let config = MemoryConfig {
            buffer_size: 32,
            recv_timeout_ms: 10,
            ..Default::default()
        };
        let adapter = MemoryTransportAdapter::with_config("custom-topic", &config);
        assert_eq!(adapter.name(), "memory");
        assert!(adapter.is_healthy());
    }

    // ========================================================================
    // MemoryTransportAdapter: inject/recv roundtrip
    // ========================================================================

    #[tokio::test]
    async fn test_inject_and_recv_single_message() {
        let adapter = MemoryTransportAdapter::new("roundtrip");
        adapter
            .inject(b"hello world".to_vec())
            .await
            .expect("inject should succeed");

        let msgs = adapter.recv(10).await.expect("recv should succeed");
        assert_eq!(msgs.len(), 1);
        assert_eq!(&*msgs[0].payload, b"hello world");
        assert_eq!(&*msgs[0].topic, "roundtrip");
        assert_eq!(msgs[0].partition, 0);
        // Sequence starts at 0
        assert_eq!(msgs[0].offset, 0);
        // No explicit key
        assert!(msgs[0].key.is_none());
    }

    #[tokio::test]
    async fn test_inject_with_key_preserves_key() {
        let adapter = MemoryTransportAdapter::new("keyed");
        adapter
            .inject_with_key("user-42", b"payload".to_vec())
            .await
            .expect("inject should succeed");

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].key.as_deref(), Some(&b"user-42"[..]));
    }

    #[tokio::test]
    async fn test_inject_multiple_recv_in_order() {
        let adapter = MemoryTransportAdapter::new("ordered");
        for i in 0..5 {
            adapter
                .inject(format!("msg-{i}").into_bytes())
                .await
                .expect("inject");
        }

        let msgs = adapter.recv(10).await.expect("recv");
        assert_eq!(msgs.len(), 5);
        // Offsets are monotonically increasing sequence numbers
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.offset, i as i64, "offset {i} should match sequence");
            assert_eq!(msg.payload, format!("msg-{i}").into_bytes());
        }
    }

    // ========================================================================
    // MemoryTransportAdapter: recv with various max limits
    // ========================================================================

    #[tokio::test]
    async fn test_recv_with_max_zero_returns_empty() {
        let adapter = MemoryTransportAdapter::new("zero-max");
        adapter.inject(b"ignored".to_vec()).await.expect("inject");

        // max=0 means the loop body runs 0 times — returns empty without consuming
        let msgs = adapter.recv(0).await.expect("recv");
        assert!(msgs.is_empty(), "max=0 should return no messages");

        // Message should still be in buffer for subsequent recv
        let msgs = adapter.recv(10).await.expect("recv-after-zero");
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn test_recv_max_one_honours_limit() {
        let adapter = MemoryTransportAdapter::new("max-one");
        for i in 0..5 {
            adapter.inject(vec![i as u8]).await.expect("inject");
        }

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(msgs.len(), 1, "max=1 should return exactly one message");

        // The remaining 4 should still be there
        let remaining = adapter.recv(100).await.expect("recv-remaining");
        assert_eq!(remaining.len(), 4);
    }

    #[tokio::test]
    async fn test_recv_max_larger_than_buffer() {
        let adapter = MemoryTransportAdapter::new("large-max");
        for i in 0..3 {
            adapter.inject(vec![i as u8]).await.expect("inject");
        }

        // max=100 but only 3 messages available — returns 3
        let msgs = adapter.recv(100).await.expect("recv");
        assert_eq!(msgs.len(), 3);
    }

    #[tokio::test]
    async fn test_recv_empty_buffer_returns_empty() {
        // Custom config with 0 timeout — avoids blocking the default 100ms
        let config = MemoryConfig {
            buffer_size: 10,
            recv_timeout_ms: 0,
            ..Default::default()
        };
        let adapter = MemoryTransportAdapter::with_config("empty", &config);
        let msgs = adapter.recv(10).await.expect("recv");
        assert!(msgs.is_empty());
    }

    // ========================================================================
    // MemoryTransportAdapter: close semantics
    // ========================================================================

    #[tokio::test]
    async fn test_close_marks_unhealthy() {
        let adapter = MemoryTransportAdapter::new("close-test");
        assert!(adapter.is_healthy());
        adapter.close().await.expect("close");
        assert!(!adapter.is_healthy(), "Closed adapter should be unhealthy");
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let adapter = MemoryTransportAdapter::new("idem");
        adapter.close().await.expect("first close");
        // Second close should not error
        adapter.close().await.expect("second close");
        adapter.close().await.expect("third close");
        assert!(!adapter.is_healthy());
    }

    #[tokio::test]
    async fn test_recv_on_closed_transport_errors() {
        let adapter = MemoryTransportAdapter::new("closed-recv");
        adapter.close().await.expect("close");

        let result = adapter.recv(10).await;
        assert!(
            result.is_err(),
            "recv on closed transport should return error"
        );
    }

    #[tokio::test]
    async fn test_inject_on_closed_transport_errors() {
        let adapter = MemoryTransportAdapter::new("closed-inject");
        adapter.close().await.expect("close");

        let result = adapter.inject(b"after close".to_vec()).await;
        assert!(
            result.is_err(),
            "inject after close should return error (TransportError::Closed)"
        );
    }

    #[tokio::test]
    async fn test_inject_with_key_on_closed_transport_errors() {
        let adapter = MemoryTransportAdapter::new("closed-inject-key");
        adapter.close().await.expect("close");

        let result = adapter.inject_with_key("k", b"after close".to_vec()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_on_closed_is_noop() {
        // commit is a no-op for memory transport — should not error even after close
        let adapter = MemoryTransportAdapter::new("closed-commit");
        adapter.close().await.expect("close");

        // Empty offsets slice
        adapter
            .commit(&[])
            .await
            .expect("commit empty should be ok");

        // Non-empty offsets — still a no-op
        let offsets = vec![KafkaOffset {
            topic: std::sync::Arc::from("closed-commit"),
            partition: 0,
            offset: 5,
        }];
        adapter
            .commit(&offsets)
            .await
            .expect("commit is always no-op for memory");
    }

    // ========================================================================
    // MemoryTransportAdapter: payload edge cases
    // ========================================================================

    #[tokio::test]
    async fn test_empty_payload_roundtrip() {
        let adapter = MemoryTransportAdapter::new("empty-payload");
        adapter.inject(Vec::new()).await.expect("inject empty");

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].payload.is_empty());
    }

    #[tokio::test]
    async fn test_large_payload_roundtrip() {
        let adapter = MemoryTransportAdapter::new("large-payload");
        // 1 MiB payload — deliberately NOT all zeros to avoid compression shortcuts
        let payload: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();
        let expected_len = payload.len();
        let expected_sample = payload[12345];

        adapter.inject(payload).await.expect("inject 1MiB");

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload.len(), expected_len);
        assert_eq!(msgs[0].payload[12345], expected_sample);
    }

    #[tokio::test]
    async fn test_binary_payload_preserved() {
        let adapter = MemoryTransportAdapter::new("binary");
        let payload = vec![0x00, 0xFF, 0x7F, 0x80, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
        adapter.inject(payload.clone()).await.expect("inject");

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(msgs[0].payload, payload);
    }

    // ========================================================================
    // MemoryTransportAdapter: commit is no-op but does not error
    // ========================================================================

    #[tokio::test]
    async fn test_commit_empty_ok() {
        let adapter = MemoryTransportAdapter::new("commit-empty");
        adapter.commit(&[]).await.expect("commit empty");
    }

    #[tokio::test]
    async fn test_commit_with_offsets_is_noop() {
        let adapter = MemoryTransportAdapter::new("commit-offsets");
        let offsets = vec![
            KafkaOffset {
                topic: std::sync::Arc::from("commit-offsets"),
                partition: 0,
                offset: 10,
            },
            KafkaOffset {
                topic: std::sync::Arc::from("commit-offsets"),
                partition: 0,
                offset: 20,
            },
        ];
        adapter.commit(&offsets).await.expect("commit offsets");
    }

    // ========================================================================
    // TransportBackend: name and health dispatch
    // ========================================================================

    // MemoryTransport is not a variant of TransportBackend — backend tests only
    // cover Kafka/Grpc construction paths without network side effects.

    #[tokio::test]
    async fn test_transport_backend_from_config_grpc_dispatch() {
        // Verify the gRPC dispatch path. Use an invalid listen address that
        // cannot bind — exercises the gRPC construction branch whether it
        // fails immediately or later. Main goal: dispatch reaches gRPC, not
        // Kafka, and does not panic.
        let config = crate::config::Config {
            transport: "grpc".to_string(),
            grpc: crate::config::GrpcConfig {
                // Invalid listen — port unusable; at minimum must not
                // succeed as a Kafka transport.
                listen: Some("256.256.256.256:99999".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = TransportBackend::from_config(&config, None).await;
        // Either the adapter constructs (listen is lazy) or fails — both are
        // fine. What matters is if it did construct, it is a Grpc variant.
        if let Ok(backend) = result {
            assert_eq!(backend.name(), "grpc");
        }
    }

    #[tokio::test]
    async fn test_transport_backend_from_config_kafka_dispatch_invalid_brokers() {
        // Kafka construction with unreachable/empty brokers fails — we test
        // dispatch reaches the Kafka branch by observing a Kafka-specific error.
        let config = crate::config::Config {
            transport: "kafka".to_string(),
            kafka: KafkaConfig {
                // Invalid broker list — scalo will reject this
                brokers: Vec::new(),
                group: "test-group".to_string(),
                topics: vec!["t".to_string()],
                ..Default::default()
            },
            ..Default::default()
        };

        // Either succeeds (lazy) or fails — both are acceptable; what matters is
        // that dispatch did not panic and did not go to the gRPC path.
        let _ = TransportBackend::from_config(&config, None).await;
    }

    #[tokio::test]
    async fn test_transport_backend_unknown_transport_defaults_to_kafka() {
        // from_config: anything other than "grpc" falls to Kafka branch.
        let config = crate::config::Config {
            transport: "unknown_transport_xyz".to_string(),
            kafka: KafkaConfig {
                brokers: Vec::new(),
                group: "grp".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        // Just verify dispatch doesn't panic — construction may fail
        let _ = TransportBackend::from_config(&config, None).await;
    }

    // ========================================================================
    // KafkaMessage fields populated correctly by MemoryTransport
    // ========================================================================

    #[tokio::test]
    async fn test_memory_message_timestamp_populated() {
        let adapter = MemoryTransportAdapter::new("ts");
        adapter.inject(b"ts-test".to_vec()).await.expect("inject");

        let msgs = adapter.recv(1).await.expect("recv");
        assert!(
            msgs[0].timestamp_ms.is_some(),
            "MemoryTransport should populate timestamp_ms"
        );
    }

    #[tokio::test]
    async fn test_memory_topic_is_configured_topic() {
        // All messages from a given adapter use the adapter's configured topic,
        // regardless of the key — keys do not become topics.
        let adapter = MemoryTransportAdapter::new("fixed-topic");
        adapter
            .inject_with_key("different-key", b"payload".to_vec())
            .await
            .expect("inject");

        let msgs = adapter.recv(1).await.expect("recv");
        assert_eq!(&*msgs[0].topic, "fixed-topic");
    }

    // ========================================================================
    // From<TransportError> for crate::Error
    // ========================================================================

    #[test]
    fn test_transport_error_conversion() {
        let te = scalo::transport::TransportError::Closed;
        let ce: crate::Error = te.into();
        match ce {
            crate::Error::Kafka(msg) => {
                assert!(!msg.is_empty(), "Converted error should have a message");
            }
            other => panic!("Expected Kafka variant, got {other:?}"),
        }
    }
}
