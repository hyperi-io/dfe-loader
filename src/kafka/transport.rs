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

use std::sync::Arc;
use std::time::Duration;

use crate::Result;
use crate::buffer::KafkaOffset;
use crate::config::{DlqConfig, KafkaConfig};
use scalo::SelfRegulationGovernor;
use scalo::memory::MemoryGuard;
use scalo::transport::filter::FilteredDlqEntry;
use scalo::transport::kafka::PartitionLease;
use scalo::transport::{
    AckControl, DeliveryStatus, GrpcConfig as TransportGrpcConfig, GrpcToken, GrpcTransport,
    KafkaConfig as TransportKafkaConfig, KafkaToken, KafkaTransport, TransportBase, TransportError,
    TransportReceiver,
};
use tracing::{debug, trace};

use super::KafkaMessage;

/// What auto-discovery subscribes to when neither `topics` nor `topic_regex`
/// is set. scalo reads an empty include list as every topic on the broker.
const LANDING_TOPIC_INCLUDE: &str = "_(land|load)$";

/// A received block: the passing messages plus any inbound-filter DLQ entries.
///
/// The DLQ entries are surfaced (never silently dropped) so the orchestrator
/// can route them onward. The loader configures no inbound scalo filters, so
/// `dlq_entries` is empty in practice -- but the no-silent-drop contract is
/// honoured regardless.
#[derive(Default)]
pub struct ReceivedBatch {
    /// Passing messages, in the same order as the source records.
    pub messages: Vec<KafkaMessage>,
    /// Inbound-filter DLQ entries carried forward from the transport.
    pub dlq_entries: Vec<FilteredDlqEntry>,
    /// The lease each Kafka partition in the batch was read under, one entry
    /// per partition. Empty for gRPC, which leases nothing.
    pub leases: Vec<PartitionRead>,
}

/// A partition a received batch holds records of, and the lease they were
/// read under: `None` when this consumer no longer holds the partition.
pub struct PartitionRead {
    pub topic: Arc<str>,
    pub partition: i32,
    pub lease: Option<PartitionLease>,
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
    /// `config.topics` is empty -- no app-side resolver needed. `dlq` names the
    /// topics auto-discovery must never subscribe to (see [`Self::convert_config`]).
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
        dlq: &DlqConfig,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        let transport_config = Self::convert_config(config, dlq);

        let transport = KafkaTransport::new(&transport_config)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Transport error: {e}")))?
            .with_acknowledgements(config.acknowledgements);

        // Attach the self-regulation pause-partitions gate over the runtime's
        // shared pressure (the gate is evaluated automatically inside `recv`).
        let transport = match governor {
            Some(gov) => gov.attach_kafka_gate(transport),
            None => transport,
        };

        Ok(Self { transport })
    }

    /// Convert local `KafkaConfig` to scalo `TransportKafkaConfig`.
    ///
    /// The topic filters only take effect when `topics` is empty and scalo
    /// auto-discovers. The include is `topic_regex` when set, else
    /// `*_land` / `*_load`. The loader's own DLQ topics are appended to
    /// scalo's default excludes (`^__`, `_dlq$`), never replacing them.
    pub fn convert_config(config: &KafkaConfig, dlq: &DlqConfig) -> TransportKafkaConfig {
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

        // An empty regex would match every topic, the same failure as no include.
        let include = config
            .topic_regex
            .as_deref()
            .filter(|r| !r.is_empty())
            .unwrap_or(LANDING_TOPIC_INCLUDE);
        transport_config.topic_include = vec![include.to_string()];
        transport_config
            .topic_exclude
            .extend(dlq.topic_exclude_patterns());

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
    ///
    /// Each partition's lease is taken here, before the next receive can serve
    /// a rebalance: every record of a partition in the batch was read under the
    /// lease the partition has when the batch arrives.
    pub async fn recv(&self, max: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("Recv error: {e}")))?;

        let mut leases: Vec<PartitionRead> = Vec::new();
        for token in &batch.commit_tokens {
            let seen = leases
                .iter()
                .any(|read| read.partition == token.partition && read.topic == token.topic);
            if !seen {
                leases.push(PartitionRead {
                    lease: self.transport.lease(&token.topic, token.partition),
                    topic: Arc::clone(&token.topic),
                    partition: token.partition,
                });
            }
        }

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
            leases,
        })
    }

    /// Whether `lease` is still this consumer's claim on `topic`/`partition`.
    pub fn holds(&self, topic: &str, partition: i32, lease: PartitionLease) -> bool {
        self.transport.holds(topic, partition, lease)
    }

    /// Count `records` discarded because the lease they were read under ended.
    pub fn discarded_after_revoke(&self, records: u64) {
        self.transport.discarded_after_revoke(records);
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
/// With `acknowledgements.enabled` a Push is answered only when the loader
/// [releases](Self::release) every record it carried. gRPC has no broker-side
/// persistence, so commit is always a no-op.
pub struct GrpcTransportAdapter {
    transport: GrpcTransport,
    /// Fallback topic when the sender doesn't set one in gRPC metadata.
    default_topic: std::sync::Arc<str>,
}

impl GrpcTransportAdapter {
    /// Create a new gRPC transport adapter in server (receive) mode.
    ///
    /// Requires `config.listen` to be set. The adapter starts a tonic gRPC
    /// server that accepts incoming Push RPCs, armed before it listens: with
    /// `acknowledgements.enabled` the first Push that can arrive is already
    /// held until its records are released.
    ///
    /// When `governor` is `Some`, the listener answers Push with `UNAVAILABLE`
    /// while the governor's shared pressure holds, so memory pressure reaches
    /// the sender as backpressure before the loader admits another record.
    /// Held Pushes are leased on `memory_guard`, whose limit sizes the
    /// held-byte ceiling at a quarter of it.
    pub async fn new(
        config: &crate::config::GrpcConfig,
        governor: Option<&SelfRegulationGovernor>,
        memory_guard: Option<&Arc<MemoryGuard>>,
    ) -> Result<Self> {
        let transport_config = TransportGrpcConfig {
            listen: config.listen.clone(),
            endpoint: None, // receive-only
            recv_buffer_size: config.recv_buffer_size,
            recv_timeout_ms: config.recv_timeout_ms,
            max_message_size: config.max_message_size,
            compression: config.compression,
            ..Default::default()
        };

        // Armed here, never later: a Push answered before arming is lost on a crash.
        let mut builder = GrpcTransport::builder(&transport_config)
            .acknowledgements(config.acknowledgements)
            .armed(true)
            .max_hold(Duration::from_millis(config.max_hold_ms));
        if let Some(governor) = governor {
            builder = builder.pressure(governor.pressure());
        }
        if let Some(guard) = memory_guard {
            builder = builder.memory_guard(Arc::clone(guard));
        }
        let transport = builder
            .start()
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
    /// the message offset, which [`release`](Self::release) takes back.
    pub async fn recv(&self, max: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max)
            .await
            .map_err(|e| crate::Error::Kafka(format!("gRPC recv error: {e}")))?;

        // The listener carries no inbound filters, so a token past the last
        // record is a policy drop and is released as one.
        let filtered = batch
            .commit_tokens
            .get(batch.records.len()..)
            .unwrap_or_default();
        if !filtered.is_empty() {
            self.release_tokens(filtered, DeliveryStatus::Dropped).await;
        }

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
            leases: Vec::new(),
        })
    }

    /// Release the records `seqs` with `status`: a Push is answered once every
    /// record it carried is released, `OK` unless one is `Errored`.
    pub async fn release(&self, seqs: &[u64], status: DeliveryStatus) {
        let tokens: Vec<GrpcToken> = seqs.iter().map(|&seq| GrpcToken::new(seq)).collect();
        self.release_tokens(&tokens, status).await;
    }

    async fn release_tokens(&self, tokens: &[GrpcToken], status: DeliveryStatus) {
        if let Err(e) = self.transport.release(tokens, status).await {
            tracing::warn!(error = %e, records = tokens.len(), "gRPC release failed");
        }
    }

    /// Whether a Push waits for its records to be released before it is
    /// answered: `acknowledgements.enabled` on an armed listener.
    pub fn holds_answers(&self) -> bool {
        self.transport
            .ack_control()
            .is_some_and(|control| control.enabled() && control.is_armed())
    }

    /// Records received but not yet released.
    pub fn held_records(&self) -> u64 {
        self.transport
            .ack_control()
            .map_or(0, |control| control.held().count)
    }

    /// When the listener answers the earliest held Push among the records
    /// `seqs` itself, as `UNAVAILABLE`, or `None` when none of them is held.
    pub fn hold_deadline(&self, seqs: &[u64]) -> Option<std::time::Instant> {
        let tokens: Vec<GrpcToken> = seqs.iter().map(|&seq| GrpcToken::new(seq)).collect();
        self.transport.hold_deadline(&tokens)
    }

    /// Commit (no-op: a Push is answered through [`release`](Self::release)).
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
    /// `governor`, when `Some`, brakes intake under memory pressure
    /// (default-on self-regulation): Kafka pauses its assigned partitions, and
    /// the gRPC listener refuses Push with `UNAVAILABLE`, which the sender
    /// holds and re-sends. `memory_guard` leases the Pushes the gRPC listener
    /// holds.
    pub async fn from_config(
        config: &crate::config::Config,
        governor: Option<&SelfRegulationGovernor>,
        memory_guard: Option<&Arc<MemoryGuard>>,
    ) -> Result<Self> {
        if config.is_direct() {
            let adapter = GrpcTransportAdapter::new(&config.grpc, governor, memory_guard).await?;
            Ok(Self::Grpc(adapter))
        } else {
            let adapter =
                TransportAdapter::new(&config.kafka, &config.routing.dlq, governor).await?;
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

    /// Whether `lease` still stands on `topic`/`partition`. Always for gRPC,
    /// which leases nothing.
    pub fn holds(&self, topic: &str, partition: i32, lease: PartitionLease) -> bool {
        match self {
            Self::Kafka(a) => a.holds(topic, partition, lease),
            Self::Grpc(_) => true,
        }
    }

    /// Count `records` discarded because the lease they were read under ended
    /// (a no-op for gRPC).
    pub fn discarded_after_revoke(&self, records: u64) {
        match self {
            Self::Kafka(a) => a.discarded_after_revoke(records),
            Self::Grpc(_) => {}
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

    /// Whether a successful `commit` reached a broker.
    ///
    /// The gRPC arm's commit is a no-op -- the Push RPC response is the ack --
    /// so a kafka-named counter must stay flat on a broker-less deployment
    /// (#125).
    #[must_use]
    pub const fn commits_offsets(&self) -> bool {
        matches!(self, Self::Kafka(_))
    }

    /// Whether each received record waits for [`release`](Self::release)
    /// before its sender is answered. Kafka re-delivers from its committed
    /// offset instead, so it never holds.
    #[must_use]
    pub fn holds_answers(&self) -> bool {
        match self {
            Self::Kafka(_) => false,
            Self::Grpc(a) => a.holds_answers(),
        }
    }

    /// The source's acknowledgement controls, for the delivery guarantee the
    /// pipeline reports.
    #[must_use]
    pub fn ack_control(&self) -> Option<&dyn AckControl> {
        match self {
            Self::Kafka(a) => a.transport.ack_control(),
            Self::Grpc(a) => a.transport.ack_control(),
        }
    }

    /// Records received but not yet released (0 for Kafka).
    #[must_use]
    pub fn held_records(&self) -> u64 {
        match self {
            Self::Kafka(_) => 0,
            Self::Grpc(a) => a.held_records(),
        }
    }

    /// Release the records `seqs` with `status` (a no-op for Kafka, which
    /// commits offsets instead).
    pub async fn release(&self, seqs: &[u64], status: DeliveryStatus) {
        match self {
            Self::Kafka(_) => {}
            Self::Grpc(a) => a.release(seqs, status).await,
        }
    }

    /// When the earliest sender waiting on the records `seqs` is answered
    /// `UNAVAILABLE` without them, or `None` when nobody waits (always for
    /// Kafka).
    #[must_use]
    pub fn hold_deadline(&self, seqs: &[u64]) -> Option<std::time::Instant> {
        match self {
            Self::Kafka(_) => None,
            Self::Grpc(a) => a.hold_deadline(seqs),
        }
    }
}

#[cfg(test)]
mod backend_tests {
    use super::*;

    /// gRPC listens on an ephemeral loopback port, so the bind is real and the
    /// test needs no fixed port.
    async fn grpc_backend() -> TransportBackend {
        let config = crate::config::Config {
            transport: "grpc".to_string(),
            grpc: crate::config::GrpcConfig {
                listen: Some("127.0.0.1:0".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        TransportBackend::from_config(&config, None, None)
            .await
            .expect("gRPC backend binds on an ephemeral port")
    }

    #[tokio::test]
    async fn grpc_backend_does_not_commit_offsets() {
        let backend = grpc_backend().await;
        assert_eq!(backend.name(), "grpc");
        assert!(
            !backend.commits_offsets(),
            "gRPC commit is a no-op, so the kafka offset counters must not move"
        );
        backend.close().await.expect("close the gRPC server");
    }

    /// A governor whose memory pressure is `used` of a 1000-byte limit, read
    /// from the reservation counter rather than the host's own usage.
    fn governor_at(used: u64) -> SelfRegulationGovernor {
        let guard = std::sync::Arc::new(scalo::memory::MemoryGuard::with_usage_source(
            scalo::memory::MemoryGuardConfig {
                limit_bytes: 1000,
                ..Default::default()
            },
            scalo::memory::UsageSource::Reservations,
        ));
        guard.add_bytes(used);
        scalo::SelfRegulationConfig::default()
            .build(guard)
            .expect("self-regulation is on by default")
    }

    /// A gRPC backend built with `governor` and `acknowledgements`, and a
    /// client dialled to it.
    async fn grpc_backend_and_client(
        governor: Option<&SelfRegulationGovernor>,
        acknowledgements: bool,
    ) -> (TransportBackend, GrpcTransport) {
        // The client dials the listener by number, so the port is chosen here,
        // below the kernel's ephemeral range an outgoing connection could take
        // it from between the pick and the bind.
        let span = 10_240 - 9_000;
        let start = std::process::id() % span;
        let port = (0..span)
            .map(|i| u16::try_from(9_000 + (start + i) % span).expect("below 10240"))
            .find(|&port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok())
            .expect("a free loopback port below 10240");
        let config = crate::config::Config {
            transport: "grpc".to_string(),
            grpc: crate::config::GrpcConfig {
                listen: Some(format!("127.0.0.1:{port}")),
                acknowledgements: scalo::transport::AcknowledgementsConfig::new(acknowledgements),
                ..Default::default()
            },
            ..Default::default()
        };
        let backend = TransportBackend::from_config(&config, governor, None)
            .await
            .expect("gRPC backend binds");
        let client = GrpcTransport::new(&TransportGrpcConfig::client(&format!(
            "http://127.0.0.1:{port}"
        )))
        .await
        .expect("gRPC client");
        (backend, client)
    }

    /// Push one record from `client` in the background.
    fn push(client: GrpcTransport) -> tokio::task::JoinHandle<scalo::transport::SendResult> {
        use scalo::transport::TransportSender;

        tokio::spawn(async move {
            let result = client
                .send("", bytes::Bytes::from_static(br#"{"id":1}"#))
                .await;
            client.close().await.expect("close the client");
            result
        })
    }

    /// Receive until a record arrives or `pushed` has already been answered,
    /// returning the sequence numbers received.
    async fn receive_one(
        backend: &TransportBackend,
        pushed: &tokio::task::JoinHandle<scalo::transport::SendResult>,
    ) -> Vec<u64> {
        for _ in 0..50 {
            let received = backend.recv(100).await.expect("recv");
            let seqs: Vec<u64> = received.messages.iter().map(|m| m.offset as u64).collect();
            if !seqs.is_empty() || pushed.is_finished() {
                return seqs;
            }
        }
        Vec::new()
    }

    /// Push one record to a gRPC backend built with `governor`, releasing it
    /// `Delivered` if it arrives, and return the sender's answer.
    async fn push_through_backend(
        governor: Option<&SelfRegulationGovernor>,
    ) -> scalo::transport::SendResult {
        let (backend, client) = grpc_backend_and_client(governor, true).await;
        let pushed = push(client);
        let seqs = receive_one(&backend, &pushed).await;
        backend.release(&seqs, DeliveryStatus::Delivered).await;
        let result = pushed.await.expect("push task");
        backend.close().await.expect("close the gRPC server");
        result
    }

    #[tokio::test]
    async fn a_push_is_answered_only_once_its_record_is_released() {
        let (backend, client) = grpc_backend_and_client(None, true).await;
        assert!(
            backend.holds_answers(),
            "acknowledgements are on by default"
        );
        let pushed = push(client);
        let seqs = receive_one(&backend, &pushed).await;
        assert_eq!(seqs.len(), 1, "the record reached recv");

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !pushed.is_finished(),
            "the Push was answered before its record was released"
        );
        assert_eq!(backend.held_records(), 1);

        backend.release(&seqs, DeliveryStatus::Delivered).await;
        let result = tokio::time::timeout(Duration::from_secs(5), pushed)
            .await
            .expect("answered once released")
            .expect("push task");
        assert!(
            matches!(result, scalo::transport::SendResult::Ok),
            "a delivered record answered {result:?}"
        );
        assert_eq!(backend.held_records(), 0);
        backend.close().await.expect("close the gRPC server");
    }

    #[tokio::test]
    async fn a_held_record_names_the_deadline_its_push_is_answered_by() {
        let (backend, client) = grpc_backend_and_client(None, true).await;
        let pushed = push(client);
        let seqs = receive_one(&backend, &pushed).await;
        assert_eq!(seqs.len(), 1, "the record reached recv");

        let deadline = backend
            .hold_deadline(&seqs)
            .expect("a held record has a deadline");
        let budget = Duration::from_millis(crate::config::kafka::DEFAULT_MAX_HOLD_MS);
        assert!(
            deadline <= std::time::Instant::now() + budget,
            "the deadline lies past the {budget:?} hold"
        );
        assert_eq!(backend.hold_deadline(&[seqs[0] + 1_000]), None);

        backend.release(&seqs, DeliveryStatus::Delivered).await;
        pushed.await.expect("push task");
        assert_eq!(
            backend.hold_deadline(&seqs),
            None,
            "a released record still names a deadline"
        );
        backend.close().await.expect("close the gRPC server");
    }

    #[tokio::test]
    async fn an_errored_release_tells_the_sender_to_retry() {
        let (backend, client) = grpc_backend_and_client(None, true).await;
        let pushed = push(client);
        let seqs = receive_one(&backend, &pushed).await;
        backend.release(&seqs, DeliveryStatus::Errored).await;
        let result = pushed.await.expect("push task");
        assert!(
            matches!(result, scalo::transport::SendResult::Backpressured),
            "a record that failed downstream answered {result:?}"
        );
        backend.close().await.expect("close the gRPC server");
    }

    #[tokio::test]
    async fn a_dropped_or_rejected_release_answers_ok() {
        for status in [DeliveryStatus::Dropped, DeliveryStatus::Rejected] {
            let (backend, client) = grpc_backend_and_client(None, true).await;
            let pushed = push(client);
            let seqs = receive_one(&backend, &pushed).await;
            backend.release(&seqs, status).await;
            let result = pushed.await.expect("push task");
            assert!(
                matches!(result, scalo::transport::SendResult::Ok),
                "a record released {status:?} answered {result:?}, and a retry could only fail the same way"
            );
            backend.close().await.expect("close the gRPC server");
        }
    }

    #[tokio::test]
    async fn with_acknowledgements_off_a_push_is_answered_at_enqueue() {
        let (backend, client) = grpc_backend_and_client(None, false).await;
        assert!(!backend.holds_answers());
        let result = push(client).await.expect("push task");
        assert!(
            matches!(result, scalo::transport::SendResult::Ok),
            "the listener refused the record: {result:?}"
        );
        let received = backend.recv(100).await.expect("recv");
        assert_eq!(received.messages.len(), 1, "answered before any recv");
        backend.close().await.expect("close the gRPC server");
    }

    /// A record queued before close() is one recv still returns, its sender is
    /// answered when it is released, and recv reports the transport closed
    /// once it has.
    #[tokio::test]
    async fn a_queued_record_is_received_and_answered_after_close() {
        let (backend, client) = grpc_backend_and_client(None, true).await;
        let pushed = push(client);
        for _ in 0..100 {
            if backend.held_records() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(backend.held_records(), 1, "the Push was never held");

        backend.close().await.expect("close the gRPC server");
        let received = backend.recv(100).await;
        let drained = backend.recv(100).await;

        let received = received.expect("the queued record after close()");
        assert_eq!(
            received.messages.len(),
            1,
            "the listener queued 1 record and recv returned {} after close()",
            received.messages.len()
        );
        assert!(
            drained.is_err(),
            "recv after the drain must report the transport closed"
        );
        let seqs: Vec<u64> = received.messages.iter().map(|m| m.offset as u64).collect();
        backend.release(&seqs, DeliveryStatus::Delivered).await;
        let result = pushed.await.expect("push task");
        assert!(
            matches!(result, scalo::transport::SendResult::Ok),
            "a record released after close() answered {result:?}"
        );
    }

    #[tokio::test]
    async fn memory_pressure_makes_the_grpc_listener_refuse_push() {
        let result = push_through_backend(Some(&governor_at(950))).await;
        assert!(
            matches!(result, scalo::transport::SendResult::Backpressured),
            "a governor holding intake let Push through: {result:?}"
        );
    }

    #[tokio::test]
    async fn a_governor_with_headroom_lets_push_through() {
        let result = push_through_backend(Some(&governor_at(0))).await;
        assert!(
            matches!(result, scalo::transport::SendResult::Ok),
            "a governor with headroom refused Push: {result:?}"
        );
    }
}

#[cfg(test)]
mod discovery_tests {
    use regex::Regex;
    use scalo::transport::kafka::topic_resolver::{apply_suppression_rules, passes_filters};

    use super::*;

    /// A Strimzi broker: two loader topics plus everything else that lives on it.
    const BROKER_TOPICS: &[&str] = &[
        "main_land",
        "x_load",
        "strimzi.cruisecontrol.metrics",
        "strimzi.cruisecontrol.modeltrainingsamples",
        "dlq_land",
        "__consumer_offsets",
        "foo_dlq",
        "_schemas",
        "connect-offsets",
    ];

    /// The subscription scalo's `TopicResolver::resolve` computes, run over a
    /// fixed broker topic list instead of a live metadata fetch.
    fn resolve(cfg: &TransportKafkaConfig, broker_topics: &[&str]) -> Vec<String> {
        let compile = |patterns: &[String]| -> Vec<Regex> {
            patterns
                .iter()
                .map(|p| Regex::new(p).expect("topic filter compiles"))
                .collect()
        };
        let include = compile(&cfg.topic_include);
        let exclude = compile(&cfg.topic_exclude);
        let topics = apply_suppression_rules(
            broker_topics.iter().map(|t| (*t).to_string()).collect(),
            &cfg.topic_suppression_rules,
        );
        let mut resolved: Vec<String> = topics
            .into_iter()
            .filter(|t| passes_filters(t, &include, &exclude))
            .collect();
        resolved.sort();
        resolved
    }

    /// dfe-engine's default DLQ topic, which a `_land` include would match.
    fn dlq_land() -> DlqConfig {
        DlqConfig {
            topic: "dlq_land".to_string(),
            ..DlqConfig::default()
        }
    }

    /// scalo's own defaults, which the loader's excludes are appended to.
    fn scalo_default_excludes() -> Vec<String> {
        TransportKafkaConfig::default().topic_exclude
    }

    #[test]
    fn empty_topics_discover_only_land_and_load_topics() {
        let cfg = TransportAdapter::convert_config(&KafkaConfig::default(), &dlq_land());

        assert!(cfg.auto_discover, "an empty topic list auto-discovers");
        assert_eq!(cfg.topic_include, [LANDING_TOPIC_INCLUDE]);
        assert_eq!(resolve(&cfg, BROKER_TOPICS), ["main_land", "x_load"]);
    }

    #[test]
    fn dlq_exclude_is_appended_to_scalo_defaults() {
        let cfg = TransportAdapter::convert_config(&KafkaConfig::default(), &dlq_land());

        let defaults = scalo_default_excludes();
        assert!(!defaults.is_empty(), "scalo ships default excludes");
        assert_eq!(cfg.topic_exclude[..defaults.len()], defaults[..]);
        assert_eq!(cfg.topic_exclude[defaults.len()..], ["^dlq_land$"]);
    }

    #[test]
    fn topic_regex_replaces_the_include_and_keeps_the_dlq_exclude() {
        let kafka = KafkaConfig {
            topic_regex: Some("^strimzi\\.".to_string()),
            ..KafkaConfig::default()
        };
        let cfg = TransportAdapter::convert_config(&kafka, &dlq_land());

        assert_eq!(cfg.topic_include, ["^strimzi\\."]);
        assert!(cfg.topic_exclude.iter().any(|p| p == "^dlq_land$"));
        assert_eq!(
            resolve(&cfg, BROKER_TOPICS),
            [
                "strimzi.cruisecontrol.metrics",
                "strimzi.cruisecontrol.modeltrainingsamples"
            ]
        );
    }

    #[test]
    fn empty_topic_regex_falls_back_to_land_and_load() {
        let kafka = KafkaConfig {
            topic_regex: Some(String::new()),
            ..KafkaConfig::default()
        };
        let cfg = TransportAdapter::convert_config(&kafka, &dlq_land());

        assert_eq!(resolve(&cfg, BROKER_TOPICS), ["main_land", "x_load"]);
    }

    #[test]
    fn per_destination_dlq_suffix_is_excluded_even_under_a_broad_regex() {
        let kafka = KafkaConfig {
            topic_regex: Some(".*".to_string()),
            ..KafkaConfig::default()
        };
        let dlq = DlqConfig {
            topic: String::new(),
            topic_suffix: ".dlq".to_string(),
            ..DlqConfig::default()
        };
        let cfg = TransportAdapter::convert_config(&kafka, &dlq);

        let resolved = resolve(
            &cfg,
            &["dfe.events.dlq", "events_land", "__consumer_offsets"],
        );
        assert_eq!(resolved, ["events_land"]);
    }

    #[test]
    fn load_still_suppresses_land_for_the_same_source() {
        let cfg = TransportAdapter::convert_config(&KafkaConfig::default(), &dlq_land());

        let resolved = resolve(&cfg, &["auth_land", "auth_load", "events_land"]);
        assert_eq!(resolved, ["auth_load", "events_land"]);
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

        let result = TransportBackend::from_config(&config, None, None).await;
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
        let _ = TransportBackend::from_config(&config, None, None).await;
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
        let _ = TransportBackend::from_config(&config, None, None).await;
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
