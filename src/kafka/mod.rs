// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka transport adapters and topic resolution
//!
//! Uses scalo `KafkaTransport` for all Kafka operations.
//! The legacy direct-rdkafka consumer has been removed — all Kafka
//! interaction goes through the transport abstraction.

pub mod transport;

use std::sync::Arc;

pub use transport::{TransportAdapter, TransportBackend};

#[cfg(feature = "transport-memory")]
pub use transport::MemoryTransportAdapter;

/// Kafka message with metadata
///
/// Uses `Arc<str>` for topic to enable zero-cost sharing with `KafkaOffset`.
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
    /// Clone for moving into the pending-schema buffer.
    ///
    /// The parallel processor borrows the original `&KafkaMessage`; a message
    /// whose schema is missing must be owned by the buffer until its schema
    /// resolves. Cheap `Arc` topic clone + payload `Vec` clone — only on the
    /// cold cache-miss path, never on the hot path.
    pub fn clone_for_pending(&self) -> KafkaMessage {
        KafkaMessage {
            payload: self.payload.clone(),
            topic: Arc::clone(&self.topic),
            partition: self.partition,
            offset: self.offset,
            key: self.key.clone(),
            timestamp_ms: self.timestamp_ms,
        }
    }

    /// Where this record sits, as `topic=X partition=N offset=M`, for the
    /// log line that reports it dead-lettered.
    #[must_use]
    pub fn location(&self) -> String {
        format!(
            "topic={} partition={} offset={}",
            self.topic, self.partition, self.offset
        )
    }
}
