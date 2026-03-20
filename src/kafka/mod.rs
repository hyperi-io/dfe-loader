// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka transport adapters and topic resolution
//!
//! Uses hyperi-rustlib `KafkaTransport` for all Kafka operations.
//! The legacy direct-rdkafka consumer has been removed — all Kafka
//! interaction goes through the transport abstraction.

pub mod topic_resolver;
pub mod transport;

use std::sync::Arc;

pub use topic_resolver::{TopicResolver, resolver_from_config};
pub use transport::{TransportAdapter, TransportBackend};

#[cfg(feature = "transport-memory")]
pub use transport::MemoryTransportAdapter;

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
