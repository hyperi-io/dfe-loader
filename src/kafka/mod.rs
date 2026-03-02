// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka consumer, transport adapter, and DLQ producer

pub mod consumer;
pub mod dlq;
pub mod transport;

pub use consumer::{Consumer, KafkaMessage};
pub use dlq::{DlqMessage, DlqProducer, DlqRoutingMode};
pub use transport::{TransportAdapter, TransportBackend};

#[cfg(feature = "transport-memory")]
pub use transport::MemoryTransportAdapter;
