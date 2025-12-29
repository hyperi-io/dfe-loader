//! Kafka consumer, transport adapter, and DLQ producer

pub mod consumer;
pub mod dlq;
pub mod transport;

pub use consumer::{Consumer, KafkaMessage};
pub use dlq::{DlqMessage, DlqProducer, DlqRoutingMode};
pub use transport::TransportAdapter;

#[cfg(feature = "transport-memory")]
pub use transport::MemoryTransportAdapter;
