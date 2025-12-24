//! Kafka consumer and DLQ producer

pub mod consumer;
pub mod dlq;

pub use consumer::{Consumer, KafkaMessage};
pub use dlq::DlqProducer;
