//! Unit tests using MemoryTransport
//!
//! These tests don't require external infrastructure (Kafka, ClickHouse).
//! They use the in-memory transport to test pipeline components in isolation.

#[cfg(feature = "transport-memory")]
mod transport;
