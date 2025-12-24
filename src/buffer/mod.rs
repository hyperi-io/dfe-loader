//! Per-table Arrow buffer management
//!
//! Each destination table (db.table) has its own ArrowBatchBuilder to ensure
//! schema uniformity. Schema is derived from the target ClickHouse table via
//! introspection.
//!
//! ## Architecture
//!
//! ```text
//! Kafka message → Route to db.table
//!              → Push to per-table ArrowBatchBuilder
//!              → Batch accumulates messages with SAME schema
//!              → Build RecordBatch when threshold reached
//!              → Insert to ClickHouse via native protocol
//!              → Ack Kafka offsets on success
//! ```
//!
//! ## Benefits
//!
//! - **Schema uniformity**: Each RecordBatch has consistent schema (same table)
//! - **Schema introspection**: Arrow schema derived from ClickHouse table
//! - **Efficient batching**: Accumulate N messages before Arrow conversion
//! - **Per-table flush**: Independent flush triggers per destination
//! - **Offset tracking**: Track Kafka offsets per batch for at-least-once

pub mod arrow;
pub mod manager;
pub mod pool;

pub use arrow::KafkaOffset;
pub use manager::{ArrowBufferStats, BufferManager, FlushBatch, TableSchema};
