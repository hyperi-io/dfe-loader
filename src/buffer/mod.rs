//! Arrow-based columnar buffer management
//!
//! Uses a chunked Arrow buffer with partition-by-destination for efficient
//! Kafka-to-ClickHouse data loading with at-least-once delivery guarantees.
//!
//! ## Architecture
//!
//! ```text
//! Kafka batch → Arrow chunk (immutable RecordBatch)
//!            → Tracks Kafka offset for ack
//!            → Contains _destination column for routing
//!            → Partition by _destination at flush time
//!            → Drop entire chunk on successful insert
//! ```
//!
//! ## Benefits
//!
//! - **No row-level removal**: Drop whole chunks on ack, O(1) memory free
//! - **Efficient partitioning**: Arrow's columnar format enables fast group-by
//! - **Zero-copy to ClickHouse**: Arrow → ClickHouse native format via clickhouse-arrow
//! - **Unified format**: JSON and MessagePack both deserialize to Arrow
//! - **Memory locality**: All data from one Kafka batch stays together

pub mod arrow;
pub mod manager;
pub mod pool;

pub use arrow::{
    ArrowBatchBuilder, ArrowBuffer, ArrowBufferStats, ArrowChunk, ChunkState, KafkaOffset,
    PartitionedBatch,
};
pub use manager::{BufferManager, FlushBatch};
