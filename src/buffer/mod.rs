// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/buffer/mod.rs
// Purpose:   Per-table row buffer management
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-table row buffer management for JSONEachRow inserts.
//!
//! Each destination table (db.table) has its own row buffer to ensure
//! schema uniformity per batch.
//!
//! ## Architecture
//!
//! ```text
//! Kafka message → Route to db.table
//!              → Push Map<String, Value> to per-table buffer
//!              → Flush when threshold reached
//!              → Insert to ClickHouse via JSONEachRow HTTP
//!              → Ack Kafka offsets on success
//! ```

pub mod manager;
pub mod pool;

pub use manager::{BufferManager, BufferStats, FlushBatch, KafkaOffset};
pub use pool::{
    BufferPools, BufferPoolsStats, MapPool, ObjectPool, OffsetsPool, PoolConfig, PoolStats,
    Poolable, Pooled, PooledMap, PooledOffsets, PooledString, StringPool,
};
