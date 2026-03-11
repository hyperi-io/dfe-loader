// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! dfe-loader: High-performance Kafka to ClickHouse data loader
//!
//! This library provides a pipeline for consuming JSON/MessagePack events from Kafka,
//! transforming them, and inserting into ClickHouse with high throughput.

pub mod buffer;
pub mod clickhouse;
pub mod column_meta;
pub mod config;
pub mod enrich;
pub mod error;
pub mod kafka;
pub mod metrics;
pub mod payload;
pub mod pipeline;
pub mod routing;
pub mod schema;
pub mod transform;

pub use error::{Error, Result};
