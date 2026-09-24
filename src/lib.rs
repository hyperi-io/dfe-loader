// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! dfe-loader: High-performance Kafka to `ClickHouse` data loader
//!
//! This library provides a pipeline for consuming JSON/MessagePack events from Kafka,
//! transforming them, and inserting into `ClickHouse` with high throughput.

// A broken intra-doc link or a malformed code block ships wrong docs silently.
#![warn(rustdoc::broken_intra_doc_links)]
#![warn(rustdoc::private_intra_doc_links)]
#![warn(rustdoc::invalid_codeblock_attributes)]
#![warn(rustdoc::invalid_rust_codeblocks)]
#![warn(rustdoc::bare_urls)]

pub mod buffer;
pub mod clickhouse;
pub mod clickhouse_ext;
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
