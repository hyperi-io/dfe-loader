// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      mod.rs
// Purpose:   ClickHouse client abstraction
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` client abstraction.
//!
//! Uses two `clickhouse::Client` instances (from the fork):
//! - `ClickHouseQueryClient` wraps one for DDL, schema queries, and health checks
//! - `Inserter` uses another for all data inserts (`RowBinary` via `DynamicInsert`,
//!   `JSONEachRow` via `InsertFormatted`)
//!
//! ## Architecture
//!
//! Core client and types:
//! - `ClickHouseQueryClient` - DDL + schema queries (no inserts)
//! - `ParsedType`, `ColumnInfo`, `TableSchema` - Type system
//! - `ClickHouseConfig` - Connection configuration
//!
//! Resilience features:
//! - `Inserter` - Batch insert with binary-split salvage
//! - `CircuitBreaker` - Per-table failure detection
//! - `SchemaCache` - TTL-based schema caching

// Core client modules
pub mod client_http;
pub mod config;
pub mod error;
pub mod types;

// Resilience modules
pub mod circuit_breaker;
pub mod inserter;
pub mod schema;

// Re-export core types
pub use client_http::{ClickHouseQueryClient, PoolStats, SharedQueryClient};
pub use config::{ClickHouseConfig, InsertFormat, Transport};
pub use error::{ClickHouseError, ErrorCategory};
pub use types::{
    ColumnInfo, NULL_STRINGS, ParsedType, ParsedTypeExt, TableSchema, default_value_for_category,
    is_null_string,
};

// Export resilience modules
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState,
};
pub use inserter::{FailedRow, InsertResult, Inserter, InserterConfig};
pub use schema::{SchemaCache, SchemaCacheConfig, SchemaCacheStats, SharedSchemaCache};
