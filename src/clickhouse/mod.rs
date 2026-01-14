// Project:   dfe-loader
// File:      mod.rs
// Purpose:   ClickHouse client abstraction
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse client abstraction
//!
//! Uses clickhouse-arrow for native Arrow protocol inserts.
//!
//! ## Architecture
//!
//! Core client and types are provided by `hs-rustlib::clickhouse`:
//! - `ArrowClickHouseClient` - Arrow protocol client
//! - `ParsedType`, `ColumnInfo`, `TableSchema` - Type system
//!
//! Resilience features are dfe-loader specific:
//! - `Inserter` - Batch insert with binary-split salvage
//! - `CircuitBreaker` - Per-table failure detection
//! - `SchemaCache` - TTL-based schema caching

// Local modules (dfe-loader specific resilience)
pub mod circuit_breaker;
pub mod inserter;
pub mod schema;

// Re-export core types from hs-rustlib
pub use hs_rustlib::clickhouse::{
    default_value_for_category, is_null_string, ArrowClickHouseClient, ClickHouseConfig,
    ClickHouseError, ColumnInfo, ParsedType, SharedArrowClient, TableSchema, NULL_STRINGS,
};

// Export local resilience modules
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState};
pub use inserter::{FailedRow, InsertResult, Inserter, InserterConfig};
pub use schema::{SchemaCache, SchemaCacheConfig, SchemaCacheStats, SharedSchemaCache};
