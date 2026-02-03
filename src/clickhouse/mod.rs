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
//! Core client and types:
//! - `ArrowClickHouseClient` - Arrow protocol client (native or HTTP)
//! - `ParsedType`, `ColumnInfo`, `TableSchema` - Type system
//! - `ClickHouseConfig` - Connection configuration
//!
//! Resilience features:
//! - `Inserter` - Batch insert with binary-split salvage
//! - `CircuitBreaker` - Per-table failure detection
//! - `SchemaCache` - TTL-based schema caching

// Core client modules
pub mod client;
pub mod config;
pub mod error;
pub mod types;

// Resilience modules
pub mod circuit_breaker;
pub mod inserter;
pub mod schema;

// Re-export core types
pub use client::{ArrowClickHouseClient, NativeArrowClient, SharedArrowClient};
pub use config::{ClickHouseConfig, Transport};
pub use error::{ClickHouseError, ErrorCategory, ServerError, Severity};
pub use types::{
    default_value_for_category, is_null_string, ColumnInfo, ParsedType, TableSchema, NULL_STRINGS,
};

// Export resilience modules
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState,
};
pub use inserter::{FailedRow, InsertResult, Inserter, InserterConfig};
pub use schema::{SchemaCache, SchemaCacheConfig, SchemaCacheStats, SharedSchemaCache};
