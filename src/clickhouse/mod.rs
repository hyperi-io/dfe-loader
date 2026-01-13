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

pub mod arrow_client;
pub mod circuit_breaker;
pub mod inserter;
pub mod schema;
pub mod types;

pub use arrow_client::{ArrowClickHouseClient, SharedArrowClient};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState};
pub use inserter::{FailedRow, InsertResult, Inserter, InserterConfig};
pub use schema::{SchemaCache, SchemaCacheConfig, SchemaCacheStats, SharedSchemaCache};
pub use types::{ColumnInfo, ParsedType, TableSchema, default_value_for_category, is_null_string, NULL_STRINGS};
