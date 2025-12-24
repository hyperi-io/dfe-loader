// Project:   dfe-loader-clickhouse
// File:      mod.rs
// Purpose:   ClickHouse client abstraction
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse client abstraction

pub mod arrow_client;
pub mod client;
pub mod inserter;
pub mod salvage;
pub mod schema;
pub mod types;

pub use arrow_client::{ArrowClickHouseClient, SharedArrowClient};
pub use client::{ClickHouseClient, ColumnInfo, SharedClickHouseClient, TableSchema};
pub use inserter::{Inserter, InserterConfig};
pub use schema::SchemaCache;
pub use types::{ParsedType, default_value_for_category, is_null_string, NULL_STRINGS};
