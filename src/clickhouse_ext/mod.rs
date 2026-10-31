// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/mod.rs
// Purpose:   HyperI dynamic-insert extension over the official ClickHouse client
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Dynamic (schema-driven) insert extension for the `clickhouse` crate.
//!
//! The crate's `Client` keeps insertion typed (`T: Row`, compile-time
//! schema). dfe-loader inserts runtime-shaped
//! `serde_json::Map<String, Value>` rows whose columns come from
//! `system.columns` at runtime. That dynamic layer is HyperI-specific and lives
//! here, on top of the upstream `Client`:
//!
//! - [`ParsedType`] -- runtime ClickHouse type-string parser.
//! - [`ColumnDef`] / [`DynamicRow`] -- the encoder: `Map<String, Value>` to
//!   RowBinary, per column, driven by the parsed type.
//! - [`DynamicSchema`] / [`fetch_dynamic_schema`] / [`DynamicSchemaCache`] --
//!   schema reflection from `system.columns`.
//! - [`DynamicInsert`] -- the single-table insert, with schema-mismatch
//!   recovery, written over HTTP as `FORMAT RowBinaryWithNamesAndTypes`.

pub mod encode;
pub mod error;
pub mod insert;
pub mod parsed_type;
pub mod schema;

pub use encode::{
    ColumnDef, DynamicRow, needs_shaping, shape_for_type, shape_json_value, shaping_changes,
};
pub use error::DynamicError;
pub use insert::DynamicInsert;
pub use parsed_type::{ParsedType, ParsedTypeExt, TypeTag};
pub use schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
