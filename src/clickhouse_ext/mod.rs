// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/mod.rs
// Purpose:   HyperI dynamic-insert extension over the clickhouse-rs hyperi-port chain
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Dynamic (schema-driven) insert extension for the clickhouse-rs HyperI fork.
//!
//! The `hyperi-port/*` chain ships a single unified `Client` plus public
//! RowBinary/Native primitives, but deliberately keeps insertion typed
//! (`T: Row`, compile-time schema). dfe-loader inserts runtime-shaped
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
//!   recovery, written through the `InsertNative::with_columns` sink.
//!
//! The insert sink is HTTP (FORMAT Native, runtime columns) today. When the
//! fork grows a TCP runtime-column constructor (clickhouse-rs#14) the loader
//! flips the sink with no change to the encoder or the public API here.

pub mod encode;
pub mod error;
pub mod insert;
pub mod parsed_type;
pub mod schema;

pub use encode::{
    ColumnDef, DynamicRow, json_shaping_changes, shape_json_for_type, shape_json_value,
};
pub use error::DynamicError;
pub use insert::DynamicInsert;
pub use parsed_type::{ParsedType, ParsedTypeExt, TypeTag};
pub use schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
