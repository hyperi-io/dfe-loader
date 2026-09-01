// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Shared types for the pipeline processing stages.

use std::sync::Arc;

use serde_json::Value;

use crate::buffer::KafkaOffset;

/// Result of processing a single message through the parallel phase.
///
/// Carries all data needed by the sequential phase (buffer push, mark_pending).
/// Produced by `super::processor::MessageProcessor::process`, consumed by
/// `super::coordinator::BatchCoordinator::apply_results`.
pub struct ProcessedMessage {
    /// Destination table (db.table) from routing.
    pub table: String,
    /// Transformed field map ready for ClickHouse insertion.
    pub data: serde_json::Map<String, Value>,
    /// Raw payload bytes for zero-copy `_json` splice (json_primary path only).
    pub raw_payload: Option<Arc<[u8]>>,
    /// Kafka offset for commit tracking.
    pub kafka_offset: KafkaOffset,
}

/// Outcome of one schema-resolution attempt for a table.
///
/// The two failure arms must stay apart: an absent table falls back to the
/// default table, while an unreachable ClickHouse keeps buffering and retrying.
/// Collapsing them would dump every source's events into the default table
/// during an outage.
pub enum SchemaResolution {
    /// Schema fetched.
    Resolved(crate::clickhouse::TableSchema),
    /// The table does not exist — the query answered, with no columns.
    TableNotFound,
    /// The fetch failed; whether the table exists is unknown.
    Unavailable,
}

/// Result of async per-table schema resolution.
///
/// Fetched by a background resolver task. Applied to per-table caches
/// (capture overrides, field mapping, computed columns, schema cache)
/// when received via the schema result channel.
pub struct TableResolutionResult {
    /// Destination table (db.table).
    pub table: String,
    /// Table-level COMMENT string (for DDL capture tags).
    pub comment: String,
    /// Schema from `system.columns` (for field mapping + `SharedSchemaCache`),
    /// or why it could not be fetched.
    pub schema: SchemaResolution,
    /// Parsed per-column directives (skip/default/renamed/computed/coerce).
    pub column_directives: rustc_hash::FxHashMap<String, crate::column_meta::ColumnDirectives>,
}
