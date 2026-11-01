// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/error.rs
// Purpose:   DynamicError for schema-driven inserts
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for dynamic (schema-driven) inserts.

use std::fmt;

/// Errors specific to dynamic schema-driven inserts.
#[derive(Debug)]
pub enum DynamicError {
    /// Column type string could not be parsed.
    UnsupportedType { column: String, type_str: String },
    /// Value could not be encoded for the target column type.
    EncodingError { column: String, message: String },
    /// Schema mismatch detected -- server rejected the insert.
    SchemaMismatch { table: String, message: String },
    /// Schema fetch from system.columns failed.
    SchemaFetch {
        table: String,
        source: dfe_clickhouse::Error,
    },
    /// The client cannot read or print a column the server sent, which the
    /// same query repeats exactly.
    ClientUnsupported {
        table: String,
        source: dfe_clickhouse::Error,
    },
    /// Table has no columns (or does not exist).
    EmptySchema { table: String },
    /// A schema fetch or insert ran past its client-side bound.
    TimedOut { table: String },
}

impl fmt::Display for DynamicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType { column, type_str } => {
                write!(f, "unsupported type '{type_str}' for column '{column}'")
            }
            Self::EncodingError { column, message } => {
                write!(f, "encoding error for column '{column}': {message}")
            }
            Self::SchemaMismatch { table, message } => {
                write!(f, "schema mismatch for table '{table}': {message}")
            }
            Self::SchemaFetch { table, source } => {
                write!(
                    f,
                    "failed to fetch schema for '{table}': {}",
                    crate::clickhouse::error::native_error_text(source)
                )
            }
            Self::ClientUnsupported { table, source } => {
                write!(
                    f,
                    "the client cannot represent a column for '{table}': {}",
                    crate::clickhouse::error::native_error_text(source)
                )
            }
            Self::EmptySchema { table } => {
                write!(f, "table '{table}' has no columns or does not exist")
            }
            Self::TimedOut { table } => {
                write!(f, "request to ClickHouse for '{table}' timed out")
            }
        }
    }
}

impl std::error::Error for DynamicError {}
