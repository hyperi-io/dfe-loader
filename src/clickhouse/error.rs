// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/error.rs
// Purpose:   ClickHouse error types with severity classification
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! ClickHouse-specific error types with severity classification.
//!
//! ## Error Classification
//!
//! Errors are classified by severity to determine retry strategy:
//!
//! - **Transient** (Server/Protocol): Backoff and retry, never DLQ
//!   - Server overloaded, memory limits, network timeouts
//! - **Data**: Salvage to isolate bad rows, DLQ those rows only
//!   - Type mismatches, corrupt data, value out of range
//! - **Fatal**: Don't retry, fail the batch
//!   - Auth failures, unknown table, syntax errors

use thiserror::Error;

/// Errors from `ClickHouse` operations.
#[derive(Error, Debug)]
pub enum ClickHouseError {
    /// Connection error.
    #[error("connection error: {0}")]
    Connection(String),

    /// Query execution error.
    #[error("query error: {0}")]
    Query(String),

    /// Insert operation error.
    #[error("insert error: {0}")]
    Insert(String),

    /// Schema introspection error.
    #[error("schema error: {0}")]
    Schema(String),

    /// Type conversion error.
    #[error("type conversion error: {0}")]
    TypeConversion(String),
}

/// Error category for retry/DLQ decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Transient server/network error - backoff and retry, never DLQ.
    Transient,

    /// Data error - salvage to isolate bad rows, DLQ those rows.
    Data,

    /// Fatal error - don't retry, fail immediately.
    Fatal,

    /// Unknown error - treat as transient (conservative).
    Unknown,
}

impl ClickHouseError {
    /// Classify this error for retry/DLQ decisions.
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            ClickHouseError::Connection(_) => ErrorCategory::Transient,
            ClickHouseError::Query(_) | ClickHouseError::Schema(_) => ErrorCategory::Fatal,
            ClickHouseError::TypeConversion(_) => ErrorCategory::Data,
            ClickHouseError::Insert(msg) => classify_from_message(msg),
        }
    }

    /// Returns true if this error is transient and should be retried with backoff.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(
            self.category(),
            ErrorCategory::Transient | ErrorCategory::Unknown
        )
    }

    /// Returns true if this error indicates bad data that should be salvaged/DLQ'd.
    #[must_use]
    pub fn is_data_error(&self) -> bool {
        matches!(self.category(), ErrorCategory::Data)
    }

    /// Returns true if this error is fatal and should not be retried.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self.category(), ErrorCategory::Fatal)
    }
}

/// Returns true if the error message suggests the RowBinary schema may be stale.
///
/// These are errors where ClickHouse rejected data because the encoding
/// doesn't match the current table schema — different from `SchemaMismatch`
/// which the fork detects locally during `write_map()`.
pub fn is_schema_drift_error(err: &str) -> bool {
    let lower = err.to_lowercase();
    lower.contains("cannot parse")
        || lower.contains("type mismatch")
        || lower.contains("incorrect_data")
        || lower.contains("incorrect data")
        || lower.contains("unknown column")
        || lower.contains("no such column")
}

/// Returns true if the error indicates ClickHouse JSON column hit `max_dynamic_paths` limit.
///
/// Data is NOT lost — paths exceeding the limit are stored in shared data (slower queries).
/// The warning should be debounced (5 min) since this fires on every affected row.
pub fn is_max_dynamic_paths_error(err: &str) -> bool {
    err.contains("max_dynamic_paths")
        || err.contains("Cannot add new dynamic path")
        || (err.contains("LOGICAL_ERROR") && err.contains("dynamic path"))
}

/// Classify error from HTTP response message.
fn classify_from_message(msg: &str) -> ErrorCategory {
    let msg_lower = msg.to_lowercase();

    // Transient patterns
    if msg_lower.contains("overload")
        || msg_lower.contains("memory limit")
        || msg_lower.contains("timeout")
        || msg_lower.contains("too many")
        || msg_lower.contains("network")
        || msg_lower.contains("connection")
        || msg_lower.contains("socket")
    {
        return ErrorCategory::Transient;
    }

    // Data error patterns
    if msg_lower.contains("type mismatch")
        || msg_lower.contains("incorrect data")
        || msg_lower.contains("corrupt")
        || msg_lower.contains("out of range")
        || msg_lower.contains("cannot parse")
        || msg_lower.contains("invalid value")
        || msg_lower.contains("value is too big")
        || msg_lower.contains("too large value")
        || msg_lower.contains("overflow")
        || msg_lower.contains("invalid ipv")
        || msg_lower.contains("cannot parse uuid")
        || msg_lower.contains("unknown element")
        || msg_lower.contains("too long for type")
    {
        return ErrorCategory::Data;
    }

    // Fatal patterns
    if msg_lower.contains("unknown table")
        || msg_lower.contains("unknown database")
        || msg_lower.contains("syntax error")
        || msg_lower.contains("access denied")
        || msg_lower.contains("authentication")
    {
        return ErrorCategory::Fatal;
    }

    ErrorCategory::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error_is_transient() {
        let err = ClickHouseError::Connection("network timeout".into());
        assert!(err.is_transient());
        assert!(!err.is_data_error());
        assert!(!err.is_fatal());
    }

    #[test]
    fn test_query_error_is_fatal() {
        let err = ClickHouseError::Query("syntax error".into());
        assert!(err.is_fatal());
        assert!(!err.is_transient());
    }

    #[test]
    fn test_type_conversion_is_data_error() {
        let err = ClickHouseError::TypeConversion("cannot convert".into());
        assert!(err.is_data_error());
        assert!(!err.is_transient());
    }

    #[test]
    fn test_classify_from_message_overload() {
        let err = ClickHouseError::Insert("Server overloaded, too many requests".into());
        assert!(err.is_transient());
    }

    #[test]
    fn test_classify_from_message_type_mismatch() {
        let err = ClickHouseError::Insert("Type mismatch for column 'x'".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_unknown_table() {
        let err = ClickHouseError::Insert("Unknown table 'foo.bar'".into());
        assert!(err.is_fatal());
    }

    #[test]
    fn test_is_schema_drift_error() {
        assert!(super::is_schema_drift_error(
            "Cannot parse JSON object here"
        ));
        assert!(super::is_schema_drift_error(
            "DB::Exception: INCORRECT_DATA"
        ));
        assert!(super::is_schema_drift_error(
            "Type mismatch for column 'foo'"
        ));
        assert!(super::is_schema_drift_error("Unknown column 'bar'"));
        assert!(super::is_schema_drift_error("No such column 'baz'"));
        assert!(!super::is_schema_drift_error("Network timeout"));
        assert!(!super::is_schema_drift_error("Authentication failed"));
        assert!(!super::is_schema_drift_error("Server overloaded"));
    }

    #[test]
    fn test_classify_from_message_unknown() {
        let err = ClickHouseError::Insert("Something weird happened".into());
        assert_eq!(err.category(), ErrorCategory::Unknown);
        assert!(err.is_transient());
    }

    #[test]
    fn test_classify_from_message_decimal_overflow() {
        let err = ClickHouseError::Insert("Decimal value is too big: 21 digits".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_fixedstring_too_long() {
        let err = ClickHouseError::Insert("String too long for type FixedString(16)".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_datetime_overflow() {
        let err = ClickHouseError::Insert("DateTime64 convert overflow".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_invalid_ipv4() {
        let err = ClickHouseError::Insert("Invalid IPv4 value".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_invalid_uuid() {
        let err = ClickHouseError::Insert("Cannot parse UUID from string".into());
        assert!(err.is_data_error());
    }

    #[test]
    fn test_classify_from_message_enum_unknown() {
        let err = ClickHouseError::Insert("Unknown element 'foo' for type Enum8".into());
        assert!(err.is_data_error());
    }
}
