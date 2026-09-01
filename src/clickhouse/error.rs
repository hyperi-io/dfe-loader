// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/error.rs
// Purpose:   ClickHouse error types with severity classification
// Language:  Rust
//
// License:   BUSL-1.1
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

    /// Schema introspection error — the query itself failed, so whether the
    /// table exists is unknown.
    #[error("schema error: {0}")]
    Schema(String),

    /// The table is genuinely absent: the introspection query succeeded and
    /// returned no columns. Callers must not conflate this with `Schema`, which
    /// a transient outage also produces.
    #[error("table not found: {0}")]
    TableNotFound(String),

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
            ClickHouseError::Query(_)
            | ClickHouseError::Schema(_)
            | ClickHouseError::TableNotFound(_) => ErrorCategory::Fatal,
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

/// Classify a failure returned by `insert.end()`.
///
/// Drift is checked FIRST and always wins. DFE promotes JSON paths to columns
/// at runtime, so an `ALTER` landing mid-flush is normal operation, and the
/// wording a server picks for it overlaps the data-error patterns -- "cannot
/// parse", "type mismatch", "incorrect data" all appear in both. Reading one
/// of those as a permanent verdict destroys a batch the next schema fetch
/// would have encoded correctly, so drift withholds the offsets and comes
/// back instead.
#[must_use]
pub fn classify_insert_end_error(msg: &str) -> ErrorCategory {
    if is_schema_drift_error(msg) {
        return ErrorCategory::Transient;
    }
    classify_from_message(msg)
}

/// Classify a dynamic-insert (`RowBinary` encode) failure, transient first.
///
/// Permanent means one thing only: this payload can never encode. A message
/// naming a network or resource fault is never a verdict on the payload, so
/// it is tested before the variant is.
///
/// Of the variants, only [`DynamicError::EncodingError`] is a payload verdict
/// -- the encoder held a schema, read the value against it, and could not
/// represent it, which the next delivery repeats exactly. Every other variant
/// clears without the payload changing:
///
/// - `SchemaFetch` -- the `system.columns` query never landed. A ClickHouse
///   blip during the first flush after a restart hits this on a cold cache.
/// - `UnsupportedType` -- THIS BUILD has no encoder for the column type. A
///   new `Variant` or nested `Tuple` column is a loader gap, not bad data.
/// - `EmptySchema` -- the table has no columns yet, i.e. it is mid-creation.
/// - `SchemaMismatch` -- the cached schema is stale; a re-fetch fixes it.
#[must_use]
pub fn classify_dynamic_error(err: &crate::clickhouse_ext::DynamicError) -> ErrorCategory {
    use crate::clickhouse_ext::DynamicError;

    if classify_from_message(&err.to_string()) == ErrorCategory::Transient {
        return ErrorCategory::Transient;
    }
    match err {
        DynamicError::EncodingError { .. } => ErrorCategory::Data,
        DynamicError::SchemaFetch { .. }
        | DynamicError::UnsupportedType { .. }
        | DynamicError::EmptySchema { .. }
        | DynamicError::SchemaMismatch { .. } => ErrorCategory::Transient,
    }
}

/// Classify error from HTTP response message.
#[must_use]
pub fn classify_from_message(msg: &str) -> ErrorCategory {
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

    // Data error patterns. ClickHouse error code 117 is INCORRECT_DATA and the
    // JSON-column rejections below are deterministic for a given payload, so
    // they must never be retried.
    if msg_lower.contains("type mismatch")
        || msg_lower.contains("incorrect data")
        || msg_lower.contains("code: 117")
        || msg_lower.contains("code 117")
        || msg_lower.contains("cannot insert data into json column")
        || msg_lower.contains("cannot read json object")
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
    fn test_table_not_found_is_distinct_from_schema() {
        // The two must never collapse: a failed query says nothing about
        // whether the table exists.
        let absent = ClickHouseError::TableNotFound("dfe.acme_widgets".into());
        let failed = ClickHouseError::Schema("Failed to fetch schema: connection reset".into());

        assert!(matches!(absent, ClickHouseError::TableNotFound(_)));
        assert!(!matches!(failed, ClickHouseError::TableNotFound(_)));
        assert!(absent.to_string().contains("dfe.acme_widgets"));
        assert!(absent.is_fatal());
        assert!(failed.is_fatal());
    }

    #[test]
    fn test_json_column_rejection_is_a_data_error() {
        // ClickHouse code 117 on a JSON column is deterministic for the payload,
        // so it must never be retried.
        let err = ClickHouseError::Insert(
            "server error code 117: Cannot insert data into JSON column: \
             Cannot read JSON object from JSON element"
                .into(),
        );
        assert!(err.is_data_error());
        assert!(!err.is_transient());
    }

    #[test]
    fn test_connection_reset_stays_transient() {
        let err = ClickHouseError::Insert("connection reset by peer".into());
        assert!(err.is_transient());
        assert!(!err.is_data_error());
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

    // ========================================================================
    // Transient-first classification -- nothing reaches a permanent verdict
    // without passing through here.
    // ========================================================================

    use crate::clickhouse_ext::DynamicError;

    fn fetch_failure(message: &str) -> DynamicError {
        DynamicError::SchemaFetch {
            table: "dfe.filebeat".to_string(),
            source: clickhouse::error::Error::Custom(message.to_string()),
        }
    }

    #[test]
    fn schema_fetch_failure_is_transient_not_permanent() {
        // A cold cache issues a live system.columns query on the first flush
        // after a restart. A ClickHouse blip there must not destroy the batch.
        assert_eq!(
            classify_dynamic_error(&fetch_failure("connection reset by peer")),
            ErrorCategory::Transient
        );
        // Even with no transient keyword in the message, the VARIANT decides.
        assert_eq!(
            classify_dynamic_error(&fetch_failure("boom")),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn unsupported_type_is_transient_not_permanent() {
        // This says the LOADER cannot encode the type, not that the payload is
        // bad -- a new Variant or nested Tuple column must not turn its table
        // into a permanent shredder.
        let err = DynamicError::UnsupportedType {
            column: "attributes".to_string(),
            type_str: "Variant(String, UInt64)".to_string(),
        };
        assert_eq!(classify_dynamic_error(&err), ErrorCategory::Transient);
    }

    #[test]
    fn empty_and_mismatched_schema_are_transient() {
        assert_eq!(
            classify_dynamic_error(&DynamicError::EmptySchema {
                table: "dfe.brand_new".to_string(),
            }),
            ErrorCategory::Transient
        );
        assert_eq!(
            classify_dynamic_error(&DynamicError::SchemaMismatch {
                table: "dfe.filebeat".to_string(),
                message: "column count changed".to_string(),
            }),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn only_an_encoding_failure_is_a_payload_verdict() {
        let err = DynamicError::EncodingError {
            column: "source_ip".to_string(),
            message: "expected an IPv4 string, got an object".to_string(),
        };
        assert_eq!(classify_dynamic_error(&err), ErrorCategory::Data);
    }

    #[test]
    fn an_encoding_failure_that_names_a_network_fault_stays_transient() {
        // Transient patterns are tested before the variant is, so a wrapped
        // transport fault never reads as "this payload can never encode".
        let err = DynamicError::EncodingError {
            column: "_json".to_string(),
            message: "connection reset by peer".to_string(),
        };
        assert_eq!(classify_dynamic_error(&err), ErrorCategory::Transient);
    }

    #[test]
    fn drift_wording_beats_the_data_patterns_on_end() {
        // These three strings match BOTH is_schema_drift_error and the Data
        // patterns. Drift self-heals on a re-fetch, so it must win.
        for msg in [
            "Cannot parse input: expected column source_ip",
            "Type mismatch for column agent_type",
            "DB::Exception: INCORRECT_DATA",
            "Unknown column host_name",
        ] {
            assert_eq!(
                classify_insert_end_error(msg),
                ErrorCategory::Transient,
                "drift must withhold offsets, not DLQ: {msg}"
            );
        }
    }

    #[test]
    fn a_non_drift_data_rejection_stays_permanent_on_end() {
        assert_eq!(
            classify_insert_end_error("server error code 117: Cannot insert data into JSON column"),
            ErrorCategory::Data
        );
    }
}
