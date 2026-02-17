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

// Re-export clickhouse-arrow error types for classification
pub use clickhouse_arrow::native::error_codes::Severity;
pub use clickhouse_arrow::ServerError;

/// Errors from ClickHouse operations.
#[derive(Error, Debug)]
pub enum ClickHouseError {
    /// Connection error.
    #[error("connection error: {0}")]
    Connection(String),

    /// Query execution error.
    #[error("query error: {0}")]
    Query(String),

    /// Insert operation error (string message, no classification).
    #[error("insert error: {0}")]
    Insert(String),

    /// Insert error with preserved server error for classification.
    #[error("insert error: {0}")]
    InsertServer(#[from] clickhouse_arrow::Error),

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
    /// Examples: ServerOverloaded, MemoryLimitExceeded, NetworkError, SocketTimeout
    Transient,

    /// Data error - salvage to isolate bad rows, DLQ those rows.
    /// Examples: TypeMismatch, IncorrectData, ValueOutOfRange, CorruptedData
    Data,

    /// Fatal error - don't retry, fail immediately.
    /// Examples: AuthFailure, UnknownTable, SyntaxError
    Fatal,

    /// Unknown error - treat as transient (conservative).
    Unknown,
}

impl ClickHouseError {
    /// Classify this error for retry/DLQ decisions.
    ///
    /// Returns the error category:
    /// - `Transient`: Backoff and retry (server overload, network issues)
    /// - `Data`: Salvage batch to isolate bad rows for DLQ
    /// - `Fatal`: Don't retry (auth, schema, syntax errors)
    /// - `Unknown`: Treat as transient (conservative approach)
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            // Preserved server errors - use clickhouse-arrow classification
            ClickHouseError::InsertServer(e) => classify_arrow_error(e),

            // Connection errors are transient (network issues)
            ClickHouseError::Connection(_) => ErrorCategory::Transient,

            // Query/Schema errors are typically fatal (bad SQL, missing table)
            ClickHouseError::Query(_) | ClickHouseError::Schema(_) => ErrorCategory::Fatal,

            // Type conversion is a data error
            ClickHouseError::TypeConversion(_) => ErrorCategory::Data,

            // String-based insert errors - try to classify from message
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

/// Classify a clickhouse-arrow error using its Severity.
fn classify_arrow_error(e: &clickhouse_arrow::Error) -> ErrorCategory {
    match e {
        clickhouse_arrow::Error::ServerException(server_err) => {
            match &server_err.error {
                // Server errors - transient, backoff and retry
                Severity::Server(_) => ErrorCategory::Transient,

                // Protocol errors - mostly transient (network, timeout)
                Severity::Protocol(_) => ErrorCategory::Transient,

                // Data errors - salvage and DLQ bad rows
                Severity::Data(_) => ErrorCategory::Data,

                // Query/Syntax errors - fatal
                Severity::Query(_) | Severity::Syntax(_) => ErrorCategory::Fatal,

                // Unknown - treat as transient (conservative)
                Severity::Unknown(_) => ErrorCategory::Unknown,
            }
        }

        // Network/IO errors are transient
        clickhouse_arrow::Error::Io(_)
        | clickhouse_arrow::Error::Network(_)
        | clickhouse_arrow::Error::ConnectionTimeout(_)
        | clickhouse_arrow::Error::ConnectionGone(_)
        | clickhouse_arrow::Error::ChannelClosed
        | clickhouse_arrow::Error::OutgoingTimeout(_) => ErrorCategory::Transient,

        // Type/serialization errors are data errors
        clickhouse_arrow::Error::TypeConversion(_)
        | clickhouse_arrow::Error::SerializeError(_)
        | clickhouse_arrow::Error::DeserializeError(_)
        | clickhouse_arrow::Error::ArrowTypeMismatch { .. }
        | clickhouse_arrow::Error::ArrowSerialize(_) => ErrorCategory::Data,

        // Protocol/startup errors are fatal
        clickhouse_arrow::Error::Protocol(_)
        | clickhouse_arrow::Error::StartupError
        | clickhouse_arrow::Error::MissingConnectionInformation
        | clickhouse_arrow::Error::MalformedConnectionInformation(_) => ErrorCategory::Fatal,

        // Default to unknown (conservative - will retry)
        _ => ErrorCategory::Unknown,
    }
}

/// Classify error from string message (fallback for wrapped errors).
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
        || msg_lower.contains("value is too big")     // Decimal overflow
        || msg_lower.contains("too large value")      // FixedString
        || msg_lower.contains("overflow")             // DateTime64, numeric
        || msg_lower.contains("invalid ipv")          // IPv4/IPv6
        || msg_lower.contains("cannot parse uuid")    // UUID
        || msg_lower.contains("unknown element")      // Enum
        || msg_lower.contains("too long for type")
    // String too long for FixedString
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

    // Default to unknown
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
    fn test_classify_from_message_unknown() {
        let err = ClickHouseError::Insert("Something weird happened".into());
        assert_eq!(err.category(), ErrorCategory::Unknown);
        // Unknown is treated as transient (conservative)
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
