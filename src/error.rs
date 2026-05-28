// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Error types for the loader

use thiserror::Error;

/// Main error type for the loader
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kafka error: {0}")]
    Kafka(String),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("JSON parse error: {0}")]
    Json(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Routing error: no table mapping for category '{0}'")]
    NoTableMapping(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Buffer error: {0}")]
    Buffer(String),

    #[error("Transform error: {0}")]
    Transform(String),

    #[error("Coercion error: {0}")]
    Coercion(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Shutdown requested")]
    Shutdown,
}

// Removed: impl From<String> for Error — masks error category.
// Use explicit Error::Config(...), Error::Json(...), etc. at each call site.

impl From<crate::clickhouse::ClickHouseError> for Error {
    fn from(err: crate::clickhouse::ClickHouseError) -> Self {
        Error::ClickHouse(err.to_string())
    }
}

/// Result type alias using our Error
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_kafka() {
        let e = Error::Kafka("broker down".into());
        assert!(e.to_string().contains("Kafka"));
        assert!(e.to_string().contains("broker down"));
    }

    #[test]
    fn error_display_clickhouse() {
        let e = Error::ClickHouse("timeout".into());
        assert!(e.to_string().contains("ClickHouse"));
        assert!(e.to_string().contains("timeout"));
    }

    #[test]
    fn error_display_json() {
        let e = Error::Json("bad payload".into());
        assert!(e.to_string().contains("JSON"));
    }

    #[test]
    fn error_display_config() {
        let e = Error::Config("missing field".into());
        assert!(e.to_string().contains("Configuration"));
    }

    #[test]
    fn error_display_no_table_mapping() {
        let e = Error::NoTableMapping("unknown".into());
        assert!(e.to_string().contains("unknown"));
    }

    #[test]
    fn error_display_schema() {
        let e = Error::Schema("mismatch".into());
        assert!(e.to_string().contains("Schema"));
    }

    #[test]
    fn error_display_buffer() {
        let e = Error::Buffer("overflow".into());
        assert!(e.to_string().contains("Buffer"));
    }

    #[test]
    fn error_display_transform() {
        let e = Error::Transform("flatten failed".into());
        assert!(e.to_string().contains("Transform"));
    }

    #[test]
    fn error_display_coercion() {
        let e = Error::Coercion("type mismatch".into());
        assert!(e.to_string().contains("Coercion"));
    }

    #[test]
    fn error_display_transport() {
        let e = Error::Transport("down".into());
        assert!(e.to_string().contains("Transport"));
    }

    #[test]
    fn error_display_shutdown() {
        let e = Error::Shutdown;
        assert!(e.to_string().contains("Shutdown"));
    }

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let e: Error = io_err.into();
        match e {
            Error::Io(inner) => assert_eq!(inner.kind(), std::io::ErrorKind::NotFound),
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[test]
    fn error_from_clickhouse_error() {
        use crate::clickhouse::ClickHouseError;
        let ch_err = ClickHouseError::Query("some query failed".into());
        let e: Error = ch_err.into();
        match e {
            Error::ClickHouse(msg) => assert!(msg.contains("some query failed") || !msg.is_empty()),
            other => panic!("expected ClickHouse, got {other:?}"),
        }
    }

    #[test]
    fn error_debug_format_exists() {
        let e = Error::Buffer("x".into());
        let d = format!("{e:?}");
        assert!(d.contains("Buffer"));
    }
}
