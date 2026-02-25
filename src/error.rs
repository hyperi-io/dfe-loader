// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Error types for the loader

use thiserror::Error;

/// Main error type for the loader
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kafka error: {0}")]
    KafkaLib(#[from] rdkafka::error::KafkaError),

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

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Config(s)
    }
}

impl From<crate::clickhouse::ClickHouseError> for Error {
    fn from(err: crate::clickhouse::ClickHouseError) -> Self {
        Error::ClickHouse(err.to_string())
    }
}

/// Result type alias using our Error
pub type Result<T> = std::result::Result<T, Error>;
