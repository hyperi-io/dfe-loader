//! Configuration structures and loading
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--kafka-brokers, --clickhouse-hosts, etc.)
//!   2. Environment variables (LOADER_KAFKA_BROKERS, etc.)
//!   3. .env file
//!   4. Config file specified by --config or LOADER_CONFIG
//!   5. Hard-coded defaults

use std::collections::HashMap;
use std::path::Path;

use config::{Config as ConfigBuilder, Environment, File, FileFormat};
use serde::{Deserialize, Serialize};

use crate::Result;

/// Main configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub clickhouse: ClickHouseConfig,
    pub payload: PayloadConfig,
    pub routing: RoutingConfig,
    pub buffer: BufferConfig,
    pub memory: MemoryConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub timestamp_dq: TimestampDqConfig,
    pub field_sanitization: FieldSanitizationConfig,
    pub metadata: MetadataConfig,
    pub coercion: CoercionConfig,
    pub schema: SchemaConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            kafka: KafkaConfig::default(),
            clickhouse: ClickHouseConfig::default(),
            payload: PayloadConfig::default(),
            routing: RoutingConfig::default(),
            buffer: BufferConfig::default(),
            memory: MemoryConfig::default(),
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            timestamp_dq: TimestampDqConfig::default(),
            field_sanitization: FieldSanitizationConfig::default(),
            metadata: MetadataConfig::default(),
            coercion: CoercionConfig::default(),
            schema: SchemaConfig::default(),
        }
    }
}

// ============================================================================
// Kafka Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group: String,
    pub topics: Vec<String>,
    pub topic_regex: Option<String>,
    pub client_id: String,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            group: "clickhouse-loader".to_string(),
            topics: vec!["events".to_string()],
            topic_regex: None,
            client_id: "clickhouse-loader".to_string(),
            sasl: None,
            tls: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    pub enabled: bool,
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: "SCRAM-SHA-512".to_string(),
            username: String::new(),
            password: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert_file: None,
            cert_file: None,
            key_file: None,
            skip_verify: false,
        }
    }
}

// ============================================================================
// ClickHouse Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClickHouseConfig {
    pub hosts: Vec<String>,
    pub database: String,
    pub username: String,
    pub password: String,
    pub protocol: String,
    pub tables: Vec<String>,
    pub tls: Option<TlsConfig>,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost:9000".to_string()],
            database: "default".to_string(),
            username: "default".to_string(),
            password: String::new(),
            protocol: "native".to_string(),
            tables: Vec::new(),
            tls: None,
        }
    }
}

// ============================================================================
// Payload Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PayloadConfig {
    /// Format mode: "auto" (default), "json", "messagepack"/"msgpack"
    pub format: String,
    /// Mismatch threshold before auto-reset (auto mode only)
    pub mismatch_threshold: u8,
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self {
            format: "auto".to_string(),
            mismatch_threshold: 10,
        }
    }
}

// ============================================================================
// Routing Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    pub routing_field: String,
    pub fallback_field: Option<String>,
    pub category_to_table: HashMap<String, String>,
    pub mapping_file: Option<String>,
    pub default_table: Option<String>,
    pub dlq: DlqConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            routing_field: "event_category".to_string(),
            fallback_field: Some("tags.event_category".to_string()),
            category_to_table: HashMap::new(),
            mapping_file: None,
            default_table: None,
            dlq: DlqConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    pub enabled: bool,
    pub topic_suffix: String,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topic_suffix: ".dlq".to_string(),
        }
    }
}

// ============================================================================
// Buffer Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BufferConfig {
    pub flush_bytes: usize,
    pub flush_rows: usize,
    pub flush_age_secs: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 1_048_576, // 1MB
            flush_rows: 10_000,
            flush_age_secs: 5,
        }
    }
}

// ============================================================================
// Memory Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    /// Maximum memory for buffers in bytes. 0 = auto-detect (67% of available)
    pub limit_bytes: usize,
    /// Pressure threshold (0.0-1.0) - pause consumption above this
    pub pressure_threshold: f64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            limit_bytes: 0, // Auto-detect
            pressure_threshold: 0.8,
        }
    }
}

// ============================================================================
// Metrics Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0:9090".to_string(),
        }
    }
}

// ============================================================================
// Logging Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
        }
    }
}

// ============================================================================
// Timestamp Data Quality Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TimestampDqConfig {
    pub enabled: bool,
    pub max_future_seconds: i64,
    pub max_past_seconds: i64,
    pub invalid_action: String,
    pub correct_known_bad: bool,
}

impl Default for TimestampDqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_future_seconds: 600,
            max_past_seconds: 0, // 0 = no limit
            invalid_action: "replace_with_now".to_string(),
            correct_known_bad: true,
        }
    }
}

// ============================================================================
// Field Sanitization Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FieldSanitizationConfig {
    pub strip_at_prefix: bool,
    pub handle_numeric_prefix: bool,
    pub numeric_prefix: String,
    pub collapse_underscores: bool,
    pub trim_underscores: bool,
    pub collision_strategy: String,
}

impl Default for FieldSanitizationConfig {
    fn default() -> Self {
        Self {
            strip_at_prefix: true,
            handle_numeric_prefix: true,
            numeric_prefix: "col_".to_string(),
            collapse_underscores: true,
            trim_underscores: true,
            collision_strategy: "last_wins".to_string(),
        }
    }
}

// ============================================================================
// Metadata Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetadataConfig {
    pub inject_timestamp_load: bool,
    pub extract_timestamp_collector: bool,
    pub collector_timestamp_path: String,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            inject_timestamp_load: true,
            extract_timestamp_collector: true,
            collector_timestamp_path: "tags.collector.timestamp".to_string(),
        }
    }
}

// ============================================================================
// Type Coercion Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CoercionConfig {
    pub unknown_type_fallback: String,
    pub array_to_json: bool,
}

impl Default for CoercionConfig {
    fn default() -> Self {
        Self {
            unknown_type_fallback: "String".to_string(),
            array_to_json: true,
        }
    }
}

// ============================================================================
// Schema Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaConfig {
    pub cache_ttl_secs: u64,
    pub refresh_on_error: bool,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            cache_ttl_secs: 300, // 5 minutes
            refresh_on_error: true,
        }
    }
}

// ============================================================================
// Configuration Loading
// ============================================================================

impl Config {
    /// Load configuration with cascade:
    /// 1. CLI args (applied separately after load)
    /// 2. Environment variables (LOADER_*)
    /// 3. .env file
    /// 4. Config file
    /// 5. Defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // Load .env file if present (before building config)
        let _ = dotenvy::dotenv();

        let mut builder = ConfigBuilder::builder();

        // Start with defaults
        builder = builder.add_source(config::Config::try_from(&Config::default())?);

        // Add config file if specified
        if let Some(path) = config_path {
            if Path::new(path).exists() {
                builder = builder.add_source(File::new(path, FileFormat::Yaml));
            }
        } else {
            // Try default config paths
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    builder = builder.add_source(File::new(path, FileFormat::Yaml));
                    break;
                }
            }
        }

        // Add environment variables with LOADER_ prefix
        // LOADER_KAFKA_BROKERS -> kafka.brokers
        builder = builder.add_source(
            Environment::with_prefix("LOADER")
                .separator("_")
                .list_separator(",")
                .with_list_parse_key("kafka.brokers")
                .with_list_parse_key("kafka.topics")
                .with_list_parse_key("clickhouse.hosts")
                .with_list_parse_key("clickhouse.tables")
                .try_parsing(true),
        );

        let config = builder.build()?;
        let result: Config = config.try_deserialize()?;

        Ok(result)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Kafka validation
        if self.kafka.brokers.is_empty() {
            return Err(crate::Error::Config(
                "At least one Kafka broker must be configured".into(),
            ));
        }
        if self.kafka.topics.is_empty() && self.kafka.topic_regex.is_none() {
            return Err(crate::Error::Config(
                "Either topics or topic_regex must be configured".into(),
            ));
        }

        // ClickHouse validation
        if self.clickhouse.hosts.is_empty() {
            return Err(crate::Error::Config(
                "At least one ClickHouse host must be configured".into(),
            ));
        }

        // Memory validation
        if self.memory.pressure_threshold < 0.0 || self.memory.pressure_threshold > 1.0 {
            return Err(crate::Error::Config(
                "memory.pressure_threshold must be between 0.0 and 1.0".into(),
            ));
        }

        // Buffer validation
        if self.buffer.flush_bytes == 0 {
            return Err(crate::Error::Config(
                "buffer.flush_bytes must be greater than 0".into(),
            ));
        }
        if self.buffer.flush_rows == 0 {
            return Err(crate::Error::Config(
                "buffer.flush_rows must be greater than 0".into(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.kafka.brokers, vec!["localhost:9092"]);
        assert_eq!(config.kafka.group, "clickhouse-loader");
        assert_eq!(config.clickhouse.hosts, vec!["localhost:9000"]);
        assert_eq!(config.buffer.flush_bytes, 1_048_576);
    }

    #[test]
    fn test_config_validation_empty_brokers() {
        let mut config = Config::default();
        config.kafka.brokers = vec![];
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_no_topics() {
        let mut config = Config::default();
        config.kafka.topics = vec![];
        config.kafka.topic_regex = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_pressure() {
        let mut config = Config::default();
        config.memory.pressure_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_valid() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }
}
