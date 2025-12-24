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

/// SASL authentication mechanism
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
#[allow(clippy::upper_case_acronyms)]
pub enum SaslMechanism {
    /// No authentication (dev/test only - full admin access)
    None,
    /// SASL/PLAIN - simple username/password (use with TLS!)
    Plain,
    /// SASL/SCRAM-SHA-256
    ScramSha256,
    /// SASL/SCRAM-SHA-512 (recommended for production)
    #[default]
    ScramSha512,
    /// SASL/OAUTHBEARER - OAuth 2.0 / OIDC token authentication
    OAuthBearer,
    /// AWS MSK IAM authentication
    AwsMskIam,
}

impl SaslMechanism {
    /// Get the rdkafka mechanism string
    pub fn as_rdkafka_mechanism(&self) -> Option<&'static str> {
        match self {
            SaslMechanism::None => None,
            SaslMechanism::Plain => Some("PLAIN"),
            SaslMechanism::ScramSha256 => Some("SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => Some("SCRAM-SHA-512"),
            SaslMechanism::OAuthBearer => Some("OAUTHBEARER"),
            SaslMechanism::AwsMskIam => Some("OAUTHBEARER"), // AWS IAM uses OAUTHBEARER
        }
    }

    /// Check if this mechanism requires username/password
    pub fn requires_credentials(&self) -> bool {
        matches!(
            self,
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512
        )
    }

    /// Check if this mechanism uses OAuth
    pub fn is_oauth(&self) -> bool {
        matches!(self, SaslMechanism::OAuthBearer)
    }

    /// Check if this mechanism uses AWS IAM
    pub fn is_aws_iam(&self) -> bool {
        matches!(self, SaslMechanism::AwsMskIam)
    }
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::None => write!(f, "NONE"),
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
            SaslMechanism::OAuthBearer => write!(f, "OAUTHBEARER"),
            SaslMechanism::AwsMskIam => write!(f, "AWS_MSK_IAM"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL authentication
    pub enabled: bool,
    /// SASL mechanism to use
    pub mechanism: SaslMechanism,

    // --- Username/Password auth (PLAIN, SCRAM-*) ---
    pub username: String,
    pub password: String,

    // --- OAuth 2.0 / OIDC auth (OAUTHBEARER) ---
    /// OAuth token endpoint URL
    pub oauth_token_endpoint: Option<String>,
    /// OAuth client ID
    pub oauth_client_id: Option<String>,
    /// OAuth client secret
    pub oauth_client_secret: Option<String>,
    /// OAuth scope (space-separated)
    pub oauth_scope: Option<String>,
    /// OAuth extensions (key=value pairs)
    pub oauth_extensions: Option<String>,

    // --- AWS MSK IAM auth ---
    /// AWS region for MSK IAM
    pub aws_region: Option<String>,
    /// AWS access key ID (optional - can use instance profile/environment)
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key
    pub aws_secret_access_key: Option<String>,
    /// AWS session token (for temporary credentials)
    pub aws_session_token: Option<String>,
    /// AWS profile name (alternative to explicit credentials)
    pub aws_profile: Option<String>,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: SaslMechanism::default(),
            username: String::new(),
            password: String::new(),
            oauth_token_endpoint: None,
            oauth_client_id: None,
            oauth_client_secret: None,
            oauth_scope: None,
            oauth_extensions: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_profile: None,
        }
    }
}

impl SaslConfig {
    /// Validate the SASL configuration based on mechanism
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        match self.mechanism {
            SaslMechanism::None => {
                // No validation needed - this is explicitly insecure
            }
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                if self.username.is_empty() {
                    return Err(format!("{} requires username", self.mechanism));
                }
                if self.password.is_empty() {
                    return Err(format!("{} requires password", self.mechanism));
                }
            }
            SaslMechanism::OAuthBearer => {
                if self.oauth_token_endpoint.is_none() {
                    return Err("OAUTHBEARER requires oauth_token_endpoint".to_string());
                }
                if self.oauth_client_id.is_none() {
                    return Err("OAUTHBEARER requires oauth_client_id".to_string());
                }
            }
            SaslMechanism::AwsMskIam => {
                // AWS region is required; credentials can come from environment/instance profile
                if self.aws_region.is_none() {
                    return Err("AWS_MSK_IAM requires aws_region".to_string());
                }
            }
        }

        Ok(())
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

/// Null handling strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NullHandling {
    /// Substitute type-appropriate default value (recommended)
    #[default]
    Default,
    /// Return error for null in non-nullable column
    Error,
    /// Pass null through (may cause ClickHouse errors)
    Passthrough,
}

/// Type coercion configuration
///
/// Following the Go clickhouse-loader pattern:
/// - Type mappings for custom types
/// - Configurable null handling
/// - Timezone handling for naive timestamps
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CoercionConfig {
    /// Map custom type names to base coercer categories
    /// e.g., {"MyCustomInt": "Int", "SpecialString": "String"}
    pub type_mappings: std::collections::HashMap<String, String>,

    /// Fallback coercer for unknown types (default: "String")
    pub unknown_type_fallback: String,

    /// How to handle null values in non-nullable columns
    pub null_handling: NullHandling,

    /// Strings to recognise as null (case-sensitive)
    pub null_strings: Vec<String>,

    /// Default timezone for naive timestamps (IANA name or +HH:MM)
    pub default_timezone: String,

    /// Event fields to check for timezone info
    pub timezone_fields: Vec<String>,

    /// Convert arrays to JSON strings if true
    pub array_to_json: bool,

    /// Strict mode: fail on any coercion error
    pub strict: bool,
}

impl Default for CoercionConfig {
    fn default() -> Self {
        Self {
            type_mappings: std::collections::HashMap::new(),
            unknown_type_fallback: "String".to_string(),
            null_handling: NullHandling::Default,
            null_strings: vec![
                "null".to_string(),
                "NULL".to_string(),
                "Null".to_string(),
                "None".to_string(),
                "nil".to_string(),
                "undefined".to_string(),
                "\\N".to_string(),
                "<null>".to_string(),
                "NA".to_string(),
                "N/A".to_string(),
                "n/a".to_string(),
                "NaN".to_string(),
            ],
            default_timezone: "UTC".to_string(),
            timezone_fields: vec!["tags_collector_timezone".to_string()],
            array_to_json: true,
            strict: false,
        }
    }
}

impl CoercionConfig {
    /// Check if a string value should be treated as null
    pub fn is_null_string(&self, value: &str) -> bool {
        value.is_empty() || self.null_strings.iter().any(|s| s == value)
    }

    /// Get the coercer category for a type, checking custom mappings first
    pub fn get_coercer_category(&self, type_name: &str) -> &str {
        self.type_mappings
            .get(type_name)
            .map(|s| s.as_str())
            .unwrap_or(&self.unknown_type_fallback)
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

    // ========================================================================
    // SASL Mechanism Tests
    // ========================================================================

    #[test]
    fn test_sasl_mechanism_default() {
        let mech = SaslMechanism::default();
        assert_eq!(mech, SaslMechanism::ScramSha512);
    }

    #[test]
    fn test_sasl_mechanism_display() {
        assert_eq!(SaslMechanism::None.to_string(), "NONE");
        assert_eq!(SaslMechanism::Plain.to_string(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.to_string(), "SCRAM-SHA-512");
        assert_eq!(SaslMechanism::OAuthBearer.to_string(), "OAUTHBEARER");
        assert_eq!(SaslMechanism::AwsMskIam.to_string(), "AWS_MSK_IAM");
    }

    #[test]
    fn test_sasl_mechanism_rdkafka_mapping() {
        assert_eq!(SaslMechanism::None.as_rdkafka_mechanism(), None);
        assert_eq!(SaslMechanism::Plain.as_rdkafka_mechanism(), Some("PLAIN"));
        assert_eq!(SaslMechanism::ScramSha256.as_rdkafka_mechanism(), Some("SCRAM-SHA-256"));
        assert_eq!(SaslMechanism::ScramSha512.as_rdkafka_mechanism(), Some("SCRAM-SHA-512"));
        assert_eq!(SaslMechanism::OAuthBearer.as_rdkafka_mechanism(), Some("OAUTHBEARER"));
        assert_eq!(SaslMechanism::AwsMskIam.as_rdkafka_mechanism(), Some("OAUTHBEARER"));
    }

    #[test]
    fn test_sasl_mechanism_requires_credentials() {
        assert!(!SaslMechanism::None.requires_credentials());
        assert!(SaslMechanism::Plain.requires_credentials());
        assert!(SaslMechanism::ScramSha256.requires_credentials());
        assert!(SaslMechanism::ScramSha512.requires_credentials());
        assert!(!SaslMechanism::OAuthBearer.requires_credentials());
        assert!(!SaslMechanism::AwsMskIam.requires_credentials());
    }

    #[test]
    fn test_sasl_mechanism_is_oauth() {
        assert!(!SaslMechanism::None.is_oauth());
        assert!(!SaslMechanism::Plain.is_oauth());
        assert!(SaslMechanism::OAuthBearer.is_oauth());
        assert!(!SaslMechanism::AwsMskIam.is_oauth());
    }

    #[test]
    fn test_sasl_mechanism_is_aws_iam() {
        assert!(!SaslMechanism::None.is_aws_iam());
        assert!(!SaslMechanism::Plain.is_aws_iam());
        assert!(!SaslMechanism::OAuthBearer.is_aws_iam());
        assert!(SaslMechanism::AwsMskIam.is_aws_iam());
    }

    // ========================================================================
    // SASL Config Validation Tests
    // ========================================================================

    #[test]
    fn test_sasl_config_disabled_no_validation() {
        let config = SaslConfig {
            enabled: false,
            mechanism: SaslMechanism::ScramSha512,
            username: String::new(), // Empty but OK because disabled
            password: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_none_no_credentials_needed() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::None,
            username: String::new(),
            password: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_scram_requires_username() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::ScramSha512,
            username: String::new(),
            password: "secret".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires username"));
    }

    #[test]
    fn test_sasl_config_scram_requires_password() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::ScramSha512,
            username: "user".to_string(),
            password: String::new(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires password"));
    }

    #[test]
    fn test_sasl_config_scram_valid() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::ScramSha512,
            username: "user".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_plain_valid() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::Plain,
            username: "user".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_oauth_requires_endpoint() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::OAuthBearer,
            oauth_token_endpoint: None,
            oauth_client_id: Some("client".to_string()),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("oauth_token_endpoint"));
    }

    #[test]
    fn test_sasl_config_oauth_requires_client_id() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::OAuthBearer,
            oauth_token_endpoint: Some("https://auth.example.com/token".to_string()),
            oauth_client_id: None,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("oauth_client_id"));
    }

    #[test]
    fn test_sasl_config_oauth_valid() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::OAuthBearer,
            oauth_token_endpoint: Some("https://auth.example.com/token".to_string()),
            oauth_client_id: Some("my-client".to_string()),
            oauth_client_secret: Some("secret".to_string()),
            oauth_scope: Some("kafka".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_aws_iam_requires_region() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::AwsMskIam,
            aws_region: None,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("aws_region"));
    }

    #[test]
    fn test_sasl_config_aws_iam_valid_with_region_only() {
        // AWS IAM can use instance profile/environment for credentials
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::AwsMskIam,
            aws_region: Some("ap-southeast-2".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_aws_iam_valid_with_explicit_creds() {
        let config = SaslConfig {
            enabled: true,
            mechanism: SaslMechanism::AwsMskIam,
            aws_region: Some("ap-southeast-2".to_string()),
            aws_access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            aws_secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    // ========================================================================
    // Coercion Config Tests
    // ========================================================================

    #[test]
    fn test_coercion_config_is_null_string() {
        let config = CoercionConfig::default();

        // Empty string is null
        assert!(config.is_null_string(""));

        // Standard null strings
        assert!(config.is_null_string("null"));
        assert!(config.is_null_string("NULL"));
        assert!(config.is_null_string("None"));
        assert!(config.is_null_string("nil"));
        assert!(config.is_null_string("undefined"));
        assert!(config.is_null_string("\\N"));
        assert!(config.is_null_string("N/A"));
        assert!(config.is_null_string("NaN"));

        // Not null strings
        assert!(!config.is_null_string("hello"));
        assert!(!config.is_null_string("0"));
        assert!(!config.is_null_string("false"));
    }

    #[test]
    fn test_coercion_config_get_coercer_category() {
        let mut config = CoercionConfig::default();
        config.type_mappings.insert("MyInt".to_string(), "Int".to_string());
        config.type_mappings.insert("SpecialString".to_string(), "String".to_string());

        // Custom mappings
        assert_eq!(config.get_coercer_category("MyInt"), "Int");
        assert_eq!(config.get_coercer_category("SpecialString"), "String");

        // Unknown type falls back to default
        assert_eq!(config.get_coercer_category("UnknownType"), "String");
    }

    #[test]
    fn test_null_handling_default() {
        let handling = NullHandling::default();
        assert_eq!(handling, NullHandling::Default);
    }
}
