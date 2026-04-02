// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--config, --log-level, etc.)
//!   2. Explicit flat env overrides (`DFE_LOADER_KAFKA_BROKERS`, etc.)
//!   3. Figment env vars with __ nesting (`DFE_LOADER_KAFKA__BROKERS`, etc.)
//!   4. .env file (via dotenvy)
//!   5. Config file specified by --config or `DFE_LOADER_CONFIG`
//!   6. Hard-coded defaults

use std::path::Path;

use hyperi_rustlib::config::sensitive::SensitiveString;
use serde::{Deserialize, Serialize};

use crate::Result;

// Re-export from sub-modules
pub use super::kafka::*;
pub use super::pipeline::*;

/// Main configuration for dfe-loader.
///
/// ## Hot-Reload Behaviour
///
/// **Hot-reloaded (takes effect on next batch):**
/// - `routing.*` — routing rules, table mapping, org routing
/// - `timestamp_dq.*` — timestamp validation thresholds
/// - `metadata.*` — common header injection, tags, _raw handling
/// - `field_sanitization.*` — field name sanitisation rules
/// - `buffer.flush_rows` / `buffer.flush_bytes` / `buffer.flush_age_secs`
/// - `coercion.*` — type coercion config
/// - `enrichment.ip_fields` — which fields to enrich
/// - `field_mapping.*` — field mapping overrides
///
/// **Requires pod restart (connections/state established at startup):**
/// - `transport` — transport type (kafka/grpc) bound at startup
/// - `kafka.*` — Kafka consumer created at startup
/// - `grpc.*` — gRPC server binds at startup
/// - `clickhouse.*` — HTTP client + `clickhouse::Client` created at startup
/// - `payload.format` — format detection set at startup
/// - `metrics.*` — HTTP metrics server binds at startup
/// - `logging.*` — tracing subscriber installed at startup
/// - `scaling.*` / `keda.*` — scaling pressure built at startup
/// - `hot_reload.*` — watcher config set at startup
/// - `schema.*` — schema cache created at startup
/// - `geoip.*` — MMDB readers opened at startup
/// - `computed_columns.*` — computed column cache built at startup
/// - `column_directives.*` — column directive cache built at startup
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    // --- Requires restart (connections/state established at startup) ---
    /// Transport backend: "kafka" (default) or "grpc". **Restart required.**
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Kafka consumer config. **Restart required.**
    pub kafka: KafkaConfig,
    /// gRPC transport config. **Restart required.**
    pub grpc: GrpcConfig,
    /// `ClickHouse` connection config. **Restart required.**
    pub clickhouse: ClickHouseConfig,
    /// Payload format detection. **Restart required.**
    pub payload: PayloadConfig,
    /// Metrics server config. **Restart required.**
    pub metrics: MetricsConfig,
    /// Logging config. **Restart required.**
    pub logging: LoggingConfig,
    /// Schema cache config. **Restart required.**
    pub schema: SchemaConfig,
    /// `GeoIP` enrichment (MMDB readers). **Restart required.**
    pub geoip: GeoIpConfig,
    /// Computed columns cache. **Restart required.**
    pub computed_columns: ComputedColumnsConfig,
    /// Per-column directive config. **Restart required.**
    pub column_directives: crate::column_meta::ColumnDirectivesConfig,
    /// Hot-reload watcher config. **Restart required.**
    pub hot_reload: HotReloadConfig,
    /// KEDA autoscaling config. **Restart required.**
    pub keda: KedaConfig,
    /// Scaling pressure config. **Restart required.**
    pub scaling: ScalingConfig,

    // --- Hot-reloaded (takes effect on next batch) ---
    /// Routing rules and table mapping. **Hot-reloaded.**
    pub routing: RoutingConfig,
    /// Buffer flush thresholds. **Hot-reloaded.**
    pub buffer: BufferConfig,
    /// Memory limits. **Hot-reloaded.**
    pub memory: MemoryConfig,
    /// Timestamp data quality validation. **Hot-reloaded.**
    pub timestamp_dq: TimestampDqConfig,
    /// Field name sanitisation. **Hot-reloaded.**
    pub field_sanitization: FieldSanitizationConfig,
    /// Common header injection, tags, _raw handling. **Hot-reloaded.**
    pub metadata: MetadataConfig,
    /// Type coercion config. **Hot-reloaded.**
    pub coercion: CoercionConfig,
    /// Field mapping overrides. **Hot-reloaded.**
    pub field_mapping: FieldMappingConfig,
    /// IP enrichment field selection. **Hot-reloaded.**
    pub enrichment: EnrichmentConfig,
}

fn default_transport() -> String {
    "kafka".to_string()
}

// ============================================================================
// ClickHouse Configuration
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClickHouseConfig {
    pub hosts: Vec<String>,
    pub database: String,
    pub username: String,
    pub password: SensitiveString,
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
            password: SensitiveString::default(),
            protocol: "native".to_string(),
            tables: Vec::new(),
            tls: None,
        }
    }
}

impl From<&ClickHouseConfig> for crate::clickhouse::ClickHouseConfig {
    fn from(cfg: &ClickHouseConfig) -> Self {
        let transport = match cfg.protocol.to_lowercase().as_str() {
            "http" => crate::clickhouse::Transport::Http,
            _ => crate::clickhouse::Transport::Native,
        };
        let tls = cfg.tls.as_ref().is_some_and(|t| t.enabled);

        crate::clickhouse::ClickHouseConfig {
            hosts: cfg.hosts.clone(),
            transport,
            insert_format: crate::clickhouse::InsertFormat::default(),
            database: cfg.database.clone(),
            username: cfg.username.clone(),
            password: cfg.password.expose().to_string(),
            tls,
            connect_timeout_ms: 5000,  // Default timeout
            request_timeout_ms: 30000, // Default timeout
            compression: true,         // Enable by default for HTTP
        }
    }
}

// ============================================================================
// Configuration Loading
// ============================================================================

/// Environment variable prefix for all dfe-loader settings
const ENV_PREFIX: &str = "DFE_LOADER";

use hyperi_rustlib::config::flat_env::{self, ApplyFlatEnv, Normalize};

impl ApplyFlatEnv for Config {
    /// Apply flat `DFE_LOADER`_* env var overrides.
    ///
    /// These are K8s-friendly single-underscore vars. Same names as before — contract
    /// with dfe-engine. For nested config use `__` nesting via figment.
    fn apply_flat_env(&mut self, prefix: &str) {
        // Kafka
        if let Some(v) = flat_env::flat_env_list(prefix, "KAFKA_BROKERS") {
            self.kafka.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_GROUP_ID") {
            self.kafka.group = v;
        }
        if let Some(v) = flat_env::flat_env_list(prefix, "KAFKA_TOPICS") {
            self.kafka.topics = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_CLIENT_ID") {
            self.kafka.client_id = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_MECHANISM") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.mechanism = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_USERNAME") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "KAFKA_SASL_PASSWORD") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.password = SensitiveString::from(v);
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SECURITY_PROTOCOL") {
            let proto = v.to_uppercase();
            if proto.contains("SSL") || proto.contains("TLS") {
                let tls = self.kafka.tls.get_or_insert_with(TlsConfig::default);
                tls.enabled = true;
            }
        }

        // ClickHouse
        if let Some(v) = flat_env::flat_env_list(prefix, "CLICKHOUSE_HOSTS") {
            self.clickhouse.hosts = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "CLICKHOUSE_DATABASE") {
            self.clickhouse.database = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "CLICKHOUSE_USERNAME") {
            self.clickhouse.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "CLICKHOUSE_PASSWORD") {
            self.clickhouse.password = SensitiveString::from(v);
        }

        // Buffer
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "BUFFER_FLUSH_ROWS") {
            self.buffer.flush_rows = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "BUFFER_FLUSH_BYTES") {
            self.buffer.flush_bytes = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "BUFFER_FLUSH_AGE_SECS") {
            self.buffer.flush_age_secs = v;
        }

        // Metrics
        if let Some(v) = flat_env::flat_env_string(prefix, "METRICS_ADDRESS") {
            self.metrics.address = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "METRICS_ENABLED") {
            self.metrics.enabled = v;
        }

        // Metadata
        if let Some(v) = flat_env::flat_env_bool(prefix, "METADATA_ENABLED") {
            self.metadata.enabled = v;
        }

        // Logging (generic names — no prefix)
        if let Some(v) = flat_env::flat_env_string(prefix, "LOG_LEVEL") {
            self.logging.level = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "LOG_FORMAT") {
            self.logging.format = v;
        }

        // Hot-reload
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "CONFIG_RELOAD_SECS") {
            self.hot_reload.poll_interval_secs = v;
            self.hot_reload.enabled = true;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "HOT_RELOAD_ENABLED") {
            self.hot_reload.enabled = v;
        }

        // Routing
        if let Some(v) = flat_env::flat_env_string(prefix, "ROUTING_DEFAULT_DB") {
            self.routing.default_db = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "ROUTING_DEFAULT_TABLE") {
            self.routing.default_table = v;
        }

        // Memory
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "MEMORY_LIMIT_BYTES") {
            self.memory.limit_bytes = v;
        }
    }
}

impl Normalize for Config {
    /// Side-effects: credentials present → enable auth, protocol → enable TLS.
    fn normalize(&mut self) {
        // SASL credentials present → auto-enable
        if let Some(ref sasl) = self.kafka.sasl
            && (!sasl.username.is_empty()
                || !sasl.password.expose().is_empty()
                || !sasl.mechanism.is_empty())
            && let Some(ref mut sasl) = self.kafka.sasl
        {
            sasl.enabled = true;
        }
    }
}

/// Apply figment env vars with __ (double underscore) nesting.
///
/// Supports arbitrary nesting: `DFE_LOADER_KAFKA__SASL__USERNAME` → kafka.sasl.username
/// Lists require bracket syntax: `DFE_LOADER_KAFKA__BROKERS`=[a, b, c]
fn apply_figment_env(config: &mut Config) -> Result<()> {
    use figment::Figment;
    use figment::providers::{Env, Serialized};

    let figment = Figment::from(Serialized::defaults(&*config))
        .merge(Env::prefixed(&format!("{ENV_PREFIX}_")).split("__"));

    *config = figment
        .extract()
        .map_err(|e| crate::Error::Config(e.to_string()))?;
    Ok(())
}

impl Config {
    /// Load configuration with cascade (highest to lowest priority):
    ///
    /// 1. CLI args (applied separately by caller)
    /// 2. Explicit flat env overrides (`DFE_LOADER_KAFKA_BROKERS`, etc.)
    /// 3. Figment env vars with `__` nesting (`DFE_LOADER_KAFKA__SASL__USERNAME`, etc.)
    /// 4. `.env` file (via dotenvy)
    /// 5. Config file (YAML, specified by `--config` or auto-detected)
    /// 6. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // Load .env file if present (before any env var reading)
        let _ = dotenvy::dotenv();

        // 1. Start with hard-coded defaults
        let mut config = Config::default();

        // 2. Load YAML config file (overrides defaults)
        if let Some(path) = config_path {
            if Path::new(path).exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                config = serde_yaml_ng::from_str(&content)
                    .map_err(|e| crate::Error::Config(format!("failed to parse {path}: {e}")))?;
            }
        } else {
            // Try default config paths
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    let content = std::fs::read_to_string(path)
                        .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                    config = serde_yaml_ng::from_str(&content).map_err(|e| {
                        crate::Error::Config(format!("failed to parse {path}: {e}"))
                    })?;
                    break;
                }
            }
        }

        // 3. Apply figment env vars (DFE_LOADER_SECTION__FIELD with __ nesting)
        apply_figment_env(&mut config)?;

        // 4. Apply explicit flat env overrides (DFE_LOADER_KAFKA_BROKERS etc.)
        config.apply_flat_env(ENV_PREFIX);

        // 5. Side-effects: credentials → enable auth, etc.
        config.normalize();

        // 6. Register all config sections in the global registry (enables /config endpoint
        // and change notifications). SensitiveString fields are auto-redacted on dump.
        config.register_sections();

        Ok(config)
    }

    /// Register all config sections in the global config registry.
    ///
    /// Enables `/config` endpoint dump (with redaction) and change notifications.
    /// Called after load and after hot-reload.
    pub fn register_sections(&self) {
        use hyperi_rustlib::config::registry;
        registry::register("kafka", &self.kafka);
        registry::register("clickhouse", &self.clickhouse);
        registry::register("routing", &self.routing);
        registry::register("buffer", &self.buffer);
        registry::register("metadata", &self.metadata);
        registry::register("metrics", &self.metrics);
        registry::register("logging", &self.logging);
        registry::register("payload", &self.payload);
        registry::register("coercion", &self.coercion);
        registry::register("enrichment", &self.enrichment);
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Kafka validation
        if self.kafka.brokers.is_empty() {
            return Err(crate::Error::Config(
                "At least one Kafka broker must be configured".into(),
            ));
        }
        // Empty topics list is valid: triggers auto-discovery of *_load/*_land topics.

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

    /// Build a deployment contract from loader config defaults.
    ///
    /// Apps provide ~20% customisation; rustlib generates ~80% boilerplate
    /// (Dockerfile, Helm chart, Compose fragment).
    pub fn deployment_contract() -> hyperi_rustlib::deployment::DeploymentContract {
        use hyperi_rustlib::deployment::{
            DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract,
            OciLabels, SecretEnvContract, SecretGroupContract,
        };

        DeploymentContract {
            schema_version: 2,
            app_name: "dfe-loader".into(),
            base_image: "ubuntu:24.04".into(),
            binary_name: "dfe-loader".into(),
            description: "High-performance Kafka to ClickHouse data loader".into(),
            metrics_port: 9090,
            health: HealthContract {
                liveness_path: "/healthz".into(),
                readiness_path: "/readyz".into(),
                metrics_path: "/metrics".into(),
            },
            env_prefix: "DFE_LOADER".into(),
            metric_prefix: "loader".into(),
            config_mount_path: "/etc/dfe/loader.yaml".into(),
            image_registry: "ghcr.io/hyperi-io".into(),
            extra_ports: vec![],
            entrypoint_args: vec!["--config".into(), "/etc/dfe/loader.yaml".into()],
            secrets: vec![
                SecretGroupContract {
                    group_name: "kafka".into(),
                    env_vars: vec![
                        SecretEnvContract {
                            env_var: "DFE_LOADER__KAFKA__SASL__USERNAME".into(),
                            key_name: "username".into(),
                            secret_key: "kafka-username".into(),
                        },
                        SecretEnvContract {
                            env_var: "DFE_LOADER__KAFKA__SASL__PASSWORD".into(),
                            key_name: "password".into(),
                            secret_key: "kafka-password".into(),
                        },
                    ],
                },
                SecretGroupContract {
                    group_name: "clickhouse".into(),
                    env_vars: vec![SecretEnvContract {
                        env_var: "DFE_LOADER__CLICKHOUSE__PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "clickhouse-password".into(),
                    }],
                },
            ],
            default_config: Some(serde_json::json!({
                "kafka": {
                    "brokers": "kafka:9092",
                    "group_id": "dfe-loader",
                    "topics": ["default_land"],
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "SCRAM-SHA-512"
                },
                "clickhouse": {
                    "url": "http://clickhouse:8123",
                    "database": "dfe",
                    "username": "default"
                },
                "routing": {
                    "default_db": "dfe",
                    "default_table": "default"
                },
                "metrics": {
                    "enabled": true,
                    "address": "0.0.0.0:9090"
                }
            })),
            depends_on: vec!["kafka".into(), "clickhouse".into()],
            keda: Some(KedaContract::default()),
            native_deps: NativeDepsContract::for_rustlib_features(
                &[
                    "transport-kafka",
                    "transport-grpc",
                    "dlq-kafka",
                    "config",
                    "config-reload",
                    "deployment",
                    "version-check",
                    "scaling",
                    "cli",
                    "top",
                    "logger",
                    "expression",
                ],
                "ubuntu:24.04",
            ),
            image_profile: ImageProfile::Production,
            oci_labels: OciLabels {
                title: "dfe-loader".into(),
                description: "High-performance Kafka to ClickHouse data loader".into(),
                ..OciLabels::default()
            },
        }
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
        // Empty topics list is valid — triggers auto-discovery mode.
        let mut config = Config::default();
        config.kafka.topics = vec![];
        config.kafka.topic_regex = None;
        assert!(config.validate().is_ok());
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
    // Metadata Config Tests
    // ========================================================================

    #[test]
    fn test_metadata_config_defaults() {
        let config = MetadataConfig::default();
        assert!(config.capture_json);
        assert_eq!(config.json_output, "_json");
        assert!(config.capture_raw);
        assert_eq!(config.raw_output, "_raw");
        assert_eq!(config.raw_source_fields, vec!["logoriginal"]);
        assert!(config.disable_json_tables.is_empty());
        assert!(config.disable_raw_tables.is_empty());
    }

    #[test]
    fn test_metadata_config_backward_compat_logjson() {
        // Old config format with capture_logjson should still work via serde alias
        let json_str = r#"{
            "capture_logjson": false,
            "logjson_output": "_logjson_custom"
        }"#;
        let config: MetadataConfig = serde_json::from_str(json_str).unwrap();
        assert!(!config.capture_json);
        assert_eq!(config.json_output, "_logjson_custom");
    }

    #[test]
    fn test_metadata_config_new_field_names() {
        let json_str = r#"{
            "capture_json": false,
            "json_output": "_custom_json",
            "capture_raw": false,
            "raw_output": "_custom_raw",
            "raw_source_fields": ["original_log", "raw_message"],
            "disable_json_tables": ["common.metrics"],
            "disable_raw_tables": ["common.health"]
        }"#;
        let config: MetadataConfig = serde_json::from_str(json_str).unwrap();
        assert!(!config.capture_json);
        assert_eq!(config.json_output, "_custom_json");
        assert!(!config.capture_raw);
        assert_eq!(config.raw_output, "_custom_raw");
        assert_eq!(
            config.raw_source_fields,
            vec!["original_log", "raw_message"]
        );
        assert_eq!(config.disable_json_tables, vec!["common.metrics"]);
        assert_eq!(config.disable_raw_tables, vec!["common.health"]);
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
        assert_eq!(
            SaslMechanism::ScramSha256.as_rdkafka_mechanism(),
            Some("SCRAM-SHA-256")
        );
        assert_eq!(
            SaslMechanism::ScramSha512.as_rdkafka_mechanism(),
            Some("SCRAM-SHA-512")
        );
        assert_eq!(
            SaslMechanism::OAuthBearer.as_rdkafka_mechanism(),
            Some("OAUTHBEARER")
        );
        assert_eq!(
            SaslMechanism::AwsMskIam.as_rdkafka_mechanism(),
            Some("OAUTHBEARER")
        );
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
            mechanism: "scram_sha_512".to_string(),
            username: String::new(), // Empty but OK because disabled
            password: SensitiveString::default(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_none_no_credentials_needed() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "none".to_string(),
            username: String::new(),
            password: SensitiveString::default(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_scram_requires_username() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "scram_sha_512".to_string(),
            username: String::new(),
            password: SensitiveString::from("secret"),
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
            mechanism: "scram_sha_512".to_string(),
            username: "user".to_string(),
            password: SensitiveString::default(),
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
            mechanism: "scram_sha_512".to_string(),
            username: "user".to_string(),
            password: SensitiveString::from("secret"),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_plain_valid() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "plain".to_string(),
            username: "user".to_string(),
            password: SensitiveString::from("secret"),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_oauth_requires_endpoint() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "oauthbearer".to_string(),
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
            mechanism: "oauthbearer".to_string(),
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
            mechanism: "oauthbearer".to_string(),
            oauth_token_endpoint: Some("https://auth.example.com/token".to_string()),
            oauth_client_id: Some("my-client".to_string()),
            oauth_client_secret: Some(SensitiveString::from("secret")),
            oauth_scope: Some("kafka".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_aws_iam_requires_region() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "aws_msk_iam".to_string(),
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
            mechanism: "aws_msk_iam".to_string(),
            aws_region: Some("ap-southeast-2".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sasl_config_aws_iam_valid_with_explicit_creds() {
        let config = SaslConfig {
            enabled: true,
            mechanism: "aws_msk_iam".to_string(),
            aws_region: Some("ap-southeast-2".to_string()),
            aws_access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            aws_secret_access_key: Some(SensitiveString::from(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            )),
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
        config
            .type_mappings
            .insert("MyInt".to_string(), "Int".to_string());
        config
            .type_mappings
            .insert("SpecialString".to_string(), "String".to_string());

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

    // ========================================================================
    // Config::load() tests
    // ========================================================================
    //
    // All env-var-dependent tests are in a single function to avoid parallel
    // contamination (env vars are process-global).

    #[test]
    fn test_load_with_env_and_yaml() {
        // SAFETY: Test-only env manipulation. Tests in this function run
        // sequentially in scoped blocks with set/remove pairs.

        // Sub-test 1: Flat env var overrides (sequential, no parallel contamination)
        {
            unsafe { std::env::set_var("DFE_LOADER_KAFKA_BROKERS", "broker1:9092,broker2:9092") };
            let config = Config::load(None).unwrap();
            assert_eq!(
                config.kafka.brokers,
                vec!["broker1:9092".to_string(), "broker2:9092".to_string()]
            );
            unsafe { std::env::remove_var("DFE_LOADER_KAFKA_BROKERS") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_KAFKA_GROUP_ID", "test-group-env") };
            let config = Config::load(None).unwrap();
            assert_eq!(config.kafka.group, "test-group-env");
            unsafe { std::env::remove_var("DFE_LOADER_KAFKA_GROUP_ID") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_CLICKHOUSE_HOSTS", "ch1:9000,ch2:9000") };
            let config = Config::load(None).unwrap();
            assert_eq!(
                config.clickhouse.hosts,
                vec!["ch1:9000".to_string(), "ch2:9000".to_string()]
            );
            unsafe { std::env::remove_var("DFE_LOADER_CLICKHOUSE_HOSTS") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_BUFFER_FLUSH_ROWS", "50000") };
            let config = Config::load(None).unwrap();
            assert_eq!(config.buffer.flush_rows, 50000);
            unsafe { std::env::remove_var("DFE_LOADER_BUFFER_FLUSH_ROWS") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_METADATA_ENABLED", "false") };
            let config = Config::load(None).unwrap();
            assert!(!config.metadata.enabled);
            unsafe { std::env::remove_var("DFE_LOADER_METADATA_ENABLED") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_CONFIG_RELOAD_SECS", "30") };
            let config = Config::load(None).unwrap();
            assert!(config.hot_reload.enabled);
            assert_eq!(config.hot_reload.poll_interval_secs, 30);
            unsafe { std::env::remove_var("DFE_LOADER_CONFIG_RELOAD_SECS") };
        }

        {
            unsafe { std::env::set_var("DFE_LOADER_LOG_LEVEL", "debug") };
            let config = Config::load(None).unwrap();
            assert_eq!(config.logging.level, "debug");
            unsafe { std::env::remove_var("DFE_LOADER_LOG_LEVEL") };
        }

        // Sub-test 2: YAML file loading
        {
            let dir = tempfile::TempDir::new().unwrap();
            let config_path = dir.path().join("test_config.yaml");
            std::fs::write(
                &config_path,
                r"
kafka:
  brokers:
    - yaml-broker:9092
  group: yaml-group
  topics:
    - yaml-topic
clickhouse:
  hosts:
    - yaml-ch:9000
buffer:
  flush_rows: 99999
",
            )
            .unwrap();

            let config = Config::load(Some(config_path.to_str().unwrap())).unwrap();
            assert_eq!(config.kafka.brokers, vec!["yaml-broker:9092"]);
            assert_eq!(config.kafka.group, "yaml-group");
            assert_eq!(config.buffer.flush_rows, 99999);
        }

        // Sub-test 3: Env overrides YAML
        {
            let dir = tempfile::TempDir::new().unwrap();
            let config_path = dir.path().join("test_config.yaml");
            std::fs::write(
                &config_path,
                r"
kafka:
  brokers:
    - yaml-broker:9092
  group: yaml-group
  topics:
    - yaml-topic
clickhouse:
  hosts:
    - yaml-ch:9000
",
            )
            .unwrap();

            unsafe { std::env::set_var("DFE_LOADER_KAFKA_BROKERS", "env-broker:9092") };
            let config = Config::load(Some(config_path.to_str().unwrap())).unwrap();
            assert_eq!(config.kafka.brokers, vec!["env-broker:9092"]);
            assert_eq!(config.kafka.group, "yaml-group");
            unsafe { std::env::remove_var("DFE_LOADER_KAFKA_BROKERS") };
        }

        // Sub-test 4: Defaults when no config
        {
            let config = Config::load(None).unwrap();
            assert_eq!(config.kafka.brokers, vec!["localhost:9092"]);
            assert_eq!(config.routing.default_db, "dfe");
            assert_eq!(config.routing.default_table, "default");
        }
    }

    #[test]
    fn test_kafka_default_disables_stats() {
        let config = KafkaConfig::default();
        assert_eq!(
            config.librdkafka_overrides.get("statistics.interval.ms"),
            Some(&"0".to_string()),
            "stats must be disabled by default to prevent log spam"
        );
    }

    #[test]
    fn test_kafka_yaml_overrides_stats() {
        let yaml = r#"
kafka:
  brokers:
    - "localhost:9092"
  librdkafka_overrides:
    statistics.interval.ms: "5000"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            config
                .kafka
                .librdkafka_overrides
                .get("statistics.interval.ms"),
            Some(&"5000".to_string()),
            "user config must override the default"
        );
    }

    #[test]
    fn test_kafka_yaml_other_override_loses_stats_default() {
        // When user provides librdkafka_overrides in YAML, serde replaces
        // the entire map — the default stats.interval.ms=0 is NOT merged.
        // Acceptable: users who set overrides are advanced and can add
        // statistics.interval.ms themselves if needed.
        let yaml = r#"
kafka:
  brokers:
    - "localhost:9092"
  librdkafka_overrides:
    message.max.bytes: "2097152"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            config
                .kafka
                .librdkafka_overrides
                .get("statistics.interval.ms"),
            None,
            "serde replaces default map — only user-specified keys present"
        );
    }
}
