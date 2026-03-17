// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--config, --log-level, etc.)
//!   2. Explicit flat env overrides (DFE_LOADER_KAFKA_BROKERS, etc.)
//!   3. Figment env vars with __ nesting (DFE_LOADER_KAFKA__BROKERS, etc.)
//!   4. .env file (via dotenvy)
//!   5. Config file specified by --config or DFE_LOADER_CONFIG
//!   6. Hard-coded defaults

use std::collections::HashMap;
use std::path::Path;

use hyperi_rustlib::config::env_compat::EnvVar;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

/// Main configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    /// Transport backend: "kafka" (default) or "grpc"
    #[serde(default = "default_transport")]
    pub transport: String,
    pub kafka: KafkaConfig,
    pub grpc: GrpcConfig,
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
    pub field_mapping: FieldMappingConfig,
    pub computed_columns: ComputedColumnsConfig,
    /// Unified per-column directive config (config wins over DDL COMMENT annotations).
    pub column_directives: crate::column_meta::ColumnDirectivesConfig,
    pub geoip: GeoIpConfig,
    pub enrichment: EnrichmentConfig,
    pub hot_reload: HotReloadConfig,
    pub keda: KedaConfig,
    pub scaling: ScalingConfig,
}

fn default_transport() -> String {
    "kafka".to_string()
}

// ============================================================================
// Kafka Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group: String,
    /// Explicit topic list. Empty = auto-discover all `*_load` / `*_land` topics
    /// from the broker, with load-over-land fallback applied.
    pub topics: Vec<String>,
    pub topic_regex: Option<String>,
    pub client_id: String,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,

    /// Regex patterns for topics to include during auto-discovery.
    /// Empty = include all discovered topics. Applied after load-over-land fallback.
    #[serde(default)]
    pub topic_include: Vec<String>,

    /// Regex patterns for topics to exclude during auto-discovery.
    /// Applied after include filter.
    #[serde(default)]
    pub topic_exclude: Vec<String>,

    /// How often (seconds) to re-check the broker for new/removed topics.
    /// 0 = disabled. Default: 60.
    #[serde(default = "default_topic_refresh_secs")]
    pub topic_refresh_secs: u64,

    /// Raw librdkafka configuration overrides (highest priority).
    pub librdkafka_overrides: HashMap<String, String>,
}

fn default_topic_refresh_secs() -> u64 {
    60
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let mut overrides = HashMap::new();
        // Disable rdkafka statistics by default — dfe-loader doesn't use
        // StatsContext so the stats just spam the log at INFO level.
        overrides.insert("statistics.interval.ms".to_string(), "0".to_string());

        Self {
            brokers: vec!["localhost:9092".to_string()],
            group: "clickhouse-loader".to_string(),
            topics: vec![], // Empty = auto-discover
            topic_regex: None,
            client_id: "clickhouse-loader".to_string(),
            sasl: None,
            tls: None,
            topic_include: vec![],
            topic_exclude: vec![],
            topic_refresh_secs: default_topic_refresh_secs(),
            librdkafka_overrides: overrides,
        }
    }
}

// ============================================================================
// gRPC Transport Configuration
// ============================================================================

/// gRPC transport configuration for receiving messages from dfe-receiver.
///
/// When `transport = "grpc"`, the loader starts a gRPC server listening on
/// `listen` and accepts Push RPCs from remote senders (e.g. dfe-receiver).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GrpcConfig {
    /// Server listen address (e.g., "0.0.0.0:6000").
    /// Required when `transport = "grpc"`.
    pub listen: Option<String>,

    /// Receive buffer size (messages buffered from incoming RPCs).
    pub recv_buffer_size: usize,

    /// Receive timeout in milliseconds (0 = non-blocking).
    pub recv_timeout_ms: u64,

    /// Maximum message size in bytes (both send and receive).
    pub max_message_size: usize,

    /// Enable gzip compression for gRPC messages.
    pub compression: bool,

    /// Default topic name for messages without a topic in gRPC metadata.
    /// Used as the routing key when the sender doesn't set a topic.
    pub default_topic: String,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen: None,
            recv_buffer_size: 10_000,
            recv_timeout_ms: 100,
            max_message_size: 16 * 1024 * 1024,
            compression: false,
            default_topic: "default_land".to_string(),
        }
    }
}

/// SASL authentication mechanism
///
/// Config file values (case-insensitive): none, plain, scram_sha_256, scram_sha_512, oauthbearer, aws_msk_iam
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    /// SASL mechanism (none, plain, scram_sha_256, scram_sha_512, oauthbearer, aws_msk_iam)
    #[serde(default = "default_mechanism_string")]
    pub mechanism: String,

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

fn default_mechanism_string() -> String {
    "scram_sha_512".to_string()
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: default_mechanism_string(),
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
    /// Parse the mechanism string into a SaslMechanism enum
    pub fn mechanism(&self) -> SaslMechanism {
        match self.mechanism.to_lowercase().replace('-', "_").as_str() {
            "none" => SaslMechanism::None,
            "plain" => SaslMechanism::Plain,
            "scram_sha_256" | "scram_sha256" => SaslMechanism::ScramSha256,
            "scram_sha_512" | "scram_sha512" => SaslMechanism::ScramSha512,
            "oauthbearer" | "oauth" => SaslMechanism::OAuthBearer,
            "aws_msk_iam" | "awsmskiam" => SaslMechanism::AwsMskIam,
            _ => SaslMechanism::ScramSha512, // Default
        }
    }

    /// Validate the SASL configuration based on mechanism
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        let mech = self.mechanism();
        match mech {
            SaslMechanism::None => {
                // No validation needed - this is explicitly insecure
            }
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                if self.username.is_empty() {
                    return Err(format!("{} requires username", mech));
                }
                if self.password.is_empty() {
                    return Err(format!("{} requires password", mech));
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
#[derive(Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
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
            database: cfg.database.clone(),
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            tls,
            connect_timeout_ms: 5000,  // Default timeout
            request_timeout_ms: 30000, // Default timeout
            compression: true,         // Enable by default for HTTP
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
    /// Pipeline processing mode.
    ///
    /// - `"json_primary"` (default): Schema-guided SIMD extraction + zero-copy `_json` splice.
    ///   Only schema-matching columns are extracted; everything else is captured via `_json`.
    ///   Requires schema resolution before per-column extraction works; messages arriving
    ///   before schema is resolved fall back to the legacy path.
    ///
    /// - `"legacy_flatten"`: Existing full-flatten + transform path. All fields are promoted
    ///   to the top level; `_json` is injected as a UTF-8 string copy of the raw payload.
    pub pipeline_mode: String,
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self {
            format: "auto".to_string(),
            mismatch_threshold: 10,
            pipeline_mode: "json_primary".to_string(),
        }
    }
}

// ============================================================================
// Routing Configuration
// ============================================================================

/// CEL-based routing rule. When `when` evaluates to true against the message,
/// route to the specified `target` table (and optionally `db` database).
///
/// Rules are evaluated top-to-bottom, first match wins. If no rule matches,
/// falls through to field-extraction routing (db_fields/table_fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// CEL expression that must evaluate to true for this rule to match
    pub when: String,

    /// Target table name
    pub target: String,

    /// Target database (optional — uses default_db if omitted)
    #[serde(default)]
    pub db: Option<String>,
}

/// Explicit per-organisation database routing.
///
/// When an org is listed here, messages from that org are routed to the
/// specified database (or `org_id` if `database` is omitted). Orgs NOT
/// listed always go to `default_db`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgRoute {
    /// Organisation identifier (matched against org_id_field value)
    pub org_id: String,

    /// Target database. If omitted, the org_id itself is used as the database name.
    #[serde(default)]
    pub database: Option<String>,
}

impl OrgRoute {
    /// Return the effective database name for this org route.
    pub fn effective_database(&self) -> &str {
        self.database.as_deref().unwrap_or(&self.org_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    /// CEL-based routing rules (top-to-bottom, first match wins).
    /// Falls through to field-extraction routing if no rule matches.
    #[serde(default)]
    pub rules: Vec<RoutingRule>,

    /// Fields to check for database name (first match wins, dot notation for nested)
    /// Example: ["org_id", "tenant.id"]
    /// NOTE: Leave empty to always use default_db (recommended for shared schema)
    pub db_fields: Vec<String>,

    /// Fields to check for table name (first match wins, dot notation for nested)
    /// Default: ["_source"] — aligned with _source field extraction
    pub table_fields: Vec<String>,

    /// Default database if no db_field matches (or db_fields is empty), or if the
    /// org is not listed in org_routes.
    /// Default: "dfe" (shared multi-tenant schema)
    pub default_db: String,

    /// Default table if no table_field matches
    /// Default: "dfe"
    pub default_table: String,

    /// Field to extract for _org_id column (stored in data for RLS)
    /// Example: "org_id" or "tenant.id"
    /// This field is extracted and stored as _org_id, regardless of routing behaviour
    pub org_id_field: Option<String>,

    /// Per-organisation database routing. Only orgs explicitly listed here receive
    /// their own database — all other orgs always go to default_db.
    /// Example: [{org_id: "acme"}, {org_id: "bigcorp", database: "bigcorp_dfe"}]
    #[serde(default)]
    pub org_routes: Vec<OrgRoute>,

    /// Source value to table name mapping
    /// Maps extracted source values to destination table names
    pub source_to_table: HashMap<String, String>,

    /// Legacy: mapping file path
    pub mapping_file: Option<String>,

    /// Topic suffixes to strip when deriving _source from Kafka topic name
    /// Example: topic "auth_land" with suffix "_land" → _source = "auth"
    pub topic_suffixes: Vec<String>,

    /// Pre-DFE 2.2 compatibility: prepend event_category/tags.event_category to
    /// source_fields and table_fields for backwards compatibility with older data formats
    pub compat_v2_source: bool,

    /// DLQ configuration
    pub dlq: DlqConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            // No CEL routing rules by default (field extraction only)
            rules: vec![],
            // Default: db_fields empty = shared schema (all to dfe.*)
            db_fields: vec![],
            table_fields: vec!["_source".to_string()],
            default_db: "dfe".to_string(),
            default_table: "default".to_string(),
            // Extract org_id for _org_id column (RLS)
            org_id_field: Some("org_id".to_string()),
            // No per-org routing by default (shared schema)
            org_routes: vec![],
            source_to_table: HashMap::new(),
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: DlqConfig::default(),
        }
    }
}

// ============================================================================
// Computed Columns Configuration
// ============================================================================

/// Config cascade overrides for computed columns.
///
/// CEL expressions that produce column values at insert time.
/// Expressions are also read from ClickHouse column comments (`@computed:` directive).
///
/// Precedence (highest wins):
/// 1. Config per-table override (`overrides."db.table".column`)
/// 2. Config global (`columns.column`)
/// 3. ClickHouse column COMMENT `@computed:` directive
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ComputedColumnsConfig {
    /// Global computed columns (applied to all tables).
    /// Key: destination column name, Value: CEL expression.
    pub columns: indexmap::IndexMap<String, String>,

    /// Per-table overrides. Key: "db.table", Value: column→expression map.
    pub overrides: indexmap::IndexMap<String, indexmap::IndexMap<String, String>>,
}

impl Default for ComputedColumnsConfig {
    fn default() -> Self {
        Self {
            columns: indexmap::IndexMap::new(),
            overrides: indexmap::IndexMap::new(),
        }
    }
}

// ============================================================================
// GeoIP Configuration
// ============================================================================

/// GeoIP enrichment provider
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeoIpProvider {
    /// DB-IP Lite — free, anonymous download, city-level, CC BY 4.0
    #[default]
    DbIpLite,
    /// MaxMind GeoLite2 — free account required (account_id + license_key)
    MaxMindGeoLite2,
    /// IPLocate.io — free, anonymous, country + ASN only
    IpLocate,
    /// IPinfo Lite — free token required, country + ASN only
    IpInfoLite,
    /// sapics/ip-location-db — free CC0, country + ASN only
    Sapics,
    /// User provides MMDB file paths directly
    Custom,
}

/// Auto-download settings for GeoIP databases
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AutoDownloadConfig {
    /// Enable auto-download on startup if MMDB files missing or stale
    pub enabled: bool,

    /// Directory to store downloaded MMDB files
    pub data_dir: String,

    /// MaxMind account ID (required for max_mind_geo_lite2 provider)
    pub maxmind_account_id: Option<String>,

    /// MaxMind license key (required for max_mind_geo_lite2 provider)
    pub maxmind_license_key: Option<String>,

    /// IPinfo token (required for ip_info_lite provider)
    pub ipinfo_token: Option<String>,

    /// Max age in days before re-downloading (default: 30)
    pub max_age_days: u32,
}

impl Default for AutoDownloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_dir: "/var/lib/dfe/geoip".into(),
            maxmind_account_id: None,
            maxmind_license_key: None,
            ipinfo_token: None,
            max_age_days: 30,
        }
    }
}

/// GeoIP enrichment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GeoIpConfig {
    /// Enable GeoIP enrichment
    pub enabled: bool,

    /// GeoIP database provider
    pub provider: GeoIpProvider,

    /// Explicit path to city MMDB file (overrides auto-download)
    pub city_db_path: Option<String>,

    /// Explicit path to ASN MMDB file (overrides auto-download)
    pub asn_db_path: Option<String>,

    /// Auto-download settings
    pub auto_download: AutoDownloadConfig,

    /// LRU cache capacity for lookup results
    pub cache_capacity: usize,
}

impl Default for GeoIpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: GeoIpProvider::DbIpLite,
            city_db_path: None,
            asn_db_path: None,
            auto_download: AutoDownloadConfig::default(),
            cache_capacity: 100_000,
        }
    }
}

// ============================================================================
// Enrichment Configuration
// ============================================================================

/// IP enrichment pipeline configuration (GeoIP + reputation + risk scoring)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EnrichmentConfig {
    /// Fields to check for IP addresses (first match wins, order matters)
    ///
    /// Common field names to try: ["src_ip", "client_ip", "ip", "source_ip"]
    pub ip_fields: Vec<String>,

    /// IP reputation enrichment (VPN, Tor, proxy, botnet detection)
    pub reputation: ReputationEnrichmentConfig,

    /// Risk scoring (weighted composite score from geo + reputation data)
    pub risk_scoring: RiskScoringConfig,
}

impl Default for EnrichmentConfig {
    fn default() -> Self {
        Self {
            ip_fields: vec![
                "src_ip".into(),
                "client_ip".into(),
                "ip".into(),
                "source_ip".into(),
            ],
            reputation: ReputationEnrichmentConfig::default(),
            risk_scoring: RiskScoringConfig::default(),
        }
    }
}

/// Reputation enrichment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReputationEnrichmentConfig {
    /// Enable reputation lookups
    pub enabled: bool,

    /// LRU cache capacity for lookup results
    pub cache_capacity: usize,

    /// Load blocklists from local files (plain text, one IP or CIDR per line)
    pub blocklist_files: Vec<String>,
}

impl Default for ReputationEnrichmentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cache_capacity: 100_000,
            blocklist_files: Vec::new(),
        }
    }
}

/// Risk scoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskScoringConfig {
    /// Enable risk scoring (requires at least GeoIP or reputation to be useful)
    pub enabled: bool,

    /// Risk preset to use for country risk tables
    ///
    /// Options: "global" (default), "us_enterprise", "eu_enterprise",
    ///          "apac_enterprise", "high_security"
    pub preset: String,
}

impl Default for RiskScoringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            preset: "global".into(),
        }
    }
}

// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    pub enabled: bool,
    /// Backend mode: cascade (default), fan_out, file_only, kafka_only
    pub mode: String,
    pub topic_suffix: String,
    /// File backend settings
    pub file_enabled: bool,
    pub file_path: String,
    /// Kafka backend settings
    pub kafka_enabled: bool,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: "cascade".to_string(),
            topic_suffix: ".dlq".to_string(),
            file_enabled: true,
            file_path: "/var/spool/dfe/dlq".to_string(),
            kafka_enabled: true,
        }
    }
}

impl DlqConfig {
    /// Convert to rustlib DlqConfig for the unified DLQ module.
    pub fn to_rustlib_config(&self) -> hyperi_rustlib::dlq::DlqConfig {
        use hyperi_rustlib::dlq::{DlqMode, FileDlqConfig};

        let (mode, enabled) = match self.mode.as_str() {
            "disabled" => (DlqMode::Cascade, false),
            "fan_out" => (DlqMode::FanOut, self.enabled),
            "file_only" => (DlqMode::FileOnly, self.enabled),
            "kafka_only" => (DlqMode::KafkaOnly, self.enabled),
            _ => (DlqMode::Cascade, self.enabled),
        };

        hyperi_rustlib::dlq::DlqConfig {
            enabled,
            mode,
            file: FileDlqConfig {
                enabled: self.file_enabled,
                path: self.file_path.clone().into(),
                ..FileDlqConfig::default()
            },
            kafka: hyperi_rustlib::dlq::KafkaDlqConfig {
                enabled: self.kafka_enabled,
                topic_suffix: self.topic_suffix.clone(),
                ..hyperi_rustlib::dlq::KafkaDlqConfig::default()
            },
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
            flush_rows: 20_000,
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
    /// Master switch for common header field injection (default: true)
    /// When false, no common header fields (_timestamp, _org_id, _raw, _json, _tags, _source)
    /// are injected. Flattening and sanitization still apply.
    pub enabled: bool,

    pub inject_timestamp_load: bool,
    pub extract_timestamp_collector: bool,
    pub collector_timestamp_path: String,

    // Tags handling (Common Header v2)
    /// Fields to check for tags (first match wins, dot notation for nested)
    pub tags_fields: Vec<String>,
    /// Output field name for tags (underscore prefix avoids collision)
    pub tags_output: String,
    /// Drop tags entirely after routing extraction (saves storage)
    pub drop_tags: bool,

    // _json capture (Common Header v2)
    /// Store complete original Kafka message as JSON before transformation
    #[serde(alias = "capture_logjson")]
    pub capture_json: bool,
    /// Output field name for _json
    #[serde(alias = "logjson_output")]
    pub json_output: String,

    // _raw field injection (Common Header v2)
    // Implements @renamed: first(source_fields...) → raw_output
    // Silent no-op if destination already present in data
    /// Enable _raw field injection from source (zero-copy rename)
    pub capture_raw: bool,
    /// Source fields to try for rename (first match wins). Default: ["logoriginal"]
    pub raw_source_fields: Vec<String>,
    /// Output field name for raw log line
    pub raw_output: String,

    // _source field (Common Header v2)
    /// Enable _source field injection (destination table identifier)
    pub capture_source: bool,
    /// Fields to check for _source value in message data (first match wins)
    pub source_fields: Vec<String>,
    /// Output field name for _source
    pub source_output: String,

    // Per-table capture overrides
    /// Tables where _json capture is disabled (e.g., ["dfe.metrics"])
    pub disable_json_tables: Vec<String>,
    /// Tables where _raw capture is disabled (e.g., ["dfe.metrics"])
    pub disable_raw_tables: Vec<String>,

    // Routing field removal (Common Header v2)
    /// Remove routing fields from output after extraction
    pub remove_routing_fields: bool,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            enabled: true,

            inject_timestamp_load: true,
            extract_timestamp_collector: true,
            collector_timestamp_path: "tags.collector.timestamp".to_string(),

            // Tags handling defaults
            tags_fields: vec![
                "tags".to_string(),
                "_tags".to_string(),
                "meta".to_string(),
                "metadata.tags".to_string(),
            ],
            tags_output: "_tags".to_string(),
            drop_tags: false,

            // _json capture defaults
            capture_json: true,
            json_output: "_json".to_string(),

            // _raw capture defaults (@renamed: logoriginal → _raw)
            capture_raw: true,
            raw_source_fields: vec!["logoriginal".to_string()],
            raw_output: "_raw".to_string(),

            // _source capture defaults
            capture_source: true,
            source_fields: vec!["_source".to_string()],
            source_output: "_source".to_string(),

            // Per-table overrides
            disable_json_tables: vec![],
            disable_raw_tables: vec![],

            // Routing field removal defaults
            remove_routing_fields: true,
        }
    }
}

// ============================================================================
// Profile Configuration
// ============================================================================

// ============================================================================
// KEDA Autoscaling Configuration
// ============================================================================

/// KEDA autoscaling thresholds (deployment-level config).
///
/// These values are the SSoT for the Helm chart's KEDA ScaledObject.
/// The contract sync test validates that chart/values.yaml matches these
/// defaults. Override at runtime via env vars:
///   DFE_LOADER__KEDA__KAFKA_LAG_THRESHOLD=5000
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KedaConfig {
    pub enabled: bool,
    pub min_replicas: u32,
    pub max_replicas: u32,
    /// Seconds between KEDA polling the scaler
    pub polling_interval: u32,
    /// Seconds before scale-down after load drops
    pub cooldown_period: u32,
    /// Scale when consumer group lag exceeds this per partition
    pub kafka_lag_threshold: u64,
    /// Wake from zero replicas when lag exceeds this
    pub activation_lag_threshold: u64,
    /// Enable CPU-based scaling trigger
    pub cpu_enabled: bool,
    /// CPU utilisation percentage threshold
    pub cpu_threshold: u32,
}

impl Default for KedaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
        }
    }
}

// ============================================================================
// Scaling Pressure Configuration
// ============================================================================

/// Scaling pressure configuration for KEDA autoscaling.
///
/// Produces a 0-100 composite metric (`loader_scaling_pressure`) based on
/// weighted application signals with two hard gates (circuit breaker, memory).
///
/// Override weights at runtime via env vars:
///   DFE_LOADER__SCALING__WEIGHT_KAFKA_LAG=0.45
///   DFE_LOADER__SCALING__SATURATION_BUFFER_DEPTH=20000
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalingConfig {
    /// Enable scaling pressure calculation.
    pub enabled: bool,
    /// Memory usage ratio (0.0-1.0) that forces scaling_pressure to 100.
    pub memory_gate_threshold: f64,
    // Component weights (should sum to ~1.0)
    pub weight_kafka_lag: f64,
    pub weight_buffer_depth: f64,
    pub weight_insert_latency: f64,
    pub weight_memory: f64,
    pub weight_errors: f64,
    // Component saturation points
    pub saturation_kafka_lag: f64,
    pub saturation_buffer_depth: f64,
    pub saturation_insert_latency: f64,
    pub saturation_errors: f64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            memory_gate_threshold: 0.8,
            weight_kafka_lag: 0.35,
            weight_buffer_depth: 0.25,
            weight_insert_latency: 0.15,
            weight_memory: 0.15,
            weight_errors: 0.10,
            saturation_kafka_lag: 100_000.0,
            saturation_buffer_depth: 10_000.0,
            saturation_insert_latency: 5.0,
            saturation_errors: 100.0,
        }
    }
}

impl ScalingConfig {
    /// Build a `ScalingPressure` engine from this config.
    pub fn build_pressure(&self) -> hyperi_rustlib::ScalingPressure {
        use hyperi_rustlib::{ScalingComponent, ScalingPressureConfig};

        let base = ScalingPressureConfig {
            enabled: self.enabled,
            memory_gate_threshold: self.memory_gate_threshold,
        };
        let components = vec![
            ScalingComponent::new(
                "kafka_lag",
                self.weight_kafka_lag,
                self.saturation_kafka_lag,
            ),
            ScalingComponent::new(
                "buffer_depth",
                self.weight_buffer_depth,
                self.saturation_buffer_depth,
            ),
            ScalingComponent::new(
                "insert_latency",
                self.weight_insert_latency,
                self.saturation_insert_latency,
            ),
            ScalingComponent::new("memory", self.weight_memory, 1.0),
            ScalingComponent::new("errors", self.weight_errors, self.saturation_errors),
        ];
        hyperi_rustlib::ScalingPressure::new(base, components)
    }
}

// ============================================================================
// Per-Table Capture Override Configuration
// ============================================================================

/// Per-table capture override configuration.
///
/// Resolved from two sources (DDL tags take precedence over config lists):
/// 1. Config: `disable_json_tables` / `disable_raw_tables` lists
/// 2. DDL: `@no_capture_json: true` / `@no_capture_raw: true` in table COMMENT
#[derive(Debug, Clone, Default)]
pub struct TableCaptureConfig {
    /// Whether _json capture is disabled for this table
    pub disable_json: bool,
    /// Whether _raw capture is disabled for this table
    pub disable_raw: bool,
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
// Auto-Initialization Configuration
// ============================================================================

/// Hot-reload configuration
///
/// Controls whether the config file is watched for changes at runtime.
/// When enabled, the config cascade is re-evaluated on file change and
/// safe-to-reload settings are applied without process restart.
///
/// **Safe to hot-reload:** buffer thresholds, routing, metadata, field
/// sanitisation, timestamp DQ, coercion settings.
///
/// **Requires restart:** Kafka brokers/topics/auth, ClickHouse hosts/auth,
/// payload format, transport type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HotReloadConfig {
    /// Enable config file watching (default: false)
    pub enabled: bool,

    /// Polling interval in seconds for checking file changes
    pub poll_interval_secs: u64,

    /// Debounce duration in milliseconds — minimum time between reloads
    pub debounce_ms: u64,
}

impl Default for HotReloadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_secs: 5,
            debounce_ms: 500,
        }
    }
}

// ============================================================================
// Field Mapping Configuration
// ============================================================================

/// Field mapping configuration for normalising source field names.
///
/// Supports renaming or copying fields from source to destination names.
/// Rules come from two sources with clear precedence:
/// 1. ClickHouse column comments (`@renamed` directives) — highest priority
/// 2. External remap files (CSV/YAML/JSON) and built-in presets — lower priority
///
/// CSV files are compatible with the elastic/ecs-mapper format:
/// `source_field,destination_field,copy_action`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FieldMappingConfig {
    /// Master switch (default: false — no impact on existing pipelines)
    pub enabled: bool,

    /// Default action when not specified per-field: "rename" or "copy"
    /// - rename: source field removed, value moved to destination (zero-copy)
    /// - copy: source field retained, value cloned to destination
    pub default_action: String,

    /// Built-in mapping preset: "ecs", "cim", "beats", or "none"
    pub builtin: String,

    /// External remap file paths (CSV/YAML/JSON, loaded in order)
    /// Later files override earlier ones for the same destination field.
    pub files: Vec<String>,

    /// Per-destination field action overrides
    pub overrides: HashMap<String, FieldMappingOverride>,
}

/// Per-field override for mapping action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMappingOverride {
    /// Action for this field: "rename" or "copy"
    pub action: String,
}

impl Default for FieldMappingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_action: "rename".to_string(),
            builtin: "none".to_string(),
            files: vec![],
            overrides: HashMap::new(),
        }
    }
}

// ============================================================================
// Configuration Loading
// ============================================================================

/// Environment variable prefix for all dfe-loader settings
const ENV_PREFIX: &str = "DFE_LOADER";

/// Read a flat env var with the DFE_LOADER_ prefix
fn env(name: &str) -> Option<String> {
    EnvVar::new(&format!("{ENV_PREFIX}_{name}")).get()
}

/// Read a flat env var as a comma-separated list
fn env_list(name: &str) -> Option<Vec<String>> {
    EnvVar::new(&format!("{ENV_PREFIX}_{name}")).get_list()
}

/// Read a flat env var as a boolean
fn env_bool(name: &str) -> Option<bool> {
    EnvVar::new(&format!("{ENV_PREFIX}_{name}")).get_bool()
}

/// Read a flat env var parsed to a type
fn env_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    EnvVar::new(&format!("{ENV_PREFIX}_{name}")).get_parsed()
}

/// Apply explicit flat environment variable overrides (DFE_LOADER_* prefix).
///
/// These are K8s-friendly single-underscore vars that override config file values.
/// For nested/advanced config, use `__` (double underscore) nesting via figment:
///   DFE_LOADER_KAFKA__SASL__OAUTH_TOKEN_ENDPOINT=https://...
fn apply_env_overrides(config: &mut Config) {
    // Kafka
    if let Some(v) = env_list("KAFKA_BROKERS") {
        config.kafka.brokers = v;
        debug!("Override: kafka.brokers from env");
    }
    if let Some(v) = env("KAFKA_GROUP_ID") {
        config.kafka.group = v;
        debug!("Override: kafka.group from env");
    }
    if let Some(v) = env_list("KAFKA_TOPICS") {
        config.kafka.topics = v;
        debug!("Override: kafka.topics from env");
    }
    if let Some(v) = env("KAFKA_CLIENT_ID") {
        config.kafka.client_id = v;
        debug!("Override: kafka.client_id from env");
    }
    if let Some(v) = env("KAFKA_SASL_MECHANISM") {
        let sasl = config.kafka.sasl.get_or_insert_with(SaslConfig::default);
        sasl.enabled = true;
        sasl.mechanism = v;
        debug!("Override: kafka.sasl.mechanism from env");
    }
    if let Some(v) = env("KAFKA_SASL_USERNAME") {
        let sasl = config.kafka.sasl.get_or_insert_with(SaslConfig::default);
        sasl.enabled = true;
        sasl.username = v;
        debug!("Override: kafka.sasl.username from env");
    }
    if let Some(v) = env("KAFKA_SASL_PASSWORD") {
        let sasl = config.kafka.sasl.get_or_insert_with(SaslConfig::default);
        sasl.enabled = true;
        sasl.password = v;
        debug!("Override: kafka.sasl.password from env (redacted)");
    }
    if let Some(v) = env("KAFKA_SECURITY_PROTOCOL") {
        // Map common protocol names to TLS/SASL config
        let proto = v.to_uppercase();
        if proto.contains("SSL") || proto.contains("TLS") {
            let tls = config.kafka.tls.get_or_insert_with(TlsConfig::default);
            tls.enabled = true;
        }
        debug!(protocol = %v, "Override: kafka security_protocol from env");
    }

    // ClickHouse
    if let Some(v) = env_list("CLICKHOUSE_HOSTS") {
        config.clickhouse.hosts = v;
        debug!("Override: clickhouse.hosts from env");
    }
    if let Some(v) = env("CLICKHOUSE_DATABASE") {
        config.clickhouse.database = v;
        debug!("Override: clickhouse.database from env");
    }
    if let Some(v) = env("CLICKHOUSE_USERNAME") {
        config.clickhouse.username = v;
        debug!("Override: clickhouse.username from env");
    }
    if let Some(v) = env("CLICKHOUSE_PASSWORD") {
        config.clickhouse.password = v;
        debug!("Override: clickhouse.password from env (redacted)");
    }

    // Buffer
    if let Some(v) = env_parsed::<usize>("BUFFER_FLUSH_ROWS") {
        config.buffer.flush_rows = v;
        debug!("Override: buffer.flush_rows from env");
    }
    if let Some(v) = env_parsed::<usize>("BUFFER_FLUSH_BYTES") {
        config.buffer.flush_bytes = v;
        debug!("Override: buffer.flush_bytes from env");
    }
    if let Some(v) = env_parsed::<u64>("BUFFER_FLUSH_AGE_SECS") {
        config.buffer.flush_age_secs = v;
        debug!("Override: buffer.flush_age_secs from env");
    }

    // Metrics
    if let Some(v) = env("METRICS_ADDRESS") {
        config.metrics.address = v;
        debug!("Override: metrics.address from env");
    }
    if let Some(v) = env_bool("METRICS_ENABLED") {
        config.metrics.enabled = v;
        debug!("Override: metrics.enabled from env");
    }

    // Metadata (common header)
    if let Some(v) = env_bool("METADATA_ENABLED") {
        config.metadata.enabled = v;
        debug!("Override: metadata.enabled from env");
    }

    // Logging
    if let Some(v) = env("LOG_LEVEL") {
        config.logging.level = v;
        debug!("Override: logging.level from env");
    }
    if let Some(v) = env("LOG_FORMAT") {
        config.logging.format = v;
        debug!("Override: logging.format from env");
    }

    // Hot-reload
    if let Some(v) = env_parsed::<u64>("CONFIG_RELOAD_SECS") {
        config.hot_reload.poll_interval_secs = v;
        config.hot_reload.enabled = true;
        debug!("Override: hot_reload.poll_interval_secs from env");
    }
    if let Some(v) = env_bool("HOT_RELOAD_ENABLED") {
        config.hot_reload.enabled = v;
        debug!("Override: hot_reload.enabled from env");
    }

    // Routing
    if let Some(v) = env("ROUTING_DEFAULT_DB") {
        config.routing.default_db = v;
        debug!("Override: routing.default_db from env");
    }
    if let Some(v) = env("ROUTING_DEFAULT_TABLE") {
        config.routing.default_table = v;
        debug!("Override: routing.default_table from env");
    }

    // Memory
    if let Some(v) = env_parsed::<usize>("MEMORY_LIMIT_BYTES") {
        config.memory.limit_bytes = v;
        debug!("Override: memory.limit_bytes from env");
    }
}

/// Apply figment env vars with __ (double underscore) nesting.
///
/// Supports arbitrary nesting: DFE_LOADER_KAFKA__SASL__USERNAME → kafka.sasl.username
/// Lists require bracket syntax: DFE_LOADER_KAFKA__BROKERS=[a, b, c]
fn apply_figment_env(config: &mut Config) -> Result<()> {
    use figment::providers::{Env, Serialized};
    use figment::Figment;

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
        // These are highest priority (after CLI args which caller handles)
        apply_env_overrides(&mut config);

        Ok(config)
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
            SecretEnvContract, SecretGroupContract,
        };

        DeploymentContract {
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
            password: String::new(),
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
            password: String::new(),
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
            mechanism: "scram_sha_512".to_string(),
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
            mechanism: "scram_sha_512".to_string(),
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
            mechanism: "plain".to_string(),
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
                r#"
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
"#,
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
                r#"
kafka:
  brokers:
    - yaml-broker:9092
  group: yaml-group
  topics:
    - yaml-topic
clickhouse:
  hosts:
    - yaml-ch:9000
"#,
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
