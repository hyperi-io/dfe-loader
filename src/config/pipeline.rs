// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pipeline configuration: routing, enrichment, coercion, metadata, field mapping.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// Payload Configuration
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
