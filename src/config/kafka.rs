// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka and gRPC transport configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// Kafka Configuration
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
}
