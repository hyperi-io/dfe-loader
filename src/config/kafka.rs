// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka and gRPC transport configuration.

use std::collections::HashMap;

use scalo::config::sensitive::SensitiveString;
use serde::{Deserialize, Serialize};

// ============================================================================
// Kafka Configuration
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group: String,
    /// Explicit topic list. Empty = auto-discover every `*_load` / `*_land`
    /// topic on the broker, with load-over-land fallback applied. The
    /// loader's own DLQ topic is never discovered.
    pub topics: Vec<String>,
    /// Regex that replaces the `*_load` / `*_land` match when auto-discovering.
    /// Ignored when `topics` is set.
    pub topic_regex: Option<String>,
    pub client_id: String,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,

    /// Opt in to an unencrypted Kafka transport (`plaintext` / `sasl_plaintext`)
    /// in production. scalo (>=2.8) rejects unencrypted transports under a
    /// production profile at transport construction unless this is set —
    /// data and SASL/PLAIN credentials would otherwise ship in the clear. Leave
    /// `false` (the default) and configure TLS, or set `true` only when the
    /// in-cluster traffic is already mesh-encrypted.
    pub allow_insecure_transport: bool,

    /// Raw librdkafka configuration overrides (highest priority).
    pub librdkafka_overrides: HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let mut overrides = HashMap::new();
        // Enable rdkafka statistics every 5s. scalo's KafkaTransport consumes
        // these to compute `kafka_consumer_group_lag` (summed over this pod's
        // ASSIGNED partitions) and the loader pushes that into the unified
        // ScalingPressure engine's kafka_lag component via
        // `set_component("kafka_lag", lag)`. With stats disabled ("0") the lag
        // snapshot is always empty so the kafka_lag term silently reads 0 — the
        // engine would never scale out on Kafka backlog. 5s is well off the data
        // hot-path and matches the engine's evaluation tick. scalo routes the
        // StatsContext output to its own metrics, not the INFO log, so this no
        // longer spams.
        overrides.insert("statistics.interval.ms".to_string(), "5000".to_string());

        Self {
            brokers: vec!["localhost:9092".to_string()],
            // DFE consumer groups carry the `dfe-` prefix: a managed broker
            // grants group access on that prefix, and a group outside it is
            // refused with GroupAuthorizationFailed.
            group: "dfe-loader".to_string(),
            topics: vec![], // Empty = auto-discover
            topic_regex: None,
            client_id: "dfe-loader".to_string(),
            sasl: None,
            tls: None,
            allow_insecure_transport: false,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
            default_topic: "main_land".to_string(),
        }
    }
}

/// SASL authentication mechanism
///
/// Config file values (case-insensitive): none, plain, `scram_sha_256`, `scram_sha_512`, oauthbearer, `aws_msk_iam`
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema,
)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL authentication
    pub enabled: bool,
    /// SASL mechanism. Connectable values: none, plain, `scram_sha_256`,
    /// `scram_sha_512`. `oauthbearer` and `aws_msk_iam` parse but are rejected
    /// at startup — see the `oauth_*` / `aws_*` fields below. An unrecognised
    /// string is a startup error, not a silent fall back to the default.
    #[serde(default = "default_mechanism_string")]
    pub mechanism: String,

    // --- Username/Password auth (PLAIN, SCRAM-*) ---
    pub username: String,
    pub password: SensitiveString,

    // --- OAuth 2.0 / OIDC auth (OAUTHBEARER) ---
    // NOT WIRED: the client is handed mechanism + username + password only, so
    // `validate()` rejects `mechanism: oauthbearer` at startup.
    /// OAuth token endpoint URL. Not wired — see the OAUTHBEARER note above.
    pub oauth_token_endpoint: Option<String>,
    /// OAuth client ID. Not wired.
    pub oauth_client_id: Option<String>,
    /// OAuth client secret. Not wired.
    pub oauth_client_secret: Option<SensitiveString>,
    /// OAuth scope (space-separated). Not wired.
    pub oauth_scope: Option<String>,
    /// OAuth extensions (key=value pairs). Not wired.
    pub oauth_extensions: Option<String>,

    // --- AWS MSK IAM auth ---
    // NOT WIRED: no SigV4 token provider is attached to the client, so
    // `validate()` rejects `mechanism: aws_msk_iam` at startup.
    /// AWS region for MSK IAM. Not wired — see the AWS MSK IAM note above.
    pub aws_region: Option<String>,
    /// AWS access key ID. Not wired.
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key. Not wired.
    pub aws_secret_access_key: Option<SensitiveString>,
    /// AWS session token. Not wired.
    pub aws_session_token: Option<SensitiveString>,
    /// AWS profile name. Not wired.
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
            password: SensitiveString::default(),
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
    /// Parse the mechanism string into a `SaslMechanism`, or `None` when the
    /// string names no mechanism this build understands.
    ///
    /// [`validate`](Self::validate) turns the `None` arm into a startup error,
    /// so a typo cannot reach the broker as some other mechanism.
    pub fn parse_mechanism(&self) -> Option<SaslMechanism> {
        match self.mechanism.to_lowercase().replace('-', "_").as_str() {
            "none" => Some(SaslMechanism::None),
            "plain" => Some(SaslMechanism::Plain),
            "scram_sha_256" | "scram_sha256" => Some(SaslMechanism::ScramSha256),
            "scram_sha_512" | "scram_sha512" => Some(SaslMechanism::ScramSha512),
            "oauthbearer" | "oauth" => Some(SaslMechanism::OAuthBearer),
            "aws_msk_iam" | "awsmskiam" => Some(SaslMechanism::AwsMskIam),
            _ => None,
        }
    }

    /// Parse the mechanism string into a `SaslMechanism` enum.
    ///
    /// Falls back to the default mechanism for a string this build does not
    /// know. Callers on the connect path can rely on that only because
    /// [`validate`](Self::validate) has already rejected such a string at
    /// startup.
    pub fn mechanism(&self) -> SaslMechanism {
        self.parse_mechanism().unwrap_or_default()
    }

    /// Validate the SASL configuration based on mechanism.
    ///
    /// Reached from `Config::validate()` at startup, so every arm below is a
    /// hard startup failure rather than a claim.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        let Some(mech) = self.parse_mechanism() else {
            return Err(format!(
                "unknown kafka.sasl.mechanism '{}' (expected one of: none, plain, \
                 scram_sha_256, scram_sha_512, oauthbearer, aws_msk_iam)",
                self.mechanism
            ));
        };
        match mech {
            SaslMechanism::None => {
                // No validation needed - this is explicitly insecure
            }
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                if self.username.is_empty() {
                    return Err(format!("{mech} requires username"));
                }
                if self.password.expose().is_empty() {
                    return Err(format!("{mech} requires password"));
                }
            }
            // Refused here because their config reaches no client: without
            // this, both connect as OAUTHBEARER with an empty username.
            SaslMechanism::OAuthBearer => {
                return Err(
                    "kafka.sasl.mechanism 'oauthbearer' is not supported by this \
                            build: no OAuth token source is wired to the Kafka client, so \
                            oauth_token_endpoint / oauth_client_id / oauth_client_secret / \
                            oauth_scope / oauth_extensions never reach it. Use \
                            scram_sha_512 (or scram_sha_256 / plain) over TLS."
                        .to_string(),
                );
            }
            SaslMechanism::AwsMskIam => {
                return Err(
                    "kafka.sasl.mechanism 'aws_msk_iam' is not supported by this \
                            build: no AWS SigV4 token provider is wired to the Kafka \
                            client, so aws_region / aws_access_key_id / \
                            aws_secret_access_key / aws_session_token / aws_profile never \
                            reach it. Use MSK's SCRAM-SHA-512 secret-manager auth."
                        .to_string(),
                );
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
#[derive(Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
}
