// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/config.rs
// Purpose:   ClickHouse connection configuration
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` connection configuration.

use serde::{Deserialize, Deserializer, Serialize};

/// Deserialise a string that may be YAML null, missing, or empty.
/// All resolve to empty string (no password).
fn deserialize_optional_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Transport protocol for `ClickHouse` connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// Native TCP protocol (port 9000).
    ///
    /// Highest performance, supports all features including streaming inserts
    /// and native compression. Recommended for production workloads.
    #[default]
    Native,

    /// HTTP protocol (port 8123).
    ///
    /// Better compatibility with proxies, load balancers, and firewalls.
    /// Uses `JSONEachRow` format for dynamic schema inserts.
    Http,
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Native => write!(f, "native"),
            Transport::Http => write!(f, "http"),
        }
    }
}

/// Insert format for data writes.
///
/// Controls how rows are encoded and sent to `ClickHouse`. `RowBinary` is the
/// default — it uses schema reflection to encode `Map<String, Value>` directly
/// to binary, so `ClickHouse` skips JSON parsing entirely. This significantly
/// reduces CPU load on the `ClickHouse` cluster at scale.
///
/// `JsonEachRow` is the fallback — simpler, self-describing, but the `ClickHouse`
/// server pays the cost of parsing every JSON row on ingest.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum InsertFormat {
    /// Schema-reflected `RowBinary` (default).
    ///
    /// Fetches schema from `system.columns`, encodes values to binary client-side.
    /// `ClickHouse` receives pre-columnarised data — zero server-side parsing.
    /// Total CPU (client + cluster) is significantly lower than `JSONEachRow`.
    #[default]
    #[serde(
        alias = "rowbinary",
        alias = "row_binary",
        alias = "native",
        alias = "binary"
    )]
    RowBinary,

    /// `JSONEachRow` over HTTP.
    ///
    /// Self-describing format — `ClickHouse` coerces types server-side.
    /// Simpler but the cluster pays the JSON parsing cost on every row.
    #[serde(alias = "json", alias = "json_each_row")]
    JsonEachRow,
}

impl std::fmt::Display for InsertFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertFormat::RowBinary => write!(f, "rowbinary"),
            InsertFormat::JsonEachRow => write!(f, "jsoneachrow"),
        }
    }
}

/// `ClickHouse` connection configuration.
///
/// Supports multiple hosts for high availability, with credentials and timeouts.
/// Configurable transport protocol (native TCP or HTTP).
///
/// ## Example
///
/// ```rust
/// use dfe_loader::clickhouse::{ClickHouseConfig, Transport};
///
/// // Native protocol (default, port 9000)
/// let native_config = ClickHouseConfig {
///     hosts: vec!["clickhouse-1:9000".to_string()],
///     database: "events".to_string(),
///     username: "app_user".to_string(),
///     password: "secret".to_string(),
///     ..Default::default()
/// };
///
/// // HTTP protocol (port 8123)
/// let http_config = ClickHouseConfig {
///     hosts: vec!["clickhouse-1:8123".to_string()],
///     transport: Transport::Http,
///     database: "events".to_string(),
///     username: "app_user".to_string(),
///     password: "secret".to_string(),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// List of host:port addresses.
    ///
    /// Default port depends on transport:
    /// - Native: 9000
    /// - HTTP: 8123
    pub hosts: Vec<String>,

    /// Transport protocol (native or http).
    #[serde(default)]
    pub transport: Transport,

    /// Insert format — how rows are encoded for INSERT.
    ///
    /// `RowBinary` (default): schema-reflected binary encoding. `ClickHouse`
    /// skips JSON parsing. Lower total CPU across client + cluster.
    ///
    /// `JsonEachRow`: JSON text via HTTP. Self-describing, `ClickHouse` coerces
    /// types server-side. Higher cluster CPU but simpler.
    #[serde(default)]
    pub insert_format: InsertFormat,

    /// Database name to connect to.
    pub database: String,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication. Empty string means no password
    /// (valid for ClickHouse default user). Accepts YAML null, empty string,
    /// or omitted field — all resolve to empty (no auth header sent).
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub password: String,

    /// Enable TLS/SSL for connections.
    #[serde(default)]
    pub tls: bool,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,

    /// Request timeout in milliseconds.
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,

    /// Enable response compression (HTTP only).
    #[serde(default = "default_compression")]
    pub compression: bool,
}

/// Does this `host:port` look like a TLS-only ClickHouse endpoint?
///
/// ClickHouse Cloud (and most managed offerings) are TLS-only: native-secure
/// on 9440, HTTPS on 8443, hostnames under `*.clickhouse.cloud`. A bare config
/// that omits the `tls` flag should still connect to those, so we auto-detect.
/// Mirrors the `secure` auto-detect in dfe-hunt-runner
/// (`utils/clickhouse.py`: `port in {8443, 9440} or "clickhouse.cloud" in host`).
///
/// This is only a *default* - an explicit `tls` setting always wins upstream.
#[must_use]
pub fn host_implies_tls(host: &str) -> bool {
    if host.contains("clickhouse.cloud") {
        return true;
    }
    host.rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .is_some_and(|port| matches!(port, 8443 | 9440))
}

const fn default_connect_timeout() -> u64 {
    5000
}

const fn default_request_timeout() -> u64 {
    30000
}

const fn default_compression() -> bool {
    true
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost:9000".to_string()],
            transport: Transport::Native,
            insert_format: InsertFormat::RowBinary,
            database: "dfe".to_string(),
            username: "default".to_string(),
            password: String::new(),
            tls: false,
            connect_timeout_ms: default_connect_timeout(),
            request_timeout_ms: default_request_timeout(),
            compression: default_compression(),
        }
    }
}

impl ClickHouseConfig {
    /// Create a new config with minimal settings.
    #[must_use]
    pub fn new(host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            hosts: vec![host.into()],
            database: database.into(),
            ..Default::default()
        }
    }

    /// Create a new HTTP config.
    #[must_use]
    pub fn http(host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            hosts: vec![host.into()],
            transport: Transport::Http,
            database: database.into(),
            ..Default::default()
        }
    }

    /// Set transport protocol.
    #[must_use]
    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    /// Set credentials.
    #[must_use]
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = username.into();
        self.password = password.into();
        self
    }

    /// Add additional hosts for high availability.
    #[must_use]
    pub fn with_hosts(mut self, hosts: Vec<String>) -> Self {
        self.hosts = hosts;
        self
    }

    /// Enable TLS.
    #[must_use]
    pub fn with_tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }

    /// Get the first host (primary).
    #[must_use]
    pub fn primary_host(&self) -> Option<&str> {
        self.hosts.first().map(String::as_str)
    }

    /// Get the default port for the configured transport.
    #[must_use]
    pub fn default_port(&self) -> u16 {
        match self.transport {
            Transport::Native => 9000,
            Transport::Http => 8123,
        }
    }

    /// Validate the config for known misconfigurations.
    ///
    /// Returns warnings for issues that can be worked around, errors for
    /// configs that will definitely fail at runtime.
    pub fn validate(&self) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();

        // JSONEachRow is HTTP-only: the dynamic insert path sends JSONEachRow
        // through `Client::insert_formatted_with` (HTTP). A native-transport
        // client has no HTTP endpoint, so this combination fails at insert
        // time. Reject it at config time so `config-check` catches it.
        if self.transport == Transport::Native && self.insert_format == InsertFormat::JsonEachRow {
            return Err(
                "insert_format 'json_each_row' requires transport 'http' (JSONEachRow is \
                 sent over HTTP). Set transport = 'http', or use insert_format = 'rowbinary' \
                 which works on both transports."
                    .to_string(),
            );
        }

        // Cloud endpoints are TLS-only. If a host looks like ClickHouse Cloud
        // (8443/9440 or *.clickhouse.cloud) but TLS is off, the connection will
        // fail - surface it as an actionable warning rather than a cryptic
        // connection error at runtime.
        if !self.tls
            && let Some(host) = self.hosts.first()
            && host_implies_tls(host)
        {
            warnings.push(format!(
                "Host {host} looks like a secure/cloud ClickHouse endpoint but TLS is \
                 disabled. Set tls = true (ClickHouse Cloud requires TLS on 8443/9440)."
            ));
        }

        // Detect port/transport mismatch
        if let Some(host) = self.hosts.first()
            && let Some(port_str) = host.rsplit(':').next()
            && let Ok(port) = port_str.parse::<u16>()
        {
            match (self.transport, port) {
                (Transport::Native, 8123) => {
                    warnings.push(format!(
                        "Transport is 'native' but host {host} uses HTTP port 8123. \
                                 Did you mean port 9000?"
                    ));
                }
                (Transport::Http, 9000 | 9440) => {
                    return Err(format!(
                        "Transport is 'http' but host {host} uses native port {port}. \
                                 Use port 8123 for HTTP or set transport to 'native'."
                    ));
                }
                _ => {}
            }
        }

        Ok(warnings)
    }

    /// Get the primary host with default port if not specified.
    #[must_use]
    pub fn primary_endpoint(&self) -> Option<String> {
        self.hosts.first().map(|host| {
            if host.contains(':') {
                host.clone()
            } else {
                format!("{}:{}", host, self.default_port())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClickHouseConfig::default();
        assert_eq!(config.hosts, vec!["localhost:9000"]);
        assert_eq!(config.transport, Transport::Native);
        assert_eq!(config.database, "dfe");
        assert_eq!(config.username, "default");
        assert!(config.password.is_empty());
        assert!(!config.tls);
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.request_timeout_ms, 30000);
        assert!(config.compression);
    }

    #[test]
    fn test_http_config() {
        let config = ClickHouseConfig::http("localhost:8123", "mydb");
        assert_eq!(config.transport, Transport::Http);
        assert_eq!(config.hosts, vec!["localhost:8123"]);
        assert_eq!(config.database, "mydb");
    }

    #[test]
    fn test_builder_pattern() {
        let config = ClickHouseConfig::new("ch.example.com:9000", "mydb")
            .with_credentials("user", "pass")
            .with_tls(true);

        assert_eq!(config.primary_host(), Some("ch.example.com:9000"));
        assert_eq!(config.database, "mydb");
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
        assert!(config.tls);
    }

    #[test]
    fn test_default_port() {
        let native = ClickHouseConfig::default();
        assert_eq!(native.default_port(), 9000);

        let http = ClickHouseConfig::default().with_transport(Transport::Http);
        assert_eq!(http.default_port(), 8123);
    }

    #[test]
    fn test_primary_endpoint() {
        let config = ClickHouseConfig::new("localhost", "db");
        assert_eq!(
            config.primary_endpoint(),
            Some("localhost:9000".to_string())
        );

        let config = ClickHouseConfig::new("localhost:9001", "db");
        assert_eq!(
            config.primary_endpoint(),
            Some("localhost:9001".to_string())
        );

        let config = ClickHouseConfig::http("localhost", "db");
        assert_eq!(
            config.primary_endpoint(),
            Some("localhost:8123".to_string())
        );
    }

    #[test]
    fn test_transport_display() {
        assert_eq!(Transport::Native.to_string(), "native");
        assert_eq!(Transport::Http.to_string(), "http");
    }

    #[test]
    fn test_insert_format_default() {
        let config = ClickHouseConfig::default();
        assert_eq!(config.insert_format, InsertFormat::RowBinary);
    }

    #[test]
    fn test_insert_format_display() {
        assert_eq!(InsertFormat::RowBinary.to_string(), "rowbinary");
        assert_eq!(InsertFormat::JsonEachRow.to_string(), "jsoneachrow");
    }

    #[test]
    fn test_insert_format_serde_aliases() {
        // All aliases should deserialise to the same variant
        let rb: InsertFormat = serde_json::from_str(r#""rowbinary""#).unwrap();
        assert_eq!(rb, InsertFormat::RowBinary);
        let rb: InsertFormat = serde_json::from_str(r#""native""#).unwrap();
        assert_eq!(rb, InsertFormat::RowBinary);
        let rb: InsertFormat = serde_json::from_str(r#""binary""#).unwrap();
        assert_eq!(rb, InsertFormat::RowBinary);

        let je: InsertFormat = serde_json::from_str(r#""jsoneachrow""#).unwrap();
        assert_eq!(je, InsertFormat::JsonEachRow);
        let je: InsertFormat = serde_json::from_str(r#""json""#).unwrap();
        assert_eq!(je, InsertFormat::JsonEachRow);
        let je: InsertFormat = serde_json::from_str(r#""json_each_row""#).unwrap();
        assert_eq!(je, InsertFormat::JsonEachRow);
    }

    #[test]
    fn test_password_empty_string() {
        let json =
            r#"{"hosts":["localhost:8123"],"database":"db","username":"default","password":""}"#;
        let config: ClickHouseConfig = serde_json::from_str(json).unwrap();
        assert!(config.password.is_empty());
    }

    #[test]
    fn test_password_null() {
        let json =
            r#"{"hosts":["localhost:8123"],"database":"db","username":"default","password":null}"#;
        let config: ClickHouseConfig = serde_json::from_str(json).unwrap();
        assert!(config.password.is_empty());
    }

    #[test]
    fn test_password_missing() {
        let json = r#"{"hosts":["localhost:8123"],"database":"db","username":"default"}"#;
        let config: ClickHouseConfig = serde_json::from_str(json).unwrap();
        assert!(config.password.is_empty());
    }

    #[test]
    fn test_validate_native_port_9000_ok() {
        let config = ClickHouseConfig {
            hosts: vec!["clickhouse:9000".to_string()],
            transport: Transport::Native,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_validate_http_port_9000_rejected() {
        let config = ClickHouseConfig {
            hosts: vec!["clickhouse:9000".to_string()],
            transport: Transport::Http,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("native port"));
    }

    #[test]
    fn test_validate_http_port_8123_ok() {
        let config = ClickHouseConfig {
            hosts: vec!["clickhouse:8123".to_string()],
            transport: Transport::Http,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_validate_native_port_8123_warns() {
        let config = ClickHouseConfig {
            hosts: vec!["clickhouse:8123".to_string()],
            transport: Transport::Native,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_ok());
        let warnings = result.unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("HTTP port 8123"));
    }

    #[test]
    fn validate_transport_insert_format_matrix() {
        // (transport, insert_format, host, expect_ok). Hosts use the matching
        // port so this isolates the transport x insert_format interaction from
        // the port/transport check.
        let cases = [
            (Transport::Native, InsertFormat::RowBinary, "ch:9000", true),
            (
                Transport::Native,
                InsertFormat::JsonEachRow,
                "ch:9000",
                false,
            ),
            (Transport::Http, InsertFormat::RowBinary, "ch:8123", true),
            (Transport::Http, InsertFormat::JsonEachRow, "ch:8123", true),
        ];
        for (transport, insert_format, host, expect_ok) in cases {
            let config = ClickHouseConfig {
                hosts: vec![host.to_string()],
                transport,
                insert_format,
                ..Default::default()
            };
            let result = config.validate();
            assert_eq!(
                result.is_ok(),
                expect_ok,
                "transport={transport} insert_format={insert_format}: got {result:?}"
            );
        }
    }

    #[test]
    fn host_implies_tls_detects_cloud_and_secure_ports() {
        // Cloud hostname (any port)
        assert!(host_implies_tls("abc123.clickhouse.cloud:9440"));
        assert!(host_implies_tls("abc123.us-east.clickhouse.cloud:8443"));
        assert!(host_implies_tls("abc123.clickhouse.cloud"));
        // Secure ports on any host
        assert!(host_implies_tls("ch.internal:9440"));
        assert!(host_implies_tls("ch.internal:8443"));
        // Plaintext defaults are NOT secure
        assert!(!host_implies_tls("localhost:9000"));
        assert!(!host_implies_tls("localhost:8123"));
        assert!(!host_implies_tls("ch.internal"));
        assert!(!host_implies_tls("clickhouse:9000"));
    }

    #[test]
    fn validate_cloud_host_without_tls_warns() {
        let config = ClickHouseConfig {
            hosts: vec!["abc.clickhouse.cloud:9440".to_string()],
            tls: false,
            ..Default::default()
        };
        let warnings = config
            .validate()
            .expect("cloud host is a warning, not error");
        assert!(
            warnings.iter().any(|w| w.contains("TLS is disabled")),
            "expected a TLS-off warning, got {warnings:?}"
        );
    }

    #[test]
    fn validate_cloud_host_with_tls_is_clean() {
        let config = ClickHouseConfig {
            hosts: vec!["abc.clickhouse.cloud:9440".to_string()],
            tls: true,
            ..Default::default()
        };
        let warnings = config.validate().expect("valid");
        assert!(
            !warnings.iter().any(|w| w.contains("TLS is disabled")),
            "TLS-on cloud host should not warn, got {warnings:?}"
        );
    }

    #[test]
    fn validate_native_json_each_row_error_is_actionable() {
        let config = ClickHouseConfig {
            hosts: vec!["ch:9000".to_string()],
            transport: Transport::Native,
            insert_format: InsertFormat::JsonEachRow,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("json_each_row"), "message: {err}");
        assert!(err.contains("http"), "message: {err}");
        assert!(err.contains("rowbinary"), "message: {err}");
    }
}
