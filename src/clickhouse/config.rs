// Project:   dfe-loader
// File:      src/clickhouse/config.rs
// Purpose:   ClickHouse connection configuration
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! ClickHouse connection configuration.

use serde::{Deserialize, Serialize};

/// Transport protocol for ClickHouse connections.
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
    /// Uses ArrowStream format for efficient data transfer.
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

/// ClickHouse connection configuration.
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

    /// Database name to connect to.
    pub database: String,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    #[serde(default)]
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
            database: "default".to_string(),
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
    pub fn with_credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
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
        assert_eq!(config.database, "default");
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
        assert_eq!(config.primary_endpoint(), Some("localhost:9000".to_string()));

        let config = ClickHouseConfig::new("localhost:9001", "db");
        assert_eq!(config.primary_endpoint(), Some("localhost:9001".to_string()));

        let config = ClickHouseConfig::http("localhost", "db");
        assert_eq!(config.primary_endpoint(), Some("localhost:8123".to_string()));
    }

    #[test]
    fn test_transport_display() {
        assert_eq!(Transport::Native.to_string(), "native");
        assert_eq!(Transport::Http.to_string(), "http");
    }
}
