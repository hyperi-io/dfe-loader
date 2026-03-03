// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Configuration fixture builders

use dfe_loader::config::{
    BufferConfig, ClickHouseConfig, DlqConfig, KafkaConfig, MetadataConfig, RoutingConfig,
    TimestampConfig,
};
use std::collections::HashMap;

/// Builder for BufferConfig
#[derive(Debug, Clone)]
pub struct BufferConfigBuilder {
    flush_rows: usize,
    flush_bytes: usize,
    flush_age_secs: u64,
}

impl BufferConfigBuilder {
    pub fn new() -> Self {
        Self {
            flush_rows: 1000,
            flush_bytes: 1_000_000,
            flush_age_secs: 60,
        }
    }

    pub fn flush_rows(mut self, rows: usize) -> Self {
        self.flush_rows = rows;
        self
    }

    pub fn flush_bytes(mut self, bytes: usize) -> Self {
        self.flush_bytes = bytes;
        self
    }

    pub fn flush_age_secs(mut self, secs: u64) -> Self {
        self.flush_age_secs = secs;
        self
    }

    pub fn build(self) -> BufferConfig {
        BufferConfig {
            flush_rows: self.flush_rows,
            flush_bytes: self.flush_bytes,
            flush_age_secs: self.flush_age_secs,
        }
    }
}

impl Default for BufferConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for ClickHouseConfig
#[derive(Debug, Clone)]
pub struct ClickHouseConfigBuilder {
    host: String,
    native_port: u16,
    http_port: u16,
    username: String,
    password: String,
    database: String,
    max_concurrent_inserts: usize,
}

impl ClickHouseConfigBuilder {
    pub fn new() -> Self {
        Self {
            host: "localhost".to_string(),
            native_port: 9000,
            http_port: 8123,
            username: "default".to_string(),
            password: String::new(),
            database: "default".to_string(),
            max_concurrent_inserts: 8,
        }
    }

    pub fn host<S: Into<String>>(mut self, host: S) -> Self {
        self.host = host.into();
        self
    }

    pub fn native_port(mut self, port: u16) -> Self {
        self.native_port = port;
        self
    }

    pub fn http_port(mut self, port: u16) -> Self {
        self.http_port = port;
        self
    }

    pub fn username<S: Into<String>>(mut self, username: S) -> Self {
        self.username = username.into();
        self
    }

    pub fn password<S: Into<String>>(mut self, password: S) -> Self {
        self.password = password.into();
        self
    }

    pub fn database<S: Into<String>>(mut self, database: S) -> Self {
        self.database = database.into();
        self
    }

    pub fn max_concurrent_inserts(mut self, max: usize) -> Self {
        self.max_concurrent_inserts = max;
        self
    }

    pub fn build(self) -> ClickHouseConfig {
        ClickHouseConfig {
            host: self.host,
            native_port: self.native_port,
            http_port: self.http_port,
            username: self.username,
            password: self.password,
            database: self.database,
            max_concurrent_inserts: self.max_concurrent_inserts,
        }
    }
}

impl Default for ClickHouseConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for RoutingConfig
#[derive(Debug, Clone)]
pub struct RoutingConfigBuilder {
    db_fields: Vec<String>,
    table_fields: Vec<String>,
    default_db: String,
    default_table: String,
    org_id_field: Option<String>,
    routed_orgs: Vec<String>,
    route_all_by_org: bool,
    category_to_table: HashMap<String, String>,
}

impl RoutingConfigBuilder {
    pub fn new() -> Self {
        Self {
            db_fields: vec!["org_id".to_string()],
            table_fields: vec!["event_category".to_string()],
            default_db: "common".to_string(),
            default_table: "events".to_string(),
            org_id_field: Some("org_id".to_string()),
            routed_orgs: vec![],
            route_all_by_org: false,
            category_to_table: HashMap::new(),
        }
    }

    pub fn db_fields(mut self, fields: Vec<&str>) -> Self {
        self.db_fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn table_fields(mut self, fields: Vec<&str>) -> Self {
        self.table_fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn default_db<S: Into<String>>(mut self, db: S) -> Self {
        self.default_db = db.into();
        self
    }

    pub fn default_table<S: Into<String>>(mut self, table: S) -> Self {
        self.default_table = table.into();
        self
    }

    pub fn org_id_field<S: Into<String>>(mut self, field: S) -> Self {
        self.org_id_field = Some(field.into());
        self
    }

    pub fn routed_orgs(mut self, orgs: Vec<&str>) -> Self {
        self.routed_orgs = orgs.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn route_all_by_org(mut self, enable: bool) -> Self {
        self.route_all_by_org = enable;
        self
    }

    pub fn category_to_table(mut self, category: &str, table: &str) -> Self {
        self.category_to_table.insert(category.to_string(), table.to_string());
        self
    }

    pub fn build(self) -> RoutingConfig {
        RoutingConfig {
            db_fields: self.db_fields,
            table_fields: self.table_fields,
            default_db: self.default_db,
            default_table: self.default_table,
            org_id_field: self.org_id_field,
            routed_orgs: self.routed_orgs,
            route_all_by_org: self.route_all_by_org,
            category_to_table: self.category_to_table,
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: DlqConfig::default(),
            rules: vec![],
        }
    }
}

impl Default for RoutingConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for TimestampConfig
#[derive(Debug, Clone)]
pub struct TimestampConfigBuilder {
    source_fields: Vec<String>,
    output_field: String,
    fallback_to_now: bool,
    warn_on_correction: bool,
}

impl TimestampConfigBuilder {
    pub fn new() -> Self {
        Self {
            source_fields: vec!["timestamp".to_string(), "@timestamp".to_string()],
            output_field: "timestamp".to_string(),
            fallback_to_now: true,
            warn_on_correction: false,
        }
    }

    pub fn source_fields(mut self, fields: Vec<&str>) -> Self {
        self.source_fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn output_field<S: Into<String>>(mut self, field: S) -> Self {
        self.output_field = field.into();
        self
    }

    pub fn fallback_to_now(mut self, enable: bool) -> Self {
        self.fallback_to_now = enable;
        self
    }

    pub fn warn_on_correction(mut self, enable: bool) -> Self {
        self.warn_on_correction = enable;
        self
    }

    pub fn build(self) -> TimestampConfig {
        TimestampConfig {
            source_fields: self.source_fields,
            output_field: self.output_field,
            fallback_to_now: self.fallback_to_now,
            warn_on_correction: self.warn_on_correction,
        }
    }
}

impl Default for TimestampConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for MetadataConfig
#[derive(Debug, Clone)]
pub struct MetadataConfigBuilder {
    tags_fields: Vec<String>,
    tags_output: String,
    drop_tags: bool,
}

impl MetadataConfigBuilder {
    pub fn new() -> Self {
        Self {
            tags_fields: vec!["tags".to_string(), "_tags".to_string()],
            tags_output: "_tags".to_string(),
            drop_tags: false,
        }
    }

    pub fn tags_fields(mut self, fields: Vec<&str>) -> Self {
        self.tags_fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn tags_output<S: Into<String>>(mut self, field: S) -> Self {
        self.tags_output = field.into();
        self
    }

    pub fn drop_tags(mut self, enable: bool) -> Self {
        self.drop_tags = enable;
        self
    }

    pub fn build(self) -> MetadataConfig {
        MetadataConfig {
            tags_fields: self.tags_fields,
            tags_output: self.tags_output,
            drop_tags: self.drop_tags,
        }
    }
}

impl Default for MetadataConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_config_builder() {
        let config = BufferConfigBuilder::new()
            .flush_rows(5000)
            .flush_bytes(2_000_000)
            .flush_age_secs(120)
            .build();

        assert_eq!(config.flush_rows, 5000);
        assert_eq!(config.flush_bytes, 2_000_000);
        assert_eq!(config.flush_age_secs, 120);
    }

    #[test]
    fn test_clickhouse_config_builder() {
        let config = ClickHouseConfigBuilder::new()
            .host("k8s.tyrell.com.au")
            .native_port(30900)
            .username("test")
            .password("secret")
            .database("test")
            .build();

        assert_eq!(config.host, "k8s.tyrell.com.au");
        assert_eq!(config.native_port, 30900);
        assert_eq!(config.username, "test");
        assert_eq!(config.password, "secret");
        assert_eq!(config.database, "test");
    }

    #[test]
    fn test_routing_config_builder() {
        let config = RoutingConfigBuilder::new()
            .db_fields(vec!["org_id", "tags.event.org_id"])
            .table_fields(vec!["event_category"])
            .default_db("shared")
            .default_table("events")
            .category_to_table("auth", "auth_events")
            .build();

        assert_eq!(config.db_fields, vec!["org_id", "tags.event.org_id"]);
        assert_eq!(config.table_fields, vec!["event_category"]);
        assert_eq!(config.default_db, "shared");
        assert_eq!(config.default_table, "events");
        assert_eq!(config.category_to_table.get("auth"), Some(&"auth_events".to_string()));
    }

    #[test]
    fn test_timestamp_config_builder() {
        let config = TimestampConfigBuilder::new()
            .source_fields(vec!["ts", "timestamp"])
            .output_field("event_time")
            .fallback_to_now(false)
            .warn_on_correction(true)
            .build();

        assert_eq!(config.source_fields, vec!["ts", "timestamp"]);
        assert_eq!(config.output_field, "event_time");
        assert_eq!(config.fallback_to_now, false);
        assert_eq!(config.warn_on_correction, true);
    }

    #[test]
    fn test_metadata_config_builder() {
        let config = MetadataConfigBuilder::new()
            .tags_fields(vec!["meta", "tags"])
            .tags_output("_metadata")
            .drop_tags(true)
            .build();

        assert_eq!(config.tags_fields, vec!["meta", "tags"]);
        assert_eq!(config.tags_output, "_metadata");
        assert_eq!(config.drop_tags, true);
    }
}
