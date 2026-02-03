// Project:   dfe-loader
// File:      auto_init.rs
// Purpose:   Auto-initialization of Kafka topics and ClickHouse schema
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Auto-initialization for pipeline infrastructure
//!
//! Creates missing Kafka topics and ClickHouse database/table on startup.
//! All operations are best-effort - failures log warnings but don't prevent startup.
//!
//! ## Features
//!
//! - **Engine auto-detection**: SharedMergeTree → ReplicatedMergeTree → MergeTree
//! - **Text search index**: full_text (25.1+) or ngrambf bloom filter fallback
//! - **Capability detection**: Queries ClickHouse version and available features
//!
//! ## Usage
//!
//! ```ignore
//! use dfe_loader::pipeline::AutoInitializer;
//! use dfe_loader::config::Config;
//!
//! let config = Config::load(None)?;
//! let initializer = AutoInitializer::new(&config);
//! initializer.run().await?;
//! ```

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use tracing::{debug, info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::config::{Config, KafkaConfig};
use crate::schema::{
    add_text_index_ddl, render_ddl_with_engine, ClusterCapabilities, DETECT_CLUSTER_SQL,
    DETECT_SHARED_MERGE_TREE_SQL, DETECT_VERSION_SQL,
};
use crate::Result;

/// Auto-initializer for pipeline infrastructure
pub struct AutoInitializer<'a> {
    config: &'a Config,
}

impl<'a> AutoInitializer<'a> {
    /// Create a new auto-initializer
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }

    /// Run all auto-initialization steps
    ///
    /// Creates missing infrastructure in order:
    /// 1. Kafka topics (if create_topics is true)
    /// 2. ClickHouse database (if create_database is true)
    /// 3. ClickHouse table (if create_table is true)
    /// 4. Text search index (if create_text_index is true)
    ///
    /// All operations are best-effort - failures log warnings but don't prevent startup.
    pub async fn run(&self) -> Result<()> {
        let auto_init = &self.config.auto_init;

        if !auto_init.enabled {
            debug!("Auto-initialization disabled");
            return Ok(());
        }

        info!("Running auto-initialization");

        // Kafka topic creation
        if auto_init.create_topics {
            if let Err(e) = self.create_kafka_topics().await {
                warn!(error = %e, "Failed to create Kafka topics (continuing anyway)");
            }
        }

        // ClickHouse database, table, and index creation
        if auto_init.create_database || auto_init.create_table || auto_init.create_text_index {
            if let Err(e) = self.create_clickhouse_schema().await {
                warn!(error = %e, "Failed to create ClickHouse schema (continuing anyway)");
            }
        }

        info!("Auto-initialization complete");
        Ok(())
    }

    /// Create Kafka topics if they don't exist
    async fn create_kafka_topics(&self) -> Result<()> {
        let kafka_config = &self.config.kafka;
        let auto_init = &self.config.auto_init;

        if kafka_config.topics.is_empty() {
            debug!("No Kafka topics configured, skipping topic creation");
            return Ok(());
        }

        // Build admin client config
        let admin_client = self.build_kafka_admin_client(kafka_config)?;

        // Create topics
        let topics: Vec<NewTopic> = kafka_config
            .topics
            .iter()
            .map(|name| {
                NewTopic::new(
                    name,
                    auto_init.topic_partitions,
                    TopicReplication::Fixed(auto_init.topic_replication_factor),
                )
            })
            .collect();

        let topic_names: Vec<&str> = topics.iter().map(|t| t.name).collect();
        info!(topics = ?topic_names, "Attempting to create Kafka topics");

        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        let results = admin_client.create_topics(&topics, &opts).await?;

        for result in results {
            match result {
                Ok(name) => {
                    info!(topic = %name, "Kafka topic created (or already exists)");
                }
                Err((name, err)) => {
                    // TopicAlreadyExists is not an error
                    let err_str = format!("{:?}", err);
                    if err_str.contains("TopicAlreadyExists") {
                        debug!(topic = %name, "Kafka topic already exists");
                    } else {
                        warn!(topic = %name, error = %err_str, "Failed to create Kafka topic");
                    }
                }
            }
        }

        Ok(())
    }

    /// Build Kafka admin client with SASL/TLS config
    fn build_kafka_admin_client(
        &self,
        kafka_config: &KafkaConfig,
    ) -> Result<AdminClient<DefaultClientContext>> {
        let mut client_config = ClientConfig::new();

        // Basic config
        client_config.set("bootstrap.servers", kafka_config.brokers.join(","));
        client_config.set("client.id", format!("{}-admin", kafka_config.client_id));

        // SASL config
        if let Some(ref sasl) = kafka_config.sasl {
            if sasl.enabled {
                let mech = sasl.mechanism();
                if let Some(mechanism) = mech.as_rdkafka_mechanism() {
                    client_config.set("sasl.mechanism", mechanism);
                    client_config.set("security.protocol", "SASL_PLAINTEXT");

                    if mech.requires_credentials() {
                        client_config.set("sasl.username", &sasl.username);
                        client_config.set("sasl.password", &sasl.password);
                    }
                }
            }
        }

        // TLS config
        if let Some(ref tls) = kafka_config.tls {
            if tls.enabled {
                // Upgrade protocol if SASL is also enabled
                if kafka_config
                    .sasl
                    .as_ref()
                    .map(|s| s.enabled)
                    .unwrap_or(false)
                {
                    client_config.set("security.protocol", "SASL_SSL");
                } else {
                    client_config.set("security.protocol", "SSL");
                }

                if let Some(ref ca) = tls.ca_cert_file {
                    client_config.set("ssl.ca.location", ca);
                }
                if let Some(ref cert) = tls.cert_file {
                    client_config.set("ssl.certificate.location", cert);
                }
                if let Some(ref key) = tls.key_file {
                    client_config.set("ssl.key.location", key);
                }
                if tls.skip_verify {
                    client_config.set("enable.ssl.certificate.verification", "false");
                }
            }
        }

        let admin: AdminClient<DefaultClientContext> = client_config.create()?;
        Ok(admin)
    }

    /// Detect ClickHouse cluster capabilities
    async fn detect_capabilities(&self, client: &ArrowClickHouseClient) -> ClusterCapabilities {
        // Get version
        let version = match client.select(DETECT_VERSION_SQL).await {
            Ok(batches) if !batches.is_empty() => {
                // Extract version string from result
                if let Some(batch) = batches.first() {
                    if batch.num_rows() > 0 {
                        // Try to extract as string
                        if let Some(col) = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                        {
                            col.value(0).to_string()
                        } else if let Some(col) = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::BinaryArray>()
                        {
                            String::from_utf8_lossy(col.value(0)).to_string()
                        } else {
                            "unknown".to_string()
                        }
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                }
            }
            _ => "unknown".to_string(),
        };

        // Check if SharedMergeTree is available
        let shared_merge_tree = match client.select(DETECT_SHARED_MERGE_TREE_SQL).await {
            Ok(batches) if !batches.is_empty() => {
                if let Some(batch) = batches.first() {
                    if batch.num_rows() > 0 {
                        // Result is a boolean-like value
                        true // If query succeeds, SharedMergeTree exists
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        };

        // Check if cluster is configured
        let is_clustered = match client.select(DETECT_CLUSTER_SQL).await {
            Ok(batches) if !batches.is_empty() => {
                batches.first().map(|b| b.num_rows() > 0).unwrap_or(false)
            }
            _ => false,
        };

        let mut caps = ClusterCapabilities::from_version(&version, is_clustered);

        // Override SharedMergeTree detection with actual query result
        caps.shared_merge_tree = shared_merge_tree;

        info!(
            version = %caps.version,
            engine = %caps.best_engine(),
            shared_merge_tree = caps.shared_merge_tree,
            full_text_index = caps.full_text_index,
            is_clustered = caps.is_clustered,
            "Detected ClickHouse capabilities"
        );

        caps
    }

    /// Create ClickHouse database and table if they don't exist
    async fn create_clickhouse_schema(&self) -> Result<()> {
        let auto_init = &self.config.auto_init;
        let routing = &self.config.routing;

        // Connect to ClickHouse
        let ch_config: crate::clickhouse::ClickHouseConfig = (&self.config.clickhouse).into();
        let client = ArrowClickHouseClient::new(&ch_config).await?;

        // Get database and table names from routing config
        let db = &routing.default_db;
        let table = &routing.default_table;

        // Detect cluster capabilities for engine selection
        let capabilities = self.detect_capabilities(&client).await;
        let engine = capabilities.best_engine();

        // Create database
        if auto_init.create_database {
            let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS {}", db);
            info!(database = %db, "Attempting to create ClickHouse database");

            match client.query(&create_db_sql).await {
                Ok(()) => {
                    info!(database = %db, "ClickHouse database created (or already exists)");
                }
                Err(e) => {
                    warn!(database = %db, error = %e, "Failed to create ClickHouse database");
                }
            }
        }

        // Create table using embedded DDL template with auto-detected engine
        if auto_init.create_table {
            let create_table_sql = render_ddl_with_engine(db, table, engine);

            info!(
                table = %format!("{}.{}", db, table),
                engine = %engine,
                "Attempting to create ClickHouse table"
            );

            match client.query(&create_table_sql).await {
                Ok(()) => {
                    info!(
                        table = %format!("{}.{}", db, table),
                        engine = %engine,
                        "ClickHouse table created (or already exists)"
                    );
                }
                Err(e) => {
                    warn!(
                        table = %format!("{}.{}", db, table),
                        error = %e,
                        "Failed to create ClickHouse table"
                    );
                }
            }
        }

        // Add text search index on _raw
        if auto_init.create_text_index {
            let index_type = if capabilities.full_text_index {
                "full_text"
            } else {
                "ngrambf_v1"
            };

            let add_index_sql = add_text_index_ddl(db, table, "_raw", &capabilities);

            info!(
                table = %format!("{}.{}", db, table),
                index_type = %index_type,
                "Attempting to add text search index"
            );

            match client.query(&add_index_sql).await {
                Ok(()) => {
                    info!(
                        table = %format!("{}.{}", db, table),
                        index_type = %index_type,
                        "Text search index added (or already exists)"
                    );
                }
                Err(e) => {
                    // Index already exists is not an error
                    let err_str = e.to_string();
                    if err_str.contains("already exists") || err_str.contains("ALREADY_EXISTS") {
                        debug!(
                            table = %format!("{}.{}", db, table),
                            "Text search index already exists"
                        );
                    } else {
                        warn!(
                            table = %format!("{}.{}", db, table),
                            error = %e,
                            "Failed to add text search index"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AutoInitConfig;
    use crate::schema::{render_ddl_with_engine, TableEngine, COMMON_TABLE_DDL};

    #[test]
    fn test_embedded_ddl_substitution() {
        let ddl = render_ddl_with_engine("common", "events", TableEngine::MergeTree);

        assert!(ddl.contains("common.events"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(ddl.contains("_org_id"));
        assert!(ddl.contains("generateUUIDv7()"));
        assert!(ddl.contains("MergeTree()"));
    }

    #[test]
    fn test_embedded_ddl_with_shared_merge_tree() {
        let ddl = render_ddl_with_engine("common", "events", TableEngine::SharedMergeTree);
        assert!(ddl.contains("SharedMergeTree()"));
    }

    #[test]
    fn test_embedded_ddl_with_replicated_merge_tree() {
        let ddl = render_ddl_with_engine("common", "events", TableEngine::ReplicatedMergeTree);
        assert!(ddl.contains("ReplicatedMergeTree("));
        assert!(ddl.contains("/common/events"));
    }

    #[test]
    fn test_embedded_ddl_available() {
        // Verify the embedded DDL template is available at compile time
        assert!(!COMMON_TABLE_DDL.is_empty());
        assert!(COMMON_TABLE_DDL.contains("{db}.{table}"));
        assert!(COMMON_TABLE_DDL.contains("{engine}"));
    }

    #[test]
    fn test_auto_init_config_defaults() {
        let config = AutoInitConfig::default();
        assert!(config.enabled);
        assert!(config.create_topics);
        assert!(config.create_database);
        assert!(config.create_table);
        assert!(config.create_text_index);
        assert_eq!(config.topic_partitions, 3);
        assert_eq!(config.topic_replication_factor, 1);
    }

    #[test]
    fn test_capabilities_engine_selection() {
        // 25.x cluster with SharedMergeTree
        let caps = ClusterCapabilities::from_version("25.1.2.123", true);
        assert_eq!(caps.best_engine(), TableEngine::SharedMergeTree);

        // 23.x cluster without SharedMergeTree
        let caps = ClusterCapabilities::from_version("23.8.1.0", true);
        assert_eq!(caps.best_engine(), TableEngine::ReplicatedMergeTree);

        // Single node
        let caps = ClusterCapabilities::from_version("23.8.1.0", false);
        assert_eq!(caps.best_engine(), TableEngine::MergeTree);
    }

    #[test]
    fn test_text_index_ddl_selection() {
        // New version with full_text
        let caps = ClusterCapabilities::from_version("25.1.0.0", false);
        let ddl = add_text_index_ddl("db", "tbl", "col", &caps);
        assert!(ddl.contains("full_text(0)"));

        // Old version with fallback
        let caps = ClusterCapabilities::from_version("23.8.0.0", false);
        let ddl = add_text_index_ddl("db", "tbl", "col", &caps);
        assert!(ddl.contains("ngrambf_v1"));
    }
}
