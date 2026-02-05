// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Testcontainers infrastructure for isolated integration testing
//!
//! Provides Docker-based ClickHouse and Kafka containers for tests.
//! Only available with `--features testcontainers`.

#[cfg(feature = "testcontainers")]
pub use testcontainers_impl::*;

#[cfg(feature = "testcontainers")]
mod testcontainers_impl {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, Image, ImageExt};
    use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;
    use testcontainers_modules::kafka::Kafka as KafkaImage;

    use dfe_loader::config::{ClickHouseConfig, KafkaConfig};

    /// Test infrastructure with isolated Docker containers
    pub struct TestInfrastructure {
        pub clickhouse: Option<ContainerAsync<ClickHouseImage>>,
        pub kafka: Option<ContainerAsync<KafkaImage>>,
    }

    impl TestInfrastructure {
        /// Create new test infrastructure
        ///
        /// # Arguments
        /// * `need_clickhouse` - Start ClickHouse container
        /// * `need_kafka` - Start Kafka container
        pub async fn new(need_clickhouse: bool, need_kafka: bool) -> Self {
            let clickhouse = if need_clickhouse {
                Some(start_clickhouse().await)
            } else {
                None
            };

            let kafka = if need_kafka {
                Some(start_kafka().await)
            } else {
                None
            };

            Self { clickhouse, kafka }
        }

        /// Get ClickHouse configuration
        pub fn clickhouse_config(&self) -> Option<ClickHouseConfig> {
            let container = self.clickhouse.as_ref()?;
            let host = container.get_host().await.ok()?;
            let native_port = container.get_host_port_ipv4(9000).await.ok()?;
            let http_port = container.get_host_port_ipv4(8123).await.ok()?;

            Some(ClickHouseConfig {
                hosts: vec![format!("{}:{}", host, native_port)],
                database: "default".to_string(),
                username: "default".to_string(),
                password: String::new(),
                protocol: "native".to_string(),
                tables: Vec::new(),
                tls: None,
            })
        }

        /// Get Kafka configuration
        pub fn kafka_config(&self) -> Option<KafkaConfig> {
            let container = self.kafka.as_ref()?;
            let host = container.get_host().await.ok()?;
            let port = container.get_host_port_ipv4(9093).await.ok()?;

            Some(KafkaConfig {
                brokers: vec![format!("{}:{}", host, port)],
                topics: vec!["test-events".to_string()],
                group: "test-group".to_string(),
                topic_regex: None,
                client_id: "test-client".to_string(),
                sasl: None,
                tls: None,
            })
        }
    }

    /// Start ClickHouse container
    async fn start_clickhouse() -> ContainerAsync<ClickHouseImage> {
        ClickHouseImage::default()
            .start()
            .await
            .expect("Failed to start ClickHouse container")
    }

    /// Start Kafka container
    async fn start_kafka() -> ContainerAsync<KafkaImage> {
        KafkaImage::default()
            .start()
            .await
            .expect("Failed to start Kafka container")
    }
}

/// Placeholder module when testcontainers feature is disabled
#[cfg(not(feature = "testcontainers"))]
mod testcontainers_impl {
    pub struct TestInfrastructure;

    impl TestInfrastructure {
        pub async fn new(_need_clickhouse: bool, _need_kafka: bool) -> Self {
            panic!("Testcontainers feature not enabled. Run with: cargo test --features testcontainers");
        }
    }
}
