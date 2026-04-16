// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

#![allow(dead_code)]

//! Testcontainers infrastructure for isolated integration testing
//!
//! Provides Docker-based `ClickHouse` and Kafka containers for tests.
//! Only available with `--features testcontainers`.

#[cfg(feature = "testcontainers")]
mod testcontainers_impl {
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
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

        /// Spin up exactly the containers the app declares it depends on.
        ///
        /// Reads `Config::deployment_contract().depends_on` (the same list that
        /// drives Helm/Compose generation) and starts the corresponding
        /// testcontainers. Keeps test infra in lockstep with deployment infra
        /// — when the app gains a new `depends_on` entry, tests pick it up
        /// automatically.
        ///
        /// Currently recognises: `kafka`, `clickhouse`. Unknown entries are
        /// logged and ignored (so a new dep doesn't break existing tests
        /// before a corresponding image is wired in).
        pub async fn from_contract() -> Self {
            let contract = dfe_loader::config::Config::deployment_contract();
            let mut need_clickhouse = false;
            let mut need_kafka = false;
            for dep in &contract.depends_on {
                match dep.as_str() {
                    "clickhouse" => need_clickhouse = true,
                    "kafka" | "redpanda" => need_kafka = true,
                    other => {
                        eprintln!(
                            "TestInfrastructure::from_contract: ignoring unknown dependency \
                             '{other}' — wire up an image in containers.rs to support it"
                        );
                    }
                }
            }
            Self::new(need_clickhouse, need_kafka).await
        }

        /// Get ClickHouse configuration
        pub async fn clickhouse_config(&self) -> Option<ClickHouseConfig> {
            let container = self.clickhouse.as_ref()?;
            let host = container.get_host().await.ok()?;
            let native_port = container.get_host_port_ipv4(9000).await.ok()?;
            let _http_port = container.get_host_port_ipv4(8123).await.ok()?;

            Some(ClickHouseConfig {
                hosts: vec![format!("{}:{}", host, native_port)],
                database: "default".to_string(),
                username: "default".to_string(),
                password: hyperi_rustlib::config::sensitive::SensitiveString::default(),
                protocol: "native".to_string(),
                tables: Vec::new(),
                tls: None,
            })
        }

        /// Get Kafka configuration
        pub async fn kafka_config(&self) -> Option<KafkaConfig> {
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
                ..Default::default()
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
            panic!(
                "Testcontainers feature not enabled. Run with: cargo test --features testcontainers"
            );
        }
    }
}

// Re-export so tests can use `crate::common::containers::TestInfrastructure`
// regardless of whether the feature is enabled. Unused in binaries that don't
// reference it — hence the allow.
#[allow(unused_imports)]
pub use testcontainers_impl::TestInfrastructure;
