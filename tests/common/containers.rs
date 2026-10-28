// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

#![allow(dead_code)]

//! Testcontainers infrastructure for isolated integration testing
//!
//! Provides Docker-based `ClickHouse` and Kafka containers for tests.
//! Only available with `--features testcontainers`.

#[cfg(feature = "testcontainers")]
mod testcontainers_impl {
    use std::time::Duration;

    use testcontainers::core::WaitFor;
    use testcontainers::core::logs::LogFrame;
    use testcontainers::core::wait::HttpWaitStrategy;
    use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
    use testcontainers_modules::clickhouse::{CLICKHOUSE_PORT, ClickHouse as ClickHouseImage};
    // The `apache` module, NOT the crate's default (`confluentinc/cp-kafka`).
    // cp-kafka is amd64-only, so on arm64 it runs a JVM under QEMU: ~30s to
    // become ready instead of ~1s, and concurrent brokers then trade
    // BrokerTransportFailure and produce timeouts. Both `apache/kafka` images
    // are multi-arch, so neither pays that cost.
    use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka as KafkaImage};

    /// ClickHouse to test against -- the server dfe-infra `versions.yaml`
    /// deploys and `docker-compose.dev.yaml` runs, pinned to the patch and its
    /// digest rather than the 26.3 minor, so a rebuild of that tag cannot
    /// change what a green run tested against. Not the module default of
    /// 23.3.8.21-alpine, which is from March 2023: testing three years behind
    /// the deployed server is how a query that works in CI meets a changed
    /// default in production.
    ///
    /// Tag and digest are separate consts because the org Renovate regex reads
    /// a bare tag on the line below the annotation and stops at a colon, so a
    /// `tag@sha256:...` value would take the pin out of review.
    // renovate: datasource=docker depName=clickhouse/clickhouse-server
    const CLICKHOUSE_TAG: &str = "26.3.42.3";

    /// Digest of `CLICKHOUSE_TAG`. Docker resolves the reference by digest, so
    /// a tag moved without this one still pulls the old server.
    const CLICKHOUSE_DIGEST: &str =
        "sha256:21d572843e59539c7d100286b6f5a6053c341fe4b33c2d7b74b1ff7cb24c5399";

    /// Bound on one ClickHouse readiness probe, so a hung connection costs a
    /// retry instead of the whole startup budget.
    const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

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
        /// * `test` - The calling test, which names the containers it starts.
        ///   Pass `test_name!()`. Two tests must not share a name: nextest runs
        ///   each in its own process, so they start their own containers and a
        ///   shared name collides rather than sharing.
        /// * `need_clickhouse` - Start ClickHouse container
        /// * `need_kafka` - Start Kafka container
        pub async fn new(test: &str, need_clickhouse: bool, need_kafka: bool) -> Self {
            crate::common::test_logging::init();

            // CI provides the daemon, so its absence here means these tests
            // would silently exercise nothing. Fail loudly instead. Locally a
            // missing daemon is just a developer with Docker stopped, and the
            // container start below reports that plainly enough.
            crate::common::require_docker_in_ci();

            let clickhouse = if need_clickhouse {
                Some(start_clickhouse(test).await)
            } else {
                None
            };

            let kafka = if need_kafka {
                Some(start_kafka(test).await)
            } else {
                None
            };

            Self { clickhouse, kafka }
        }

        /// ClickHouse where the passwordless `default` user may create users
        /// and grant to them, with Kafka beside it if `need_kafka`.
        pub async fn clickhouse_managing_users(test: &str, need_kafka: bool) -> Self {
            crate::common::test_logging::init();
            crate::common::require_docker_in_ci();
            let clickhouse = start_clickhouse_with(test, Some(CLICKHOUSE_ACCESS_MANAGEMENT)).await;
            let kafka = if need_kafka {
                Some(start_kafka(test).await)
            } else {
                None
            };
            Self {
                clickhouse: Some(clickhouse),
                kafka,
            }
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
        pub async fn from_contract(test: &str) -> Self {
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
            Self::new(test, need_clickhouse, need_kafka).await
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
                password: scalo::config::sensitive::SensitiveString::default(),
                protocol: "native".to_string(),
                insert_format: dfe_loader::clickhouse::InsertFormat::default(),
                tables: Vec::new(),
                tls: None,
                ..Default::default()
            })
        }

        /// Get Kafka configuration
        pub async fn kafka_config(&self) -> Option<KafkaConfig> {
            let container = self.kafka.as_ref()?;
            let host = container.get_host().await.ok()?;
            let port = container.get_host_port_ipv4(KAFKA_PORT).await.ok()?;

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

    /// Client for the readiness probe: testcontainers' default client has no
    /// request timeout.
    fn readiness_probe_client() -> testcontainers_reqwest::Client {
        testcontainers_reqwest::Client::builder()
            .timeout(READINESS_PROBE_TIMEOUT)
            .build()
            .expect("Failed to build the ClickHouse readiness probe client")
    }

    /// The module's own ready condition (GET / on 8123 answers 200), declared
    /// here only to bound each probe.
    fn clickhouse_ready() -> WaitFor {
        WaitFor::http(
            HttpWaitStrategy::new("/")
                .with_port(CLICKHOUSE_PORT)
                .with_expected_status_code(200_u16)
                .with_client(readiness_probe_client()),
        )
    }

    /// Echoes container output to stderr under the container's name, because
    /// testcontainers removes the container, and its logs, on a failed start.
    fn echo_logs(name: String) -> impl Fn(&LogFrame) + Send + Sync + 'static {
        move |frame| {
            for line in String::from_utf8_lossy(frame.bytes()).lines() {
                eprintln!("[{name} {}] {line}", frame.source());
            }
        }
    }

    /// Users config that lets `default` create users and grant to them.
    const CLICKHOUSE_ACCESS_MANAGEMENT: &str = r"<clickhouse>
    <users>
        <default>
            <access_management>1</access_management>
        </default>
    </users>
</clickhouse>
";

    /// Start ClickHouse container
    async fn start_clickhouse(test: &str) -> ContainerAsync<ClickHouseImage> {
        start_clickhouse_with(test, None).await
    }

    /// Start ClickHouse, with `users_config` added under `users.d` if given.
    async fn start_clickhouse_with(
        test: &str,
        users_config: Option<&str>,
    ) -> ContainerAsync<ClickHouseImage> {
        let name = crate::common::container_name(Some(test), "clickhouse");
        crate::common::reap_stale(&name);
        let mut image = ClickHouseImage::default()
            .with_tag(format!("{CLICKHOUSE_TAG}@{CLICKHOUSE_DIGEST}"))
            // From 25.x the entrypoint refuses to leave `default` passwordless
            // unless told to, and rejects every connection with "Authentication
            // failed" instead. The module's own config predates that. Tests
            // connect as default with no password, so keep that and say so.
            .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1");
        if let Some(users_config) = users_config {
            image = image.with_copy_to(
                "/etc/clickhouse-server/users.d/dfe-test-users.xml",
                users_config.as_bytes().to_vec(),
            );
        }
        image
            .with_container_name(&name)
            .with_labels(crate::common::test_labels("clickhouse"))
            .with_ready_conditions(vec![clickhouse_ready()])
            .with_log_consumer(echo_logs(name.clone()))
            // The 60s testcontainers default is too tight under CI container
            // contention: the server cold-start intermittently overruns it and
            // fails the run with WaitContainer(StartupTimeout).
            .with_startup_timeout(Duration::from_secs(180))
            .start()
            .await
            .expect("Failed to start ClickHouse container")
    }

    /// ClickHouse's HTTPS port inside the container.
    const CLICKHOUSE_HTTPS_PORT: u16 = 8443;

    /// Server config that opens HTTPS on 8443 with the certificate copied in beside it.
    const CLICKHOUSE_HTTPS_CONFIG: &str = r"<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>false</loadDefaultCAFile>
        </server>
    </openSSL>
</clickhouse>
";

    /// Start ClickHouse serving HTTP on `http_host_port` and HTTPS with
    /// `cert_pem` and `key_pem` on `https_host_port`.
    ///
    /// Both are mapped explicitly: one explicit mapping turns off publishing
    /// the image's own ports, and the readiness probe needs 8123.
    pub async fn start_clickhouse_https(
        test: &str,
        cert_pem: String,
        key_pem: String,
        http_host_port: u16,
        https_host_port: u16,
    ) -> ContainerAsync<ClickHouseImage> {
        let name = crate::common::container_name(Some(test), "clickhouse");
        crate::common::reap_stale(&name);
        ClickHouseImage::default()
            .with_tag(format!("{CLICKHOUSE_TAG}@{CLICKHOUSE_DIGEST}"))
            .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
            .with_copy_to(
                "/etc/clickhouse-server/config.d/dfe-https.xml",
                CLICKHOUSE_HTTPS_CONFIG.as_bytes().to_vec(),
            )
            .with_copy_to(
                "/etc/clickhouse-server/certs/server.crt",
                cert_pem.into_bytes(),
            )
            .with_copy_to(
                "/etc/clickhouse-server/certs/server.key",
                key_pem.into_bytes(),
            )
            .with_mapped_port(http_host_port, CLICKHOUSE_PORT)
            .with_mapped_port(
                https_host_port,
                testcontainers::core::ContainerPort::Tcp(CLICKHOUSE_HTTPS_PORT),
            )
            .with_container_name(&name)
            .with_labels(crate::common::test_labels("clickhouse"))
            .with_ready_conditions(vec![clickhouse_ready()])
            .with_log_consumer(echo_logs(name.clone()))
            .with_startup_timeout(Duration::from_secs(180))
            .start()
            .await
            .expect("Failed to start ClickHouse container with HTTPS")
    }

    /// Start Kafka container
    async fn start_kafka(test: &str) -> ContainerAsync<KafkaImage> {
        start_kafka_with(test, None).await
    }

    /// Start Kafka listening on `host_port`, for a loader configured with that
    /// address before the broker exists.
    pub async fn start_kafka_on(test: &str, host_port: u16) -> ContainerAsync<KafkaImage> {
        start_kafka_with(test, Some(host_port)).await
    }

    /// Start Kafka, on `host_port` if given.
    async fn start_kafka_with(test: &str, host_port: Option<u16>) -> ContainerAsync<KafkaImage> {
        let name = crate::common::container_name(Some(test), "kafka");
        crate::common::reap_stale(&name);
        // The module's default `apache/kafka-native` segfaults in `getpwuid`
        // during its GraalVM `setup` binary, so use the JVM image.
        let mut image = KafkaImage::default()
            .with_jvm_image()
            .with_tag(scalo::transport::kafka::test_support::kafka_tag());
        if let Some(host_port) = host_port {
            image = image.with_mapped_port(host_port, KAFKA_PORT);
        }
        image
            .with_container_name(&name)
            .with_labels(crate::common::test_labels("kafka"))
            // A JVM broker takes tens of seconds to print "Kafka Server
            // started", which the 60s testcontainers default cuts too close
            // under CI container contention.
            .with_startup_timeout(Duration::from_secs(180))
            .start()
            .await
            .expect("Failed to start Kafka container")
    }

    #[cfg(test)]
    mod tests {
        use super::{READINESS_PROBE_TIMEOUT, readiness_probe_client};

        /// A server that accepts the connection and never answers must fail the
        /// probe within its bound, or the readiness wait never gets to retry.
        #[tokio::test]
        async fn readiness_probe_gives_up_on_a_server_that_never_answers() {
            // Bound but never accepted: the kernel completes the handshake and nothing replies.
            let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/", silent.local_addr().unwrap());

            let started = std::time::Instant::now();
            let sent = tokio::time::timeout(
                READINESS_PROBE_TIMEOUT * 3,
                readiness_probe_client().get(url).send(),
            )
            .await
            .expect("the probe was still waiting at three times its bound");
            let elapsed = started.elapsed();

            let err = sent.expect_err("a server that never answers cannot produce a response");
            assert!(err.is_timeout(), "expected a timeout, got: {err}");
            assert!(
                elapsed >= READINESS_PROBE_TIMEOUT,
                "probe gave up after {elapsed:?}, before its {READINESS_PROBE_TIMEOUT:?} bound"
            );
        }
    }
}

/// Placeholder module when testcontainers feature is disabled
#[cfg(not(feature = "testcontainers"))]
mod testcontainers_impl {
    pub struct TestInfrastructure;

    impl TestInfrastructure {
        pub async fn new(_test: &str, _need_clickhouse: bool, _need_kafka: bool) -> Self {
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
#[cfg(feature = "testcontainers")]
#[allow(unused_imports)]
pub use testcontainers_impl::{start_clickhouse_https, start_kafka_on};
