// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

#![allow(dead_code)]

//! The backing services the suite tests against, and how a test gets one.
//!
//! [`TestInfrastructure::new`] hands every test the run's one shared
//! `ClickHouse` and one shared Kafka, started by [`super::shared_service`].
//! [`TestInfrastructure::exclusive`] starts containers a test owns alone, for
//! a test that needs the whole server or broker to itself.
//! Only available with `--features testcontainers`.

#[cfg(feature = "testcontainers")]
mod testcontainers_impl {
    use std::sync::OnceLock;
    use std::time::Duration;

    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair,
        KeyUsagePurpose,
    };
    use testcontainers::core::logs::LogFrame;
    use testcontainers::core::wait::HttpWaitStrategy;
    use testcontainers::core::{ContainerPort, WaitFor};
    use testcontainers::{ContainerAsync, Image, ImageExt, runners::AsyncRunner};
    use testcontainers_modules::clickhouse::{CLICKHOUSE_PORT, ClickHouse as ClickHouseImage};
    // The `apache` module, NOT the crate's default (`confluentinc/cp-kafka`).
    // cp-kafka is amd64-only, so on arm64 it runs a JVM under QEMU: ~30s to
    // become ready instead of ~1s, and concurrent brokers then trade
    // BrokerTransportFailure and produce timeouts. Both `apache/kafka` images
    // are multi-arch, so neither pays that cost.
    use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka as KafkaImage};

    use crate::common::shared_service::{self, Ready, ServiceSpec, SharedContainer, Suite};

    /// This suite, for container names, labels and the `HYPERI_TEST_*` lookup.
    pub const SUITE: Suite = Suite {
        repo: "dfe-loader",
        manifest_dir: env!("CARGO_MANIFEST_DIR"),
    };

    /// ClickHouse to test against -- the server dfe-infra `versions.yaml`
    /// deploys and `docker-compose.dev.yaml` runs, pinned to the patch and its
    /// digest rather than the 26.3 minor, so a rebuild of that tag cannot
    /// change what a green run tested against.
    ///
    /// Tag and digest are separate consts because the org Renovate regex reads
    /// a bare tag on the line below the annotation and stops at a colon, so a
    /// `tag@sha256:...` value would take the pin out of review.
    // renovate: datasource=docker depName=clickhouse/clickhouse-server
    pub const CLICKHOUSE_TAG: &str = "26.3.42.3";

    /// Digest of `CLICKHOUSE_TAG`. Docker resolves the reference by digest, so
    /// a tag moved without this one still pulls the old server.
    pub const CLICKHOUSE_DIGEST: &str =
        "sha256:21d572843e59539c7d100286b6f5a6053c341fe4b33c2d7b74b1ff7cb24c5399";

    /// ClickHouse's HTTP port inside the container.
    pub const CLICKHOUSE_HTTP_PORT: u16 = 8123;

    /// ClickHouse's native port inside the container.
    pub const CLICKHOUSE_NATIVE_PORT: u16 = 9000;

    /// ClickHouse's HTTPS port inside the container.
    pub const CLICKHOUSE_HTTPS_PORT: u16 = 8443;

    /// ClickHouse's native TLS port inside the container.
    pub const CLICKHOUSE_NATIVE_TLS_PORT: u16 = 9440;

    /// Where the shared ClickHouse keeps the CA that signed its certificate.
    pub const CLICKHOUSE_CA_PATH: &str = "/etc/clickhouse-server/certs/ca.crt";

    /// Bound on one ClickHouse readiness probe, so a hung connection costs a
    /// retry instead of the whole startup budget.
    const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

    /// A JVM broker or a cold ClickHouse under CI container contention can take
    /// most of a minute, so 60 s is too tight.
    const STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

    /// Where the Kafka entrypoint waits for its start script.
    const KAFKA_START_SCRIPT: &str = "/opt/kafka/hyperi_start.sh";

    /// Server config that opens HTTPS on 8443 and native TLS on 9440 with the
    /// certificate copied in beside it.
    const CLICKHOUSE_TLS_CONFIG: &str = r"<clickhouse>
    <https_port>8443</https_port>
    <tcp_port_secure>9440</tcp_port_secure>
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

    /// Users config that lets `default` create users and grant to them.
    const CLICKHOUSE_ACCESS_MANAGEMENT: &str = r"<clickhouse>
    <users>
        <default>
            <access_management>1</access_management>
        </default>
    </users>
</clickhouse>
";

    /// The run's ClickHouse: HTTP, native, HTTPS and native TLS, with
    /// `default` passwordless and allowed to manage users.
    fn clickhouse_spec() -> ServiceSpec {
        ServiceSpec {
            service: "clickhouse",
            image: format!("clickhouse/clickhouse-server:{CLICKHOUSE_TAG}@{CLICKHOUSE_DIGEST}"),
            ports: vec![
                CLICKHOUSE_HTTP_PORT,
                CLICKHOUSE_NATIVE_PORT,
                CLICKHOUSE_HTTPS_PORT,
                CLICKHOUSE_NATIVE_TLS_PORT,
            ],
            // From 25.x the entrypoint refuses to leave `default` passwordless
            // unless told to, and rejects every connection instead.
            env: vec![("CLICKHOUSE_SKIP_USER_SETUP".into(), "1".into())],
            entrypoint: None,
            args: Vec::new(),
            files: clickhouse_files,
            after_start: |_| {},
            ready: Ready::Http {
                port: CLICKHOUSE_HTTP_PORT,
                path: "/ping",
                body: "Ok.",
            },
            startup_timeout: STARTUP_TIMEOUT,
        }
    }

    /// TLS and users config, and a certificate for `localhost` and `host`
    /// signed by a CA made for this run.
    fn clickhouse_files(host: &str) -> Vec<(String, Vec<u8>)> {
        let ca = private_ca("dfe-loader shared ClickHouse CA");
        let (cert, key) = server_cert(&ca, host);
        vec![
            (
                "/etc/clickhouse-server/config.d/dfe-tls.xml".into(),
                CLICKHOUSE_TLS_CONFIG.into(),
            ),
            (
                "/etc/clickhouse-server/users.d/dfe-test-users.xml".into(),
                CLICKHOUSE_ACCESS_MANAGEMENT.into(),
            ),
            ("/etc/clickhouse-server/certs/".into(), Vec::new()),
            (CLICKHOUSE_CA_PATH.into(), ca.pem().into_bytes()),
            (
                "/etc/clickhouse-server/certs/server.crt".into(),
                cert.into_bytes(),
            ),
            (
                "/etc/clickhouse-server/certs/server.key".into(),
                key.into_bytes(),
            ),
        ]
    }

    /// A self-signed CA named `name`.
    #[must_use]
    pub fn private_ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        CertifiedIssuer::self_signed(params, KeyPair::generate().expect("CA key")).expect("CA cert")
    }

    /// A server certificate for `localhost` and `host`, as (cert, key) PEM.
    fn server_cert(ca: &CertifiedIssuer<'static, KeyPair>, host: &str) -> (String, String) {
        let key = KeyPair::generate().expect("server key");
        let mut names = vec!["localhost".to_string()];
        if host != "localhost" && host.parse::<std::net::IpAddr>().is_err() {
            names.push(host.to_string());
        }
        let mut params = CertificateParams::new(names).expect("server params");
        params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        let cert = params.signed_by(&key, ca).expect("server cert");
        (cert.pem(), key.serialize_pem())
    }

    /// The run's Kafka: one KRaft broker, PLAINTEXT, advertised on the host
    /// port Docker picked.
    fn kafka_spec() -> ServiceSpec {
        let wait = format!(
            "while [ ! -f {KAFKA_START_SCRIPT} ]; do sleep 0.1; done; \
             chmod 755 {KAFKA_START_SCRIPT} && {KAFKA_START_SCRIPT}"
        );
        let env = [
            (
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
            ),
            ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            ),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
            ("KAFKA_NODE_ID", "1"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094"),
        ];
        ServiceSpec {
            service: "kafka",
            image: scalo::transport::kafka::test_support::KAFKA_IMAGE.to_string(),
            ports: vec![KAFKA_PORT.as_u16()],
            env: env
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
            entrypoint: Some("bash".into()),
            args: vec!["-c".into(), wait],
            files: |_| Vec::new(),
            after_start: write_kafka_start_script,
            ready: Ready::Log("Kafka Server started"),
            startup_timeout: STARTUP_TIMEOUT,
        }
    }

    /// The broker advertises the published port, which only exists once the
    /// container runs, so the entrypoint waits for this script.
    fn write_kafka_start_script(container: &SharedContainer) {
        let host = advertised(&container.host);
        let port = container.port(KAFKA_PORT.as_u16());
        let script = format!(
            "printf '#!/usr/bin/env bash\\nexport \
             KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{host}:{port},BROKER://localhost:9093\\n\
             /etc/kafka/docker/run\\n' > {KAFKA_START_SCRIPT}"
        );
        let _ = container.exec(&["sh", "-c", &script]);
    }

    /// `localhost` can resolve to ::1 where loopback has no IPv6.
    fn advertised(host: &str) -> &str {
        if host == "localhost" {
            "127.0.0.1"
        } else {
            host
        }
    }

    /// The run's shared ClickHouse, started by the first process to ask.
    pub async fn shared_clickhouse() -> SharedContainer {
        static SHARED: OnceLock<SharedContainer> = OnceLock::new();
        shared(&SHARED, clickhouse_spec).await
    }

    /// The run's shared Kafka, started by the first process to ask.
    pub async fn shared_kafka() -> SharedContainer {
        static SHARED: OnceLock<SharedContainer> = OnceLock::new();
        shared(&SHARED, kafka_spec).await
    }

    async fn shared(
        cell: &'static OnceLock<SharedContainer>,
        spec: fn() -> ServiceSpec,
    ) -> SharedContainer {
        if let Some(container) = cell.get() {
            return container.clone();
        }
        crate::common::require_docker_in_ci();
        tokio::task::spawn_blocking(move || {
            cell.get_or_init(|| shared_service::acquire(&SUITE, &spec()))
                .clone()
        })
        .await
        .expect("acquire a shared container")
    }

    /// PEM of the CA the shared ClickHouse's certificate chains to.
    pub async fn shared_clickhouse_ca() -> Vec<u8> {
        let container = shared_clickhouse().await;
        tokio::task::spawn_blocking(move || container.read_file(CLICKHOUSE_CA_PATH))
            .await
            .expect("read the shared ClickHouse CA")
    }

    /// A backing service a test holds: the run's shared one, or its own.
    pub enum Service<I: Image> {
        Shared(SharedContainer),
        Owned(Box<ContainerAsync<I>>),
    }

    impl<I: Image> Service<I> {
        /// Host the service's ports are published on.
        ///
        /// # Errors
        ///
        /// The container runtime could not report it.
        pub async fn get_host(&self) -> Result<String, String> {
            match self {
                Self::Shared(c) => Ok(c.host.clone()),
                Self::Owned(c) => c
                    .get_host()
                    .await
                    .map(|h| h.to_string())
                    .map_err(|e| e.to_string()),
            }
        }

        /// Host port a container port is published on.
        ///
        /// # Errors
        ///
        /// The container runtime could not report it.
        pub async fn get_host_port_ipv4(
            &self,
            port: impl Into<ContainerPort>,
        ) -> Result<u16, String> {
            let port: ContainerPort = port.into();
            match self {
                Self::Shared(c) => Ok(c.port(port.as_u16())),
                Self::Owned(c) => c.get_host_port_ipv4(port).await.map_err(|e| e.to_string()),
            }
        }
    }

    /// The containers one test runs against.
    pub struct TestInfrastructure {
        pub clickhouse: Option<Service<ClickHouseImage>>,
        pub kafka: Option<Service<KafkaImage>>,
    }

    impl TestInfrastructure {
        /// The run's shared ClickHouse and Kafka, as the test asks for them.
        ///
        /// `test` is unused here and kept so a test can move between this and
        /// [`Self::exclusive`] by changing the constructor alone.
        pub async fn new(_test: &str, need_clickhouse: bool, need_kafka: bool) -> Self {
            crate::common::test_logging::init();
            let clickhouse = if need_clickhouse {
                Some(Service::Shared(shared_clickhouse().await))
            } else {
                None
            };
            let kafka = if need_kafka {
                Some(Service::Shared(shared_kafka().await))
            } else {
                None
            };
            Self { clickhouse, kafka }
        }

        /// ClickHouse where `default` may create users and grant to them, with
        /// Kafka beside it if `need_kafka`. The shared ClickHouse allows it.
        pub async fn clickhouse_managing_users(test: &str, need_kafka: bool) -> Self {
            Self::new(test, true, need_kafka).await
        }

        /// Containers `test` owns alone, for a test whose assertions cover the
        /// whole server or broker. Each is removed when the test drops it.
        ///
        /// # Arguments
        /// * `test` - The calling test, which names the containers it starts.
        ///   Pass `test_name!()`. Two tests must not share a name: nextest runs
        ///   each in its own process, so they start their own containers and a
        ///   shared name collides rather than sharing.
        pub async fn exclusive(test: &str, need_clickhouse: bool, need_kafka: bool) -> Self {
            crate::common::test_logging::init();
            crate::common::require_docker_in_ci();
            let clickhouse = if need_clickhouse {
                Some(Service::Owned(Box::new(start_clickhouse(test).await)))
            } else {
                None
            };
            let kafka = if need_kafka {
                Some(Service::Owned(Box::new(start_kafka_with(test, None).await)))
            } else {
                None
            };
            Self { clickhouse, kafka }
        }

        /// Spin up exactly the containers the app declares it depends on.
        ///
        /// Reads `Config::deployment_contract().depends_on` (the same list that
        /// drives Helm/Compose generation), so test infra follows deployment
        /// infra when the app gains a dependency. Unknown entries are logged
        /// and ignored until an image is wired in.
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
                             '{other}' -- wire up an image in containers.rs to support it"
                        );
                    }
                }
            }
            Self::new(test, need_clickhouse, need_kafka).await
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

    /// A ClickHouse this test owns alone.
    async fn start_clickhouse(test: &str) -> ContainerAsync<ClickHouseImage> {
        let name = crate::common::container_name(Some(test), "clickhouse");
        crate::common::reap_stale(&name);
        ClickHouseImage::default()
            .with_tag(format!("{CLICKHOUSE_TAG}@{CLICKHOUSE_DIGEST}"))
            .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
            .with_copy_to(
                "/etc/clickhouse-server/users.d/dfe-test-users.xml",
                CLICKHOUSE_ACCESS_MANAGEMENT.as_bytes().to_vec(),
            )
            .with_container_name(&name)
            .with_labels(crate::common::test_labels("clickhouse"))
            .with_ready_conditions(vec![clickhouse_ready()])
            .with_log_consumer(echo_logs(name.clone()))
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .expect("Failed to start ClickHouse container")
    }

    /// Start Kafka listening on `host_port`, for a loader configured with that
    /// address before the broker exists.
    pub async fn start_kafka_on(test: &str, host_port: u16) -> ContainerAsync<KafkaImage> {
        start_kafka_with(test, Some(host_port)).await
    }

    /// A Kafka this test owns alone, on `host_port` if given.
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
            .with_startup_timeout(STARTUP_TIMEOUT)
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

// Re-exported so tests reach these through `crate::common::containers`
// whichever features are on. Unused in binaries that don't reference them.
#[allow(unused_imports)]
pub use testcontainers_impl::TestInfrastructure;
#[cfg(feature = "testcontainers")]
#[allow(unused_imports)]
pub use testcontainers_impl::{
    CLICKHOUSE_DIGEST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_HTTPS_PORT, CLICKHOUSE_NATIVE_PORT,
    CLICKHOUSE_NATIVE_TLS_PORT, CLICKHOUSE_TAG, SUITE, Service, private_ca, shared_clickhouse,
    shared_clickhouse_ca, shared_kafka, start_kafka_on,
};
