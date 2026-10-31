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

    use crate::common::shared_service::{
        self, OwnedContainer, Ready, ServiceSpec, SharedContainer, Suite,
    };

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

    /// Kafka's PLAINTEXT port inside the container.
    pub const KAFKA_PORT: u16 = 9092;

    /// The ClickHouse user the tests administer the server as.
    pub const CLICKHOUSE_USER: &str = "default";

    /// Where the shared ClickHouse keeps the CA that signed its certificate.
    pub const CLICKHOUSE_CA_PATH: &str = "/etc/clickhouse-server/certs/ca.crt";

    /// Where a ClickHouse container keeps the password made for it.
    const CLICKHOUSE_PASSWORD_PATH: &str = "/etc/clickhouse-server/dfe-test/admin-password";

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

    /// A single-node embedded Keeper, so a `Replicated` database can run.
    const CLICKHOUSE_KEEPER_CONFIG: &str = r"<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    <zookeeper>
        <node>
            <host>localhost</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
";

    /// A ClickHouse with HTTP, native, HTTPS and native TLS, where `default`
    /// has a password made for the container and may manage users.
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
            pinned_ports: Vec::new(),
            // The entrypoint leaves `default` to users.d rather than generating it.
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

    /// TLS and users config, the admin password, and a certificate for
    /// `localhost` and `host` signed by a CA made for this container.
    fn clickhouse_files(host: &str) -> Vec<(String, Vec<u8>)> {
        let ca = private_ca("dfe-loader test ClickHouse CA");
        let (cert, key) = server_cert(&ca, host);
        let password = uuid::Uuid::new_v4().simple().to_string();
        let users = format!(
            "<clickhouse>\n    <users>\n        <{CLICKHOUSE_USER}>\n            \
             <password>{password}</password>\n            \
             <access_management>1</access_management>\n        \
             </{CLICKHOUSE_USER}>\n    </users>\n</clickhouse>\n"
        );
        vec![
            (
                "/etc/clickhouse-server/config.d/dfe-tls.xml".into(),
                CLICKHOUSE_TLS_CONFIG.into(),
            ),
            (
                "/etc/clickhouse-server/config.d/dfe-keeper.xml".into(),
                CLICKHOUSE_KEEPER_CONFIG.into(),
            ),
            (
                "/etc/clickhouse-server/users.d/dfe-test-users.xml".into(),
                users.into_bytes(),
            ),
            ("/etc/clickhouse-server/dfe-test/".into(), Vec::new()),
            (CLICKHOUSE_PASSWORD_PATH.into(), password.into_bytes()),
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

    /// One KRaft broker, PLAINTEXT, advertised on its published port, or on
    /// `host_port` when given.
    fn kafka_spec(host_port: Option<u16>) -> ServiceSpec {
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
        let (ports, pinned_ports) = match host_port {
            Some(host) => (Vec::new(), vec![(KAFKA_PORT, host)]),
            None => (vec![KAFKA_PORT], Vec::new()),
        };
        ServiceSpec {
            service: "kafka",
            image: scalo::transport::kafka::test_support::KAFKA_IMAGE.to_string(),
            ports,
            pinned_ports,
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
        let port = container.port(KAFKA_PORT);
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
        let container = shared(&SHARED, clickhouse_spec).await;
        remember_password(&container).await;
        ensure_replicated_database(&container).await;
        container
    }

    /// Create [`crate::common::REPLICATED_DATABASE`] on the embedded Keeper,
    /// once per process. Every process may race to it, so it retries until the
    /// database reads back as `Replicated`.
    async fn ensure_replicated_database(container: &SharedContainer) {
        static READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        let db = crate::common::REPLICATED_DATABASE;
        let client = clickhouse::Client::default()
            .with_url(format!("http://{}", container.addr(CLICKHOUSE_HTTP_PORT)))
            .with_user(CLICKHOUSE_USER)
            .with_password(clickhouse_password());
        READY
            .get_or_init(|| async move {
                let create = format!(
                    "CREATE DATABASE IF NOT EXISTS {db} \
                     ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'replica1')"
                );
                let deadline = std::time::Instant::now() + STARTUP_TIMEOUT;
                loop {
                    let created = client.query(&create).execute().await;
                    let engine: Option<String> = client
                        .query("SELECT engine FROM system.databases WHERE name = ?")
                        .bind(db)
                        .fetch_optional()
                        .await
                        .unwrap_or(None);
                    if engine.as_deref() == Some("Replicated") {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the Replicated database {db} never came up: {created:?}, engine {engine:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
            .await;
    }

    /// The run's shared Kafka, started by the first process to ask.
    pub async fn shared_kafka() -> SharedContainer {
        static SHARED: OnceLock<SharedContainer> = OnceLock::new();
        shared(&SHARED, || kafka_spec(None)).await
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

    /// The admin password of the ClickHouse this process uses.
    static PASSWORD: OnceLock<String> = OnceLock::new();

    async fn remember_password(container: &SharedContainer) {
        if PASSWORD.get().is_some() {
            return;
        }
        let container = container.clone();
        let password = tokio::task::spawn_blocking(move || {
            String::from_utf8_lossy(&container.read_file(CLICKHOUSE_PASSWORD_PATH)).into_owned()
        })
        .await
        .expect("read the ClickHouse admin password");
        let _ = PASSWORD.set(password);
    }

    /// The password for [`CLICKHOUSE_USER`] on this process's ClickHouse.
    ///
    /// # Panics
    ///
    /// Panics before the test has a ClickHouse.
    #[must_use]
    pub fn clickhouse_password() -> String {
        PASSWORD
            .get()
            .cloned()
            .expect("ClickHouse acquired before its password is asked for")
    }

    /// PEM of the CA the shared ClickHouse's certificate chains to.
    pub async fn shared_clickhouse_ca() -> Vec<u8> {
        let container = shared_clickhouse().await;
        tokio::task::spawn_blocking(move || container.read_file(CLICKHOUSE_CA_PATH))
            .await
            .expect("read the shared ClickHouse CA")
    }

    /// A backing service a test holds: the run's shared one, or its own.
    pub enum Service {
        Shared(SharedContainer),
        Owned(OwnedContainer),
    }

    impl Service {
        fn container(&self) -> &SharedContainer {
            match self {
                Self::Shared(c) => c,
                Self::Owned(c) => &c.container,
            }
        }

        /// Host the service's ports are reachable on.
        #[must_use]
        pub fn host(&self) -> String {
            self.container().host.clone()
        }

        /// Host port a container port is reachable on.
        #[must_use]
        pub fn port(&self, port: u16) -> u16 {
            self.container().port(port)
        }
    }

    /// The containers one test runs against.
    pub struct TestInfrastructure {
        pub clickhouse: Option<Service>,
        pub kafka: Option<Service>,
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
                let owned = start_alone(test, clickhouse_spec).await;
                remember_password(&owned.container).await;
                Some(Service::Owned(owned))
            } else {
                None
            };
            let kafka = if need_kafka {
                Some(Service::Owned(start_alone(test, || kafka_spec(None)).await))
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

    /// A container named for `test` that only this test process holds.
    async fn start_alone(
        test: &str,
        spec: impl FnOnce() -> ServiceSpec + Send + 'static,
    ) -> OwnedContainer {
        let spec = spec();
        let name = crate::common::container_name(Some(test), spec.service);
        tokio::task::spawn_blocking(move || shared_service::start_alone(&SUITE, &spec, &name))
            .await
            .expect("start a container of the test's own")
    }

    /// Start Kafka listening on `host_port`, for a loader configured with that
    /// address before the broker exists.
    pub async fn start_kafka_on(test: &str, host_port: u16) -> OwnedContainer {
        crate::common::require_docker_in_ci();
        start_alone(test, move || kafka_spec(Some(host_port))).await
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
    CLICKHOUSE_NATIVE_TLS_PORT, CLICKHOUSE_TAG, CLICKHOUSE_USER, KAFKA_PORT, SUITE, Service,
    clickhouse_password, private_ca, shared_clickhouse, shared_clickhouse_ca, shared_kafka,
    start_kafka_on,
};
