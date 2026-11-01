// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The core tier's one table test per heavy service.
//!
//! Each runs the loader's main cases against the run's shared instance: a pet
//! named by `HYPERI_TEST_*` when the run may use one and it answers, else the
//! shared container. Every other real-server test is in the full tier.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Map, Value, json};

use dfe_loader::buffer::KafkaOffset;
use dfe_loader::clickhouse::client_http::build_client;
use dfe_loader::clickhouse::config::{ClickHouseConfig, InsertFormat, Transport};
use dfe_loader::clickhouse::{ClickHouseQueryClient, Inserter, InserterConfig};
use dfe_loader::config::{DlqConfig, KafkaConfig, SaslConfig, TlsConfig};
use dfe_loader::kafka::TransportAdapter;

use crate::common::containers::{
    CLICKHOUSE_HTTP_PORT, CLICKHOUSE_HTTPS_PORT, CLICKHOUSE_NATIVE_PORT,
    CLICKHOUSE_NATIVE_TLS_PORT, SUITE, shared_clickhouse, shared_clickhouse_ca, shared_kafka,
};
use crate::common::shared_service::{self, lookup, pets_allowed, record};

/// Run databases and topics older than this are a killed run's, and swept.
const STALE_AFTER: Duration = Duration::from_secs(6 * 3600);

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// `<prefix>t<epoch>_<run>`, so a sweep can read a run name's age.
fn run_name(prefix: &str, separator: char) -> String {
    format!(
        "{prefix}t{}{separator}{}",
        now_secs(),
        shared_service::run_id()
    )
}

/// Whether `name`, made by [`run_name`] with `prefix`, is past [`STALE_AFTER`].
fn is_stale(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('t'))
        .and_then(|rest| rest.split(['_', '-']).next())
        .and_then(|epoch| epoch.parse::<u64>().ok())
        .is_some_and(|epoch| now_secs().saturating_sub(epoch) > STALE_AFTER.as_secs())
}

// ============================================================================
// ClickHouse
// ============================================================================

/// Where the ClickHouse cases run, and how to reach each protocol there.
struct ClickHouseTarget {
    rung: &'static str,
    host: String,
    http: Option<u16>,
    https: Option<u16>,
    native: Option<u16>,
    native_tls: Option<u16>,
    user: String,
    password: String,
    cluster: Option<String>,
    ca_pem: Option<Vec<u8>>,
    db_prefix: String,
}

/// The pet the `HYPERI_TEST_CLICKHOUSE_*` settings read through `key` name,
/// with `ca_pem` read from `_CA_FILE`. `None` without a host.
fn clickhouse_pet_from(
    key: impl Fn(&str) -> Option<String>,
    ca_pem: Option<Vec<u8>>,
) -> Option<ClickHouseTarget> {
    let port = |name: &str| key(name).and_then(|v| v.parse::<u16>().ok());
    Some(ClickHouseTarget {
        rung: "pet",
        host: key("HOST")?,
        http: port("HTTP_PORT"),
        // TLS cases need the pet's private CA, so without it they are left out.
        https: port("HTTPS_PORT").filter(|_| ca_pem.is_some()),
        native: port("NATIVE_PORT"),
        native_tls: port("NATIVE_TLS_PORT").filter(|_| ca_pem.is_some()),
        user: key("USER").unwrap_or_else(|| "default".into()),
        password: key("PASSWORD").unwrap_or_default(),
        cluster: key("CLUSTER"),
        ca_pem,
        db_prefix: key("DB_PREFIX").unwrap_or_else(|| "dfe_loader_test_".into()),
    })
}

/// The pet, when the run may use one and it answers `/ping`.
fn clickhouse_pet() -> Option<ClickHouseTarget> {
    if !pets_allowed() {
        return None;
    }
    let key = |name: &str| lookup(&SUITE, &format!("HYPERI_TEST_CLICKHOUSE_{name}"));
    let ca_pem = key("CA_FILE").and_then(|path| std::fs::read(path).ok());
    let target = clickhouse_pet_from(key, ca_pem)?;
    let answers = target.http.is_some_and(|p| {
        shared_service::http_get(&target.host, p, "/ping").is_some_and(|(s, _)| s == 200)
    });
    if answers {
        return Some(target);
    }
    record(&format!(
        "ClickHouse pet at {} did not answer /ping, using the shared container",
        target.host
    ));
    None
}

/// What the pet settings resolve to: a host or nothing, and TLS ports only
/// with a CA.
#[test]
fn clickhouse_pet_settings() {
    let cases: &[(
        &str,
        &[(&str, &str)],
        bool,
        Option<(Option<u16>, Option<u16>)>,
    )] = &[
        ("nothing set", &[], false, None),
        (
            "ports without a host",
            &[("HTTP_PORT", "8123")],
            false,
            None,
        ),
        (
            "host and ports, no CA",
            &[("HOST", "h"), ("HTTP_PORT", "8123"), ("HTTPS_PORT", "8443")],
            false,
            Some((Some(8123), None)),
        ),
        (
            "host and ports with a CA",
            &[("HOST", "h"), ("HTTP_PORT", "8123"), ("HTTPS_PORT", "8443")],
            true,
            Some((Some(8123), Some(8443))),
        ),
        (
            "a port that is not a number",
            &[("HOST", "h"), ("HTTP_PORT", "http")],
            false,
            Some((None, None)),
        ),
    ];
    for (label, vars, with_ca, want) in cases {
        let key = |name: &str| {
            vars.iter()
                .find_map(|(k, v)| (*k == name).then(|| (*v).to_string()))
        };
        let ca = with_ca.then(|| b"pem".to_vec());
        let got = clickhouse_pet_from(key, ca).map(|t| (t.http, t.https));
        assert_eq!(got, *want, "{label}");
    }
}

async fn clickhouse_target() -> ClickHouseTarget {
    if let Some(pet) = tokio::task::spawn_blocking(clickhouse_pet)
        .await
        .expect("probe the ClickHouse pet")
    {
        return pet;
    }
    let shared = shared_clickhouse().await;
    ClickHouseTarget {
        rung: "container",
        host: shared.host.clone(),
        http: Some(shared.port(CLICKHOUSE_HTTP_PORT)),
        https: Some(shared.port(CLICKHOUSE_HTTPS_PORT)),
        native: Some(shared.port(CLICKHOUSE_NATIVE_PORT)),
        native_tls: Some(shared.port(CLICKHOUSE_NATIVE_TLS_PORT)),
        user: "default".into(),
        password: crate::common::containers::clickhouse_password(),
        cluster: None,
        ca_pem: Some(shared_clickhouse_ca().await),
        db_prefix: "dfe_loader_test_".into(),
    }
}

impl ClickHouseTarget {
    fn config(
        &self,
        transport: Transport,
        port: u16,
        tls: bool,
        database: &str,
    ) -> ClickHouseConfig {
        let host = if tls {
            crate::common::containers::tls_name(&self.host)
        } else {
            self.host.as_str()
        };
        ClickHouseConfig {
            hosts: vec![format!("{host}:{port}")],
            transport,
            database: database.to_string(),
            username: self.user.clone(),
            password: self.password.clone(),
            tls,
            ..Default::default()
        }
    }

    fn on_cluster(&self) -> String {
        self.cluster
            .as_ref()
            .map_or_else(String::new, |c| format!(" ON CLUSTER '{c}'"))
    }

    /// The run database: `Replicated` on a cluster, so every replica behind
    /// the pet's one hostname sees the same tables.
    fn create_database_sql(&self, db: &str) -> String {
        match &self.cluster {
            Some(c) => format!(
                "CREATE DATABASE IF NOT EXISTS {db} ON CLUSTER '{c}' \
                 ENGINE = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')"
            ),
            None => format!("CREATE DATABASE IF NOT EXISTS {db}"),
        }
    }

    fn table_engine(&self) -> &'static str {
        if self.cluster.is_some() {
            "ReplicatedMergeTree"
        } else {
            "MergeTree"
        }
    }
}

fn rows(count: u64) -> Vec<Map<String, Value>> {
    (0..count)
        .map(|i| {
            json!({ "id": i, "name": format!("row_{i}"), "value": i as f64 * 1.5 })
                .as_object()
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

/// Drop run databases a killed run left behind.
async fn sweep_databases(admin: &ClickHouseQueryClient, target: &ClickHouseTarget, port: u16) {
    let listed: Vec<String> = clickhouse::Client::default()
        .with_url(format!("http://{}:{port}", target.host))
        .with_user(&target.user)
        .with_password(&target.password)
        .query("SELECT name FROM system.databases WHERE startsWith(name, ?)")
        .bind(&target.db_prefix)
        .fetch_all()
        .await
        .unwrap_or_default();
    for db in listed.iter().filter(|db| is_stale(db, &target.db_prefix)) {
        let dropped = admin
            .execute(&format!(
                "DROP DATABASE IF EXISTS {db}{} SYNC",
                target.on_cluster()
            ))
            .await;
        record(&format!("swept stale run database {db}: {dropped:?}"));
    }
}

/// The loader's main ClickHouse cases, each over its own protocol: schema
/// fetch, a RowBinary insert, the count back, and a missing table refused.
#[tokio::test(flavor = "multi_thread")]
async fn clickhouse_main_cases() {
    if !crate::common::has_docker() && lookup(&SUITE, "HYPERI_TEST_CLICKHOUSE_HOST").is_none() {
        crate::common::require_docker_in_ci();
        eprintln!("Skipping: no Docker daemon and no ClickHouse pet");
        return;
    }
    let target = clickhouse_target().await;
    record(&format!(
        "ClickHouse rung: {} at {}",
        target.rung, target.host
    ));

    let dir = tempfile::tempdir().expect("CA bundle directory");
    if let Some(pem) = &target.ca_pem {
        let bundle = dir.path().join("ca.pem");
        std::fs::write(&bundle, pem).expect("write the CA bundle");
        // SAFETY: nextest runs this test in a process of its own.
        unsafe { std::env::set_var("SSL_CERT_FILE", &bundle) };
    }

    let db = run_name(&target.db_prefix, '_');
    let admin_port = target
        .http
        .expect("the HTTP port the cases are administered over");
    let admin =
        ClickHouseQueryClient::new(&target.config(Transport::Http, admin_port, false, "default"))
            .expect("admin client");
    sweep_databases(&admin, &target, admin_port).await;
    admin
        .execute(&target.create_database_sql(&db))
        .await
        .expect("create the run database");

    let cases = [
        ("http", Transport::Http, target.http, false),
        ("native", Transport::Native, target.native, false),
        ("https", Transport::Http, target.https, true),
        ("native_tls", Transport::Native, target.native_tls, true),
    ];
    let mut failures = Vec::new();
    let mut ran = Vec::new();
    for (label, transport, port, tls) in cases {
        let Some(port) = port else {
            record(&format!(
                "ClickHouse case {label}: not offered by the {} rung",
                target.rung
            ));
            continue;
        };
        ran.push(label);
        if let Err(e) = Box::pin(clickhouse_case(&target, &db, label, transport, port, tls)).await {
            failures.push(format!("{label}: {e}"));
        }
    }

    let dropped = admin
        .execute(&format!(
            "DROP DATABASE IF EXISTS {db}{} SYNC",
            target.on_cluster()
        ))
        .await;
    assert!(
        failures.is_empty(),
        "ClickHouse cases failed on the {} rung:\n{}",
        target.rung,
        failures.join("\n")
    );
    assert!(
        ran.contains(&"http") && ran.contains(&"native"),
        "both plain protocols must run, ran {ran:?}"
    );
    dropped.expect("drop the run database");
    print_run_log();
}

async fn clickhouse_case(
    target: &ClickHouseTarget,
    db: &str,
    label: &str,
    transport: Transport,
    port: u16,
    tls: bool,
) -> Result<(), String> {
    let config = target.config(transport, port, tls, db);
    let client = Arc::new(ClickHouseQueryClient::new(&config).map_err(|e| format!("client: {e}"))?);
    client
        .health_check()
        .await
        .map_err(|e| format!("health check: {e}"))?;

    let table = format!("{db}.main_{label}");
    client
        .execute(&format!(
            "CREATE TABLE {table} (id UInt64, name String, value Float64) \
             ENGINE = {} ORDER BY id",
            target.table_engine()
        ))
        .await
        .map_err(|e| format!("create table: {e}"))?;

    let schema = client
        .fetch_table_schema(&table)
        .await
        .map_err(|e| format!("schema fetch: {e}"))?;
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    if names != ["id", "name", "value"] {
        return Err(format!("schema columns {names:?}"));
    }

    let insert_client = build_client(&config).map_err(|e| format!("insert client: {e}"))?;
    let inserter = Inserter::new(client.clone(), insert_client, InserterConfig::default())
        .with_insert_format(InsertFormat::RowBinary);
    let inserted = inserter
        .insert_rows(&table, &rows(7), &[])
        .await
        .map_err(|e| format!("insert: {e}"))?;
    if target.cluster.is_some() {
        client
            .execute(&format!(
                "SYSTEM SYNC REPLICA{} {table}",
                target.on_cluster()
            ))
            .await
            .map_err(|e| format!("sync replica: {e}"))?;
    }
    let count = client
        .query_count(&table, None)
        .await
        .map_err(|e| format!("count: {e}"))?;
    if (inserted, count) != (7, 7) {
        return Err(format!(
            "inserted {inserted}, counted {count}, want 7 and 7"
        ));
    }

    let reader_port = target
        .http
        .ok_or("no HTTP port to read the cases back over")?;
    let reader_host = target.host.clone();
    let reader = clickhouse::Client::default()
        .with_url(format!("http://{reader_host}:{reader_port}"))
        .with_user(&target.user)
        .with_password(&target.password);
    let coercion = Inserter::new(
        client.clone(),
        build_client(&config).map_err(|e| format!("insert client: {e}"))?,
        InserterConfig {
            max_retries: 0,
            base_retry_delay_ms: 10,
            max_retry_delay_ms: 10,
            ..InserterConfig::default()
        },
    )
    .with_insert_format(InsertFormat::RowBinary);
    let wrong = Box::pin(super::insert_coercions::coercion_failures(
        &client,
        &coercion,
        &reader,
        db,
        target.table_engine(),
        &target.on_cluster(),
        label,
    ))
    .await;
    let mut wrong = wrong;
    wrong.extend(
        Box::pin(super::insert_coercions::type_change_failures(
            &client,
            &coercion,
            &reader,
            db,
            target.table_engine(),
            label,
        ))
        .await,
    );
    if !wrong.is_empty() {
        return Err(format!(
            "{} coercion cases WRONG:\n{}",
            wrong.len(),
            wrong.join("\n")
        ));
    }

    match client
        .fetch_table_schema(&format!("{db}.no_such_table"))
        .await
    {
        Ok(schema) if schema.columns.is_empty() => Ok(()),
        Ok(schema) => Err(format!(
            "a missing table reported columns {:?}",
            schema.columns
        )),
        Err(_) => Ok(()),
    }
}

// ============================================================================
// Kafka
// ============================================================================

/// Where the Kafka cases run.
struct KafkaTarget {
    rung: &'static str,
    bootstrap: String,
    security_protocol: String,
    sasl: Option<(String, String, String)>,
    ca_file: Option<String>,
    topic_prefix: String,
}

impl KafkaTarget {
    fn client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.bootstrap)
            .set("security.protocol", &self.security_protocol)
            .set("message.timeout.ms", "30000");
        if let Some((mechanism, user, password)) = &self.sasl {
            cfg.set("sasl.mechanism", mechanism)
                .set("sasl.username", user)
                .set("sasl.password", password);
        }
        if let Some(ca) = &self.ca_file {
            cfg.set("ssl.ca.location", ca);
        }
        cfg
    }

    fn loader_config(&self, group: &str, topic: &str) -> KafkaConfig {
        let sasl = self
            .sasl
            .as_ref()
            .map(|(mechanism, user, password)| SaslConfig {
                enabled: true,
                mechanism: mechanism.to_ascii_lowercase().replace('-', "_"),
                username: user.clone(),
                password: password.clone().into(),
                ..Default::default()
            });
        let tls = self.security_protocol.contains("SSL").then(|| TlsConfig {
            enabled: true,
            ca_cert_file: self.ca_file.clone(),
            ..Default::default()
        });
        KafkaConfig {
            brokers: vec![self.bootstrap.clone()],
            group: group.to_string(),
            topics: vec![topic.to_string()],
            client_id: "dfe-loader-shared".to_string(),
            sasl,
            tls,
            ..Default::default()
        }
    }
}

/// The pet from `HYPERI_TEST_KAFKA_*`, when the run may use it and it answers.
fn kafka_pet() -> Option<KafkaTarget> {
    let key = |name: &str| lookup(&SUITE, &format!("HYPERI_TEST_KAFKA_{name}"));
    if !pets_allowed() {
        return None;
    }
    let bootstrap = key("BOOTSTRAP")?;
    let security_protocol = key("SECURITY_PROTOCOL").unwrap_or_else(|| "PLAINTEXT".into());
    let ca_file = key("CA_FILE");
    if security_protocol.contains("SSL") && ca_file.is_none() {
        record("Kafka pet needs TLS and no CA file is set, using the shared container");
        return None;
    }
    let sasl = key("SASL_MECHANISM").map(|m| {
        (
            m,
            key("USER").unwrap_or_default(),
            key("PASSWORD").unwrap_or_default(),
        )
    });
    let first = bootstrap
        .split(',')
        .next()
        .unwrap_or(&bootstrap)
        .to_string();
    let reachable = std::net::ToSocketAddrs::to_socket_addrs(first.as_str())
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok());
    if !reachable {
        record(&format!(
            "Kafka pet at {first} did not answer, using the shared container"
        ));
        return None;
    }
    Some(KafkaTarget {
        rung: "pet",
        bootstrap,
        security_protocol,
        sasl,
        ca_file,
        topic_prefix: key("TOPIC_PREFIX").unwrap_or_else(|| "dfe-loader-test-".into()),
    })
}

async fn kafka_target() -> KafkaTarget {
    if let Some(pet) = tokio::task::spawn_blocking(kafka_pet)
        .await
        .expect("probe the Kafka pet")
    {
        return pet;
    }
    let shared = shared_kafka().await;
    KafkaTarget {
        rung: "container",
        bootstrap: shared.addr(9092),
        security_protocol: "PLAINTEXT".into(),
        sasl: None,
        ca_file: None,
        topic_prefix: "dfe-loader-test-".into(),
    }
}

/// Delete run topics a killed run left behind.
async fn sweep_topics(admin: &AdminClient<DefaultClientContext>, target: &KafkaTarget) {
    let Ok(metadata) = admin.inner().fetch_metadata(None, Duration::from_secs(10)) else {
        return;
    };
    let stale: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|name| is_stale(name, &target.topic_prefix))
        .collect();
    if stale.is_empty() {
        return;
    }
    let names: Vec<&str> = stale.iter().map(String::as_str).collect();
    let deleted = admin.delete_topics(&names, &AdminOptions::new()).await;
    record(&format!(
        "swept stale run topics {stale:?}: {:?}",
        deleted.map(|r| r.len())
    ));
}

/// The loader's main Kafka cases on one run topic: records produced before
/// the consumer starts all arrive in order, and a commit holds for the group.
#[tokio::test(flavor = "multi_thread")]
async fn kafka_main_cases() {
    if !crate::common::has_docker() && lookup(&SUITE, "HYPERI_TEST_KAFKA_BOOTSTRAP").is_none() {
        crate::common::require_docker_in_ci();
        eprintln!("Skipping: no Docker daemon and no Kafka pet");
        return;
    }
    let target = kafka_target().await;
    record(&format!(
        "Kafka rung: {} at {}",
        target.rung, target.bootstrap
    ));

    let admin: AdminClient<DefaultClientContext> =
        target.client_config().create().expect("Kafka admin client");
    sweep_topics(&admin, &target).await;
    let topic = run_name(&target.topic_prefix, '-');
    let group = format!("{topic}-group");
    let brokers = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(30))
        .expect("broker metadata")
        .brokers()
        .len();
    let replication = i32::try_from(brokers.clamp(1, 3)).unwrap_or(1);
    let created = admin
        .create_topics(
            &[NewTopic::new(
                &topic,
                1,
                TopicReplication::Fixed(replication),
            )],
            &AdminOptions::new(),
        )
        .await
        .expect("create the run topic");
    assert!(
        created.iter().all(Result::is_ok),
        "create {topic}: {created:?}"
    );

    let outcome = kafka_cases(&target, &topic, &group).await;
    let deleted = admin
        .delete_topics(&[topic.as_str()], &AdminOptions::new())
        .await;
    if let Err(e) = outcome {
        panic!("Kafka cases failed on the {} rung: {e}", target.rung);
    }
    deleted.expect("delete the run topic");
    print_run_log();
}

async fn kafka_cases(target: &KafkaTarget, topic: &str, group: &str) -> Result<(), String> {
    let producer: FutureProducer = target
        .client_config()
        .create()
        .map_err(|e| format!("producer: {e}"))?;
    for i in 0..6 {
        let payload = format!("msg-{i}");
        producer
            .send(
                FutureRecord::<str, str>::to(topic).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(e, _)| format!("produce {i}: {e}"))?;
    }

    let config = target.loader_config(group, topic);
    let first = TransportAdapter::new(&config, &DlqConfig::default(), None)
        .await
        .map_err(|e| format!("first adapter: {e}"))?;
    let got = receive(&first, 6).await;
    let payloads: Vec<String> = got
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    let want: Vec<String> = (0..6).map(|i| format!("msg-{i}")).collect();
    if payloads != want {
        return Err(format!("received {payloads:?}, want {want:?}"));
    }
    let offsets: Vec<KafkaOffset> = got
        .iter()
        .take(3)
        .map(|m| KafkaOffset {
            topic: m.topic.clone(),
            partition: m.partition,
            // The last message handled: the transport commits the one after it.
            offset: m.offset,
        })
        .collect();
    first
        .commit(&offsets)
        .await
        .map_err(|e| format!("commit: {e}"))?;
    first.close().await.map_err(|e| format!("close: {e}"))?;
    // The consumer leaves the group on drop, not on close, and a member still
    // in the group holds the partition from the second one.
    drop(first);

    let second = TransportAdapter::new(&config, &DlqConfig::default(), None)
        .await
        .map_err(|e| format!("second adapter: {e}"))?;
    let rest: Vec<String> = receive(&second, 3)
        .await
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    second.close().await.map_err(|e| format!("close: {e}"))?;
    if rest != want[3..] {
        return Err(format!(
            "after the commit the group read {rest:?}, want {:?}",
            &want[3..]
        ));
    }
    Ok(())
}

/// Messages until `want` have arrived, or 60 s have passed.
async fn receive(adapter: &TransportAdapter, want: usize) -> Vec<dfe_loader::kafka::KafkaMessage> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut got = Vec::with_capacity(want);
    while got.len() < want && std::time::Instant::now() < deadline {
        if let Ok(batch) = adapter.recv(want - got.len()).await {
            got.extend(batch.messages);
        }
    }
    got
}

/// Print what this run's shared containers did, shown by nextest for this test.
fn print_run_log() {
    let log = std::fs::read_to_string(shared_service::log_path()).unwrap_or_default();
    eprintln!(
        "shared-service run log {}:\n{log}",
        shared_service::log_path().display()
    );
}
