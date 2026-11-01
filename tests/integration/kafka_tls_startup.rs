// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The shipped `dfe-loader` binary consuming from a Kafka broker with TLS, mTLS
//! and SASL_SSL SCRAM-SHA-512 listeners, and landing records in a real
//! `ClickHouse`.
//!
//! The loader starts the way an operator starts it: the binary, a config file
//! and the `DFE_LOADER_*` environment, so everything `main` does before the
//! first poll (including the TLS provider setup) is on the path. The records are
//! produced, and the topics created, by a raw `rdkafka` client configured from
//! the test's own settings, so the loader's mapping from `kafka.tls` and
//! `kafka.sasl` to librdkafka is not what builds the other side.
//!
//! One broker and one `ClickHouse` serve one test, which runs a table of loaders
//! at once. Each accepted row must land its records. Each refused row changes one
//! thing from an accepted row (the CA, the password) and must land nothing while
//! the broker logs the refusal.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::transport::kafka::test_support::{SCRAM_PASSWORD, SCRAM_USER, TlsBroker};

use super::grpc_outage::{clickhouse_address, free_port, query_client, wait_landed};
use crate::common::containers::TestInfrastructure;
use crate::common::{has_docker, require_docker_in_ci, unique_table_name};
use crate::test_name;

/// Records produced for each loader.
const RECORDS: u64 = 20;

/// How long an accepted loader has to land its records: start-up, the group join and a flush.
const LAND_WITHIN: Duration = Duration::from_secs(120);

/// How long a refused loader is watched for records it must never land.
const REFUSED_FOR: Duration = Duration::from_secs(25);

/// How a loader is given the kafka security settings.
#[derive(Clone, Copy)]
enum Form {
    /// `kafka.tls` and `kafka.sasl` in the config file.
    File,
    /// `DFE_LOADER_KAFKA_SECURITY_PROTOCOL`, `DFE_LOADER_KAFKA_SASL_*` and the CA through the
    /// double-underscore form.
    Env,
    /// As `Env`, but the CA arrives the way the chart delivers a private one: `SSL_CERT_FILE`
    /// names the trust store and the loader sets no CA file.
    EnvSystemTrust,
}

#[derive(Clone, Copy)]
enum Listener {
    Tls,
    SaslSsl,
    Mtls,
}

/// A loader to run against the broker.
struct Row {
    name: &'static str,
    listener: Listener,
    form: Form,
    /// CA file the loader trusts, a name in the PKI directory.
    ca: &'static str,
    /// Password the loader sends, when the listener is SASL_SSL.
    password: &'static str,
    /// What the broker logs when it turns this loader away. `None` for a loader that must land.
    refused_with: Option<&'static str>,
}

const ROWS: &[Row] = &[
    Row {
        name: "tls-config-file",
        listener: Listener::Tls,
        form: Form::File,
        ca: "ca.crt",
        password: "",
        refused_with: None,
    },
    Row {
        name: "sasl-ssl-scram-env",
        listener: Listener::SaslSsl,
        form: Form::Env,
        ca: "ca.crt",
        password: SCRAM_PASSWORD,
        refused_with: None,
    },
    Row {
        name: "sasl-ssl-scram-config-file",
        listener: Listener::SaslSsl,
        form: Form::File,
        ca: "ca.crt",
        password: SCRAM_PASSWORD,
        refused_with: None,
    },
    Row {
        name: "sasl-ssl-scram-trust-from-ssl-cert-file",
        listener: Listener::SaslSsl,
        form: Form::EnvSystemTrust,
        ca: "ca.crt",
        password: SCRAM_PASSWORD,
        refused_with: None,
    },
    Row {
        name: "mtls-config-file",
        listener: Listener::Mtls,
        form: Form::File,
        ca: "ca.crt",
        password: "",
        refused_with: None,
    },
    Row {
        name: "tls-trusting-another-ca",
        listener: Listener::Tls,
        form: Form::File,
        ca: "rogue-ca.crt",
        password: "",
        refused_with: Some("SSL handshake failed"),
    },
    Row {
        name: "sasl-ssl-scram-wrong-password",
        listener: Listener::SaslSsl,
        form: Form::Env,
        ca: "ca.crt",
        password: "not-the-password",
        refused_with: Some("invalid credentials with SASL mechanism SCRAM-SHA-512"),
    },
];

/// What every row shares.
struct Shared {
    broker: TlsBroker,
    clickhouse: String,
}

fn address(shared: &Shared, listener: Listener) -> &str {
    match listener {
        Listener::Tls => &shared.broker.tls,
        Listener::SaslSsl => &shared.broker.sasl_ssl,
        Listener::Mtls => &shared.broker.mtls,
    }
}

/// librdkafka settings a client that is allowed in uses, set here from the PKI
/// and not through the loader's config mapping.
fn good_client(shared: &Shared, listener: Listener) -> ClientConfig {
    let pki = &shared.broker.pki;
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", address(shared, listener));
    config.set("ssl.ca.location", pki.file("ca.crt"));
    match listener {
        Listener::Tls => {
            config.set("security.protocol", "ssl");
        }
        Listener::SaslSsl => {
            config
                .set("security.protocol", "sasl_ssl")
                .set("sasl.mechanism", "SCRAM-SHA-512")
                .set("sasl.username", SCRAM_USER)
                .set("sasl.password", SCRAM_PASSWORD);
        }
        Listener::Mtls => {
            config
                .set("security.protocol", "ssl")
                .set("ssl.certificate.location", pki.file("client.crt"))
                .set("ssl.key.location", pki.file("client.key"));
        }
    }
    config
}

/// Create the row's topic and produce `{"id":N}` records onto it.
async fn fill_topic(shared: &Shared, row: &Row, topic: &str) -> Result<(), String> {
    let admin: AdminClient<_> = good_client(shared, row.listener)
        .create()
        .map_err(|e| format!("admin client: {e}"))?;
    let created = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(30))),
        )
        .await
        .map_err(|e| format!("create topic: {e}"))?;
    for result in created {
        result.map_err(|(name, code)| format!("create topic {name}: {code:?}"))?;
    }

    let producer: FutureProducer = good_client(shared, row.listener)
        .set("message.timeout.ms", "60000")
        .create()
        .map_err(|e| format!("producer: {e}"))?;
    for id in 0..RECORDS {
        let payload = format!("{{\"id\":{id}}}");
        producer
            .send(
                FutureRecord::<(), str>::to(topic).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(e, _)| format!("produce {id}: {e}"))?;
    }
    Ok(())
}

/// The config file the binary reads. Security settings sit in it only for [`Form::File`].
fn config_file(shared: &Shared, row: &Row, table: &str, topic: &str) -> String {
    let pki = &shared.broker.pki;
    let security = match (row.form, row.listener) {
        (Form::Env | Form::EnvSystemTrust, _) => String::new(),
        (Form::File, Listener::Tls) => {
            format!(
                "  tls:\n    enabled: true\n    ca_cert_file: \"{}\"\n",
                pki.file(row.ca)
            )
        }
        (Form::File, Listener::SaslSsl) => format!(
            "  tls:\n    enabled: true\n    ca_cert_file: \"{}\"\n  sasl:\n    enabled: true\n    \
             mechanism: scram_sha_512\n    username: \"{SCRAM_USER}\"\n    password: \"{}\"\n",
            pki.file(row.ca),
            row.password
        ),
        (Form::File, Listener::Mtls) => format!(
            "  tls:\n    enabled: true\n    ca_cert_file: \"{}\"\n    cert_file: \"{}\"\n    \
             key_file: \"{}\"\n",
            pki.file(row.ca),
            pki.file("client.crt"),
            pki.file("client.key")
        ),
    };
    format!(
        "transport: kafka\n\
         kafka:\n  brokers:\n    - \"{}\"\n  group: \"dfe-{table}\"\n  topics:\n    - \"{topic}\"\n{security}\
         clickhouse:\n  hosts:\n    - \"{}\"\n  database: default\n  protocol: http\n  password: \"{}\"\n\
         routing:\n  default_db: default\n  default_table: {table}\n  dlq:\n    enabled: false\n\
         buffer:\n  flush_rows: 10\n  flush_age_secs: 1\n\
         schema:\n  cache_ttl_secs: 3600\n  pre_warm_retry_secs: 10\n",
        address(shared, row.listener),
        shared.clickhouse,
        crate::common::containers::clickhouse_password(),
    )
}

/// The real loader binary, killed if the test ends without stopping it.
struct LoaderProcess(std::process::Child);

impl LoaderProcess {
    /// Send SIGTERM, the signal Kubernetes stops a pod with, and wait up to
    /// `within` for the exit.
    async fn terminate(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(status)) = self.0.try_wait() {
                return Some(status);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

impl Drop for LoaderProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start the binary for `row`, run it until the outcome is known and report it.
async fn run_row(shared: Arc<Shared>, row: &'static Row) -> Result<String, String> {
    let table = unique_table_name(&row.name.replace('-', "_"));
    let topic = format!("{table}_land");
    query_client(&shared.clickhouse)
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .map_err(|e| format!("create table: {e}"))?;
    fill_topic(&shared, row, &topic).await?;

    let dir = tempfile::tempdir().map_err(|e| format!("working directory: {e}"))?;
    let config_path = dir.path().join("loader.yaml");
    std::fs::write(&config_path, config_file(&shared, row, &table, &topic))
        .map_err(|e| format!("write config: {e}"))?;
    let log_path = dir.path().join("loader.log");
    let log = std::fs::File::create(&log_path).map_err(|e| format!("loader log: {e}"))?;

    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-loader"));
    command
        .arg("--config")
        .arg(&config_path)
        .arg("--metrics-addr")
        .arg(format!("127.0.0.1:{}", free_port()))
        // An empty directory, so no `.env` reaches the binary.
        .current_dir(dir.path())
        .env("DFE_LOADER_VERSION_CHECK__ENABLED", "false")
        .stdout(log.try_clone().map_err(|e| format!("log handle: {e}"))?)
        .stderr(log);
    if matches!(row.form, Form::Env | Form::EnvSystemTrust) {
        command
            .env("DFE_LOADER_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
            .env("DFE_LOADER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
            .env("DFE_LOADER_KAFKA_SASL_USERNAME", SCRAM_USER)
            .env("DFE_LOADER_KAFKA_SASL_PASSWORD", row.password);
        if let Form::EnvSystemTrust = row.form {
            command.env("SSL_CERT_FILE", shared.broker.pki.file(row.ca));
        } else {
            command.env(
                "DFE_LOADER_KAFKA__TLS__CA_CERT_FILE",
                shared.broker.pki.file(row.ca),
            );
        }
    }
    let mut loader = LoaderProcess(
        command
            .spawn()
            .map_err(|e| format!("start the binary: {e}"))?,
    );

    let want: BTreeSet<u64> = (0..RECORDS).collect();
    let budget = if row.refused_with.is_some() {
        REFUSED_FOR
    } else {
        LAND_WITHIN
    };
    let landed = wait_landed(&shared.clickhouse, &table, &want, budget).await;
    let exit = loader.terminate(Duration::from_secs(30)).await;
    let log_text = std::fs::read_to_string(&log_path).unwrap_or_default();

    match row.refused_with {
        None if want.is_subset(&landed) => Ok(format!(
            "{} records landed in default.{table}, loader exit after SIGTERM {exit:?}",
            landed.len()
        )),
        None => Err(format!(
            "{} of {RECORDS} records landed within {LAND_WITHIN:?}. Loader log:\n{log_text}",
            landed.len()
        )),
        Some(_) if landed.is_empty() => Ok(format!(
            "nothing landed in {REFUSED_FOR:?}, loader exit after SIGTERM {exit:?}"
        )),
        Some(_) => Err(format!(
            "{} records landed through a connection the broker should refuse",
            landed.len()
        )),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_binary_consumes_from_a_broker_with_tls_on() {
    require_docker_in_ci();
    if !has_docker() {
        eprintln!("skipping: no Docker daemon answers");
        return;
    }
    let test = test_name!();
    let (broker, infra) = tokio::join!(
        TlsBroker::start("dfe-loader", test),
        TestInfrastructure::new(test, true, false)
    );
    let broker = broker.unwrap_or_else(|e| panic!("the TLS broker would not start: {e}"));
    let shared = Arc::new(Shared {
        broker,
        clickhouse: clickhouse_address(&infra).await,
    });

    let running: Vec<_> = ROWS
        .iter()
        .map(|row| {
            let shared = Arc::clone(&shared);
            (row, tokio::spawn(run_row(shared, row)))
        })
        .collect();
    let mut failures = Vec::new();
    for (row, handle) in running {
        match handle
            .await
            .unwrap_or_else(|e| Err(format!("row task panicked: {e}")))
        {
            Ok(detail) => eprintln!("PASS {}: {detail}", row.name),
            Err(why) => {
                eprintln!("FAIL {}: {why}", row.name);
                failures.push(format!("{}: {why}", row.name));
            }
        }
    }

    // The broker names the listener in the channel id (`channelId=127.0.0.1:<port>-...`).
    let broker_log = shared.broker.logs().await;
    for row in ROWS {
        let listener = address(&shared, row.listener);
        if let Some(says) = row.refused_with
            && !broker_log.lines().any(|line| {
                line.contains("Failed authentication")
                    && line.contains(&format!("channelId={listener}-"))
                    && line.contains(says)
            })
        {
            failures.push(format!(
                "{}: the broker never logged a failed authentication on {listener} saying {says:?}",
                row.name
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} row(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
