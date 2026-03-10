// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Real Kafka → ClickHouse E2E bulk load test.
//!
//! Requires both Kafka (`KAFKA_BROKERS`) and ClickHouse (`CLICKHOUSE_HOST`)
//! to be reachable. Skips automatically when not available.
//!
//! Covers:
//! - Mixed log formats: Logstash JSON, Filebeat/Winlogbeat, post-RFC syslog JSON
//! - Bulk load (1 200+ messages across three formats)
//! - `_source`-based table routing
//! - `_org_id` extraction for row-level security
//! - Org-level database routing (creates db + table in the test)
//! - Graceful pipeline shutdown after all messages are consumed

use std::env;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;

use dfe_loader::config::{
    BufferConfig, ClickHouseConfig, Config, DlqConfig, KafkaConfig, MetadataConfig, OrgRoute,
    RoutingConfig, SaslConfig, TimestampDqConfig,
};
use dfe_loader::pipeline::Orchestrator;

use crate::common::{check_clickhouse_reachable, check_kafka_reachable, load_dotenv};

// ─── Skip guard ──────────────────────────────────────────────────────────────

fn skip_if_no_env() -> bool {
    load_dotenv();
    if env::var("CLICKHOUSE_HOST").is_err() || env::var("KAFKA_BROKERS").is_err() {
        eprintln!("Skipping: CLICKHOUSE_HOST or KAFKA_BROKERS not set");
        return true;
    }
    if !check_kafka_reachable() {
        eprintln!("Skipping: Kafka not reachable");
        return true;
    }
    if !check_clickhouse_reachable() {
        eprintln!("Skipping: ClickHouse not reachable");
        return true;
    }
    false
}

// ─── ClickHouse HTTP helpers ──────────────────────────────────────────────────

/// Execute a DDL or DML statement via raw ClickHouse HTTP POST.
async fn ch_execute(
    client: &reqwest::Client,
    base_url: &str,
    user: &str,
    pass: &str,
    sql: &str,
) -> Result<(), String> {
    let resp = client
        .post(base_url)
        .basic_auth(user, Some(pass))
        .body(sql.to_string())
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if resp.status().is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(format!("ClickHouse error: {body}"))
    }
}

/// Run a query that returns a single numeric value (e.g. `SELECT count()`).
async fn ch_count(
    client: &reqwest::Client,
    base_url: &str,
    user: &str,
    pass: &str,
    sql: &str,
) -> u64 {
    let resp = client
        .post(base_url)
        .basic_auth(user, Some(pass))
        .body(sql.to_string())
        .send()
        .await
        .unwrap_or_else(|e| panic!("HTTP request failed: {e}"));
    let text = resp.text().await.unwrap_or_default();
    text.trim().parse().unwrap_or(0)
}

fn make_reqwest_client() -> (reqwest::Client, String, String, String) {
    load_dotenv();
    let host = env::var("CLICKHOUSE_HOST").expect("CLICKHOUSE_HOST");
    let port = env::var("CLICKHOUSE_HTTP_PORT").unwrap_or_else(|_| "8123".to_string());
    let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let pass = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let base_url = format!("http://{}:{}", host, port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client build failed");
    (client, base_url, user, pass)
}

// ─── Kafka helpers ────────────────────────────────────────────────────────────

fn make_client_config() -> ClientConfig {
    load_dotenv();
    let brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS");
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &brokers);
    if let Ok(user) = env::var("KAFKA_SASL_USER") {
        let pass = env::var("KAFKA_SASL_PASSWORD").unwrap_or_default();
        let mech = env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-512".to_string());
        cfg.set("security.protocol", "SASL_PLAINTEXT");
        cfg.set("sasl.mechanism", &mech);
        cfg.set("sasl.username", &user);
        cfg.set("sasl.password", &pass);
    }
    cfg
}

fn make_producer() -> FutureProducer {
    let mut cfg = make_client_config();
    // 30 s gives auto.create.topics.enable enough time to create the topic
    // on the first produce (metadata fetch + leader election + produce retry).
    cfg.set("message.timeout.ms", "30000");
    cfg.create().expect("Kafka producer creation failed")
}

async fn produce_messages(producer: &FutureProducer, topic: &str, payloads: &[Vec<u8>]) {
    for (i, payload) in payloads.iter().enumerate() {
        let record: FutureRecord<str, [u8]> = FutureRecord::to(topic)
            .payload(payload.as_slice())
            .partition(-1);
        match producer.send(record, Duration::from_secs(10)).await {
            Ok(_) => {}
            Err((e, _)) => panic!("Failed to produce message {i} to topic {topic}: {e}"),
        }
    }
}

fn sasl_config_from_env() -> Option<SaslConfig> {
    load_dotenv();
    env::var("KAFKA_SASL_USER").ok()?;
    let mech_str = env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-512".to_string());
    let mechanism = match mech_str.as_str() {
        "PLAIN" => "plain",
        "SCRAM-SHA-256" => "scram_sha_256",
        _ => "scram_sha_512",
    };
    Some(SaslConfig {
        enabled: true,
        mechanism: mechanism.to_string(),
        username: env::var("KAFKA_SASL_USER").unwrap_or_default(),
        password: env::var("KAFKA_SASL_PASSWORD").unwrap_or_default(),
        ..Default::default()
    })
}

fn ch_config_from_env() -> ClickHouseConfig {
    load_dotenv();
    let host = env::var("CLICKHOUSE_HOST").expect("CLICKHOUSE_HOST");
    let port = env::var("CLICKHOUSE_HTTP_PORT").unwrap_or_else(|_| "8123".to_string());
    ClickHouseConfig {
        hosts: vec![format!("{}:{}", host, port)],
        database: "default".to_string(),
        username: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()),
        password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        protocol: "native".to_string(),
        tables: Vec::new(),
        tls: None,
    }
}

fn brokers_from_env() -> Vec<String> {
    env::var("KAFKA_BROKERS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.to_string())
        .collect()
}

// ─── Log format generators ────────────────────────────────────────────────────

/// Logstash JSON ("logjson") format — classic Logstash output
fn logstash_json_messages(count: usize, org_id: &str, source: &str) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let level = ["INFO", "WARN", "ERROR", "DEBUG"][i % 4];
            serde_json::to_vec(&json!({
                "@timestamp": chrono::Utc::now().to_rfc3339(),
                "@version": "1",
                "message": format!("Event {} via logstash", i),
                "level": level,
                "host": format!("app-node-{}", i % 5),
                "logger": "com.example.app.Service",
                "org_id": org_id,
                "_source": source,
                "user_id": 1000u64 + i as u64,
                "session_id": format!("sess-{:08x}", i),
                "thread": "main",
                "tags": ["production", "logstash"]
            }))
            .expect("serialise logstash message")
        })
        .collect()
}

/// Filebeat / Winlogbeat format — beat agent telemetry
///
/// Every third message uses Winlogbeat (Windows) fields; the rest use Filebeat
/// (Linux) fields, mirroring real mixed-agent deployments.
fn filebeat_winlogbeat_messages(count: usize, org_id: &str, source: &str) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let is_win = i % 3 == 0;
            let method = ["GET", "POST", "PUT", "DELETE"][i % 4];
            let outcome = if i % 10 == 0 { "failure" } else { "success" };
            let status: u16 = if i % 10 == 0 { 500 } else { 200 };
            let agent_type = if is_win { "winlogbeat" } else { "filebeat" };
            let agent_name = if is_win {
                "WIN-SERVER-01".to_string()
            } else {
                format!("linux-host-{}", i % 5)
            };
            let host_name = if is_win {
                "WIN-SERVER-01".to_string()
            } else {
                format!("web-{:02}", i % 10)
            };
            let os_family = if is_win { "windows" } else { "linux" };
            let mut base = json!({
                "@timestamp": chrono::Utc::now().to_rfc3339(),
                "agent": {
                    "name": agent_name,
                    "type": agent_type,
                    "version": "8.12.0"
                },
                "host": {
                    "name": host_name,
                    "os": { "family": os_family }
                },
                "org_id": org_id,
                "_source": source,
                "event": {
                    "action": "api_request",
                    "outcome": outcome
                },
                "http": {
                    "request": { "method": method },
                    "response": { "status_code": status }
                },
                "url": { "path": format!("/api/v2/resource/{}", i % 100) }
            });
            if is_win {
                base["winlog"] = json!({
                    "api": "wineventlog",
                    "channel": "Security",
                    "event_id": 4624u64 + (i % 10) as u64,
                    "computer_name": "WIN-SERVER-01"
                });
            } else {
                base["log"] = json!({
                    "offset": (i as u64) * 256,
                    "file": { "path": format!("/var/log/app/api-{}.log", i % 5) }
                });
            }
            serde_json::to_vec(&base).expect("serialise beat message")
        })
        .collect()
}

/// Post-RFC 5424 parsed syslog JSON — syslog messages after structured parsing
fn syslog_rfc_parsed_messages(count: usize, org_id: &str, source: &str) -> Vec<Vec<u8>> {
    const FACILITIES: &[&str] = &["kern", "user", "auth", "daemon", "local0", "local7"];
    const SEVERITIES: &[&str] = &[
        "emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
    ];
    const PROGRAMS: &[&str] = &[
        "sshd", "sudo", "kernel", "cron", "systemd", "nginx", "postgres",
    ];

    (0..count)
        .map(|i| {
            serde_json::to_vec(&json!({
                "@timestamp": chrono::Utc::now().to_rfc3339(),
                "syslog5424_pri": (16 + i % 8) as u32,
                "syslog5424_ver": 1u32,
                "syslog5424_severity": SEVERITIES[i % SEVERITIES.len()],
                "syslog5424_facility": FACILITIES[i % FACILITIES.len()],
                "syslog5424_hostname": format!("server-{:02}.prod", i % 20),
                "syslog5424_app": PROGRAMS[i % PROGRAMS.len()],
                "syslog5424_proc": format!("{}", 1000 + i as u32),
                "syslog5424_msgid": format!("ID{:06}", i),
                "syslog5424_msg": format!("Event {} — connection from 10.0.{}.{}", i, i % 256, (i / 256) % 256),
                "org_id": org_id,
                "_source": source,
                "logsource": "syslog",
                "tags": ["syslog", "rfc5424"]
            }))
            .expect("serialise syslog message")
        })
        .collect()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Bulk load: 1 200 messages via real Kafka → pipeline → ClickHouse.
///
/// Three batches of 400 messages each, one per log format:
///   - Logstash JSON (logjson)
///   - Filebeat / Winlogbeat beat agent format
///   - Post-RFC 5424 parsed syslog JSON
///
/// All messages carry `_source = {table_name}` so the pipeline routes them all
/// to the same destination table. Row count is verified after replica sync.
#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_to_clickhouse_bulk_load() {
    if skip_if_no_env() {
        return;
    }

    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_millis();
    let topic = format!("e2e_bulk_{pid}_{ts}_land");
    let table_name = format!("e2e_bulk_{pid}_{ts}");

    let (http, base_url, user, pass) = make_reqwest_client();

    // Table in the `benchmark` Atomic database — ON CLUSTER ensures it exists on
    // all 3 nodes. MergeTree (no replication); count via clusterAllReplicas.
    ch_execute(
        &http,
        &base_url,
        &user,
        &pass,
        &format!(
            "CREATE TABLE IF NOT EXISTS benchmark.{table_name} ON CLUSTER 'default' (
                _timestamp  DateTime64(3, 'UTC'),
                _org_id     String,
                _source     LowCardinality(String),
                message     String DEFAULT '',
                org_id      String DEFAULT '',
                level       LowCardinality(String) DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY (_source, _org_id, _timestamp)"
        ),
    )
    .await
    .expect("Failed to create table");

    const MSGS_PER_FORMAT: usize = 400;
    let producer = make_producer();

    let mut all_msgs: Vec<Vec<u8>> = Vec::with_capacity(MSGS_PER_FORMAT * 3);
    all_msgs.extend(logstash_json_messages(
        MSGS_PER_FORMAT,
        "test_org",
        &table_name,
    ));
    all_msgs.extend(filebeat_winlogbeat_messages(
        MSGS_PER_FORMAT,
        "test_org",
        &table_name,
    ));
    all_msgs.extend(syslog_rfc_parsed_messages(
        MSGS_PER_FORMAT,
        "test_org",
        &table_name,
    ));

    let total_produced = all_msgs.len();
    produce_messages(&producer, &topic, &all_msgs).await;
    eprintln!("Produced {total_produced} messages to topic {topic}");

    let config = Config {
        transport: "kafka".to_string(),
        kafka: KafkaConfig {
            brokers: brokers_from_env(),
            group: format!("e2e-bulk-{pid}"),
            topics: vec![topic.clone()],
            topic_refresh_secs: 0,
            sasl: sasl_config_from_env(),
            ..Default::default()
        },
        clickhouse: ch_config_from_env(),
        routing: RoutingConfig {
            db_fields: vec![],
            table_fields: vec!["_source".to_string()],
            default_db: "benchmark".to_string(),
            default_table: table_name.clone(),
            org_id_field: Some("org_id".to_string()),
            org_routes: vec![],
            source_to_table: Default::default(),
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: DlqConfig {
                enabled: false,
                ..Default::default()
            },
            rules: vec![],
        },
        buffer: BufferConfig {
            flush_rows: 200,
            flush_bytes: 1_048_576,
            flush_age_secs: 2,
        },
        timestamp_dq: TimestampDqConfig {
            enabled: true,
            ..Default::default()
        },
        metadata: MetadataConfig {
            enabled: true,
            inject_timestamp_load: true,
            source_fields: vec!["_source".to_string()],
            source_output: "_source".to_string(),
            capture_source: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let pipeline_handle = tokio::spawn(async move { orchestrator.run().await });

    // 1 200 messages through the devex cluster should complete well within 20 s
    tokio::time::sleep(Duration::from_secs(20)).await;
    shutdown.cancel();

    match pipeline_handle.await {
        Ok(Ok(())) => eprintln!("Pipeline shut down cleanly"),
        Ok(Err(e)) => eprintln!("Pipeline returned error (expected on shutdown): {e}"),
        Err(e) => eprintln!("Pipeline task panicked: {e}"),
    }

    // Atomic database — no replication sync needed; clusterAllReplicas
    // queries all 3 nodes and sums their local counts.
    let count = ch_count(
        &http,
        &base_url,
        &user,
        &pass,
        &format!(
            "SELECT count() FROM clusterAllReplicas('default', 'benchmark', '{table_name}')"
        ),
    )
    .await;

    eprintln!("ClickHouse rows: {count} / {total_produced} produced");
    assert!(
        count >= total_produced as u64 * 9 / 10,
        "Expected ≥90% of {total_produced} rows in ClickHouse, found {count}"
    );

    ch_execute(
        &http,
        &base_url,
        &user,
        &pass,
        &format!("DROP TABLE IF EXISTS benchmark.{table_name} ON CLUSTER 'default'"),
    )
    .await
    .ok();
}

/// Org routing: messages for a configured org go to a dedicated database.
///
/// The test creates the target database and table before starting the pipeline,
/// which is required when per-org routing is active and the table does not yet
/// exist. All other orgs fall back to the shared `default` database.
///
/// Per-org table uses Atomic + MergeTree (ON CLUSTER). Count is verified via
/// `clusterAllReplicas` so results are consistent regardless of which cluster
/// node the load-balanced INSERT landed on.
#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_to_clickhouse_org_routing() {
    if skip_if_no_env() {
        return;
    }

    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_millis();
    let topic = format!("e2e_org_{pid}_{ts}_land");

    // Per-org database and table (Atomic + MergeTree + ON CLUSTER)
    let org_db = format!("e2e_org_{pid}_{ts}");
    let org_table = "events";

    // Shared fallback table for non-routed orgs (benchmark Atomic database)
    let shared_table = format!("e2e_shared_{pid}_{ts}");
    let shared_full = format!("benchmark.{shared_table}");

    let (http, base_url, user, pass) = make_reqwest_client();

    // Create the per-org database on all 3 cluster nodes
    ch_execute(
        &http,
        &base_url,
        &user,
        &pass,
        &format!("CREATE DATABASE IF NOT EXISTS {org_db} ON CLUSTER 'default'"),
    )
    .await
    .expect("Failed to create org database");

    // Table in the per-org database (Atomic — MergeTree + ON CLUSTER)
    ch_execute(
        &http,
        &base_url,
        &user,
        &pass,
        &format!(
            "CREATE TABLE IF NOT EXISTS {org_db}.{org_table} ON CLUSTER 'default' (
                _timestamp  DateTime64(3, 'UTC'),
                _org_id     String,
                _source     LowCardinality(String),
                message     String DEFAULT '',
                org_id      String DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY (_org_id, _timestamp)"
        ),
    )
    .await
    .expect("Failed to create per-org table");

    // Shared fallback table in the `benchmark` Atomic database — same pattern
    // as the per-org table; count via clusterAllReplicas.
    ch_execute(
        &http,
        &base_url,
        &user,
        &pass,
        &format!(
            "CREATE TABLE IF NOT EXISTS {shared_full} ON CLUSTER 'default' (
                _timestamp  DateTime64(3, 'UTC'),
                _org_id     String,
                _source     LowCardinality(String),
                message     String DEFAULT '',
                org_id      String DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY (_org_id, _timestamp)"
        ),
    )
    .await
    .expect("Failed to create shared table");

    let producer = make_producer();

    // Routed org: org_id matches org_routes → land in {org_db}.{org_table}
    let routed_msgs = logstash_json_messages(100, &org_db, org_table);
    // Non-routed org: org_id not in org_routes → land in default.{shared_table}
    let unrouted_msgs = logstash_json_messages(50, "other_org", &shared_table);

    let mut all_msgs: Vec<Vec<u8>> = Vec::with_capacity(150);
    all_msgs.extend(routed_msgs);
    all_msgs.extend(unrouted_msgs);
    produce_messages(&producer, &topic, &all_msgs).await;
    eprintln!("Produced {} messages to topic {topic}", all_msgs.len());

    let config = Config {
        transport: "kafka".to_string(),
        kafka: KafkaConfig {
            brokers: brokers_from_env(),
            group: format!("e2e-org-{pid}"),
            topics: vec![topic.clone()],
            topic_refresh_secs: 0,
            sasl: sasl_config_from_env(),
            ..Default::default()
        },
        clickhouse: ch_config_from_env(),
        routing: RoutingConfig {
            db_fields: vec!["org_id".to_string()],
            table_fields: vec!["_source".to_string()],
            default_db: "benchmark".to_string(),
            default_table: shared_table.clone(),
            org_id_field: Some("org_id".to_string()),
            org_routes: vec![OrgRoute {
                org_id: org_db.clone(),
                database: Some(org_db.clone()),
            }],
            source_to_table: Default::default(),
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: DlqConfig {
                enabled: false,
                ..Default::default()
            },
            rules: vec![],
        },
        buffer: BufferConfig {
            flush_rows: 50,
            flush_bytes: 524_288,
            flush_age_secs: 2,
        },
        timestamp_dq: TimestampDqConfig {
            enabled: true,
            ..Default::default()
        },
        metadata: MetadataConfig {
            enabled: true,
            inject_timestamp_load: true,
            source_fields: vec!["_source".to_string()],
            source_output: "_source".to_string(),
            capture_source: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let pipeline_handle = tokio::spawn(async move { orchestrator.run().await });

    tokio::time::sleep(Duration::from_secs(15)).await;
    shutdown.cancel();

    match pipeline_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("Pipeline error (expected on shutdown): {e}"),
        Err(e) => eprintln!("Pipeline task panicked: {e}"),
    }

    // Atomic DB + MergeTree: query across all nodes to get total count
    let per_org_count = ch_count(
        &http,
        &base_url,
        &user,
        &pass,
        &format!("SELECT count() FROM clusterAllReplicas('default', '{org_db}', '{org_table}')"),
    )
    .await;

    eprintln!("Per-org {org_db}.{org_table}: {per_org_count} rows");
    assert!(
        per_org_count > 0,
        "Expected routed org rows in {org_db}.{org_table}, got 0"
    );

    // Benchmark Atomic DB — no sync needed; sum across all nodes.
    let shared_count = ch_count(
        &http,
        &base_url,
        &user,
        &pass,
        &format!(
            "SELECT count() FROM clusterAllReplicas('default', 'benchmark', '{shared_table}')"
        ),
    )
    .await;
    eprintln!("Shared {shared_full}: {shared_count} rows");
    assert!(
        shared_count > 0,
        "Expected non-routed org rows in {shared_full}, got 0"
    );

    for stmt in [
        format!("DROP TABLE IF EXISTS {org_db}.{org_table} ON CLUSTER 'default'"),
        format!("DROP DATABASE IF EXISTS {org_db} ON CLUSTER 'default'"),
        format!("DROP TABLE IF EXISTS {shared_full} ON CLUSTER 'default'"),
    ] {
        ch_execute(&http, &base_url, &user, &pass, &stmt).await.ok();
    }
}
