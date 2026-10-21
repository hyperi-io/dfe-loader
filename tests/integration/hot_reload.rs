// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A routing edit to the mounted config file reaches a running loader with no
//! restart, and no record is lost or written twice across the reload.
//!
//! Runs the real orchestrator and config watcher on the Kafka transport against
//! Kafka and `ClickHouse` containers, editing the file while records stream.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use dfe_loader::config::{Config, ConfigWatcher, SharedConfig, WatcherConfig};
use dfe_loader::pipeline::Orchestrator;

use super::grpc_outage::{clickhouse_address, query_client, wait_landed};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader, produce, wait_committed};
use super::kafka_transport_e2e::{ensure_topic, make_producer};
use super::tenancy::{create_header_table, landed_orgs};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// How long a record has to land.
const BUDGET: Duration = Duration::from_secs(90);

/// Records streamed across the reload, one every [`STREAM_GAP`].
const STREAM: u64 = 600;
const STREAM_GAP: Duration = Duration::from_millis(25);

/// How long the watcher has to see the edited file.
const RELOAD_SEEN: Duration = Duration::from_secs(30);

/// Write `config` as the loader's mounted config file.
fn write_config(path: &Path, config: &Config) {
    let yaml = scalo::config::sensitive::expose_during(|| scalo::yaml::to_string(config))
        .expect("config renders as YAML");
    std::fs::write(path, yaml).expect("write config file");
}

/// The rows `default.{table}` holds, duplicates included.
async fn row_count(clickhouse: &str, table: &str) -> u64 {
    let sql = format!("SELECT count() FROM default.{table} FORMAT TabSeparated");
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .query(&[("query", sql.as_str())])
        .send()
        .await
    else {
        return 0;
    };
    response
        .text()
        .await
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_routing_edit_applies_with_no_restart_and_loses_nothing() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let before = unique_table_name("reload_before");
    let after = unique_table_name("reload_after");
    for table in [&before, &after] {
        create_header_table(&direct, table).await;
    }
    let topic = format!("reload-{before}");
    let group = format!("dfe-{before}");
    ensure_topic(&bootstrap, &topic).await;

    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &before);
    config.routing.default_org_id = "org-before".to_string();
    config.hot_reload.poll_interval_secs = 1;
    config.hot_reload.debounce_ms = 100;
    let dir = tempfile::tempdir().expect("config directory");
    let path = dir.path().join("loader.yaml");
    write_config(&path, &config);

    // The wiring main.rs does when hot_reload.enabled is on.
    let shared = SharedConfig::new(config.clone());
    let watcher = ConfigWatcher::new(
        WatcherConfig {
            config_path: path.clone(),
            poll_interval: Duration::from_secs(config.hot_reload.poll_interval_secs),
            debounce: Duration::from_millis(config.hot_reload.debounce_ms),
            enabled: true,
        },
        shared.clone(),
    )
    .expect("config watcher")
    .start();
    let mut orchestrator = Orchestrator::new(config.clone()).with_shared_config(shared.clone());
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });

    let producer = make_producer(&bootstrap);
    let stream = {
        let (producer, topic) = (producer.clone(), topic.clone());
        tokio::spawn(async move {
            for id in 1..=STREAM {
                produce(&producer, &topic, &format!(r#"{{"id":{id}}}"#)).await;
                tokio::time::sleep(STREAM_GAP).await;
            }
        })
    };
    let first = wait_landed(&clickhouse, &before, &BTreeSet::from([1]), BUDGET).await;

    // Only routing changes, the one section the contract lists as a hot path.
    let mut edited = config.clone();
    edited.routing.default_table = after.clone();
    edited.routing.default_org_id = "org-after".to_string();
    write_config(&path, &edited);
    let deadline = tokio::time::Instant::now() + RELOAD_SEEN;
    while shared.version() == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let reloaded = shared.version();
    let streaming_at_reload = !stream.is_finished();
    stream.await.expect("stream task");

    // Produced after the stream, so the loader has applied the edit by the time
    // it reads them. The second names a table that does not exist, which falls
    // back to whatever the default table is now.
    let sentinel = STREAM + 1;
    produce(&producer, &topic, &format!(r#"{{"id":{sentinel}}}"#)).await;
    let unknown = STREAM + 2;
    let missing = unique_table_name("reload_missing");
    let last = produce(
        &producer,
        &topic,
        &format!(r#"{{"id":{unknown},"_source":"{missing}"}}"#),
    )
    .await;

    let all: BTreeSet<u64> = (1..=unknown).collect();
    let deadline = tokio::time::Instant::now() + BUDGET;
    let (mut in_before, mut in_after);
    loop {
        in_before = landed_orgs(&clickhouse, &before).await;
        in_after = landed_orgs(&clickhouse, &after).await;
        let landed: BTreeSet<u64> = in_before.keys().chain(in_after.keys()).copied().collect();
        if all.is_subset(&landed) || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let committed = wait_committed(&bootstrap, &group, &topic, last + 1).await;
    let rows = row_count(&clickhouse, &before).await + row_count(&clickhouse, &after).await;
    let still_running = !loader.is_finished();

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    watcher.abort();
    for table in [&before, &after] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(first.contains(&1), "the stream never started landing");
    assert!(reloaded >= 1, "the watcher never applied the edited file");
    assert!(
        streaming_at_reload,
        "the stream ended before the reload, so it did not run across it"
    );
    assert!(still_running, "the loader stopped instead of reloading");

    let landed: BTreeSet<u64> = in_before.keys().chain(in_after.keys()).copied().collect();
    let lost: Vec<u64> = all.difference(&landed).copied().collect();
    assert!(lost.is_empty(), "records lost across the reload: {lost:?}");
    let both: Vec<&u64> = in_before
        .keys()
        .filter(|id| in_after.contains_key(id))
        .collect();
    assert!(both.is_empty(), "records written to both tables: {both:?}");
    assert_eq!(
        rows,
        all.len() as u64,
        "a record was written twice across the reload"
    );

    assert!(
        in_before.values().all(|org| org == "org-before"),
        "a row routed by the old config took another tenant"
    );
    assert!(
        in_after.values().all(|org| org == "org-after"),
        "a row routed by the new config kept the old default org"
    );
    assert!(
        in_after.contains_key(&sentinel),
        "a record read after the reload did not land in the new default table"
    );
    assert!(
        in_after.contains_key(&unknown),
        "a record for an absent table fell back to the old default table"
    );
    assert!(
        committed >= Some(last + 1),
        "the group never committed past the last record: {committed:?}"
    );
}
