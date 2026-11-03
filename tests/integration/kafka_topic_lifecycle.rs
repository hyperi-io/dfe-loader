// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A source removed while the loader runs takes its routing, table and topic
//! with it, and the loader keeps consuming every topic discovered after.
//!
//! Runs the real orchestrator with topic auto-discovery against its own Kafka
//! broker and the shared `ClickHouse`.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::time::Duration;

use dfe_loader::config::{BufferConfig, SharedConfig};
use dfe_loader::pipeline::Orchestrator;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};

use super::grpc_outage::{clickhouse_address, query_client, wait_landed};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader, produce, wait_committed};
use super::kafka_transport_e2e::{delete_topic, make_producer, wait_for_topics};
use super::tenancy::{create_header_table, landed_orgs};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// How long a record has to land once its topic is subscribed.
const BUDGET: Duration = Duration::from_secs(90);

/// Discovery refreshes every 60 s, so a new topic can wait two refreshes.
const DISCOVERED: Duration = Duration::from_secs(180);

/// Longer than one discovery refresh, so the new topic arrives in a later one
/// than the delete.
const QUIET: Duration = Duration::from_secs(75);

/// Partitions per topic: each partition of a deleted topic queues its own errors.
const PARTITIONS: i32 = 3;

/// Gap between the smoke records.
const SEND_GAP: Duration = Duration::from_millis(50);

/// Create `topic` with [`PARTITIONS`] partitions.
async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, PARTITIONS, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("create request");
    wait_for_topics(bootstrap, &[topic]).await;
}

/// The `id`s `default.{table}` holds.
async fn landed_ids(clickhouse: &str, table: &str) -> BTreeSet<u64> {
    landed_orgs(clickhouse, table).await.into_keys().collect()
}

/// The smoke-test source lifecycle on an otherwise idle broker: a source's
/// topic is discovered and consumed, then the source is removed, which reloads
/// the routing without it, drops its table and deletes its topic while records
/// are still buffered. A topic created a refresh later must be consumed and
/// committed.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_source_does_not_stop_consumption() {
    let kafka = TestInfrastructure::exclusive(test_name!(), false, true).await;
    let shared_ch = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&shared_ch).await;
    let bootstrap = kafka_bootstrap(&kafka).await;
    let direct = query_client(&clickhouse);

    let fallback = unique_table_name("lifecycle_main");
    let removed = unique_table_name("lifecycle_postroute");
    let next = unique_table_name("lifecycle_next");
    for table in [&fallback, &removed, &next] {
        create_header_table(&direct, table).await;
    }
    let removed_topic = format!("{removed}_land");
    let next_topic = format!("{next}_load");
    create_topic(&bootstrap, &removed_topic).await;
    // A deployment always has other topics subscribed, idle or not.
    create_topic(&bootstrap, &format!("{fallback}_land")).await;

    let group = format!("dfe-{removed}");
    let mut config = kafka_loader(&bootstrap, "", &group, clickhouse.clone(), &fallback);
    config.kafka.topics = vec![];
    config.buffer = BufferConfig::default();
    config
        .routing
        .source_to_table
        .insert(removed.clone(), removed.clone());
    let shared = SharedConfig::new(config.clone());
    let mut orchestrator = Orchestrator::new(config.clone()).with_shared_config(shared.clone());
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });

    // The smoke test posts until a row lands, so records are still arriving
    // and buffered when the source goes.
    let producer = make_producer(&bootstrap);
    let mut sent = 0;
    let mut landed_smoke = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + BUDGET;
    for id in 1_u64.. {
        produce(&producer, &removed_topic, &format!(r#"{{"id":{id}}}"#)).await;
        sent += 1;
        landed_smoke = landed_ids(&clickhouse, &removed).await;
        if !landed_smoke.is_empty() || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(SEND_GAP).await;
    }

    // The source's removal: routing reloaded without it, then table and topic go.
    let mut without = config.clone();
    without.routing.source_to_table.remove(&removed);
    shared.update(without);
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{removed}"))
        .await;
    delete_topic(&bootstrap, &removed_topic).await;

    tokio::time::sleep(QUIET).await;
    create_topic(&bootstrap, &next_topic).await;
    let after: BTreeSet<u64> = (101..=103).collect();
    let mut last = 0;
    for id in &after {
        last = produce(&producer, &next_topic, &format!(r#"{{"id":{id}}}"#)).await;
    }
    let landed_next = wait_landed(&clickhouse, &next, &after, DISCOVERED).await;
    let committed = wait_committed(&bootstrap, &group, &next_topic, last + 1).await;
    let still_running = !loader.is_finished();

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&fallback, &next] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(
        !landed_smoke.is_empty(),
        "none of the {sent} records sent to the source landed"
    );
    assert!(still_running, "the loader stopped");
    assert_eq!(
        landed_next, after,
        "a topic discovered after a source was removed was not consumed"
    );
    assert!(
        committed >= Some(last + 1),
        "the new topic's offsets were never committed: {committed:?}"
    );
}
