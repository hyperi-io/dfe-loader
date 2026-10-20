// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A loader started before its Kafka broker waits for it rather than exiting.
//!
//! The loader runs against a real `ClickHouse` with a broker address nothing
//! listens on yet. Topic auto-discovery cannot list topics without the broker,
//! so the loader consumes nothing until a Kafka container comes up on that
//! address and a topic refresh finds the topic. The records then land.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::time::Duration;

use dfe_loader::metrics::Metrics;
use dfe_loader::pipeline::Orchestrator;
use rdkafka::producer::FutureRecord;
use scalo::metrics::MetricsManager;

use super::grpc_outage::{clickhouse_address, free_port, query_client, wait_landed};
use super::kafka_offset_floor::kafka_loader;
use super::kafka_transport_e2e::{ensure_topic, make_producer, wait_for_broker};
use crate::common::containers::{TestInfrastructure, start_kafka_on};
use crate::common::unique_table_name;
use crate::test_name;

/// Records written once the broker is up.
const RECORDS: u64 = 20;

/// How long the broker stays down: past one 10 s metadata fetch, so a failed
/// topic listing has had time to end the loader.
const DOWN_FOR: Duration = Duration::from_secs(15);

/// How long records have to land once the broker is up: past the 60 s topic
/// refresh, in case the loader starts before the topic exists.
const LAND_WITHIN: Duration = Duration::from_secs(90);

#[tokio::test(flavor = "multi_thread")]
async fn a_loader_started_before_its_broker_joins_once_the_broker_is_up() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let table = unique_table_name("broker_startup");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let topic = format!("{table}_land");
    let port = free_port();
    let bootstrap = format!("127.0.0.1:{port}");
    let mut config = kafka_loader(
        &bootstrap,
        &topic,
        &format!("dfe-{table}"),
        clickhouse.clone(),
        &table,
    );
    // Empty topics auto-discover: the start that needs a broker to succeed.
    config.kafka.topics.clear();
    config.kafka.topic_regex = Some(format!("^{topic}$"));
    let manager = MetricsManager::new("loader_it");
    let mut loader = Orchestrator::with_metrics(config, Metrics::new(&manager));
    let stop = loader.shutdown_token();
    let running = tokio::spawn(async move { loader.run().await });

    tokio::time::sleep(DOWN_FOR).await;
    let alive_while_down = !running.is_finished();

    let _kafka = start_kafka_on(test_name!(), port).await;
    wait_for_broker(&bootstrap).await;
    ensure_topic(&bootstrap, &topic).await;
    let producer = make_producer(&bootstrap);
    for id in 0..RECORDS {
        let payload = format!(r#"{{"id":{id}}}"#);
        producer
            .send(
                FutureRecord::<(), str>::to(&topic).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("deliver record {id}: {e}"));
    }
    let want: BTreeSet<u64> = (0..RECORDS).collect();
    let landed = wait_landed(&clickhouse, &table, &want, LAND_WITHIN).await;
    stop.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(60), running).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(
        alive_while_down,
        "the loader exited while its broker was down"
    );
    assert_eq!(
        landed, want,
        "the records did not land once the broker came up"
    );
    assert!(
        matches!(stopped, Ok(Ok(Ok(())))),
        "the loader did not stop cleanly within 60s of the shutdown"
    );
}
