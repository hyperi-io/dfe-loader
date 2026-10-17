// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The consumer's `rdkafka_topic_*` series are published with the Kafka DLQ on (#153).
//!
//! Each test runs a real loader against Kafka and `ClickHouse` containers, lets
//! it commit, and reads the lag series from the Prometheus render `/metrics`
//! serves: once with the DLQ's Kafka producer built at startup and carrying a
//! dead letter, once with the DLQ off. Lag is published only for a partition
//! with a committed offset, so the rows landing comes first.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::time::Duration;

use dfe_loader::metrics::Metrics;
use dfe_loader::pipeline::Orchestrator;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::metrics::MetricsManager;

use super::grpc_outage::{clickhouse_address, query_client};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader};
use super::kafka_transport_e2e::{ensure_topic, make_producer};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// Loadable records produced before the loader starts.
const RECORDS: usize = 5;

/// How long the loader has for each step the test waits on.
const BUDGET: Duration = Duration::from_secs(90);

/// The lag series the consumer publishes for each partition it holds, after
/// the manager's namespace.
const LAG_SERIES: &str = "rdkafka_topic_partition_consumer_lag";

/// What one loader run published.
struct Observed {
    /// `LAG_SERIES` lines for the run's topic.
    lag: Vec<String>,
    /// Every `rdkafka` line of the last render, for a failure to show.
    rdkafka: Vec<String>,
    /// Records read back from the DLQ topic, when the run had one.
    dead_letters: usize,
}

/// Whether a render line is a sample of the series `name`, under any namespace.
fn is_series(line: &str, name: &str) -> bool {
    !line.starts_with('#')
        && line
            .split(['{', ' '])
            .next()
            .is_some_and(|metric| metric.ends_with(name))
}

async fn send(producer: &FutureProducer, topic: &str, payload: &str) {
    producer
        .send(
            FutureRecord::<(), str>::to(topic)
                .partition(0)
                .payload(payload),
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|(e, _)| panic!("deliver {payload}: {e}"));
}

/// Records on `topic`, read from the start until one arrives or `BUDGET` passes.
fn dead_letters(bootstrap: &str, topic: &str) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("{topic}-reader"))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("DLQ reader");
    consumer
        .subscribe(&[topic])
        .expect("subscribe to the DLQ topic");
    let deadline = std::time::Instant::now() + BUDGET;
    let mut read = 0;
    while read == 0 && std::time::Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(500)) {
            read += 1;
        }
    }
    read
}

/// Run a loader over a fresh topic, with the Kafka DLQ on or off.
async fn run_loader(test: &str, kafka_dlq: bool) -> Observed {
    let infra = TestInfrastructure::new(test, true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("dlq_metrics");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("{table}_land");
    let dlq_topic = format!("{table}_dlq");
    ensure_topic(&bootstrap, &topic).await;
    ensure_topic(&bootstrap, &dlq_topic).await;
    let producer = make_producer(&bootstrap);
    for id in 0..RECORDS {
        send(&producer, &topic, &format!(r#"{{"id":{id}}}"#)).await;
    }

    let mut config = kafka_loader(
        &bootstrap,
        &topic,
        &format!("dfe-{table}"),
        clickhouse.clone(),
        &table,
    );
    if kafka_dlq {
        config.routing.dlq.enabled = true;
        config.routing.dlq.mode = "kafka_only".to_string();
        config.routing.dlq.kafka_enabled = true;
        config.routing.dlq.file_enabled = false;
        config.routing.dlq.topic.clone_from(&dlq_topic);
        // An id no UInt64 holds: the server refuses it, and only the DLQ takes it.
        send(&producer, &topic, r#"{"id":"not a number"}"#).await;
    }
    let manager = MetricsManager::new("loader_it");
    let mut orchestrator = Orchestrator::with_metrics(config, Metrics::new(&manager));
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });

    let deadline = tokio::time::Instant::now() + BUDGET;
    while direct.query_count(&table, None).await.unwrap_or(0) < RECORDS {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the loader landed fewer than {RECORDS} rows within {BUDGET:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let topic_label = format!(r#"topic="{topic}""#);
    let deadline = tokio::time::Instant::now() + BUDGET;
    let (lag, rdkafka) = loop {
        let render = manager.render();
        let lag: Vec<String> = render
            .lines()
            .filter(|line| is_series(line, LAG_SERIES) && line.contains(&topic_label))
            .map(str::to_string)
            .collect();
        if !lag.is_empty() || tokio::time::Instant::now() >= deadline {
            let rdkafka = render
                .lines()
                .filter(|line| line.contains("rdkafka"))
                .map(str::to_string)
                .collect();
            break (lag, rdkafka);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let dead_letters = if kafka_dlq {
        let bootstrap = bootstrap.clone();
        tokio::task::spawn_blocking(move || dead_letters(&bootstrap, &dlq_topic))
            .await
            .expect("DLQ reader thread")
    } else {
        0
    };

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(60), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;
    Observed {
        lag,
        rdkafka,
        dead_letters,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_lag_is_published_with_the_kafka_dlq_on() {
    let observed = run_loader(test_name!(), true).await;
    eprintln!("Kafka DLQ on:\n{}", observed.lag.join("\n"));
    assert!(
        observed.dead_letters > 0,
        "the Kafka DLQ carried no dead letter, so its producer was never in play"
    );
    assert!(
        !observed.lag.is_empty(),
        "no {LAG_SERIES} series for the topic with the Kafka DLQ on; rdkafka lines:\n{}",
        observed.rdkafka.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_lag_is_published_with_the_dlq_off() {
    let observed = run_loader(test_name!(), false).await;
    eprintln!("DLQ off:\n{}", observed.lag.join("\n"));
    assert!(
        !observed.lag.is_empty(),
        "no {LAG_SERIES} series for the topic with the DLQ off; rdkafka lines:\n{}",
        observed.rdkafka.join("\n")
    );
}
