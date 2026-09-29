// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! No row lands twice when a rebalance moves a partition off a loader that
//! still buffers it.
//!
//! A loader roll starts the new pod while the old one still holds buffered
//! rows. The rebalance hands some of the old pod's partitions to the new one,
//! which reads them again from the committed offset and writes them. Two real
//! loaders in one consumer group, against Kafka and `ClickHouse` containers,
//! run that sequence: the first buffers every record, the second joins and
//! writes what it takes over, and the first then flushes. Every record must be
//! in `ClickHouse` exactly once, `transport_revoke_discarded_total` must
//! count what the first loader dropped, and neither loader may end with bytes
//! still reserved against its memory guard.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::sync::Arc;
use std::time::Duration;

use dfe_loader::metrics::Metrics;
use dfe_loader::pipeline::Orchestrator;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::metrics::MetricsManager;

use super::grpc_outage::{clickhouse_address, counter_value, query_client};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader};
use super::kafka_transport_e2e::make_producer;
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// Partitions of the topic: enough that a second member takes some.
const PARTITIONS: i32 = 4;

/// Records written to each partition before either loader starts.
const PER_PARTITION: u64 = 10;

/// Every record written.
const RECORDS: u64 = PER_PARTITION * PARTITIONS as u64;

/// How long a loader has to receive or write what the test waits for.
const BUDGET: Duration = Duration::from_secs(90);

/// How long `ClickHouse`'s row count must hold still before it counts as
/// settled: several of the second loader's one-second flush cycles.
const SETTLE: Duration = Duration::from_secs(5);

/// Create `topic` with [`PARTITIONS`] partitions.
async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, PARTITIONS, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(30))),
        )
        .await
        .expect("create topic");
    for result in results {
        result.unwrap_or_else(|(name, e)| panic!("create topic {name}: {e}"));
    }
}

/// Write [`PER_PARTITION`] records to each partition: record `id` sits on
/// partition `id / PER_PARTITION`.
async fn fill(producer: &FutureProducer, topic: &str) {
    for id in 0..RECORDS {
        let partition = i32::try_from(id / PER_PARTITION).expect("fits");
        let payload = format!(r#"{{"id":{id}}}"#);
        producer
            .send(
                FutureRecord::<(), str>::to(topic)
                    .partition(partition)
                    .payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("deliver record {id}: {e}"));
    }
}

/// Rows in `default.{table}`, and how many distinct ids they hold.
async fn row_counts(clickhouse: &str, table: &str) -> (u64, u64) {
    let sql = format!("SELECT count(), uniqExact(id) FROM default.{table} FORMAT TabSeparated");
    let body = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .query(&[("query", sql)])
        .send()
        .await
        .expect("query ClickHouse")
        .text()
        .await
        .expect("read the answer");
    let mut fields = body.split_whitespace().map(|n| n.parse().expect("a count"));
    (
        fields.next().expect("count()"),
        fields.next().expect("uniqExact(id)"),
    )
}

/// Wait until the row count holds still for [`SETTLE`] with at least one row,
/// returning it.
async fn settled_rows(clickhouse: &str, table: &str) -> u64 {
    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut last = (0, tokio::time::Instant::now());
    loop {
        let (rows, _) = row_counts(clickhouse, table).await;
        let now = tokio::time::Instant::now();
        if rows != last.0 {
            last = (rows, now);
        } else if rows > 0 && now - last.1 >= SETTLE {
            return rows;
        }
        assert!(
            now < deadline,
            "no settled write within {BUDGET:?}: {rows} rows"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_moved_mid_buffer_lands_each_row_once() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("kafka_rebalance");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("rebalance-{table}");
    let group = format!("dfe-{table}");
    create_topic(&bootstrap, &topic).await;
    fill(&make_producer(&bootstrap), &topic).await;

    // The first loader buffers everything it reads: no row count or age in the
    // test reaches a flush threshold, so only its shutdown writes.
    let manager = MetricsManager::new("loader_it");
    let mut first = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &table);
    first.buffer.flush_rows = 100_000;
    first.buffer.flush_age_secs = 3_600;
    let mut first = Orchestrator::with_metrics(first, Metrics::new(&manager));
    let first_guard = Arc::clone(first.memory_guard());
    let stop_first = first.shutdown_token();
    let first = tokio::spawn(async move {
        let _ = first.run().await;
    });
    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut buffered = 0.0;
    while buffered < RECORDS as f64 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first loader received {buffered} of {RECORDS} records within {BUDGET:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        buffered = counter_value(&manager, "messages_received_total", "");
    }
    let written_before_join = row_counts(&clickhouse, &table).await.0;

    // The second loader joins the group, takes some partitions over, and writes
    // what it reads of them at once.
    let mut second = Orchestrator::new(kafka_loader(
        &bootstrap,
        &topic,
        &group,
        clickhouse.clone(),
        &table,
    ));
    let second_guard = Arc::clone(second.memory_guard());
    let stop_second = second.shutdown_token();
    let second = tokio::spawn(async move {
        let _ = second.run().await;
    });
    let taken_over = settled_rows(&clickhouse, &table).await;

    // The first loader's shutdown flushes what it buffered.
    stop_first.cancel();
    let first_stopped = tokio::time::timeout(Duration::from_secs(60), first).await;
    // Past the first loader, the second takes its partitions back from their
    // committed offsets; anything it read again would land here as well.
    tokio::time::sleep(SETTLE * 2).await;
    stop_second.cancel();
    let second_stopped = tokio::time::timeout(Duration::from_secs(60), second).await;
    let (rows, ids) = row_counts(&clickhouse, &table).await;
    let discarded = counter_value(
        &manager,
        "transport_revoke_discarded_total",
        r#"stage="buffer""#,
    ) as u64;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    eprintln!(
        "the second loader wrote {taken_over} rows it took over; after both stopped \
         {table} holds {rows} rows of {ids} distinct ids, and the first loader discarded \
         {discarded} records"
    );
    assert_eq!(
        written_before_join, 0,
        "the first loader wrote before the second joined, so nothing was moved mid-buffer"
    );
    assert!(
        first_stopped.is_ok(),
        "the first loader did not stop within 60s"
    );
    assert!(
        second_stopped.is_ok(),
        "the second loader did not stop within 60s"
    );
    assert!(
        taken_over > 0 && taken_over < RECORDS,
        "the second loader wrote {taken_over} of {RECORDS} rows, so no partition moved mid-buffer"
    );
    assert_eq!(
        ids, RECORDS,
        "a record was lost: {ids} of {RECORDS} ids landed"
    );
    assert_eq!(
        rows,
        RECORDS,
        "{} rows landed twice: the first loader wrote the rows of partitions the second had \
         taken over and written",
        rows.saturating_sub(RECORDS)
    );
    assert_eq!(
        discarded, taken_over,
        "transport_revoke_discarded_total{{stage=\"buffer\"}} must count every record the \
         first loader dropped for the second to write"
    );
    assert_eq!(
        first_guard.reserved_bytes(),
        0,
        "the first loader's discarded and flushed rows left bytes reserved"
    );
    assert_eq!(
        second_guard.reserved_bytes(),
        0,
        "the second loader's written rows left bytes reserved"
    );
}
