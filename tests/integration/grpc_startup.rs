// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A table `ClickHouse` does not have never holds back the loader.
//!
//! At startup the loader resolves the schema of every table its config names,
//! retrying a fetch that fails. None of that may stand in front of intake: the
//! listener accepts a record for a table that exists as soon as it is up,
//! whatever the config names that `ClickHouse` lacks. A record for a missing
//! default table waits for it without being resolved again on every receive.
//! Each test runs the real orchestrator on the gRPC transport against a
//! `ClickHouse` container.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dfe_loader::clickhouse::ClickHouseQueryClient;
use dfe_loader::config::Config;
use dfe_loader::metrics::Metrics;
use dfe_loader::pipeline::Orchestrator;
use scalo::metrics::MetricsManager;
use scalo::transport::{SendResult, TransportSender};

use super::grpc_outage::{
    clickhouse_address, counter_value, free_port, grpc_loader, push_ids, query_client, start,
    start_loader, wait_landed,
};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// The production `schema.pre_warm_retry_secs`.
const PRE_WARM_BUDGET_SECS: u64 = 60;

/// How long a record for a missing table is watched while it waits.
const WATCH: Duration = Duration::from_secs(5);

/// Queries naming a missing table allowed in [`WATCH`]: twice the three
/// three-query resolutions a 2 s re-request interval sends in it.
const MAX_QUERIES_WATCHED: u64 = 18;

/// Longest the first record may wait to be accepted: a sixth of the pre-warm
/// budget, so a listener held for the budget fails by a wide margin.
const FIRST_ACCEPT: Duration = Duration::from_secs(10);

/// How long a sender keeps retrying the first record, past the pre-warm budget
/// so a held listener is measured rather than given up on.
const SENDER_PATIENCE: Duration = Duration::from_secs(120);

/// Create `default.{table}` with one `id` column.
async fn create_id_table(direct: &ClickHouseQueryClient, table: &str) {
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
}

/// Start a loader on `config`, push one record with `payload` until it is
/// accepted, and return how long that took from the start and whether it was.
async fn time_to_first_accept(
    config: Config,
    listen_port: u16,
    payload: String,
) -> (Duration, bool) {
    let started = tokio::time::Instant::now();
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    let (accepted, refused) = push_ids(&client, 1..2, started + SENDER_PATIENCE, |_| {
        payload.clone()
    })
    .await;
    let elapsed = started.elapsed();
    eprintln!(
        "first record accepted={} after {elapsed:?}, told to retry {refused} times",
        !accepted.is_empty()
    );
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    (elapsed, !accepted.is_empty())
}

/// The observed case: the default table does not exist, and a record for a
/// table that does is accepted at once and lands.
#[tokio::test]
async fn a_missing_default_table_does_not_hold_the_listener() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let present = unique_table_name("startup_present");
    let absent = unique_table_name("startup_absent_default");
    create_id_table(&direct, &present).await;

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &absent);
    config.schema.pre_warm_retry_secs = PRE_WARM_BUDGET_SECS;
    let payload = format!(r#"{{"_source":"{present}","id":1}}"#);
    let (elapsed, accepted) = time_to_first_accept(config, listen_port, payload).await;
    let landed = wait_landed(
        &clickhouse,
        &present,
        &BTreeSet::from([1]),
        Duration::from_secs(30),
    )
    .await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{present}"))
        .await;

    assert!(accepted, "the first record was never accepted");
    assert!(
        elapsed < FIRST_ACCEPT,
        "the first record waited {elapsed:?} with the default table missing, past {FIRST_ACCEPT:?}"
    );
    assert!(landed.contains(&1), "the accepted record never landed");
}

/// A table a routing rule names that does not exist holds back no record for
/// the default table.
#[tokio::test]
async fn a_missing_rule_target_does_not_hold_the_listener() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let present = unique_table_name("startup_default");
    let absent = unique_table_name("startup_absent_target");
    create_id_table(&direct, &present).await;

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &present);
    config.schema.pre_warm_retry_secs = PRE_WARM_BUDGET_SECS;
    config
        .routing
        .source_to_table
        .insert("not_yet_provisioned".to_string(), absent);
    let (elapsed, accepted) =
        time_to_first_accept(config, listen_port, r#"{"id":1}"#.to_string()).await;
    let landed = wait_landed(
        &clickhouse,
        &present,
        &BTreeSet::from([1]),
        Duration::from_secs(30),
    )
    .await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{present}"))
        .await;

    assert!(accepted, "the first record was never accepted");
    assert!(
        elapsed < FIRST_ACCEPT,
        "the first record waited {elapsed:?} with a rule target missing, past {FIRST_ACCEPT:?}"
    );
    assert!(landed.contains(&1), "the accepted record never landed");
}

/// The number a one-row, one-column `sql` query returns over the HTTP
/// interface.
async fn query_count(clickhouse: &str, sql: &str) -> u64 {
    let body = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .query(&[("query", sql)])
        .send()
        .await
        .expect("query ClickHouse")
        .text()
        .await
        .expect("read the answer");
    body.trim()
        .parse()
        .map_err(|e| format!("{e}: {body}"))
        .expect("ClickHouse answered a count")
}

/// The queries `ClickHouse` has finished that name `table`, other than these
/// counts themselves.
async fn queries_naming(direct: &ClickHouseQueryClient, clickhouse: &str, table: &str) -> u64 {
    direct
        .execute("SYSTEM FLUSH LOGS")
        .await
        .expect("flush the query log");
    query_count(
        clickhouse,
        &format!(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' \
             AND query LIKE '%{table}%' AND query NOT LIKE '%system.query_log%'"
        ),
    )
    .await
}

/// A record routed to a default table that does not exist waits for it in the
/// pending-schema buffer, resolved again on the re-request interval and not on
/// every receive, and lands once the table exists.
#[tokio::test]
async fn a_record_for_a_missing_default_table_waits_for_it_without_a_busy_loop() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let absent = unique_table_name("startup_late_default");

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &absent);
    // One pre-warm round, so what is watched is the running loader, not its start.
    config.schema.pre_warm_retry_secs = 0;
    let manager = MetricsManager::new("loader_it");
    let metrics = Metrics::new(&manager);
    let (shutdown, loader, client) =
        start(Orchestrator::with_metrics(config, metrics), listen_port).await;
    let client = Arc::new(client);
    let pushed = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.send("", Bytes::from(r#"{"id":7}"#)).await })
    };

    // Long enough for the record to reach the pending-schema buffer.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let fallbacks_before = counter_value(&manager, "unknown_table_fallback_total", "");
    let queries_before = queries_naming(&direct, &clickhouse, &absent).await;
    tokio::time::sleep(WATCH).await;
    let fallbacks = counter_value(&manager, "unknown_table_fallback_total", "") - fallbacks_before;
    let queries = queries_naming(&direct, &clickhouse, &absent)
        .await
        .saturating_sub(queries_before);
    eprintln!("over {WATCH:?}: {fallbacks} fallbacks, {queries} queries naming the table");

    create_id_table(&direct, &absent).await;
    let landed = wait_landed(
        &clickhouse,
        &absent,
        &BTreeSet::from([7]),
        Duration::from_secs(60),
    )
    .await;
    let answer = tokio::time::timeout(Duration::from_secs(30), pushed).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{absent}"))
        .await;

    assert!(
        fallbacks.abs() < f64::EPSILON,
        "a record for the default table was diverted {fallbacks} times in {WATCH:?}, \
         from the default table to itself"
    );
    assert!(
        queries <= MAX_QUERIES_WATCHED,
        "{queries} queries named the missing table in {WATCH:?}, past {MAX_QUERIES_WATCHED}"
    );
    assert!(
        landed.contains(&7),
        "the record never landed once its table existed"
    );
    assert!(
        matches!(answer, Ok(Ok(SendResult::Ok | SendResult::Backpressured))),
        "the Push got no answer a sender retries or stops on: {answer:?}"
    );
}

/// The control: with every table the config names present, the first record
/// is accepted inside the same budget the tests above are held to.
#[tokio::test]
async fn with_every_table_present_the_first_record_is_accepted_at_once() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let present = unique_table_name("startup_all_present");
    create_id_table(&direct, &present).await;

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &present);
    config.schema.pre_warm_retry_secs = PRE_WARM_BUDGET_SECS;
    let (elapsed, accepted) =
        time_to_first_accept(config, listen_port, r#"{"id":1}"#.to_string()).await;
    let landed = wait_landed(
        &clickhouse,
        &present,
        &BTreeSet::from([1]),
        Duration::from_secs(30),
    )
    .await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{present}"))
        .await;

    assert!(accepted, "the first record was never accepted");
    assert!(
        elapsed < FIRST_ACCEPT,
        "the first record waited {elapsed:?} with every table present, past {FIRST_ACCEPT:?}"
    );
    assert!(landed.contains(&1), "the accepted record never landed");
}
