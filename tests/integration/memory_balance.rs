// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Every byte the loader reserves on the memory guard for a record comes back
//! once the record's rows land.
//!
//! The guard is what pauses intake under memory pressure, so a reservation
//! that is never released reads as memory in use forever and eventually holds
//! intake paused for good. Each test runs the real orchestrator on the gRPC
//! transport against a `ClickHouse` container under one capture mode, pushes
//! records until they land, and reads the guard's reservations back to zero.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use dfe_loader::config::CaptureMode;
use dfe_loader::pipeline::Orchestrator;

use super::grpc_outage::{
    clickhouse_address, free_port, grpc_loader, push_ids, query_client, start, wait_landed,
};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// Records pushed under each capture mode.
const RECORDS: u64 = 20;

/// How long every record has to land.
const BUDGET: Duration = Duration::from_secs(90);

/// How long the reservations have to drain once the records have landed.
const DRAIN: Duration = Duration::from_secs(15);

/// Push [`RECORDS`] records through a loader under `mode` on the pipeline
/// `pipeline_mode` names, and return the bytes still reserved once they land.
async fn reserved_after_landing(
    clickhouse: &str,
    mode: CaptureMode,
    pipeline_mode: &str,
) -> (BTreeSet<u64>, u64) {
    let direct = query_client(clickhouse);
    let table = unique_table_name("memory_balance");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.to_string(), &table);
    config.metadata.capture_mode = mode;
    config.payload.pipeline_mode = pipeline_mode.to_string();
    let orchestrator = Orchestrator::new(config);
    let guard = Arc::clone(orchestrator.memory_guard());
    let (shutdown, loader, client) = start(orchestrator, listen_port).await;

    let until = tokio::time::Instant::now() + BUDGET;
    let (accepted, _) = push_ids(&client, 0..RECORDS, until, |id| {
        format!(r#"{{"id":{id},"message":"a record of some length to reserve"}}"#)
    })
    .await;
    let landed = wait_landed(clickhouse, &table, &accepted, BUDGET).await;

    // The release follows the insert in the same flush, so give it a moment.
    let deadline = tokio::time::Instant::now() + DRAIN;
    let mut reserved = guard.reserved_bytes();
    while reserved != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        reserved = guard.reserved_bytes();
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;
    assert_eq!(
        accepted.len() as u64,
        RECORDS,
        "the listener refused records under {mode:?}/{pipeline_mode}"
    );
    (landed, reserved)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_reserved_byte_is_released_once_its_rows_land_in_every_capture_mode() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let every_id: BTreeSet<u64> = (0..RECORDS).collect();

    let mut leaked = Vec::new();
    for (mode, pipeline_mode) in [
        (CaptureMode::Full, "json_primary"),
        (CaptureMode::JsonOnly, "json_primary"),
        (CaptureMode::RawOnly, "json_primary"),
        (CaptureMode::ExtractedOnly, "json_primary"),
        (CaptureMode::Full, "legacy_flatten"),
        (CaptureMode::RawOnly, "legacy_flatten"),
    ] {
        let (landed, reserved) = reserved_after_landing(&clickhouse, mode, pipeline_mode).await;
        eprintln!(
            "{mode:?}/{pipeline_mode}: {} of {RECORDS} records landed, {reserved} bytes still reserved",
            landed.len()
        );
        assert!(
            every_id.is_subset(&landed),
            "records never landed under {mode:?}/{pipeline_mode}: {landed:?}"
        );
        if reserved != 0 {
            leaked.push(format!("{mode:?}/{pipeline_mode}: {reserved} bytes"));
        }
    }

    assert!(
        leaked.is_empty(),
        "the memory guard kept reservations for rows that had landed: {leaked:?}"
    );
}
