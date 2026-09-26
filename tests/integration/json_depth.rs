// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A deeply nested record is dead-lettered and the loader keeps loading.
//!
//! Runs the real orchestrator on the gRPC transport against a `ClickHouse`
//! container, on one current-thread runtime inside a thread with a Tokio
//! worker's 2 MiB stack, so the intake, pre-route and the processor all run
//! on the stack production gives them. A record sonic-rs had to recurse
//! through would abort the test binary here, and on Kafka the same record
//! would come back after every restart.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use super::grpc_outage::{
    clickhouse_address, decode_base64, free_port, grpc_loader, push_ids, query_client,
    start_loader, wait_for_loader, wait_landed, with_file_dlq,
};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// The stack a Tokio worker thread gets by default.
const WORKER_STACK: usize = 2 * 1024 * 1024;

/// The reason every depth refusal carries into the DLQ.
const TOO_DEEP: &str = "payload nesting exceeds the maximum parse depth of 64";

fn nested_array(depth: usize) -> String {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
}

fn nested_object(depth: usize) -> String {
    format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth))
}

/// Every payload the file DLQ under `dir` holds with the depth reason.
fn dead_lettered_too_deep(dir: &Path) -> Vec<Vec<u8>> {
    fn walk(dir: &Path, out: &mut Vec<Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
                continue;
            }
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in body.lines() {
                let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                if entry["reason"].as_str() == Some(TOO_DEEP)
                    && let Some(payload) = entry["payload"].as_str().and_then(decode_base64)
                {
                    out.push(payload);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out
}

#[test]
fn a_deeply_nested_record_is_dead_lettered_and_the_loader_keeps_loading() {
    let test = test_name!();
    std::thread::Builder::new()
        .name("json-depth".to_string())
        .stack_size(WORKER_STACK)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(run(test));
        })
        .expect("spawn the worker-sized thread")
        .join()
        .expect("the loader thread must return rather than panic");
}

async fn run(test: &'static str) {
    let infra = TestInfrastructure::new(test, true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("json_depth");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    let deep = vec![
        nested_object(20_000),
        nested_array(100_000),
        // A deep sibling ahead of the routing field, which pre-route walks past.
        format!(r#"{{"sibling":{},"id":90}}"#, nested_array(20_000)),
        format!(r#"[{{"id":91}},{}]"#, nested_object(20_000)),
    ];
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let (before, _) = push_ids(&client, 1..2, until, |id| format!(r#"{{"id":{id}}}"#)).await;
    for body in &deep {
        let (pushed, _) = push_ids(&client, 0..1, until, |_| body.clone()).await;
        assert_eq!(pushed.len(), 1, "the listener refused a deep record");
    }
    let (after, _) = push_ids(&client, 2..3, until, |id| format!(r#"{{"id":{id}}}"#)).await;

    let good: BTreeSet<u64> = before.union(&after).copied().collect();
    let landed = wait_landed(&clickhouse, &table, &good, Duration::from_secs(120)).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut dead_lettered = dead_lettered_too_deep(dlq_dir.path());
    while dead_lettered.len() < deep.len() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        dead_lettered = dead_lettered_too_deep(dlq_dir.path());
    }
    let still_running = !loader.is_finished();

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(still_running, "the loader stopped after a deep record");
    assert!(
        good.is_subset(&landed),
        "records around the deep ones never landed: want {good:?}, landed {landed:?}"
    );
    assert!(
        !landed.contains(&90) && !landed.contains(&91),
        "a record travelling with a deep one landed: {landed:?}"
    );
    let mut want: Vec<Vec<u8>> = deep.into_iter().map(String::into_bytes).collect();
    want.sort();
    dead_lettered.sort();
    assert_eq!(
        dead_lettered.len(),
        want.len(),
        "every deep record reaches the DLQ with the depth reason"
    );
    assert!(
        dead_lettered == want,
        "each deep record reaches the DLQ as it arrived"
    );
}
