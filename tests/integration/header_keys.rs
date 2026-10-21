// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! A record carrying `_org_id` or `_source` more than once is dead-lettered
//! with its reason on every pipeline mode, and the records around it land.
//!
//! Runs the real orchestrator on the gRPC transport against a `ClickHouse`
//! container, once per pipeline mode.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::time::Duration;

use super::grpc_outage::{
    clickhouse_address, dead_letter_reasons, free_port, grpc_loader, push_ids, query_client,
    start_loader, wait_dead_lettered, wait_for_loader, wait_landed, with_file_dlq,
};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// How long a record has to land or reach the DLQ.
const BUDGET: Duration = Duration::from_secs(90);

/// `(id, payload, the key it repeats)` for each record the loader must refuse.
fn repeated(backslash: char) -> Vec<(u64, String, &'static str)> {
    vec![
        (
            90,
            r#"{"id":90,"_org_id":"a","_org_id":"b"}"#.to_string(),
            "_org_id",
        ),
        (
            91,
            format!(r#"{{"id":91,"_org_id":"a","{backslash}u005forg_id":"b"}}"#),
            "_org_id",
        ),
        (
            92,
            r#"{"id":92,"_source":"x","_source":"y"}"#.to_string(),
            "_source",
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_header_key_is_dead_lettered_on_every_pipeline_mode() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    for mode in ["json_primary", "legacy_flatten"] {
        let table = unique_table_name("header_keys");
        direct
            .execute(&format!(
                "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("create table");

        let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
        let listen_port = free_port();
        let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
        config.payload.pipeline_mode = mode.to_string();
        with_file_dlq(&mut config, dlq_dir.path());
        let (shutdown, loader, client) = start_loader(config, listen_port).await;
        wait_for_loader(&client, &clickhouse, &table).await;

        let until = tokio::time::Instant::now() + BUDGET;
        let refused = repeated('\\');
        for (_, body, _) in &refused {
            let (pushed, _) = push_ids(&client, 0..1, until, |_| body.clone()).await;
            assert_eq!(pushed.len(), 1, "{mode}: the listener refused {body}");
        }
        let (after, _) = push_ids(&client, 2..3, until, |id| format!(r#"{{"id":{id}}}"#)).await;

        let refused_ids: BTreeSet<u64> = refused.iter().map(|(id, _, _)| *id).collect();
        let dead_lettered = wait_dead_lettered(dlq_dir.path(), &refused_ids, BUDGET).await;
        let landed = wait_landed(&clickhouse, &table, &after, BUDGET).await;
        let reasons = dead_letter_reasons(dlq_dir.path());
        let still_running = !loader.is_finished();

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;

        assert!(still_running, "{mode}: the loader stopped");
        assert!(
            after.is_subset(&landed),
            "{mode}: the record after them never landed"
        );
        assert!(
            landed.is_disjoint(&refused_ids),
            "{mode}: a record naming a header key twice landed: {landed:?}"
        );
        assert!(
            refused_ids.is_subset(&dead_lettered),
            "{mode}: not every repeated-key record reached the DLQ: {dead_lettered:?}"
        );
        for (id, _, key) in &refused {
            assert_eq!(
                reasons.get(id).map(String::as_str),
                Some(dfe_loader::payload::header_keys::duplicate_header_reason(key).as_str()),
                "{mode}: record {id} must carry the reason naming {key}"
            );
        }
    }
}
