// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! What `_raw` holds for JSON records under `full` capture, through the real
//! orchestrator on the gRPC transport into the landing table's header columns.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeMap;
use std::time::Duration;

use scalo::transport::{SendResult, TransportSender};

use super::grpc_outage::{clickhouse_address, free_port, grpc_loader, query_client, start_loader};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// The landing table dfe-engine builds from the timeseries header profile.
async fn create_landing_table(clickhouse: &str, table: &str) {
    let ddl = format!(
        "CREATE TABLE default.`{table}` (
            `_timestamp_load` DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '@generated: now64(3) - Insertion timestamp (ms precision)',
            `_timestamp` DateTime64(3, 'UTC') COMMENT '@source: timestamp | now() - Event timestamp from source data',
            `_timestamp_received` DateTime64(3, 'UTC') COMMENT '@source: first(timestamp_received/received_at) - When the event was first received by the receiver',
            `_uuid` UUID DEFAULT generateUUIDv7() COMMENT '@generated: generateUUIDv7() - Time-ordered unique event identifier',
            `_org_id` LowCardinality(String) COMMENT '@source: org_id - Tenant/organisation identifier',
            `_source` LowCardinality(Nullable(String)) COMMENT '@source: first(_source) | topic_name - Data source label',
            `_raw` Nullable(String) COMMENT '@captured: raw_payload - Original event payload as text (full-text indexed)',
            `_json` JSON(max_dynamic_paths = 2048) COMMENT '@captured: raw_payload as JSON - Original event payload as structured JSON',
            `_tags` JSON COMMENT '@source: first(tags/_tags/meta/metadata.tags) - Event metadata tags'
        ) ENGINE = MergeTree() ORDER BY (_timestamp_load, _timestamp, _org_id)"
    );
    query_client(clickhouse)
        .execute(&ddl)
        .await
        .expect("create the landing table");
}

/// `event` -> `_raw` for every row of `table`, `None` where `_raw` is NULL.
async fn raw_by_event(clickhouse: &str, table: &str) -> BTreeMap<String, Option<String>> {
    let sql = format!(
        "SELECT toString(_json.event) AS event, _raw AS raw FROM default.`{table}` FORMAT JSONEachRow"
    );
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .basic_auth(
            "default",
            Some(crate::common::containers::clickhouse_password()),
        )
        .query(&[("query", sql.as_str())])
        .send()
        .await
    else {
        return BTreeMap::new();
    };
    let body = response.text().await.unwrap_or_default();
    body.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .map(|row| {
            let event = row["event"].as_str().unwrap_or_default().to_string();
            let raw = row["raw"].as_str().map(str::to_string);
            (event, raw)
        })
        .collect()
}

const OWN_RAW: &str = "towel-check raw line: this frood really knows where his towel is";
const LOGORIGINAL: &str = "Deep Thought: the Answer is 42";

/// Load one record of each shape into a fresh landing table and return what
/// `_raw` holds per event. `None` keeps the shipped `raw_source_fields`.
async fn load_records(
    clickhouse: &str,
    raw_source_fields: Option<Vec<String>>,
) -> BTreeMap<String, Option<String>> {
    let table = unique_table_name("raw_capture");
    create_landing_table(clickhouse, &table).await;

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.to_string(), &table);
    if let Some(fields) = raw_source_fields {
        config.metadata.raw_source_fields = fields;
    }
    let (shutdown, loader, client) = start_loader(config, listen_port).await;

    let records = [
        format!(
            r#"{{"org_id":"zaphod","logoriginal":"{LOGORIGINAL}","event":"ultimate-answer","answer":42}}"#
        ),
        format!(
            r#"{{"org_id":"hoopy-frood","logoriginal":"towel-check: knows where his towel is","_raw":"{OWN_RAW}","event":"towel-check"}}"#
        ),
        r#"{"org_id":"magrathea","event":"planet-build","restaurant":"Milliways"}"#.to_string(),
    ];
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    for record in &records {
        loop {
            match client.send("", bytes::Bytes::from(record.clone())).await {
                SendResult::Ok => break,
                other if tokio::time::Instant::now() < until => {
                    tracing::debug!(?other, "push refused, retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                other => panic!("the listener never accepted {record}: {other:?}"),
            }
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let landed = loop {
        let landed = raw_by_event(clickhouse, &table).await;
        if landed.len() >= records.len() || tokio::time::Instant::now() >= deadline {
            break landed;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = query_client(clickhouse)
        .execute(&format!("DROP TABLE IF EXISTS default.`{table}`"))
        .await;
    landed
}

/// By default a JSON record keeps only its own `_raw` field, as sent, and any
/// other JSON record lands with `_raw` NULL, `logoriginal` included. Naming
/// `logoriginal` in `raw_source_fields` promotes it.
#[tokio::test]
async fn a_json_record_fills_raw_from_its_own_raw_field_unless_configured() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;

    let cases = [
        (None, None),
        (
            Some(vec!["logoriginal".to_string()]),
            Some(LOGORIGINAL.to_string()),
        ),
    ];
    for (raw_source_fields, promoted) in cases {
        let landed = load_records(&clickhouse, raw_source_fields.clone()).await;
        let want = BTreeMap::from([
            ("planet-build".to_string(), None),
            ("towel-check".to_string(), Some(OWN_RAW.to_string())),
            ("ultimate-answer".to_string(), promoted),
        ]);
        assert_eq!(
            landed, want,
            "_raw by event with raw_source_fields {raw_source_fields:?}"
        );
    }
}
