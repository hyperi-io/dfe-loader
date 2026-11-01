// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The tenant comes from `_org_id` alone, and a record with no org or no source
//! lands in the deployment's default org and table, never in no org or the DLQ.
//!
//! Runs the real orchestrator on the Kafka transport against Kafka and
//! `ClickHouse` containers, with the DLQ on as it is by default.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::grpc_outage::{
    clickhouse_address, dead_lettered_ids, query_client, wait_landed, with_file_dlq,
};
use super::kafka_offset_floor::{kafka_bootstrap, kafka_loader, produce, start_loader};
use super::kafka_transport_e2e::{ensure_topic, make_producer};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// How long a record has to land.
const BUDGET: Duration = Duration::from_secs(90);

/// `id` -> `_org_id` for every row `default.{table}` holds.
pub(super) async fn landed_orgs(clickhouse: &str, table: &str) -> BTreeMap<u64, String> {
    let sql = format!("SELECT id, _org_id FROM default.{table} FORMAT TabSeparated");
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
        .filter_map(|line| {
            let (id, org) = line.split_once('\t')?;
            Some((id.parse().ok()?, org.to_string()))
        })
        .collect()
}

/// Create `default.{table}` with the header profile's `_org_id` and `_source`
/// columns, `_org_id` still carrying the directive that named the sender's field.
pub(super) async fn create_header_table(
    direct: &dfe_loader::clickhouse::ClickHouseQueryClient,
    table: &str,
) {
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (\
               id UInt64, \
               `_org_id` LowCardinality(String) \
                 COMMENT '@source: org_id - Tenant/organisation identifier', \
               `_source` LowCardinality(Nullable(String)) \
                 COMMENT '@source: first(_source) | topic_name - Data source label'\
             ) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unstamped_record_lands_in_the_default_org_and_table() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("tenancy_landing");
    create_header_table(&direct, &landing).await;
    let topic = format!("tenancy-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);

    // (id, payload, the tenant the row must carry)
    let records = [
        (1, r#"{"id":1}"#, "default"),
        (2, r#"{"id":2,"_org_id":null}"#, "default"),
        (3, r#"{"id":3,"_org_id":""}"#, "default"),
        (4, r#"{"id":4,"_org_id":"acme"}"#, "acme"),
        (5, r#"{"id":5,"org_id":"someone-else"}"#, "default"),
        (6, r#"{"id":6,"_source":""}"#, "default"),
        (7, r#"{"id":7,"_source":null,"_org_id":"acme"}"#, "acme"),
    ];
    for (_, payload, _) in &records {
        produce(&producer, &topic, payload).await;
    }
    let want: BTreeSet<u64> = records.iter().map(|(id, _, _)| *id).collect();
    let landed = wait_landed(&clickhouse, &landing, &want, BUDGET).await;
    let orgs = landed_orgs(&clickhouse, &landing).await;
    let dead_lettered = dead_lettered_ids(dlq_dir.path());

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{landing}"))
        .await;

    assert_eq!(
        landed, want,
        "every record lands in the default table, an empty _source included"
    );
    assert!(
        dead_lettered.is_disjoint(&want),
        "a record with no org or no source reached the DLQ: {dead_lettered:?}"
    );
    let expected: BTreeMap<u64, String> = records
        .iter()
        .map(|(id, _, org)| (*id, (*org).to_string()))
        .collect();
    assert_eq!(
        orgs, expected,
        "the tenant is _org_id, else the default org, never the sender's org_id"
    );
}
