// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! No record the gRPC listener accepts is lost to a `ClickHouse` outage.
//!
//! The direct path has no broker: a Push is answered once its record is queued,
//! so the loader holds the only copy. The test runs the real orchestrator on the
//! gRPC transport against a `ClickHouse` container reached through an in-process
//! TCP proxy, cuts the proxy mid-stream, restores it, and checks every accepted
//! record landed.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dfe_loader::clickhouse::ClickHouseQueryClient;
use dfe_loader::clickhouse::config::{ClickHouseConfig, Transport};
use dfe_loader::config::Config;
use dfe_loader::pipeline::Orchestrator;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{SendResult, TransportSender};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// Records pushed before, during and after the outage.
const BEFORE: u64 = 20;
const DURING: u64 = 200;
const AFTER: u64 = 20;

/// Long enough for the inserter to exhaust its own retries on a batch.
const OUTAGE: Duration = Duration::from_secs(20);

/// How long every accepted record has to land once `ClickHouse` is back.
const LANDING_BUDGET: Duration = Duration::from_secs(120);

/// A loopback TCP proxy that can refuse and cut every connection on demand.
struct OutageProxy {
    port: u16,
    up: Arc<AtomicBool>,
    cut: watch::Sender<u64>,
}

impl OutageProxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let up = Arc::new(AtomicBool::new(true));
        let (cut, _) = watch::channel(0_u64);

        let accept_up = Arc::clone(&up);
        let accept_cut = cut.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                if !accept_up.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let upstream = upstream.clone();
                let mut cut_rx = accept_cut.subscribe();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let (mut client, mut server) = (client, server);
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = cut_rx.changed() => {}
                    }
                });
            }
        });

        Self { port, up, cut }
    }

    /// Refuse new connections and sever every open one.
    fn down(&self) {
        self.up.store(false, Ordering::SeqCst);
        self.cut.send_modify(|n| *n += 1);
    }

    fn up(&self) {
        self.up.store(true, Ordering::SeqCst);
    }
}

fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Push each id until the listener accepts it or `until` passes, returning the
/// ids accepted and how many pushes the listener refused as backpressure.
async fn push_ids(
    client: &GrpcTransport,
    ids: std::ops::Range<u64>,
    until: tokio::time::Instant,
) -> (BTreeSet<u64>, u64) {
    let mut accepted = BTreeSet::new();
    let mut refused = 0_u64;
    for id in ids {
        let payload = bytes::Bytes::from(format!(r#"{{"id":{id}}}"#));
        loop {
            match client.send("", payload.clone()).await {
                SendResult::Ok => {
                    accepted.insert(id);
                    break;
                }
                SendResult::Backpressured if tokio::time::Instant::now() < until => {
                    refused += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                _ => break,
            }
        }
    }
    (accepted, refused)
}

/// The ids `ClickHouse` holds for `table`, read over its HTTP interface.
async fn landed_ids(http_port: u16, table: &str) -> BTreeSet<u64> {
    let sql = format!("SELECT DISTINCT id FROM default.{table} FORMAT TabSeparated");
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{http_port}/"))
        .query(&[("query", sql)])
        .send()
        .await
    else {
        return BTreeSet::new();
    };
    let body = response.text().await.unwrap_or_default();
    body.lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

/// Wait until every id in `want` has landed, or the budget runs out.
async fn wait_landed(
    http_port: u16,
    table: &str,
    want: &BTreeSet<u64>,
    budget: Duration,
) -> BTreeSet<u64> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let landed = landed_ids(http_port, table).await;
        if want.is_subset(&landed) || tokio::time::Instant::now() >= deadline {
            return landed;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn no_accepted_record_is_lost_to_a_clickhouse_outage() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let container = infra.clickhouse.as_ref().expect("ClickHouse container");
    let http_port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("HTTP port mapping");

    // Queries go straight to the container; the loader goes through the proxy.
    let direct = ClickHouseQueryClient::new(&ClickHouseConfig {
        hosts: vec![format!("127.0.0.1:{http_port}")],
        transport: Transport::Http,
        database: "default".to_string(),
        username: "default".to_string(),
        password: String::new(),
        tls: false,
        ..Default::default()
    })
    .expect("direct query client");
    let proxy = OutageProxy::start(format!("127.0.0.1:{http_port}")).await;

    let table = unique_table_name("grpc_outage");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let listen_port = random_port();
    let mut config = Config::default();
    config.transport = "grpc".to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{listen_port}"));
    // A small queue makes a paused loader refuse Push within a few records.
    config.grpc.recv_buffer_size = 64;
    config.clickhouse.hosts = vec![format!("127.0.0.1:{}", proxy.port)];
    config.clickhouse.database = "default".to_string();
    config.clickhouse.protocol = "http".to_string();
    config.routing.default_db = "default".to_string();
    config.routing.default_table = table.clone();
    config.routing.dlq.enabled = false;
    config.buffer.flush_rows = 10;
    config.buffer.flush_age_secs = 1;
    config.schema.cache_ttl_secs = 3600;
    config.schema.pre_warm_retry_secs = 10;

    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move { orchestrator.run().await });

    let client = GrpcTransport::new(&GrpcConfig::client(&format!(
        "http://127.0.0.1:{listen_port}"
    )))
    .await
    .expect("gRPC client");

    // Before: ClickHouse up, every record lands.
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (before, _) = push_ids(&client, 0..BEFORE, settle).await;
    assert_eq!(
        before.len() as u64,
        BEFORE,
        "the listener refused records before the outage"
    );
    let landed = wait_landed(http_port, &table, &before, Duration::from_secs(60)).await;
    assert!(
        before.is_subset(&landed),
        "records never landed before the outage: {landed:?}"
    );

    // During: ClickHouse unreachable for longer than one insert's retries.
    proxy.down();
    let outage_end = tokio::time::Instant::now() + OUTAGE;
    let (during, refused) = push_ids(&client, BEFORE..BEFORE + DURING, outage_end).await;
    tokio::time::sleep_until(outage_end).await;

    // After: ClickHouse back.
    proxy.up();
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (after, _) = push_ids(&client, BEFORE + DURING..BEFORE + DURING + AFTER, settle).await;

    let accepted: BTreeSet<u64> = before
        .iter()
        .chain(&during)
        .chain(&after)
        .copied()
        .collect();
    let landed = wait_landed(http_port, &table, &accepted, LANDING_BUDGET).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    let lost: Vec<u64> = accepted.difference(&landed).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} accepted records never landed (accepted before {}, during {}, after {}): {lost:?}",
        lost.len(),
        accepted.len(),
        before.len(),
        during.len(),
        after.len()
    );
    assert_eq!(
        after.len() as u64,
        AFTER,
        "the listener still refused records after recovery"
    );
    // A paused loader takes at most one receive batch plus a full listener
    // queue before refusing, so it cannot have taken every record offered.
    assert!(
        refused > 0 && (during.len() as u64) < DURING,
        "intake never paused: the listener took {} of {DURING} records while ClickHouse was down \
         and refused {refused} pushes",
        during.len()
    );
}
