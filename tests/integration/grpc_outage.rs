// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! No record the gRPC listener accepts is lost while the loader runs.
//!
//! The direct path has no broker: a Push is answered once its record is queued,
//! so the loader holds the only copy. Each test runs the real orchestrator on the
//! gRPC transport against a `ClickHouse` container, breaks one thing a record
//! needs -- `ClickHouse` itself, the DLQ that takes what `ClickHouse` rejects, or
//! the schema fetch a new table waits on -- restores it, and checks every
//! accepted record reached `ClickHouse` or the DLQ.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dfe_loader::clickhouse::config::{ClickHouseConfig, Transport};
use dfe_loader::clickhouse::{ClickHouseQueryClient, InsertFormat};
use dfe_loader::config::Config;
use dfe_loader::pipeline::Orchestrator;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{SendResult, TransportSender};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// Records pushed before, during and after the outage.
const BEFORE: u64 = 20;
const DURING: u64 = 200;
const AFTER: u64 = 20;

/// Long enough for the inserter to exhaust its own retries on a batch.
const OUTAGE: Duration = Duration::from_secs(20);

/// How long every accepted record has to land once the fault is cleared.
const LANDING_BUDGET: Duration = Duration::from_secs(120);

/// Ids at or above this break the table's CHECK constraint, so `ClickHouse`
/// rejects them for good and only the DLQ can take them.
const REJECTED_FROM: u64 = 1_000_000;

/// Rows `ClickHouse` rejects, pushed while the DLQ refuses every write.
const REJECTED: u64 = 30;

/// How long the DLQ refuses every write.
const DLQ_DOWN: Duration = Duration::from_secs(10);

/// `SIGXFSZ` on Linux: raised on a write past `RLIMIT_FSIZE`, fatal unless handled.
pub(super) const SIGXFSZ: i32 = 25;

/// Records pushed that name an empty table, which routing sends to the DLQ.
const UNROUTABLE: u64 = 30;

/// Pending-schema caps one Push overflows on its own.
const PENDING_PER_TABLE: usize = 20;
const PENDING_TOTAL: usize = 40;

/// Records per Push: more than a table's pending-schema cap, so one receive
/// batch runs past it.
const PENDING_PER_PUSH: usize = 25;

/// Records pushed for a table whose schema cannot be fetched: more Pushes than
/// the listener's queue holds.
const PENDING_PUSHED: u64 = 2_500;

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

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
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

/// A record the loader routes to its default table.
fn id_payload(id: u64) -> String {
    format!(r#"{{"id":{id}}}"#)
}

/// Push each id until the listener accepts it or `until` passes, returning the
/// ids accepted and how many pushes the listener refused as backpressure.
async fn push_ids(
    client: &GrpcTransport,
    ids: std::ops::Range<u64>,
    until: tokio::time::Instant,
    payload: impl Fn(u64) -> String,
) -> (BTreeSet<u64>, u64) {
    let mut accepted = BTreeSet::new();
    let mut refused = 0_u64;
    for id in ids {
        let body = bytes::Bytes::from(payload(id));
        loop {
            match client.send("", body.clone()).await {
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

/// Push `ids` as JSON arrays of `per_push` records, retrying a Push until the
/// listener accepts it or `until` passes. Returns the ids accepted and how many
/// Pushes the listener refused as backpressure.
async fn push_id_arrays(
    client: &GrpcTransport,
    ids: std::ops::Range<u64>,
    per_push: usize,
    until: tokio::time::Instant,
    record: impl Fn(u64) -> String,
) -> (BTreeSet<u64>, u64) {
    let mut accepted = BTreeSet::new();
    let mut refused = 0_u64;
    let ids: Vec<u64> = ids.collect();
    for chunk in ids.chunks(per_push) {
        let records: Vec<String> = chunk.iter().map(|&id| record(id)).collect();
        let body = bytes::Bytes::from(format!("[{}]", records.join(",")));
        loop {
            match client.send("", body.clone()).await {
                SendResult::Ok => {
                    accepted.extend(chunk);
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
async fn landed_ids(clickhouse: &str, table: &str) -> BTreeSet<u64> {
    let sql = format!("SELECT DISTINCT id FROM default.{table} FORMAT TabSeparated");
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
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
pub(super) async fn wait_landed(
    clickhouse: &str,
    table: &str,
    want: &BTreeSet<u64>,
    budget: Duration,
) -> BTreeSet<u64> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let landed = landed_ids(clickhouse, table).await;
        if want.is_subset(&landed) || tokio::time::Instant::now() >= deadline {
            return landed;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The `ClickHouse` container's HTTP address.
pub(super) async fn clickhouse_address(infra: &TestInfrastructure) -> String {
    let container = infra.clickhouse.as_ref().expect("ClickHouse container");
    let http_port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("HTTP port mapping");
    // `localhost` can resolve to ::1 where loopback has no IPv6.
    let host = match container
        .get_host()
        .await
        .expect("container host")
        .to_string()
    {
        h if h == "localhost" => "127.0.0.1".to_string(),
        h => h,
    };
    format!("{host}:{http_port}")
}

/// A query client that talks to `ClickHouse` directly, never through a proxy.
pub(super) fn query_client(clickhouse: &str) -> ClickHouseQueryClient {
    ClickHouseQueryClient::new(&ClickHouseConfig {
        hosts: vec![clickhouse.to_string()],
        transport: Transport::Http,
        database: "default".to_string(),
        username: "default".to_string(),
        password: String::new(),
        tls: false,
        ..Default::default()
    })
    .expect("direct query client")
}

/// A gRPC-transport loader writing to `default.{table}` through `clickhouse`.
fn grpc_loader(listen_port: u16, clickhouse: String, table: &str) -> Config {
    let mut config = Config::default();
    config.transport = "grpc".to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{listen_port}"));
    // A small queue makes a paused loader refuse Push within a few records.
    config.grpc.recv_buffer_size = 64;
    config.clickhouse.hosts = vec![clickhouse];
    config.clickhouse.database = "default".to_string();
    config.clickhouse.protocol = "http".to_string();
    config.routing.default_db = "default".to_string();
    config.routing.default_table = table.to_string();
    config.routing.dlq.enabled = false;
    config.buffer.flush_rows = 10;
    config.buffer.flush_age_secs = 1;
    config.schema.cache_ttl_secs = 3600;
    config.schema.pre_warm_retry_secs = 10;
    config
}

/// Send the loader's dead letters to a file DLQ under `dir`, and nowhere else.
pub(super) fn with_file_dlq(config: &mut Config, dir: &Path) {
    config.routing.dlq.enabled = true;
    config.routing.dlq.mode = "file_only".to_string();
    config.routing.dlq.file_enabled = true;
    config.routing.dlq.file_path = dir.display().to_string();
    config.routing.dlq.kafka_enabled = false;
}

/// Run the orchestrator and connect a gRPC client to its listener.
async fn start_loader(
    config: Config,
    listen_port: u16,
) -> (CancellationToken, JoinHandle<()>, GrpcTransport) {
    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        let _ = orchestrator.run().await;
    });
    let client = GrpcTransport::new(&GrpcConfig::client(&format!(
        "http://127.0.0.1:{listen_port}"
    )))
    .await
    .expect("gRPC client");
    (shutdown, loader, client)
}

/// Push one record and wait for it to land: the loader is running from here.
async fn wait_for_loader(client: &GrpcTransport, clickhouse: &str, table: &str) {
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (first, _) = push_ids(client, 0..1, settle, id_payload).await;
    assert_eq!(first.len(), 1, "the listener refused the first record");
    let landed = wait_landed(clickhouse, table, &first, Duration::from_secs(60)).await;
    assert!(first.is_subset(&landed), "the first record never landed");
}

/// Decode standard-alphabet base64, the encoding the file DLQ gives a payload.
fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0_u32;
    let mut bits = 0_u32;
    for c in text.bytes().take_while(|&c| c != b'=') {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Every id the file DLQ under `dir` has written, skipping any torn line.
pub(super) fn dead_lettered_ids(dir: &Path) -> BTreeSet<u64> {
    fn walk(dir: &Path, ids: &mut BTreeSet<u64>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, ids);
                continue;
            }
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in body.lines() {
                let id = serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|entry| decode_base64(entry["payload"].as_str()?))
                    .and_then(|payload| serde_json::from_slice::<serde_json::Value>(&payload).ok())
                    .and_then(|record| record["id"].as_u64());
                ids.extend(id);
            }
        }
    }
    let mut ids = BTreeSet::new();
    walk(dir, &mut ids);
    ids
}

/// Wait until the DLQ holds every id in `want`, or the budget runs out.
pub(super) async fn wait_dead_lettered(
    dir: &Path,
    want: &BTreeSet<u64>,
    budget: Duration,
) -> BTreeSet<u64> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let ids = dead_lettered_ids(dir);
        if want.is_subset(&ids) || tokio::time::Instant::now() >= deadline {
            return ids;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// This process's soft `RLIMIT_FSIZE`, as `prlimit` spells it.
pub(super) fn file_size_soft_limit() -> String {
    let limits = std::fs::read_to_string("/proc/self/limits").expect("read /proc/self/limits");
    limits
        .lines()
        .find(|line| line.starts_with("Max file size"))
        .and_then(|line| line.split_whitespace().nth(3))
        .expect("a Max file size row")
        .to_string()
}

/// Set this process's soft `RLIMIT_FSIZE`; a write past it fails with `EFBIG`.
pub(super) fn set_file_size_soft_limit(soft: &str) {
    let status = std::process::Command::new("prlimit")
        .arg("--pid")
        .arg(std::process::id().to_string())
        .arg(format!("--fsize={soft}:"))
        .status()
        .expect("run prlimit (util-linux)");
    assert!(status.success(), "prlimit --fsize={soft}: failed: {status}");
}

#[tokio::test]
async fn no_accepted_record_is_lost_to_a_clickhouse_outage() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;

    // Queries go straight to the container; the loader goes through the proxy.
    let direct = query_client(&clickhouse);
    let proxy = OutageProxy::start(clickhouse.clone()).await;

    let table = unique_table_name("grpc_outage");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let listen_port = random_port();
    let config = grpc_loader(listen_port, proxy.address(), &table);
    let (shutdown, loader, client) = start_loader(config, listen_port).await;

    // Before: ClickHouse up, every record lands.
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (before, _) = push_ids(&client, 0..BEFORE, settle, id_payload).await;
    assert_eq!(
        before.len() as u64,
        BEFORE,
        "the listener refused records before the outage"
    );
    let landed = wait_landed(&clickhouse, &table, &before, Duration::from_secs(60)).await;
    assert!(
        before.is_subset(&landed),
        "records never landed before the outage: {landed:?}"
    );

    // During: ClickHouse unreachable for longer than one insert's retries.
    proxy.down();
    let outage_end = tokio::time::Instant::now() + OUTAGE;
    let (during, refused) =
        push_ids(&client, BEFORE..BEFORE + DURING, outage_end, id_payload).await;
    tokio::time::sleep_until(outage_end).await;

    // After: ClickHouse back.
    proxy.up();
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (after, _) = push_ids(
        &client,
        BEFORE + DURING..BEFORE + DURING + AFTER,
        settle,
        id_payload,
    )
    .await;

    let accepted: BTreeSet<u64> = before
        .iter()
        .chain(&during)
        .chain(&after)
        .copied()
        .collect();
    let landed = wait_landed(&clickhouse, &table, &accepted, LANDING_BUDGET).await;

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

/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test]
async fn no_rejected_row_is_lost_while_the_dlq_refuses_writes() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_dlq_hold");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64, \
             CONSTRAINT below_limit CHECK id < {REJECTED_FROM}) \
             ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = random_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    // JSONEachRow classifies by the server's code, and 469 VIOLATED_CONSTRAINT
    // is a permanent rejection there.
    config.clickhouse.insert_format = InsertFormat::JsonEachRow;
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    // The file DLQ keeps its file open, so only a write the kernel refuses
    // fails it and then clears: EFBIG past RLIMIT_FSIZE.
    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");

    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let (good, _) = push_ids(&client, 1..BEFORE + 1, until, id_payload).await;
    let (rejected, _) = push_ids(
        &client,
        REJECTED_FROM..REJECTED_FROM + REJECTED,
        until,
        id_payload,
    )
    .await;
    tokio::time::sleep(DLQ_DOWN).await;
    set_file_size_soft_limit(&unlimited);

    let landed = wait_landed(&clickhouse, &table, &good, LANDING_BUDGET).await;
    let dead_lettered = wait_dead_lettered(dlq_dir.path(), &rejected, LANDING_BUDGET).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    let lost_good: Vec<u64> = good.difference(&landed).copied().collect();
    let lost_rejected: Vec<u64> = rejected.difference(&dead_lettered).copied().collect();
    assert!(
        lost_good.is_empty() && lost_rejected.is_empty(),
        "{} of {} rejected rows reached neither ClickHouse nor the DLQ, and {} of {} good rows \
         never landed: rejected {lost_rejected:?}, good {lost_good:?}",
        lost_rejected.len(),
        rejected.len(),
        lost_good.len(),
        good.len()
    );
    assert_eq!(
        rejected.len() as u64,
        REJECTED,
        "the listener refused rejected rows it had room to queue"
    );
}

#[tokio::test]
async fn no_record_waiting_on_its_schema_is_lost_while_clickhouse_is_down() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = OutageProxy::start(clickhouse.clone()).await;

    let landing = unique_table_name("grpc_pending_landing");
    let source = unique_table_name("grpc_pending_source");
    for table in [&landing, &source] {
        direct
            .execute(&format!(
                "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("create table");
    }

    let listen_port = random_port();
    let mut config = grpc_loader(listen_port, proxy.address(), &landing);
    config.schema.pending_max_per_table = PENDING_PER_TABLE;
    config.schema.pending_max_total = PENDING_TOTAL;
    // Short enough for the expiry sweep to run many times inside the outage.
    config.schema.pending_max_age_secs = 2;
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &landing).await;

    // Every record names a table the loader has not seen, whose schema fetch
    // fails for the whole outage.
    proxy.down();
    let outage_end = tokio::time::Instant::now() + OUTAGE;
    let (pending, refused) = push_id_arrays(
        &client,
        1..PENDING_PUSHED + 1,
        PENDING_PER_PUSH,
        outage_end,
        |id| format!(r#"{{"_source":"{source}","id":{id}}}"#),
    )
    .await;
    tokio::time::sleep_until(outage_end).await;
    proxy.up();

    let landed = wait_landed(&clickhouse, &source, &pending, LANDING_BUDGET).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&landing, &source] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    let lost: Vec<u64> = pending.difference(&landed).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} records accepted for a table whose schema could not be fetched never landed: \
         {lost:?}",
        lost.len(),
        pending.len()
    );
    assert!(
        refused > 0 && (pending.len() as u64) < PENDING_PUSHED,
        "intake never paused: the listener took {} of {PENDING_PUSHED} records with the \
         pending-schema buffer full and refused {refused} pushes",
        pending.len()
    );
}

/// A record naming an empty table fails processing, so only the DLQ can take
/// it -- the same hand-over as a parse error or a pre-route reject.
///
/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test]
async fn no_unroutable_record_is_lost_while_the_dlq_refuses_writes() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_unroutable");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = random_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");

    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let (unroutable, _) = push_ids(&client, 1..UNROUTABLE + 1, until, |id| {
        format!(r#"{{"_source":"","id":{id}}}"#)
    })
    .await;
    tokio::time::sleep(DLQ_DOWN).await;
    set_file_size_soft_limit(&unlimited);

    let dead_lettered = wait_dead_lettered(dlq_dir.path(), &unroutable, LANDING_BUDGET).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    let lost: Vec<u64> = unroutable.difference(&dead_lettered).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} unroutable records never reached the DLQ: {lost:?}",
        lost.len(),
        unroutable.len()
    );
    assert_eq!(
        unroutable.len() as u64,
        UNROUTABLE,
        "the listener refused unroutable records it had room to queue"
    );
}
