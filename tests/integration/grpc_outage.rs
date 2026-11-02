// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! No record the gRPC listener answers OK is lost while the loader runs.
//!
//! The direct path has no broker. With acknowledgements on, the default, a Push
//! is answered only once its rows are in `ClickHouse`, dead-lettered with the
//! DLQ's confirmation, or dropped because nothing can ever take them. A failure
//! answers `UNAVAILABLE`, and the sender keeps its copy and retries. Each test
//! runs the real orchestrator on the gRPC transport against a `ClickHouse`
//! container, breaks one thing a record needs -- `ClickHouse` itself, the DLQ
//! that takes what `ClickHouse` rejects, or the schema fetch a new table waits
//! on -- restores it, and checks every record answered OK reached `ClickHouse`
//! or the DLQ.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dfe_loader::clickhouse::config::{ClickHouseConfig, Transport};
use dfe_loader::clickhouse::{ClickHouseQueryClient, InsertFormat};
use dfe_loader::config::Config;
use dfe_loader::metrics::{Metrics, ServerState};
use dfe_loader::pipeline::Orchestrator;
use scalo::ScalingPressure;
use scalo::metrics::MetricsManager;
use scalo::scaling::ScalingPressureConfig;
use scalo::transport::grpc::proto::transport_client::TransportClient;
use scalo::transport::grpc::proto::{Format, PushRequest};
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{SendResult, TransportSender};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::common::containers::{
    TestInfrastructure, restart_clickhouse, start_clickhouse_on, stop_container,
};
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

/// `grpc.max_hold_ms` for a test whose DLQ refuses for several holds.
const SHORT_HOLD_MS: u64 = 4_000;

/// How long the DLQ refuses every write in that test: three holds.
const DLQ_DOWN_PAST_THE_HOLD: Duration = Duration::from_millis(3 * SHORT_HOLD_MS);

/// Senders pushing rows that insert while that DLQ refuses.
const GOOD_SENDERS: usize = 4;

/// Good records pushed in one Push with a record `ClickHouse` refuses.
const GOOD_WITH_OVERFLOW: u64 = 20;

/// Id of the record whose JSON holds an integer no `ClickHouse` integer holds.
const OVERFLOW_ID: u64 = 500_000;

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

/// Records pushed for a table whose schema cannot be fetched.
const PENDING_PUSHED: u64 = 2_500;

/// Senders pushing at once: together they queue more records than the
/// listener's queue holds.
const PENDING_SENDERS: usize = 5;

/// Ports a test binds on the host: below the kernel's ephemeral range, where an
/// outgoing connection could take a port between the pick and the bind.
const TEST_PORTS: std::ops::Range<u16> = 9_000..10_240;

/// One marker socket per port this process has handed out, held until it exits.
static HANDED_OUT: Mutex<Vec<std::net::TcpListener>> = Mutex::new(Vec::new());

/// A loopback port in [`TEST_PORTS`] nothing listens on and no other test
/// process has been handed.
///
/// Probing the port is not enough: the loader binds it a moment later, and a
/// test process started beside this one (consecutive pids search from
/// consecutive ports) probes it free too. So a port is also claimed by binding
/// a marker socket one range-width above it, which only one process can do.
pub(super) fn free_port() -> u16 {
    let span = TEST_PORTS.end - TEST_PORTS.start;
    let start = (std::process::id() % u32::from(span)) as u16;
    (0..span)
        .map(|i| TEST_PORTS.start + (start + i) % span)
        .find(|&port| {
            let Ok(marker) = std::net::TcpListener::bind(("127.0.0.1", port + span)) else {
                return false;
            };
            if std::net::TcpListener::bind(("127.0.0.1", port)).is_err() {
                return false;
            }
            HANDED_OUT.lock().expect("port marker list").push(marker);
            true
        })
        .expect("a free loopback port below 10240")
}

/// Two picks made before either port is bound differ, as they must for two test
/// processes picking at once.
#[test]
fn a_port_is_handed_out_once() {
    assert_ne!(free_port(), free_port());
}

/// A loopback TCP proxy that can refuse and cut every connection on demand.
struct OutageProxy {
    port: u16,
    up: Arc<AtomicBool>,
    cut: watch::Sender<u64>,
}

impl OutageProxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", free_port()))
            .await
            .expect("bind proxy");
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

/// A record the loader routes to its default table.
fn id_payload(id: u64) -> String {
    format!(r#"{{"id":{id}}}"#)
}

/// Push `body` until the listener answers OK or `until` passes. Returns whether
/// it was answered OK and how many answers told the sender to retry.
async fn push_until_accepted(
    client: &GrpcTransport,
    body: bytes::Bytes,
    until: tokio::time::Instant,
) -> (bool, u64) {
    let mut refused = 0_u64;
    loop {
        match client.send("", body.clone()).await {
            SendResult::Ok => return (true, refused),
            SendResult::Backpressured if tokio::time::Instant::now() < until => {
                refused += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            _ => return (false, refused),
        }
    }
}

/// Push each id until the listener accepts it, starting none once `until` has
/// passed, and return the ids accepted and how many answers were retries.
pub(super) async fn push_ids(
    client: &GrpcTransport,
    ids: std::ops::Range<u64>,
    until: tokio::time::Instant,
    payload: impl Fn(u64) -> String,
) -> (BTreeSet<u64>, u64) {
    let mut accepted = BTreeSet::new();
    let mut refused = 0_u64;
    for id in ids {
        if tokio::time::Instant::now() >= until {
            break;
        }
        let (ok, retries) =
            push_until_accepted(client, bytes::Bytes::from(payload(id)), until).await;
        refused += retries;
        if ok {
            accepted.insert(id);
        }
    }
    (accepted, refused)
}

/// Push `ids` as JSON arrays of `per_push` records from `senders` tasks at
/// once, each retrying a Push until the listener accepts it or `until` passes.
/// Returns the ids accepted and how many answers were retries.
async fn push_id_arrays(
    client: Arc<GrpcTransport>,
    ids: std::ops::Range<u64>,
    per_push: usize,
    senders: usize,
    until: tokio::time::Instant,
    record: Arc<dyn Fn(u64) -> String + Send + Sync>,
) -> (BTreeSet<u64>, u64) {
    let ids: Vec<u64> = ids.collect();
    let chunks: Vec<Vec<u64>> = ids.chunks(per_push).map(<[u64]>::to_vec).collect();
    let mut tasks = Vec::with_capacity(senders);
    for sender in 0..senders {
        let mine: Vec<Vec<u64>> = chunks
            .iter()
            .skip(sender)
            .step_by(senders)
            .cloned()
            .collect();
        let client = Arc::clone(&client);
        let record = Arc::clone(&record);
        tasks.push(tokio::spawn(async move {
            let mut accepted = BTreeSet::new();
            let mut refused = 0_u64;
            for chunk in mine {
                let records: Vec<String> = chunk.iter().map(|&id| record(id)).collect();
                let body = bytes::Bytes::from(format!("[{}]", records.join(",")));
                let (ok, retries) = push_until_accepted(&client, body, until).await;
                refused += retries;
                if ok {
                    accepted.extend(chunk);
                }
            }
            (accepted, refused)
        }));
    }
    let mut accepted = BTreeSet::new();
    let mut refused = 0_u64;
    for task in tasks {
        let (ids, retries) = task.await.expect("sender task");
        accepted.extend(ids);
        refused += retries;
    }
    (accepted, refused)
}

/// The ids `ClickHouse` holds for `table`, read over its HTTP interface.
async fn landed_ids(clickhouse: &str, table: &str) -> BTreeSet<u64> {
    query_ids(
        clickhouse,
        &format!("SELECT DISTINCT id FROM default.{table} FORMAT TabSeparated"),
    )
    .await
}

/// The ids `ClickHouse` holds more than one row of for `table`.
async fn duplicated_ids(clickhouse: &str, table: &str) -> BTreeSet<u64> {
    query_ids(
        clickhouse,
        &format!(
            "SELECT id FROM default.{table} GROUP BY id HAVING count() > 1 FORMAT TabSeparated"
        ),
    )
    .await
}

/// The ids a one-column `sql` query returns over the HTTP interface, or none
/// when it fails.
async fn query_ids(clickhouse: &str, sql: &str) -> BTreeSet<u64> {
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .basic_auth(
            "default",
            Some(crate::common::containers::clickhouse_password()),
        )
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
    format!("{}:{}", container.host(), container.port(8123))
}

/// A query client that talks to `ClickHouse` directly, never through a proxy.
pub(super) fn query_client(clickhouse: &str) -> ClickHouseQueryClient {
    ClickHouseQueryClient::new(&ClickHouseConfig {
        hosts: vec![clickhouse.to_string()],
        transport: Transport::Http,
        database: "default".to_string(),
        username: "default".to_string(),
        password: crate::common::containers::clickhouse_password(),
        tls: false,
        ..Default::default()
    })
    .expect("direct query client")
}

/// Create `default.{table}` with one `id` column, and `constraint` if given.
async fn create_id_table(direct: &ClickHouseQueryClient, table: &str, constraint: &str) {
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64{constraint}) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
}

/// A gRPC-transport loader writing to `default.{table}` through `clickhouse`.
pub(super) fn grpc_loader(listen_port: u16, clickhouse: String, table: &str) -> Config {
    let mut config = Config::default();
    config.transport = "grpc".to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{listen_port}"));
    // A small queue makes a paused loader refuse Push within a few records.
    config.grpc.recv_buffer_size = 64;
    config.clickhouse.hosts = vec![clickhouse];
    config.clickhouse.password = crate::common::containers::clickhouse_password().into();
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

/// A gRPC client dialled to the loader's listener.
async fn client_for(listen_port: u16) -> GrpcTransport {
    GrpcTransport::new(&GrpcConfig::client(&format!(
        "http://127.0.0.1:{listen_port}"
    )))
    .await
    .expect("gRPC client")
}

/// Run `orchestrator` and connect a gRPC client to its listener.
pub(super) async fn start(
    mut orchestrator: Orchestrator,
    listen_port: u16,
) -> (CancellationToken, JoinHandle<()>, GrpcTransport) {
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });
    (shutdown, loader, client_for(listen_port).await)
}

/// Run the orchestrator and connect a gRPC client to its listener.
pub(super) async fn start_loader(
    config: Config,
    listen_port: u16,
) -> (CancellationToken, JoinHandle<()>, GrpcTransport) {
    start(Orchestrator::new(config), listen_port).await
}

/// Push one record and wait for it to land: the loader is running from here.
pub(super) async fn wait_for_loader(client: &GrpcTransport, clickhouse: &str, table: &str) {
    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (first, _) = push_ids(client, 0..1, settle, id_payload).await;
    assert_eq!(first.len(), 1, "the listener refused the first record");
    let landed = wait_landed(clickhouse, table, &first, Duration::from_secs(60)).await;
    assert!(first.is_subset(&landed), "the first record never landed");
}

/// The value of the counter or gauge `name` in `manager`'s Prometheus output,
/// summed over label sets matching `labels`, or 0 when it has never been set.
pub(super) fn counter_value(manager: &MetricsManager, name: &str, labels: &str) -> f64 {
    manager
        .render()
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| {
            line.split(['{', ' '])
                .next()
                .is_some_and(|metric| metric.ends_with(name))
        })
        .filter(|line| line.contains(labels))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Decode standard-alphabet base64, the encoding the file DLQ gives a payload.
pub(super) fn decode_base64(text: &str) -> Option<Vec<u8>> {
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
    dead_letter_reasons(dir).into_keys().collect()
}

/// The reason the file DLQ under `dir` recorded for each id it has written,
/// skipping any torn line.
pub(super) fn dead_letter_reasons(dir: &Path) -> std::collections::BTreeMap<u64, String> {
    fn walk(dir: &Path, reasons: &mut std::collections::BTreeMap<u64, String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, reasons);
                continue;
            }
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in body.lines() {
                let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let id = decode_base64(entry["payload"].as_str().unwrap_or_default())
                    .and_then(|payload| serde_json::from_slice::<serde_json::Value>(&payload).ok())
                    .and_then(|record| record["id"].as_u64());
                if let Some(id) = id {
                    let reason = entry["reason"].as_str().unwrap_or_default().to_string();
                    reasons.insert(id, reason);
                }
            }
        }
    }
    let mut reasons = std::collections::BTreeMap::new();
    walk(dir, &mut reasons);
    reasons
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

/// Push `ids` in the background, retrying each until the listener accepts it or
/// `until` passes, while the DLQ refuses every write for [`DLQ_DOWN`]. Returns
/// the ids accepted, having checked none was answered OK before the DLQ took
/// writes again.
async fn push_while_the_dlq_refuses(
    client: GrpcTransport,
    ids: std::ops::Range<u64>,
    payload: fn(u64) -> String,
) -> BTreeSet<u64> {
    // The file DLQ keeps its file open, so only a write the kernel refuses
    // fails it and then clears: EFBIG past RLIMIT_FSIZE.
    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");

    let recovered = Arc::new(AtomicBool::new(false));
    let until = tokio::time::Instant::now() + DLQ_DOWN + LANDING_BUDGET;
    let pushing = {
        let recovered = Arc::clone(&recovered);
        tokio::spawn(async move {
            let mut accepted = BTreeSet::new();
            let mut early = Vec::new();
            for id in ids {
                if tokio::time::Instant::now() >= until {
                    break;
                }
                let body = bytes::Bytes::from(payload(id));
                let (ok, _) = push_until_accepted(&client, body, until).await;
                if !ok {
                    continue;
                }
                if !recovered.load(Ordering::SeqCst) {
                    early.push(id);
                }
                accepted.insert(id);
            }
            (accepted, early)
        })
    };
    tokio::time::sleep(DLQ_DOWN).await;
    // Marked before the limit lifts, so no answer to a write that could
    // succeed is read as early.
    recovered.store(true, Ordering::SeqCst);
    set_file_size_soft_limit(&unlimited);
    let (accepted, early) = pushing.await.expect("sender task");
    assert!(
        early.is_empty(),
        "{} records were answered OK while the DLQ refused every write: {early:?}",
        early.len()
    );
    accepted
}

#[tokio::test]
async fn no_accepted_record_is_lost_to_a_clickhouse_outage() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;

    // Queries go straight to the container; the loader goes through the proxy.
    let direct = query_client(&clickhouse);
    let proxy = OutageProxy::start(clickhouse.clone()).await;

    let table = unique_table_name("grpc_outage");
    create_id_table(&direct, &table, "").await;

    let listen_port = free_port();
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
    // A record ClickHouse cannot take is answered with a retry, never OK.
    assert!(
        refused > 0 && (during.len() as u64) < DURING,
        "the listener answered {} of {DURING} records OK while ClickHouse was down and told \
         senders to retry {refused} times",
        during.len()
    );
}

/// The readiness state a loader marks ready once `ClickHouse` accepts it: what
/// `/readyz` answers from.
pub(super) fn readiness_state(metrics: Metrics) -> Arc<ServerState> {
    let scaling = ScalingPressure::new(ScalingPressureConfig::default(), Vec::new());
    Arc::new(ServerState::new(metrics, Arc::new(scaling)))
}

/// Run `orchestrator` through the production start, which marks `state` ready
/// once `ClickHouse` accepts the loader.
pub(super) fn start_ready(
    mut orchestrator: Orchestrator,
    state: &Arc<ServerState>,
) -> (CancellationToken, JoinHandle<()>) {
    let shutdown = orchestrator.shutdown_token();
    let state = Arc::clone(state);
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run_once_clickhouse_accepts(&state).await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });
    (shutdown, loader)
}

/// Sample `state` until the returned token is cancelled, counting the samples
/// that read not ready.
pub(super) fn watch_readiness(state: &Arc<ServerState>) -> (CancellationToken, JoinHandle<u64>) {
    let stop = CancellationToken::new();
    let (state, until) = (Arc::clone(state), stop.clone());
    let watcher = tokio::spawn(async move {
        let mut unready = 0_u64;
        while !until.is_cancelled() {
            unready += u64::from(!state.is_ready());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        unready
    });
    (stop, watcher)
}

/// Wait until the metric `name` reads `want`, or `budget` runs out, and return
/// the last value read.
pub(super) async fn wait_metric(
    manager: &MetricsManager,
    name: &str,
    want: f64,
    budget: Duration,
) -> f64 {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let value = counter_value(manager, name, "");
        if (value - want).abs() < f64::EPSILON || tokio::time::Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The gRPC code a raw Push of `body` is answered with, `None` for OK.
async fn raw_push_code(listen_port: u16, body: &str) -> Option<tonic::Code> {
    let mut raw = TransportClient::connect(format!("http://127.0.0.1:{listen_port}"))
        .await
        .expect("raw gRPC client");
    raw.push(PushRequest {
        payload: bytes::Bytes::from(body.to_string()),
        format: Format::Auto.into(),
        metadata: std::collections::HashMap::new(),
    })
    .await
    .err()
    .map(|status| status.code())
}

/// How long the restart tests keep `ClickHouse` stopped.
pub(super) const STOPPED: Duration = Duration::from_secs(15);

/// `ClickHouse` stopped and started again under a running loader. A Push
/// whose rows cannot insert is answered `UNAVAILABLE`, the gauge reads the
/// outage, `/readyz` stays ready, and the same loader lands every record it
/// answered OK once the server is back.
#[tokio::test(flavor = "multi_thread")]
async fn a_clickhouse_restart_loses_no_accepted_record_and_leaves_the_loader_ready() {
    let http_port = free_port();
    let server = start_clickhouse_on(test_name!(), http_port).await;
    let clickhouse = format!("{}:{http_port}", server.container.host);
    let direct = query_client(&clickhouse);
    let table = unique_table_name("grpc_restart");
    create_id_table(&direct, &table, "").await;

    let manager = MetricsManager::new("loader_it");
    let metrics = Metrics::new(&manager);
    let state = readiness_state(metrics.clone());
    let listen_port = free_port();
    let orchestrator = Orchestrator::with_metrics(
        grpc_loader(listen_port, clickhouse.clone(), &table),
        metrics,
    );
    let guard = Arc::clone(orchestrator.memory_guard());
    let (shutdown, loader) = start_ready(orchestrator, &state);
    let client = client_for(listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;
    let up_before = wait_metric(&manager, "clickhouse_connected", 1.0, LANDING_BUDGET).await;
    let errors_before = counter_value(&manager, "clickhouse_insert_errors_total", "");
    let (stop_watching, readiness) = watch_readiness(&state);

    stop_container(&server.container).await;
    let outage_end = tokio::time::Instant::now() + STOPPED;
    let refusal = raw_push_code(listen_port, &id_payload(BEFORE)).await;
    let down = wait_metric(&manager, "clickhouse_connected", 0.0, STOPPED).await;
    let (during, refused) =
        push_ids(&client, BEFORE + 1..BEFORE + DURING, outage_end, id_payload).await;
    let errors_during = counter_value(&manager, "clickhouse_insert_errors_total", "");
    let reserved_during = guard.reserved_bytes();
    tokio::time::sleep_until(outage_end).await;

    restart_clickhouse(&server.container).await;
    let settle = tokio::time::Instant::now() + LANDING_BUDGET;
    let (after, _) = push_ids(
        &client,
        BEFORE + DURING..BEFORE + DURING + AFTER,
        settle,
        id_payload,
    )
    .await;
    let up_after = wait_metric(&manager, "clickhouse_connected", 1.0, LANDING_BUDGET).await;
    let accepted: BTreeSet<u64> = std::iter::once(0)
        .chain(during.iter().copied())
        .chain(after.iter().copied())
        .collect();
    let landed = wait_landed(&clickhouse, &table, &accepted, LANDING_BUDGET).await;
    stop_watching.cancel();
    let unready_samples = readiness.await.expect("readiness watcher");
    let still_running = !loader.is_finished();

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    drop(server);

    assert_eq!(
        refusal,
        Some(tonic::Code::Unavailable),
        "a Push whose rows could not reach ClickHouse was answered {refusal:?}: scalo's held \
         Push answers UNAVAILABLE, which the sender reads as Backpressured"
    );
    assert!(
        refused > 0 && during.is_empty(),
        "with ClickHouse stopped the listener answered {} Pushes OK and told senders to retry \
         {refused} times",
        during.len()
    );
    assert!(
        (up_before - 1.0).abs() < f64::EPSILON
            && down.abs() < f64::EPSILON
            && (up_after - 1.0).abs() < f64::EPSILON,
        "clickhouse_connected read {up_before} up, {down} stopped and {up_after} restarted"
    );
    assert!(
        errors_during > errors_before,
        "clickhouse_insert_errors_total stayed at {errors_before} while ClickHouse was stopped"
    );
    assert!(
        reserved_during <= guard.limit_bytes(),
        "the memory guard held {reserved_during} bytes against a {} byte limit",
        guard.limit_bytes()
    );
    assert_eq!(
        unready_samples, 0,
        "readiness read not ready {unready_samples} times while ClickHouse was stopped"
    );
    assert!(still_running, "the loader stopped during the outage");
    assert_eq!(
        after.len() as u64,
        AFTER,
        "the same loader refused records after ClickHouse restarted"
    );
    let lost: Vec<u64> = accepted.difference(&landed).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} accepted records never landed: {lost:?}",
        lost.len(),
        accepted.len()
    );
}

/// Direct mode holds each Push until its row is in `ClickHouse`: nothing is
/// answered OK while `ClickHouse` is unreachable, and the OK that does come
/// finds the row already there.
#[tokio::test]
async fn a_push_is_answered_only_once_its_row_is_in_clickhouse() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = OutageProxy::start(clickhouse.clone()).await;

    let table = unique_table_name("grpc_held");
    create_id_table(&direct, &table, "").await;

    let listen_port = free_port();
    let (shutdown, loader, client) = start_loader(
        grpc_loader(listen_port, proxy.address(), &table),
        listen_port,
    )
    .await;
    wait_for_loader(&client, &clickhouse, &table).await;

    proxy.down();
    let client = Arc::new(client);
    let mut pushed = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.send("", bytes::Bytes::from(id_payload(1))).await })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    // An answer while ClickHouse is down may only tell the sender to retry.
    let early = if pushed.is_finished() {
        Some((&mut pushed).await.expect("push task"))
    } else {
        None
    };
    let landed_early = landed_ids(&clickhouse, &table).await.contains(&1);
    let early_answer = format!("{early:?}");
    let answered_ok_early = early.as_ref().is_some_and(SendResult::is_ok);

    proxy.up();
    let mut result = match early {
        Some(result) => result,
        None => pushed.await.expect("push task"),
    };
    if !result.is_ok() {
        let until = tokio::time::Instant::now() + Duration::from_secs(60);
        let (ok, _) = push_until_accepted(&client, bytes::Bytes::from(id_payload(1)), until).await;
        result = if ok { SendResult::Ok } else { result };
    }
    let landed_at_answer = landed_ids(&clickhouse, &table).await.contains(&1);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(
        !answered_ok_early && !landed_early,
        "the Push was answered {early_answer} or its row landed ({landed_early}) while \
         ClickHouse was unreachable"
    );
    assert!(result.is_ok(), "the Push was never accepted: {result:?}");
    assert!(
        landed_at_answer,
        "the Push was answered OK before its row was in ClickHouse"
    );
}

/// A Push whose rows never insert is told to retry, and its rows are not
/// counted delivered.
#[tokio::test]
async fn a_push_whose_rows_never_insert_is_answered_unavailable_and_not_counted_delivered() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = OutageProxy::start(clickhouse.clone()).await;

    let table = unique_table_name("grpc_undelivered");
    create_id_table(&direct, &table, "").await;

    let manager = MetricsManager::new("loader_it");
    let metrics = Metrics::new(&manager);
    let listen_port = free_port();
    let orchestrator =
        Orchestrator::with_metrics(grpc_loader(listen_port, proxy.address(), &table), metrics);
    let (shutdown, loader, client) = start(orchestrator, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;
    let delivered_before = counter_value(&manager, "records_delivered_total", "");

    proxy.down();
    let result = client.send("", bytes::Bytes::from(id_payload(2))).await;
    let delivered_after = counter_value(&manager, "records_delivered_total", "");
    let landed = landed_ids(&clickhouse, &table).await;

    proxy.up();
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(
        matches!(result, SendResult::Backpressured),
        "a Push ClickHouse never took was answered {result:?}"
    );
    assert!(!landed.contains(&2));
    assert!(
        (delivered_before - 1.0).abs() < f64::EPSILON,
        "the one inserted record read as {delivered_before} delivered"
    );
    assert!(
        (delivered_after - delivered_before).abs() < f64::EPSILON,
        "a record that never inserted moved records_delivered_total from {delivered_before} to \
         {delivered_after}"
    );
}

/// A row `ClickHouse` rejects for good, with no DLQ to take it, is answered as
/// a drop: a retry would fail the same way at every hop.
#[tokio::test]
async fn a_row_that_can_never_insert_with_no_dlq_is_answered_as_a_drop() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_no_dlq");
    create_id_table(
        &direct,
        &table,
        &format!(", CONSTRAINT below_limit CHECK id < {REJECTED_FROM}"),
    )
    .await;

    let manager = MetricsManager::new("loader_it");
    let metrics = Metrics::new(&manager);
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    // JSONEachRow classifies by the server's code, and 469 VIOLATED_CONSTRAINT
    // is a permanent rejection there.
    config.clickhouse.insert_format = InsertFormat::JsonEachRow;
    let (shutdown, loader, client) =
        start(Orchestrator::with_metrics(config, metrics), listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        client.send("", bytes::Bytes::from(id_payload(REJECTED_FROM))),
    )
    .await
    .expect("the Push was answered");
    let dropped = counter_value(
        &manager,
        "dead_letters_dropped_total",
        r#"reason="dead_letter""#,
    );
    let landed = landed_ids(&clickhouse, &table).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(
        result.is_ok(),
        "a row that can never insert was answered {result:?}, so its sender retries it forever"
    );
    assert!(!landed.contains(&REJECTED_FROM));
    assert!(
        (dropped - 1.0).abs() < f64::EPSILON,
        "the dropped row read as {dropped} in pipeline_dead_letters_dropped_total{{reason=\"dead_letter\"}}"
    );
}

/// A shutdown while a sender still pushes loses no record the listener acked:
/// intake closes first and the loader drains what was queued before its final
/// flush.
#[tokio::test]
async fn no_acked_record_is_lost_when_shutdown_races_a_sender() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_shutdown");
    create_id_table(&direct, &table, "").await;

    let listen_port = free_port();
    let mut orchestrator = Orchestrator::new(grpc_loader(listen_port, clickhouse.clone(), &table));
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        let ran = orchestrator.run().await.is_ok();
        (ran, orchestrator.stats().messages_received)
    });
    let client = client_for(listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    // One Push a millisecond, counting the ones the listener acked.
    let stop = Arc::new(AtomicBool::new(false));
    let sender = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut acked = BTreeSet::new();
            let mut id = 1_u64;
            while !stop.load(Ordering::SeqCst) {
                let body = bytes::Bytes::from(id_payload(id));
                if client.send("", body).await.is_ok() {
                    acked.insert(id);
                }
                id += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            acked
        })
    };

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(60), loader).await;
    stop.store(true, Ordering::SeqCst);
    let acked = sender.await.expect("sender task");
    let landed = landed_ids(&clickhouse, &table).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    let (ran, received) = stopped
        .expect("the loader stopped within 60 s of shutdown")
        .expect("loader task");
    assert!(ran, "the loader returned an error");
    assert!(!acked.is_empty(), "the listener acked nothing");
    // wait_for_loader's record is the one received beyond the sender's acks.
    assert_eq!(
        received,
        acked.len() as u64 + 1,
        "the listener acked {} records and the loader received {}",
        acked.len() + 1,
        received
    );
    let lost: Vec<u64> = acked.difference(&landed).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} acked records never landed: {lost:?}",
        lost.len(),
        acked.len()
    );
}

/// A rejected row is answered only once the DLQ proves it written: while the
/// DLQ refuses, the loader holds the answer and retries the write.
///
/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test]
async fn no_rejected_row_is_lost_while_the_dlq_refuses_writes() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_dlq_hold");
    create_id_table(
        &direct,
        &table,
        &format!(", CONSTRAINT below_limit CHECK id < {REJECTED_FROM}"),
    )
    .await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    // JSONEachRow classifies by the server's code, and 469 VIOLATED_CONSTRAINT
    // is a permanent rejection there.
    config.clickhouse.insert_format = InsertFormat::JsonEachRow;
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let (good, _) = push_ids(&client, 1..BEFORE + 1, until, id_payload).await;
    let rejected =
        push_while_the_dlq_refuses(client, REJECTED_FROM..REJECTED_FROM + REJECTED, id_payload)
            .await;

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
        "rejected rows were never accepted once the DLQ took writes again"
    );
}

/// A DLQ refusing for longer than a sender's hold keeps only the senders of
/// the rows it must take waiting: every row that inserts is answered OK within
/// the hold, first time, and lands once, while the rejected row's sender is
/// told to retry until the DLQ takes it.
///
/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test]
async fn a_dlq_refusing_past_the_hold_holds_only_the_rows_it_must_take() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_dlq_past_hold");
    create_id_table(
        &direct,
        &table,
        &format!(", CONSTRAINT below_limit CHECK id < {REJECTED_FROM}"),
    )
    .await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    // JSONEachRow classifies by the server's code, and 469 VIOLATED_CONSTRAINT
    // is a permanent rejection there.
    config.clickhouse.insert_format = InsertFormat::JsonEachRow;
    config.grpc.max_hold_ms = SHORT_HOLD_MS;
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, rejected_client) = start_loader(config, listen_port).await;
    wait_for_loader(&rejected_client, &clickhouse, &table).await;
    let mut good_clients = Vec::with_capacity(GOOD_SENDERS);
    for _ in 0..GOOD_SENDERS {
        good_clients.push(client_for(listen_port).await);
    }

    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");
    let outage_end = tokio::time::Instant::now() + DLQ_DOWN_PAST_THE_HOLD;

    // One sender retries a row ClickHouse rejects until the DLQ takes it.
    let rejecting = tokio::spawn(async move {
        push_until_accepted(
            &rejected_client,
            bytes::Bytes::from(id_payload(REJECTED_FROM)),
            outage_end + LANDING_BUDGET,
        )
        .await
    });
    // The others push good rows at once, one Push after another, each row
    // once, timing each answer. Several at once put a Push close behind every
    // rejected one.
    let good_pushing: Vec<_> = good_clients
        .into_iter()
        .zip(0_u64..)
        .map(|(client, sender)| {
            tokio::spawn(async move {
                let mut answered_ok = BTreeSet::new();
                let mut not_ok = Vec::new();
                let mut slowest = Duration::ZERO;
                let mut id = 1 + sender;
                while tokio::time::Instant::now() < outage_end {
                    let started = tokio::time::Instant::now();
                    let result = client.send("", bytes::Bytes::from(id_payload(id))).await;
                    slowest = slowest.max(started.elapsed());
                    if matches!(result, SendResult::Ok) {
                        answered_ok.insert(id);
                    } else {
                        not_ok.push((id, format!("{result:?}")));
                    }
                    id += GOOD_SENDERS as u64;
                }
                (answered_ok, not_ok, slowest)
            })
        })
        .collect();

    tokio::time::sleep_until(outage_end).await;
    set_file_size_soft_limit(&unlimited);
    let mut good = BTreeSet::new();
    let mut not_ok = Vec::new();
    let mut slowest = Duration::ZERO;
    for task in good_pushing {
        let (answered_ok, refused, slowest_here) = task.await.expect("good sender task");
        good.extend(answered_ok);
        not_ok.extend(refused);
        slowest = slowest.max(slowest_here);
    }
    let (rejected_accepted, told_to_retry) = rejecting.await.expect("rejecting sender task");

    let landed = wait_landed(&clickhouse, &table, &good, LANDING_BUDGET).await;
    let rejected = BTreeSet::from([REJECTED_FROM]);
    let dead_lettered = wait_dead_lettered(dlq_dir.path(), &rejected, LANDING_BUDGET).await;
    let duplicated = duplicated_ids(&clickhouse, &table).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    let hold = Duration::from_millis(SHORT_HOLD_MS);
    assert!(
        not_ok.is_empty(),
        "{} of {} rows that insert were not answered OK while the DLQ refused: {not_ok:?}",
        not_ok.len(),
        good.len() + not_ok.len()
    );
    assert!(
        slowest < hold,
        "a row that inserts waited {slowest:?} for its answer, past the {hold:?} hold"
    );
    let lost: Vec<u64> = good.difference(&landed).copied().collect();
    assert!(lost.is_empty(), "rows answered OK never landed: {lost:?}");
    assert!(duplicated.is_empty(), "rows landed twice: {duplicated:?}");
    assert!(
        told_to_retry > 0,
        "the rejected row was never answered retryable in a DLQ outage of {DLQ_DOWN_PAST_THE_HOLD:?}"
    );
    assert!(
        rejected_accepted && dead_lettered.contains(&REJECTED_FROM),
        "the rejected row was not taken by the DLQ once it recovered"
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
        create_id_table(&direct, table, "").await;
    }

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, proxy.address(), &landing);
    config.schema.pending_max_per_table = PENDING_PER_TABLE;
    config.schema.pending_max_total = PENDING_TOTAL;
    // Short enough for the expiry sweep to run many times inside the outage.
    config.schema.pending_max_age_secs = 2;
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &landing).await;

    // Every record names a table the loader has not seen, whose schema fetch
    // fails for the whole outage. The senders keep retrying past it.
    proxy.down();
    let outage_end = tokio::time::Instant::now() + OUTAGE;
    let record_source = source.clone();
    let pushing = tokio::spawn(push_id_arrays(
        Arc::new(client),
        1..PENDING_PUSHED + 1,
        PENDING_PER_PUSH,
        PENDING_SENDERS,
        outage_end + LANDING_BUDGET,
        Arc::new(move |id| format!(r#"{{"_source":"{record_source}","id":{id}}}"#)),
    ));
    tokio::time::sleep_until(outage_end).await;
    proxy.up();

    let (pending, refused) = pushing.await.expect("sender tasks");
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
    assert_eq!(
        pending.len() as u64,
        PENDING_PUSHED,
        "records were never accepted once their schema could be fetched"
    );
    assert!(
        refused > 0,
        "no sender was told to retry while no schema could be fetched"
    );
}

/// A record nested past the loader's 64-level bound, which only the DLQ can
/// take. The test's own parser reads it back, since `serde_json` allows 128.
pub(super) fn too_deep_record(id: u64) -> String {
    const LEVELS: usize = 70;
    format!(
        r#"{{"id":{id},"deep":{}{}}}"#,
        "[".repeat(LEVELS),
        "]".repeat(LEVELS)
    )
}

/// A record too deeply nested to process, so only the DLQ can take it -- the
/// same hand-over as a parse error or a pre-route reject.
///
/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test]
async fn no_unroutable_record_is_lost_while_the_dlq_refuses_writes() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_unroutable");
    create_id_table(&direct, &table, "").await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    let unroutable = push_while_the_dlq_refuses(client, 1..UNROUTABLE + 1, too_deep_record).await;

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
        "unroutable records were never accepted once the DLQ took writes again"
    );
}

/// A record whose JSON holds an integer no `ClickHouse` integer type holds.
fn overflow_payload(id: u64) -> String {
    format!(
        r#"{{"id":{id},"message":"an integer no ClickHouse integer type holds","big":123456789012345678901234567890}}"#
    )
}

/// A record `ClickHouse` refuses for its data -- an integer no integer type
/// holds, in the JSON column the default `RowBinary` insert has the server
/// parse (code 117) -- is dead-lettered with the server's reason, and the good
/// records flushed with it land. The refusal is not taken for schema drift, so
/// nothing behind it waits.
#[tokio::test]
async fn a_row_clickhouse_refuses_for_its_data_is_dead_lettered_and_the_rest_land() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);

    let table = unique_table_name("grpc_data_refusal");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64, message String DEFAULT '', \
             _json JSON(max_dynamic_paths = 2048)) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    wait_for_loader(&client, &clickhouse, &table).await;

    // One Push with the refused record among good ones, so they flush as one
    // batch, then good records one at a time behind it.
    let mut records: Vec<String> = (1..=GOOD_WITH_OVERFLOW).map(id_payload).collect();
    records.insert(records.len() / 2, overflow_payload(OVERFLOW_ID));
    let until = tokio::time::Instant::now() + Duration::from_secs(45);
    let (batch_accepted, told_to_retry) = push_until_accepted(
        &client,
        bytes::Bytes::from(format!("[{}]", records.join(","))),
        until,
    )
    .await;
    let behind = GOOD_WITH_OVERFLOW + 1..GOOD_WITH_OVERFLOW + 11;
    let (accepted_behind, _) = push_ids(&client, behind.clone(), until, id_payload).await;

    let good: BTreeSet<u64> = (1..=GOOD_WITH_OVERFLOW).chain(behind).collect();
    let landed = wait_landed(&clickhouse, &table, &good, Duration::from_secs(30)).await;
    let refused = BTreeSet::from([OVERFLOW_ID]);
    wait_dead_lettered(dlq_dir.path(), &refused, Duration::from_secs(30)).await;
    let reasons = dead_letter_reasons(dlq_dir.path());

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert!(
        batch_accepted,
        "the Push carrying the refused record was never answered OK: told to retry {told_to_retry} times"
    );
    let missing: Vec<u64> = good.difference(&landed).copied().collect();
    assert!(
        missing.is_empty(),
        "{} of {} good records never landed: {missing:?}",
        missing.len(),
        good.len()
    );
    assert_eq!(
        accepted_behind.len(),
        10,
        "records behind the refused one waited"
    );
    assert!(!landed.contains(&OVERFLOW_ID), "the refused record landed");
    let reason = reasons
        .get(&OVERFLOW_ID)
        .expect("the refused record never reached the DLQ");
    assert!(
        reason.contains("117"),
        "the dead letter does not carry the server's refusal: {reason}"
    );
}

/// One `(id, name)` row read back as text, whatever the column's type.
async fn names_by_id(clickhouse: &str, table: &str) -> std::collections::BTreeMap<u64, String> {
    let Ok(response) = reqwest::Client::new()
        .get(format!("http://{clickhouse}/"))
        .basic_auth(
            "default",
            Some(crate::common::containers::clickhouse_password()),
        )
        .query(&[(
            "query",
            format!("SELECT id, toString(name) FROM default.{table} FORMAT TabSeparated"),
        )])
        .send()
        .await
    else {
        return std::collections::BTreeMap::new();
    };
    let body = response.text().await.unwrap_or_default();
    body.lines()
        .filter_map(|line| {
            let (id, name) = line.split_once('\t')?;
            Some((id.parse().ok()?, name.to_string()))
        })
        .collect()
}

/// Push each `(id, record)` once its predecessor is answered, retrying until
/// the listener answers OK or `until` passes. Returns the ids answered OK, and
/// those answered OK while their row was in neither `ClickHouse` nor the DLQ.
async fn push_checking_placement(
    client: &GrpcTransport,
    clickhouse: &str,
    table: &str,
    dlq: &Path,
    records: &[(u64, String)],
    until: tokio::time::Instant,
) -> (BTreeSet<u64>, Vec<u64>) {
    let mut accepted = BTreeSet::new();
    let mut early = Vec::new();
    for (id, record) in records {
        let (ok, _) = push_until_accepted(client, bytes::Bytes::from(record.clone()), until).await;
        if !ok {
            continue;
        }
        accepted.insert(*id);
        let placed =
            landed_ids(clickhouse, table).await.contains(id) || dead_lettered_ids(dlq).contains(id);
        if !placed {
            early.push(*id);
        }
    }
    (accepted, early)
}

/// A table changed under the loader's cached schema, first by a dropped column
/// and then by being recreated with `name` as a number. Each change is caught
/// from the server's refusal of the first insert after it, with no manual
/// cache invalidation: rows that fit the new table land as sent, rows that
/// cannot go to the DLQ, and no Push is answered OK before its row is in one
/// or the other.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_changed_under_the_cached_schema_is_caught_and_nothing_is_acked_early() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let table = unique_table_name("grpc_schema_change");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64, name String, extra String) \
             ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, clickhouse.clone(), &table);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader, client) = start_loader(config, listen_port).await;
    let until = tokio::time::Instant::now() + LANDING_BUDGET;
    let full = |id: u64| {
        (
            id,
            format!(r#"{{"id":{id},"name":"n{id}","extra":"e{id}"}}"#),
        )
    };

    // Both schema caches hold the three-column table from here.
    let (cached, _) = push_checking_placement(
        &client,
        &clickhouse,
        &table,
        dlq_dir.path(),
        &(1..=3).map(full).collect::<Vec<_>>(),
        until,
    )
    .await;

    direct
        .execute(&format!("ALTER TABLE default.{table} DROP COLUMN extra"))
        .await
        .expect("drop column");
    let (after_drop, early_drop) = push_checking_placement(
        &client,
        &clickhouse,
        &table,
        dlq_dir.path(),
        &(10..=13).map(full).collect::<Vec<_>>(),
        until,
    )
    .await;
    let names_after_drop = names_by_id(&clickhouse, &table).await;

    direct
        .execute(&format!("DROP TABLE default.{table} SYNC"))
        .await
        .expect("drop table");
    direct
        .execute(&format!(
            "CREATE TABLE default.{table} (id UInt64, name UInt32) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("recreate table");
    // A number goes first: its String encoding is shorter than a UInt32, so the
    // server refuses the first insert against the old schema. A word cannot fit
    // the new column, including one whose encoding is as long as a UInt32.
    let numeric: Vec<(u64, String)> = (20..=22)
        .map(|id| (id, format!(r#"{{"id":{id},"name":{id}}}"#)))
        .collect();
    let wordy: Vec<(u64, String)> = [(30, "seven"), (31, "abc"), (32, "a long word")]
        .into_iter()
        .map(|(id, name)| (id, format!(r#"{{"id":{id},"name":"{name}"}}"#)))
        .collect();
    let records: Vec<(u64, String)> = numeric.iter().chain(&wordy).cloned().collect();
    let (after_recreate, early_recreate) = push_checking_placement(
        &client,
        &clickhouse,
        &table,
        dlq_dir.path(),
        &records,
        until,
    )
    .await;
    let names_after_recreate = names_by_id(&clickhouse, &table).await;
    let dead_lettered = dead_lettered_ids(dlq_dir.path());

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert_eq!(cached.len(), 3, "the rows that warm the cache were refused");
    assert!(
        early_drop.is_empty() && early_recreate.is_empty(),
        "Pushes answered OK before their row was in ClickHouse or the DLQ: after the drop \
         {early_drop:?}, after the recreate {early_recreate:?}"
    );
    let landed_after_drop: Vec<(u64, String)> = (10..=13)
        .filter_map(|id| Some((id, names_after_drop.get(&id)?.clone())))
        .collect();
    assert_eq!(
        (after_drop.len(), landed_after_drop),
        (
            4,
            (10..=13)
                .map(|id| (id, format!("n{id}")))
                .collect::<Vec<_>>()
        ),
        "rows sent after the column drop did not land as sent"
    );
    let numeric_landed: Vec<(u64, String)> = (20..=22)
        .filter_map(|id| Some((id, names_after_recreate.get(&id)?.clone())))
        .collect();
    assert_eq!(
        numeric_landed,
        (20..=22).map(|id| (id, id.to_string())).collect::<Vec<_>>(),
        "numbers sent after the recreate did not land as sent"
    );
    let stored_words: Vec<(u64, String)> = (30..=32)
        .filter_map(|id| Some((id, names_after_recreate.get(&id)?.clone())))
        .collect();
    assert!(
        stored_words.is_empty(),
        "words the numeric column cannot hold were stored as {stored_words:?}"
    );
    let wordy_ids: BTreeSet<u64> = wordy.iter().map(|(id, _)| *id).collect();
    assert!(
        wordy_ids.is_subset(&dead_lettered) && wordy_ids.is_subset(&after_recreate),
        "the rows the new table cannot hold were not answered once dead-lettered: accepted \
         {after_recreate:?}, dead-lettered {dead_lettered:?}"
    );
}

/// A loopback proxy to `ClickHouse` HTTP that holds every connection opening
/// with anything but an INSERT for `delay` before passing it on, so every read
/// the loader makes on a fresh connection answers that much later.
async fn slow_read_proxy(upstream: String, delay: Duration) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind(("127.0.0.1", free_port()))
        .await
        .expect("bind proxy");
    let address = listener.local_addr().expect("proxy addr").to_string();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let upstream = upstream.clone();
            tokio::spawn(async move {
                let mut head = vec![0_u8; 4096];
                let Ok(n) = client.read(&mut head).await else {
                    return;
                };
                head.truncate(n);
                if !String::from_utf8_lossy(&head).contains("query=INSERT") {
                    tokio::time::sleep(delay).await;
                }
                let Ok(mut server) = TcpStream::connect(&upstream).await else {
                    return;
                };
                if server.write_all(&head).await.is_err() {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            });
        }
    });
    address
}

/// How long the slow-read proxy holds a read in the slow-schema test.
const SLOW_READ: Duration = Duration::from_secs(3);

/// The loader's request bound in the slow-schema test: below `SLOW_READ`.
const SHORT_REQUEST_TIMEOUT_MS: u64 = 2_000;

#[tokio::test]
async fn every_record_lands_when_schema_reads_answer_slower_than_the_request_bound() {
    let infra = TestInfrastructure::new(test_name!(), true, false).await;
    let clickhouse = clickhouse_address(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = slow_read_proxy(clickhouse.clone(), SLOW_READ).await;

    let table = unique_table_name("grpc_slow_schema");
    create_id_table(&direct, &table, "").await;

    let listen_port = free_port();
    let mut config = grpc_loader(listen_port, proxy, &table);
    config.clickhouse.request_timeout_ms = SHORT_REQUEST_TIMEOUT_MS;
    let (shutdown, loader, client) = start_loader(config, listen_port).await;

    let settle = tokio::time::Instant::now() + Duration::from_secs(60);
    let (accepted, refused) = push_ids(&client, 0..BEFORE, settle, id_payload).await;
    let landed = wait_landed(&clickhouse, &table, &accepted, LANDING_BUDGET).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
        .await;

    assert_eq!(
        accepted.len() as u64,
        BEFORE,
        "with every schema read {SLOW_READ:?} slow against a {SHORT_REQUEST_TIMEOUT_MS} ms \
         bound the listener accepted {} of {BEFORE} records in 60 s, after {refused} retries: \
         the bound never let the schema fetch finish",
        accepted.len()
    );
    let missing: Vec<u64> = accepted.difference(&landed).copied().collect();
    assert!(
        missing.is_empty(),
        "accepted records never landed: {missing:?}"
    );
}
