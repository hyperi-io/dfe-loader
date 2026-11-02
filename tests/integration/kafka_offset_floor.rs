// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! No Kafka offset is committed past a record the loader has not placed.
//!
//! A commit is a per-partition watermark, so a later record committing past one
//! still held in memory loses that one to a crash. Each test runs the real
//! orchestrator on the Kafka transport against Kafka and `ClickHouse`
//! containers, keeps one record from being placed -- its table schema cannot be
//! fetched, its insert fails, its table is dropped, or the DLQ refuses every
//! write -- lets a later record on the same partition insert, and reads the
//! consumer group's committed offset.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::common::containers::{
    KAFKA_PORT, restart_clickhouse, start_clickhouse_on, stop_container,
};
use dfe_loader::clickhouse::InsertFormat;
use dfe_loader::config::{Config, SharedConfig};
use dfe_loader::metrics::Metrics;
use dfe_loader::pipeline::Orchestrator;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use scalo::memory::UsageSource;
use scalo::metrics::MetricsManager;
use scalo::{MemoryGuard, MemoryGuardConfig, SelfRegulationConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::grpc_outage::{
    SIGXFSZ, STOPPED, clickhouse_address, counter_value, dead_lettered_ids, file_size_soft_limit,
    free_port, query_client, readiness_state, set_file_size_soft_limit, start_ready,
    too_deep_record, wait_dead_lettered, wait_landed, wait_metric, watch_readiness, with_file_dlq,
};
use super::kafka_transport_e2e::{ensure_topic, make_producer, wait_for_broker};
use crate::common::containers::TestInfrastructure;
use crate::common::unique_table_name;
use crate::test_name;

/// How long a test watches the committed offset after the later record lands:
/// several flush cycles, so any commit that insert made has been sent.
const WATCH: Duration = Duration::from_secs(5);

/// How long a record has to land while nothing is broken.
const BUDGET: Duration = Duration::from_secs(90);

/// How long a held record has to be placed, and the watermark to move past
/// it, once the fault clears: well past the hold's retry schedule.
const RECOVERY: Duration = Duration::from_secs(30);

/// How long the DLQ refuses every write.
const DLQ_DOWN: Duration = Duration::from_secs(10);

/// Records parked waiting on their schema when the loader is stopped.
const PARKED: u64 = 20;

/// The lowest id a table's constraint rejects, so `ClickHouse` rejects a row
/// carrying it for good.
const REJECTED_ID: u64 = 1_000_000;

/// How long the loader trusts a cached schema in the dropped-table test: long
/// enough that the record after the drop is still buffered for the old table.
const SCHEMA_TTL: Duration = Duration::from_secs(20);

/// A loopback proxy to `ClickHouse` HTTP that can refuse every connection
/// opening with anything but an INSERT, so schema fetches fail while inserts
/// still land.
///
/// Inserts carry their SQL in the request line (`query=INSERT...`); every read
/// the loader makes sends its SQL in the body, so the first request line on a
/// connection tells the two apart.
struct ReadRefusingProxy {
    port: u16,
    refuse: Arc<AtomicBool>,
    cut: watch::Sender<u64>,
}

impl ReadRefusingProxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let refuse = Arc::new(AtomicBool::new(false));
        let (cut, _) = watch::channel(0_u64);

        let accept_refuse = Arc::clone(&refuse);
        let accept_cut = cut.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let refuse = Arc::clone(&accept_refuse);
                let upstream = upstream.clone();
                let mut cut_rx = accept_cut.subscribe();
                tokio::spawn(async move {
                    let mut client = client;
                    let mut head = Vec::new();
                    if refuse.load(Ordering::SeqCst) {
                        match read_request_line(&mut client, &mut head).await {
                            Some(line) if line.contains("query=INSERT") => {}
                            _ => return,
                        }
                    }
                    let Ok(mut server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    if server.write_all(&head).await.is_err() {
                        return;
                    }
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = cut_rx.changed() => {}
                    }
                });
            }
        });

        Self { port, refuse, cut }
    }

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Sever every open connection and refuse any new one that is not an insert.
    fn refuse_reads(&self) {
        self.refuse.store(true, Ordering::SeqCst);
        self.cut.send_modify(|n| *n += 1);
    }

    fn allow_reads(&self) {
        self.refuse.store(false, Ordering::SeqCst);
    }
}

/// A loopback proxy to `ClickHouse` HTTP that can stall: every connection opened
/// while stalled is held open and never answered, so a request on it ends only
/// at the loader's own bound. Counts the inserts it has stalled.
struct StallingProxy {
    port: u16,
    stall: Arc<AtomicBool>,
    stalled_inserts: Arc<AtomicUsize>,
    cut: watch::Sender<u64>,
}

impl StallingProxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let stall = Arc::new(AtomicBool::new(false));
        let stalled_inserts = Arc::new(AtomicUsize::new(0));
        let (cut, _) = watch::channel(0_u64);

        let accept_stall = Arc::clone(&stall);
        let accept_count = Arc::clone(&stalled_inserts);
        let accept_cut = cut.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let stall = Arc::clone(&accept_stall);
                let count = Arc::clone(&accept_count);
                let upstream = upstream.clone();
                let mut cut_rx = accept_cut.subscribe();
                tokio::spawn(async move {
                    let mut client = client;
                    if stall.load(Ordering::SeqCst) {
                        let mut head = Vec::new();
                        if read_request_line(&mut client, &mut head)
                            .await
                            .is_some_and(|line| line.contains("query=INSERT"))
                        {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                        let _ = cut_rx.changed().await;
                        return;
                    }
                    let Ok(mut server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = cut_rx.changed() => {}
                    }
                });
            }
        });

        Self {
            port,
            stall,
            stalled_inserts,
            cut,
        }
    }

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Sever every open connection and hold every new one unanswered.
    fn stall(&self) {
        self.stall.store(true, Ordering::SeqCst);
        self.cut.send_modify(|n| *n += 1);
    }

    /// Release the held connections and forward again.
    fn resume(&self) {
        self.stall.store(false, Ordering::SeqCst);
        self.cut.send_modify(|n| *n += 1);
    }

    fn stalled_inserts(&self) -> usize {
        self.stalled_inserts.load(Ordering::SeqCst)
    }
}

/// Read from `stream` into `head` until it holds the first request line, which
/// is returned. `None` if the connection closes first or sends no line.
async fn read_request_line(stream: &mut TcpStream, head: &mut Vec<u8>) -> Option<String> {
    let mut chunk = [0_u8; 4096];
    loop {
        if let Some(end) = head.iter().position(|&b| b == b'\n') {
            return Some(String::from_utf8_lossy(&head[..end]).into_owned());
        }
        if head.len() > 64 * 1024 {
            return None;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        head.extend_from_slice(&chunk[..n]);
    }
}

/// The Kafka container's bootstrap address, once the broker serves consumers.
pub(super) async fn kafka_bootstrap(infra: &TestInfrastructure) -> String {
    let container = infra.kafka.as_ref().expect("Kafka container");
    let host = container.host();
    let port = container.port(KAFKA_PORT);
    let bootstrap = format!("{host}:{port}");
    wait_for_broker(&bootstrap).await;
    bootstrap
}

/// A Kafka-transport loader reading `topic` as `group` and writing to
/// `default.{table}` through `clickhouse`.
pub(super) fn kafka_loader(
    bootstrap: &str,
    topic: &str,
    group: &str,
    clickhouse: String,
    table: &str,
) -> Config {
    let mut config = Config::default();
    config.transport = "kafka".to_string();
    config.kafka.brokers = vec![bootstrap.to_string()];
    config.kafka.topics = vec![topic.to_string()];
    config.kafka.group = group.to_string();
    config.kafka.allow_insecure_transport = true;
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

/// Run the orchestrator until the returned token is cancelled.
pub(super) fn start_loader(config: Config) -> (CancellationToken, JoinHandle<()>) {
    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });
    (shutdown, loader)
}

/// Produce one record to partition 0 of `topic` and return its offset.
pub(super) async fn produce(producer: &FutureProducer, topic: &str, payload: &str) -> i64 {
    let record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(payload).partition(0);
    let delivery = producer
        .send(record, Duration::from_secs(30))
        .await
        .unwrap_or_else(|(e, _)| panic!("produce to {topic}: {e}"));
    delivery.offset
}

/// Produce `{"id":0}` for the default table, and wait for it to land: the
/// loader is consuming from here.
async fn wait_for_loader(producer: &FutureProducer, topic: &str, clickhouse: &str, table: &str) {
    produce(producer, topic, r#"{"id":0}"#).await;
    let first = BTreeSet::from([0]);
    let landed = wait_landed(clickhouse, table, &first, BUDGET).await;
    assert!(first.is_subset(&landed), "the first record never landed");
}

/// The group's committed offset on partition 0 of `topic`: the next offset it
/// reads. `None` before any commit.
async fn committed_offset(bootstrap: &str, group: &str, topic: &str) -> Option<i64> {
    let (bootstrap, group, topic) = (bootstrap.to_string(), group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let reader: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .create()
            .expect("offset reader");
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&topic, 0);
        let committed = reader
            .committed_offsets(partitions, Duration::from_secs(10))
            .ok()?;
        match committed.find_partition(&topic, 0)?.offset() {
            Offset::Offset(next) => Some(next),
            _ => None,
        }
    })
    .await
    .expect("offset reader task")
}

/// The highest committed offset seen over `WATCH`.
async fn highest_committed_over_watch(bootstrap: &str, group: &str, topic: &str) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + WATCH;
    let mut highest = None;
    while tokio::time::Instant::now() < deadline {
        highest = highest.max(committed_offset(bootstrap, group, topic).await);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    highest
}

/// Wait until the committed offset reaches `want`, returning the last read.
pub(super) async fn wait_committed(
    bootstrap: &str,
    group: &str,
    topic: &str,
    want: i64,
) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + RECOVERY;
    loop {
        let committed = committed_offset(bootstrap, group, topic).await;
        if committed >= Some(want) || tokio::time::Instant::now() >= deadline {
            return committed;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_record_waiting_on_its_schema_holds_the_commit_below_it() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = ReadRefusingProxy::start(clickhouse.clone()).await;

    let landing = unique_table_name("kafka_floor_landing");
    let source = unique_table_name("kafka_floor_source");
    for table in [&landing, &source] {
        direct
            .execute(&format!(
                "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("create table");
    }
    // No `_land` / `_load` suffix, so the topic names no source of its own.
    let topic = format!("floor-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let config = kafka_loader(&bootstrap, &topic, &group, proxy.address(), &landing);
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    // The source table is new to the loader, and its schema fetch now fails.
    proxy.refuse_reads();
    let parked = produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{source}","id":1}}"#),
    )
    .await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let later_landed = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    let while_parked = highest_committed_over_watch(&bootstrap, &group, &topic).await;

    // Once the schema can be fetched the parked record lands, and a record
    // after it moves the watermark past both.
    proxy.allow_reads();
    let parked_landed = wait_landed(&clickhouse, &source, &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":3}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&landing, &source] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(
        later_landed.contains(&2),
        "the record after the parked one never landed"
    );
    assert!(
        while_parked.is_some_and(|next| next <= parked),
        "the group committed {while_parked:?} while the record at offset {parked} waited on \
         its schema: a crash then would have lost it"
    );
    assert!(
        parked_landed.contains(&1),
        "the parked record never landed once its schema could be fetched"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the parked record once it landed: {after:?}"
    );
}

/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_letter_the_dlq_refuses_holds_the_commit_below_it() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_floor_dlq");
    direct
        .execute(&format!(
            "CREATE TABLE default.{landing} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("floor-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");

    // Nested past the depth bound, so only the DLQ can take this record.
    let dead_letter = produce(&producer, &topic, &too_deep_record(1)).await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let later_landed = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    let while_refused = highest_committed_over_watch(&bootstrap, &group, &topic).await;
    tokio::time::sleep(DLQ_DOWN.saturating_sub(WATCH)).await;
    set_file_size_soft_limit(&unlimited);

    let dead_lettered = wait_dead_lettered(dlq_dir.path(), &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":3}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{landing}"))
        .await;

    assert!(
        later_landed.contains(&2),
        "the record after the dead letter never landed"
    );
    assert!(
        while_refused.is_some_and(|next| next <= dead_letter),
        "the group committed {while_refused:?} while the DLQ refused the record at offset \
         {dead_letter}: it was in neither the DLQ nor a re-delivery"
    );
    assert!(
        dead_lettered.contains(&1),
        "the dead letter never reached the DLQ once it took writes again"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the dead letter once the DLQ took it: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_record_waiting_on_its_schema_is_lost_to_a_shutdown() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = ReadRefusingProxy::start(clickhouse.clone()).await;

    let landing = unique_table_name("kafka_stop_landing");
    let source = unique_table_name("kafka_stop_source");
    direct
        .execute(&format!(
            "CREATE TABLE default.{landing} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("stop-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let mut config = kafka_loader(&bootstrap, &topic, &group, proxy.address(), &landing);
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    proxy.refuse_reads();
    let parked: BTreeSet<u64> = (1..=PARKED).collect();
    for id in &parked {
        produce(
            &producer,
            &topic,
            &format!(r#"{{"_source":"{source}","id":{id}}}"#),
        )
        .await;
    }
    // One partition is read in order, so once this lands every parked record
    // has been received.
    let marker = PARKED + 1;
    produce(&producer, &topic, &format!(r#"{{"id":{marker}}}"#)).await;
    let marker_landed = wait_landed(&clickhouse, &landing, &BTreeSet::from([marker]), BUDGET).await;

    shutdown.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(60), loader).await;
    let dead_lettered = dead_lettered_ids(dlq_dir.path());
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{landing}"))
        .await;

    assert!(
        marker_landed.contains(&marker),
        "the record after the parked ones never landed"
    );
    assert!(
        marker_landed.is_disjoint(&parked),
        "records meant to wait on their schema landed in the default table instead"
    );
    assert!(stopped.is_ok(), "the loader did not stop within 60s");
    let lost: Vec<u64> = parked.difference(&dead_lettered).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {PARKED} records waiting on their schema were in neither ClickHouse nor the DLQ \
         after a clean stop: {lost:?}",
        lost.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_whose_insert_failed_holds_the_commit_below_it_until_it_lands() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_held_landing");
    let failing = unique_table_name("kafka_held_failing");
    let create = |table: &str| {
        format!("CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id")
    };
    for table in [&landing, &failing] {
        direct.execute(&create(table)).await.expect("create table");
    }
    let topic = format!("held-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    // A record lands in the second table first, so the loader holds its schema
    // and buffers the next one straight to it.
    produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{failing}","id":100}}"#),
    )
    .await;
    let warmed = wait_landed(&clickhouse, &failing, &BTreeSet::from([100]), BUDGET).await;

    // With the table gone its insert fails, which ClickHouse can recover from,
    // so the batch comes back for another attempt.
    direct
        .execute(&format!("DROP TABLE default.{failing} SYNC"))
        .await
        .expect("drop table");
    let held = produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{failing}","id":1}}"#),
    )
    .await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let first_later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    // Produced after id 2 landed, so it inserts in a later flush cycle than the
    // failed batch, whose buffer is older than id 2's.
    produce(&producer, &topic, r#"{"id":3}"#).await;
    let later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2, 3]), BUDGET).await;
    let while_held = highest_committed_over_watch(&bootstrap, &group, &topic).await;

    // Once the table is back the held batch lands, and a record after it moves
    // the watermark past both.
    direct
        .execute(&create(&failing))
        .await
        .expect("re-create table");
    let held_landed = wait_landed(&clickhouse, &failing, &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":4}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&landing, &failing] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(
        warmed.contains(&100),
        "the record that caches the second table's schema never landed"
    );
    assert!(
        first_later.contains(&2) && later.contains(&3),
        "the records after the failed insert never landed: {later:?}"
    );
    assert!(
        while_held.is_some_and(|next| next <= held),
        "the group committed {while_held:?} while the batch holding offset {held} had not \
         landed: a crash then would have lost it"
    );
    assert!(
        held_landed.contains(&1),
        "the held batch never landed once its table was back"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the held batch once it landed: {after:?}"
    );
}

/// How long an insert waits for `ClickHouse`'s answer in the stalled-insert test.
const SHORT_INSERT_TIMEOUT_MS: u64 = 2_000;

/// How long the stalled-insert test waits for the loader to retry a stalled
/// insert: several insert bounds plus the hold's retry schedule.
const RETRY_SEEN: Duration = Duration::from_secs(45);

#[tokio::test(flavor = "multi_thread")]
async fn an_insert_clickhouse_never_answers_times_out_and_is_held_until_it_lands() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);
    let proxy = StallingProxy::start(clickhouse.clone()).await;

    let landing = unique_table_name("kafka_stall_landing");
    direct
        .execute(&format!(
            "CREATE TABLE default.{landing} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("stall-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let mut config = kafka_loader(&bootstrap, &topic, &group, proxy.address(), &landing);
    config.clickhouse.insert_timeout_ms = SHORT_INSERT_TIMEOUT_MS;
    config.clickhouse.request_timeout_ms = SHORT_INSERT_TIMEOUT_MS;
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    // From here ClickHouse takes every request and answers none.
    proxy.stall();
    let held = produce(&producer, &topic, r#"{"id":1}"#).await;

    // A second stalled insert means the first ended at the loader's bound and
    // the held batch was sent again.
    let deadline = tokio::time::Instant::now() + RETRY_SEEN;
    while proxy.stalled_inserts() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let stalled_inserts = proxy.stalled_inserts();
    let while_held = highest_committed_over_watch(&bootstrap, &group, &topic).await;

    // Once ClickHouse answers again the held batch lands, and a record after it
    // moves the watermark past both.
    proxy.resume();
    let held_landed = wait_landed(&clickhouse, &landing, &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":2}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{landing}"))
        .await;

    assert!(
        stalled_inserts >= 2,
        "the loader sent {stalled_inserts} insert(s) into the stall in {RETRY_SEEN:?}: an \
         unanswered insert never reached its {SHORT_INSERT_TIMEOUT_MS} ms bound"
    );
    assert!(
        while_held.is_none_or(|next| next <= held),
        "the group committed {while_held:?} while the batch holding offset {held} had not \
         landed: a crash then would have lost it"
    );
    assert!(
        held_landed.contains(&1),
        "the timed-out batch never landed once ClickHouse answered again"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the timed-out batch once it landed: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_held_for_a_dropped_table_follows_new_records_to_the_default_table() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_gone_landing");
    let gone = unique_table_name("kafka_gone_source");
    for table in [&landing, &gone] {
        direct
            .execute(&format!(
                "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("create table");
    }
    let topic = format!("gone-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    config.schema.cache_ttl_secs = SCHEMA_TTL.as_secs();
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    // The loader caches the second table's schema as this record lands.
    produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{gone}","id":100}}"#),
    )
    .await;
    let warmed = wait_landed(&clickhouse, &gone, &BTreeSet::from([100]), BUDGET).await;
    let cached = tokio::time::Instant::now();

    // Dropped for good while the cached schema still names it, so the next
    // record is buffered for it and its failed insert is held.
    direct
        .execute(&format!("DROP TABLE default.{gone} SYNC"))
        .await
        .expect("drop table");
    let held = produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{gone}","id":1}}"#),
    )
    .await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    let while_held = highest_committed_over_watch(&bootstrap, &group, &topic).await;

    // Past the cache TTL a record for the table finds it gone and falls back to
    // the default table, and the held batch must follow it there.
    tokio::time::sleep_until(cached + SCHEMA_TTL).await;
    produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{gone}","id":3}}"#),
    )
    .await;
    let fell_back = wait_landed(&clickhouse, &landing, &BTreeSet::from([3]), BUDGET).await;
    let followed = wait_landed(&clickhouse, &landing, &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":4}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&landing, &gone] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(
        warmed.contains(&100),
        "the record that caches the second table's schema never landed"
    );
    assert!(
        later.contains(&2),
        "the record after the held batch never landed"
    );
    assert!(
        while_held.is_some_and(|next| next <= held),
        "the group committed {while_held:?} while the batch holding offset {held} had not \
         landed: a crash then would have lost it"
    );
    assert!(
        fell_back.contains(&3),
        "a record for the dropped table never fell back to the default table"
    );
    assert!(
        followed.contains(&1),
        "the batch held for the dropped table never reached the default table, so its \
         partition's commit stays frozen until a restart"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the held batch once it landed: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_held_for_a_removed_source_follows_it_to_the_default_table() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_removed_landing");
    let removed = unique_table_name("kafka_removed_source");
    for table in [&landing, &removed] {
        direct
            .execute(&format!(
                "CREATE TABLE default.{table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("create table");
    }
    let topic = format!("removed-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    // The source is routed by name, as dfe-engine routes every source it
    // defines, and its schema stays cached for the whole test.
    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    config
        .routing
        .source_to_table
        .insert(removed.clone(), removed.clone());
    let shared = SharedConfig::new(config.clone());
    let mut orchestrator = Orchestrator::new(config.clone()).with_shared_config(shared.clone());
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        if let Err(error) = orchestrator.run().await {
            tracing::error!(%error, "the loader stopped with an error");
        }
    });
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{removed}","id":100}}"#),
    )
    .await;
    let warmed = wait_landed(&clickhouse, &removed, &BTreeSet::from([100]), BUDGET).await;

    // The source is removed with a record in flight: its table goes first, so
    // the record's insert fails and is held, then the routing drops it.
    direct
        .execute(&format!("DROP TABLE default.{removed} SYNC"))
        .await
        .expect("drop table");
    let held = produce(
        &producer,
        &topic,
        &format!(r#"{{"_source":"{removed}","id":1}}"#),
    )
    .await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    let while_held = highest_committed_over_watch(&bootstrap, &group, &topic).await;

    config.routing.source_to_table.clear();
    shared.update(config);
    let followed = wait_landed(&clickhouse, &landing, &BTreeSet::from([1]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":3}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    for table in [&landing, &removed] {
        let _ = direct
            .execute(&format!("DROP TABLE IF EXISTS default.{table}"))
            .await;
    }

    assert!(
        warmed.contains(&100),
        "the record that caches the source's schema never landed"
    );
    assert!(
        later.contains(&2),
        "the record after the held batch never landed"
    );
    assert!(
        while_held.is_some_and(|next| next <= held),
        "the group committed {while_held:?} while the batch holding offset {held} had not \
         landed: a crash then would have lost it"
    );
    assert!(
        followed.contains(&1),
        "the batch held for the removed source never reached the default table, so its \
         partition's commit stays frozen"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the held batch once it landed: {after:?}"
    );
}

/// Needs process-per-test (nextest): the file size limit is process-wide.
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_row_the_dlq_refuses_holds_the_commit_below_it() {
    let infra = TestInfrastructure::new(test_name!(), true, true).await;
    let clickhouse = clickhouse_address(&infra).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_floor_reject");
    direct
        .execute(&format!(
            "CREATE TABLE default.{landing} (id UInt64, \
             CONSTRAINT below_limit CHECK id < {REJECTED_ID}) \
             ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("floor-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    let dlq_dir = tempfile::tempdir().expect("DLQ spool directory");
    let mut config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    // JSONEachRow classifies by the server's code, and 469 VIOLATED_CONSTRAINT
    // is a permanent rejection there.
    config.clickhouse.insert_format = InsertFormat::JsonEachRow;
    with_file_dlq(&mut config, dlq_dir.path());
    let (shutdown, loader) = start_loader(config);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;

    let _xfsz = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(SIGXFSZ))
        .expect("handle SIGXFSZ");
    let unlimited = file_size_soft_limit();
    set_file_size_soft_limit("0");

    let rejected = produce(&producer, &topic, &format!(r#"{{"id":{REJECTED_ID}}}"#)).await;
    produce(&producer, &topic, r#"{"id":2}"#).await;
    let first_later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2]), BUDGET).await;
    // Produced after id 2 landed, so it inserts in a later flush cycle than the
    // rejected row.
    produce(&producer, &topic, r#"{"id":3}"#).await;
    let later = wait_landed(&clickhouse, &landing, &BTreeSet::from([2, 3]), BUDGET).await;
    let while_refused = highest_committed_over_watch(&bootstrap, &group, &topic).await;
    set_file_size_soft_limit(&unlimited);

    let dead_lettered =
        wait_dead_lettered(dlq_dir.path(), &BTreeSet::from([REJECTED_ID]), RECOVERY).await;
    let last = produce(&producer, &topic, r#"{"id":4}"#).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    let _ = direct
        .execute(&format!("DROP TABLE IF EXISTS default.{landing}"))
        .await;

    assert!(
        first_later.contains(&2) && later.contains(&3),
        "the records after the rejected row never landed: {later:?}"
    );
    assert!(
        while_refused.is_some_and(|next| next <= rejected),
        "the group committed {while_refused:?} while the DLQ refused the rejected row at offset \
         {rejected}: it was in neither the DLQ nor a re-delivery"
    );
    assert!(
        dead_lettered.contains(&REJECTED_ID),
        "the rejected row never reached the DLQ once it took writes again"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the rejected row once the DLQ took it: {after:?}"
    );
}

/// Records produced while `ClickHouse` is stopped: several times what the
/// restart test's memory guard holds before intake pauses.
const PRODUCED_WHILE_STOPPED: u64 = 3_000;

/// The memory guard limit in the restart test, reached by about a third of
/// [`PRODUCED_WHILE_STOPPED`].
const GUARD_LIMIT: u64 = 256 * 1024;

/// How long the received count has to hold still to read as paused.
const QUIET: Duration = Duration::from_secs(3);

/// A record of about 230 bytes for the default table.
fn padded_record(id: u64) -> String {
    format!(r#"{{"id":{id},"pad":"{}"}}"#, "x".repeat(200))
}

/// Records the loader has received, once that count has held still for
/// [`QUIET`], or the last count read when `budget` runs out first.
async fn received_once_still(manager: &MetricsManager, budget: Duration) -> f64 {
    let deadline = tokio::time::Instant::now() + budget;
    let mut last = counter_value(manager, "messages_received_total", "");
    let mut since = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = counter_value(manager, "messages_received_total", "");
        if (now - last).abs() > f64::EPSILON {
            (last, since) = (now, tokio::time::Instant::now());
        } else if since.elapsed() >= QUIET {
            return now;
        }
        if tokio::time::Instant::now() >= deadline {
            return now;
        }
    }
}

/// Sample `guard` until the returned token is cancelled, keeping the most it
/// held.
fn watch_reserved(guard: &Arc<MemoryGuard>) -> (CancellationToken, JoinHandle<u64>) {
    let stop = CancellationToken::new();
    let (guard, until) = (Arc::clone(guard), stop.clone());
    let watcher = tokio::spawn(async move {
        let mut most = 0_u64;
        while !until.is_cancelled() {
            most = most.max(guard.reserved_bytes());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        most
    });
    (stop, watcher)
}

/// `ClickHouse` stopped and started again under a Kafka loader. The commit
/// stays below the first record that could not land, the governor pauses the
/// consumer before the guard's reservations pass its limit, the gauge reads
/// the outage, `/readyz` stays ready, and once the server is back the same
/// loader lands every record and commits past them.
#[tokio::test(flavor = "multi_thread")]
async fn a_clickhouse_restart_holds_the_commit_and_pauses_intake_without_a_restart() {
    let http_port = free_port();
    let server = start_clickhouse_on(test_name!(), http_port).await;
    let clickhouse = format!("{}:{http_port}", server.container.host);
    let infra = TestInfrastructure::new(test_name!(), false, true).await;
    let bootstrap = kafka_bootstrap(&infra).await;
    let direct = query_client(&clickhouse);

    let landing = unique_table_name("kafka_restart");
    direct
        .execute(&format!(
            "CREATE TABLE default.{landing} (id UInt64) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("create table");
    let topic = format!("restart-{landing}");
    let group = format!("dfe-{landing}");
    ensure_topic(&bootstrap, &topic).await;

    // Pressure is read from what the loader reserves, so nothing else on the
    // host moves it. A zero max hold keeps the pause until pressure falls,
    // rather than admitting a batch every 30 s.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: GUARD_LIMIT,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let governor = SelfRegulationConfig {
        max_hold_secs: 0,
        ..Default::default()
    }
    .build(Arc::clone(&guard))
    .expect("self-regulation is on by default");
    let manager = MetricsManager::new("loader_it");
    let metrics = Metrics::new(&manager);
    let state = readiness_state(metrics.clone());
    let config = kafka_loader(&bootstrap, &topic, &group, clickhouse.clone(), &landing);
    let orchestrator = Orchestrator::with_metrics(config, metrics)
        .with_memory_guard(Arc::clone(&guard))
        .with_governor(Some(governor));
    let (shutdown, loader) = start_ready(orchestrator, &state);
    let producer = make_producer(&bootstrap);
    wait_for_loader(&producer, &topic, &clickhouse, &landing).await;
    let up_before = wait_metric(&manager, "clickhouse_connected", 1.0, BUDGET).await;
    let errors_before = counter_value(&manager, "clickhouse_insert_errors_total", "");
    let (stop_watching, readiness) = watch_readiness(&state);
    let (stop_sampling, reserved) = watch_reserved(&guard);

    stop_container(&server.container).await;
    let first_held = produce(&producer, &topic, &padded_record(1)).await;
    let mut last = first_held;
    for id in 2..=PRODUCED_WHILE_STOPPED {
        last = produce(&producer, &topic, &padded_record(id)).await;
    }
    let down = wait_metric(&manager, "clickhouse_connected", 0.0, STOPPED).await;
    // The record wait_for_loader landed counts as received too.
    let produced = PRODUCED_WHILE_STOPPED as f64 + 1.0;
    let paused_at = received_once_still(&manager, BUDGET).await;
    let while_stopped = highest_committed_over_watch(&bootstrap, &group, &topic).await;
    let after_watch = counter_value(&manager, "messages_received_total", "");
    let errors_during = counter_value(&manager, "clickhouse_insert_errors_total", "");

    restart_clickhouse(&server.container).await;
    let every_id: BTreeSet<u64> = (0..=PRODUCED_WHILE_STOPPED).collect();
    let landed = wait_landed(&clickhouse, &landing, &every_id, BUDGET).await;
    let up_after = wait_metric(&manager, "clickhouse_connected", 1.0, BUDGET).await;
    let after = wait_committed(&bootstrap, &group, &topic, last + 1).await;
    let received_after = counter_value(&manager, "messages_received_total", "");
    stop_sampling.cancel();
    let most_reserved = reserved.await.expect("reservation sampler");
    stop_watching.cancel();
    let unready_samples = readiness.await.expect("readiness watcher");
    let still_running = !loader.is_finished();

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), loader).await;
    drop(server);

    assert!(
        while_stopped.is_some_and(|next| next <= first_held),
        "the group committed {while_stopped:?} while the record at offset {first_held} could \
         not reach ClickHouse: a crash then would have lost it"
    );
    assert!(
        paused_at < produced && (after_watch - paused_at).abs() < f64::EPSILON,
        "intake read {paused_at} of {produced} records, then {after_watch} after the watch: the \
         consumer did not hold its position while the held rows filled the guard"
    );
    assert!(
        most_reserved <= GUARD_LIMIT,
        "the memory guard held {most_reserved} bytes against its {GUARD_LIMIT} byte limit"
    );
    assert!(
        errors_during > errors_before,
        "clickhouse_insert_errors_total stayed at {errors_before} while ClickHouse was stopped"
    );
    assert!(
        (up_before - 1.0).abs() < f64::EPSILON
            && down.abs() < f64::EPSILON
            && (up_after - 1.0).abs() < f64::EPSILON,
        "clickhouse_connected read {up_before} up, {down} stopped and {up_after} restarted"
    );
    assert_eq!(
        unready_samples, 0,
        "readiness read not ready {unready_samples} times while ClickHouse was stopped"
    );
    assert!(still_running, "the loader stopped during the outage");
    let lost: Vec<u64> = every_id.difference(&landed).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} produced records never landed: {lost:?}",
        lost.len(),
        every_id.len()
    );
    assert!(
        (received_after - produced).abs() < f64::EPSILON,
        "the loader received {received_after} of {produced} records once ClickHouse was back"
    );
    assert!(
        after >= Some(last + 1),
        "the watermark never moved past the held records once they landed: {after:?}"
    );
}
