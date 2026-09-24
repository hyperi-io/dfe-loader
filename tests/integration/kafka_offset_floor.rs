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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dfe_loader::clickhouse::InsertFormat;
use dfe_loader::config::Config;
use dfe_loader::pipeline::Orchestrator;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use testcontainers_modules::kafka::apache::KAFKA_PORT;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::grpc_outage::{
    SIGXFSZ, clickhouse_address, dead_lettered_ids, file_size_soft_limit, query_client,
    set_file_size_soft_limit, wait_dead_lettered, wait_landed, with_file_dlq,
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
async fn kafka_bootstrap(infra: &TestInfrastructure) -> String {
    let container = infra.kafka.as_ref().expect("Kafka container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("Kafka port mapping");
    let bootstrap = format!("{host}:{port}");
    wait_for_broker(&bootstrap).await;
    bootstrap
}

/// A Kafka-transport loader reading `topic` as `group` and writing to
/// `default.{table}` through `clickhouse`.
fn kafka_loader(
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
fn start_loader(config: Config) -> (CancellationToken, JoinHandle<()>) {
    let mut orchestrator = Orchestrator::new(config);
    let shutdown = orchestrator.shutdown_token();
    let loader = tokio::spawn(async move {
        let _ = orchestrator.run().await;
    });
    (shutdown, loader)
}

/// Produce one record to partition 0 of `topic` and return its offset.
async fn produce(producer: &FutureProducer, topic: &str, payload: &str) -> i64 {
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
async fn wait_committed(bootstrap: &str, group: &str, topic: &str, want: i64) -> Option<i64> {
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

    // An empty table name fails routing, so only the DLQ can take this record.
    let dead_letter = produce(&producer, &topic, r#"{"_source":"","id":1}"#).await;
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
