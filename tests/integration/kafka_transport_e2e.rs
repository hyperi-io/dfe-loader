// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! End-to-end integration tests for `KafkaTransportAdapter` and
//! `TransportBackend::Kafka` using scope-local testcontainers.
//!
//! These tests exercise the real Kafka code paths in
//! `src/kafka/transport.rs` — the bulk of which cannot be covered by
//! `MemoryTransportAdapter` unit tests. Each test spins up its own
//! isolated Kafka container via `TestInfrastructure`. The container
//! drops automatically at end of scope (`ContainerAsync` has a
//! blocking-drop that stops & removes the container).
//!
//! Gated behind `#[cfg(feature = "testcontainers")]` — enable with:
//! ```bash
//! cargo test --features testcontainers --test integration_tests
//! ```
//!
//! Coverage goals (`src/kafka/transport.rs`):
//! - `TransportAdapter::new` / `convert_config`
//! - `TransportAdapter::recv` (empty, with data, max limit)
//! - `TransportAdapter::commit` (offsets persisted across consumer restarts)
//! - `TransportBackend::from_config` dispatch to Kafka variant
//! - Error handling on invalid brokers
//!
//! Test messages are produced with the `rdkafka` crate directly — the
//! `TransportAdapter` is consumer-only, so we need a separate producer to
//! inject test traffic into the broker.

#![cfg(feature = "testcontainers")]

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};

use dfe_loader::buffer::KafkaOffset;
use dfe_loader::config::{Config, KafkaConfig};
use dfe_loader::kafka::{TransportAdapter, TransportBackend};

use crate::common::containers::TestInfrastructure;

// ============================================================================
// Helpers
// ============================================================================

/// Spin up an isolated Kafka container and return it along with the
/// bootstrap broker string and a ready-to-use loader `KafkaConfig`.
///
/// Keep `infra` in scope until the end of the test — dropping it stops and
/// removes the container.
async fn spin_up_kafka(
    group: &str,
    topics: Vec<String>,
) -> (TestInfrastructure, String, KafkaConfig) {
    let infra = TestInfrastructure::new(false, true).await;
    let container = infra
        .kafka
        .as_ref()
        .expect("Kafka container must be running");

    let host = container.get_host().await.expect("container get_host");
    // The module's own constant, not a literal -- 9093 was the Confluent
    // image's broker port and silently stopped existing when the harness moved
    // to the Apache one.
    let port = container
        .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
        .await
        .expect("Kafka port mapping");
    let bootstrap = format!("{host}:{port}");
    wait_for_broker(&bootstrap).await;

    let config = KafkaConfig {
        brokers: vec![bootstrap.clone()],
        group: group.to_string(),
        topics,
        topic_regex: None,
        client_id: "dfe-loader-e2e".to_string(),
        sasl: None,
        tls: None,
        ..Default::default()
    };

    (infra, bootstrap, config)
}

/// Create a plain (no-SASL) rdkafka `FutureProducer` for the given bootstrap
/// broker.
fn make_producer(bootstrap: &str) -> FutureProducer {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap);
    // 30 s gives auto.create.topics.enable enough time to create the topic
    // on the first produce (metadata fetch + leader election + produce retry).
    cfg.set("message.timeout.ms", "30000");
    cfg.create().expect("producer creation")
}

/// Produce `payloads` to `topic` via an rdkafka `FutureProducer`. Each send is
/// awaited to completion so the test sees stable offsets before consuming.
async fn produce(producer: &FutureProducer, topic: &str, payloads: &[Vec<u8>]) {
    for (i, payload) in payloads.iter().enumerate() {
        let record: FutureRecord<str, [u8]> = FutureRecord::to(topic)
            .payload(payload.as_slice())
            .partition(-1);
        producer
            .send(record, Duration::from_secs(10))
            .await
            .unwrap_or_else(|(e, _)| panic!("Failed to produce message {i} to {topic}: {e}"));
    }
}

/// Best-effort pre-create a topic with a short `linger` so auto-create races
/// don't cost the test valuable wall time. Errors are logged but not fatal —
/// `auto.create.topics.enable` will catch the rest.
async fn ensure_topic(bootstrap: &str, topic: &str) {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap);
    let admin: AdminClient<_> = match cfg.create() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("admin client create failed: {e} — falling back to auto-create");
            return;
        }
    };
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let _ = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await;
}

/// Block until the broker actually answers a metadata request.
///
/// Container "ready" is not the same as "serving". testcontainers returns as
/// soon as its wait strategy is satisfied, which for the native Kafka image is
/// a second or so before the listener accepts clients. Everything downstream
/// then fails with BrokerTransportFailure, and because `ensure_topic` swallows
/// its error the symptom surfaces later as a consumer reading a topic that was
/// never created. The old emulated image hid this: it was so slow that the
/// broker was always up by the time anything connected.
async fn wait_for_broker(bootstrap: &str) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("probe consumer");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        // Metadata alone is not enough. Under KRaft the group coordinator comes
        // up after the broker listener, and a consumer that joins in between
        // gets BrokerTransportFailure. fetch_group_list forces a coordinator
        // lookup, so it only succeeds once group joins will work.
        let serving = consumer
            .fetch_metadata(None, Duration::from_secs(2))
            .is_ok()
            && consumer
                .fetch_group_list(None, Duration::from_secs(2))
                .is_ok();
        if serving {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "broker at {bootstrap} never became ready to serve consumers"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Produce `count` small JSON-ish payloads like `msg-0`, `msg-1`, ... for use
/// in roundtrip tests.
fn gen_payloads(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("msg-{i}").into_bytes())
        .collect()
}

/// Receive messages until `want` have arrived or `timeout` elapses. Returns
/// everything received (which may be less than `want` on timeout).
async fn recv_until(
    adapter: &TransportAdapter,
    want: usize,
    timeout: Duration,
) -> Vec<dfe_loader::kafka::KafkaMessage> {
    let deadline = std::time::Instant::now() + timeout;
    let mut collected = Vec::with_capacity(want);
    while collected.len() < want && std::time::Instant::now() < deadline {
        match adapter
            .recv(want.saturating_sub(collected.len()).max(1))
            .await
        {
            Ok(batch) => collected.extend(batch.messages),
            Err(e) => {
                eprintln!("recv error (retrying): {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    collected
}

// ============================================================================
// Construction
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_construction() {
    let (_infra, _bootstrap, config) =
        spin_up_kafka("construction-group", vec!["construction-topic".to_string()]).await;

    let adapter = TransportAdapter::new(&config, None)
        .await
        .expect("TransportAdapter should build against a live broker");

    assert!(
        adapter.is_healthy(),
        "Freshly constructed adapter should be healthy"
    );
    // Name is a static string owned by the transport implementation.
    let name = adapter.name();
    assert!(!name.is_empty(), "transport name must be non-empty");

    // Close cleanly — exercises the `close()` code path.
    adapter.close().await.expect("close should succeed");
}

// ============================================================================
// recv on empty topic returns empty within poll timeout
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_recv_empty() {
    let topic = "recv-empty-topic";
    let (_infra, bootstrap, config) =
        spin_up_kafka("recv-empty-group", vec![topic.to_string()]).await;

    // Pre-create the topic so the subscription has something to attach to.
    ensure_topic(&bootstrap, topic).await;

    let adapter = TransportAdapter::new(&config, None)
        .await
        .expect("adapter build");

    // What is under test is that an empty topic yields an empty batch promptly
    // instead of hanging -- not that the very first poll is clean. A cold
    // consumer can surface one transient BrokerTransportFailure while it
    // finishes connecting, which librdkafka then heals; every other consumer
    // test here absorbs exactly that via `recv_until`. Retry on error and fail
    // only if it never settles.
    let start = std::time::Instant::now();
    let deadline = start + Duration::from_secs(10);
    let messages = loop {
        match adapter.recv(10).await {
            Ok(batch) => break batch.messages,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "recv never returned cleanly on an empty topic: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    let elapsed = start.elapsed();

    assert!(
        messages.is_empty(),
        "empty topic should yield zero messages, got {}",
        messages.len()
    );
    // Sanity: scalo's default recv timeout is 1s; don't hang for >10s.
    assert!(
        elapsed < Duration::from_secs(10),
        "recv on empty topic took too long: {elapsed:?}"
    );

    adapter.close().await.expect("close");
}

// ============================================================================
// send → recv roundtrip with offset preservation
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_send_recv_roundtrip() {
    let topic = "roundtrip-topic";
    let (_infra, bootstrap, config) =
        spin_up_kafka("roundtrip-group", vec![topic.to_string()]).await;

    ensure_topic(&bootstrap, topic).await;

    // Produce BEFORE the consumer starts — with auto.offset.reset=earliest
    // (scalo default) the consumer will replay these on first poll.
    let payloads = gen_payloads(5);
    let producer = make_producer(&bootstrap);
    produce(&producer, topic, &payloads).await;

    let adapter = TransportAdapter::new(&config, None)
        .await
        .expect("adapter build");

    let messages = recv_until(&adapter, 5, Duration::from_secs(30)).await;
    assert_eq!(
        messages.len(),
        5,
        "Expected 5 messages, got {}",
        messages.len()
    );

    // Payload content preserved (order within a single partition is guaranteed).
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(
            msg.payload,
            format!("msg-{i}").as_bytes(),
            "payload mismatch at index {i}"
        );
        assert_eq!(&*msg.topic, topic, "topic name should match");
        // The Kafka adapter intentionally discards keys; verify that contract.
        assert!(
            msg.key.is_none(),
            "TransportAdapter does not preserve keys from Kafka"
        );
        // Broker assigns timestamps — scalo exposes them.
        assert!(
            msg.timestamp_ms.is_some(),
            "timestamp_ms should be populated by the broker"
        );
    }

    // Offsets are monotonically increasing within a single partition.
    for i in 1..messages.len() {
        assert!(
            messages[i].offset > messages[i - 1].offset,
            "offsets must increase: [{}]={} vs [{}]={}",
            i,
            messages[i].offset,
            i - 1,
            messages[i - 1].offset
        );
    }

    adapter.close().await.expect("close");
}

// ============================================================================
// commit → new consumer does not re-consume already-committed messages
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_commit_offset() {
    let topic = "commit-topic";
    let group = "commit-group";
    let (_infra, bootstrap, config) = spin_up_kafka(group, vec![topic.to_string()]).await;

    ensure_topic(&bootstrap, topic).await;

    let payloads = gen_payloads(6);
    let producer = make_producer(&bootstrap);
    produce(&producer, topic, &payloads).await;

    // --- First consumer: read 3, commit their offsets ---
    let adapter_a = TransportAdapter::new(&config, None)
        .await
        .expect("first adapter");

    let first_batch = recv_until(&adapter_a, 6, Duration::from_secs(30)).await;
    assert!(
        first_batch.len() >= 3,
        "Need at least 3 messages to commit a meaningful offset, got {}",
        first_batch.len()
    );

    // Commit only the first 3 — the consumer acknowledges up to offset of msg[2].
    let offsets: Vec<KafkaOffset> = first_batch
        .iter()
        .take(3)
        .map(|m| KafkaOffset {
            topic: m.topic.clone(),
            partition: m.partition,
            // Kafka commit semantics: offset = "next message to consume", so
            // we commit last_offset + 1.
            offset: m.offset + 1,
        })
        .collect();

    adapter_a
        .commit(&offsets)
        .await
        .expect("commit should succeed");
    adapter_a.close().await.expect("close first adapter");

    // Empty-offset commit is a no-op — exercise the early-return branch.
    let adapter_noop = TransportAdapter::new(&config, None)
        .await
        .expect("noop adapter");
    adapter_noop
        .commit(&[])
        .await
        .expect("commit of empty slice should succeed as no-op");
    adapter_noop.close().await.expect("close noop adapter");

    // --- Second consumer, same group: should NOT see the first 3 ---
    let adapter_b = TransportAdapter::new(&config, None)
        .await
        .expect("second adapter");

    let second_batch = recv_until(&adapter_b, 3, Duration::from_secs(30)).await;

    // The committed offset should make Kafka deliver only messages 3..=5 (3
    // remaining). We assert none of the first 3 payloads reappear.
    for msg in &second_batch {
        for already_committed in first_batch.iter().take(3) {
            assert_ne!(
                msg.offset, already_committed.offset,
                "second consumer must not re-deliver committed offset {}",
                already_committed.offset
            );
        }
    }

    adapter_b.close().await.expect("close second adapter");
}

// ============================================================================
// recv respects the max_messages limit
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_max_messages_limit() {
    let topic = "max-limit-topic";
    let (_infra, bootstrap, config) =
        spin_up_kafka("max-limit-group", vec![topic.to_string()]).await;

    ensure_topic(&bootstrap, topic).await;

    let payloads = gen_payloads(10);
    let producer = make_producer(&bootstrap);
    produce(&producer, topic, &payloads).await;

    let adapter = TransportAdapter::new(&config, None)
        .await
        .expect("adapter build");

    // Accumulate up to 3 messages via a bounded recv loop. Subsequent batches
    // top us up until we hit the cap — this is how the orchestrator calls the
    // adapter in practice.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got = Vec::new();
    while got.len() < 3 && std::time::Instant::now() < deadline {
        // Retry rather than expect: a cold consumer can surface one transient
        // transport error before it settles, and the orchestrator this mimics
        // keeps polling too. What is under test is the cap, not first-poll luck.
        let Ok(batch) = adapter.recv(3 - got.len()).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        for m in batch.messages {
            got.push(m);
            if got.len() == 3 {
                break;
            }
        }
    }

    assert_eq!(
        got.len(),
        3,
        "recv with max=3 accumulated must not exceed 3, got {}",
        got.len()
    );

    // The remaining 7 messages should still be available for subsequent
    // recv calls — verifying the limit did not drop data.
    let mut remaining = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while remaining.len() < 7 && std::time::Instant::now() < deadline {
        let batch = adapter.recv(100).await.expect("recv remaining");
        remaining.extend(batch.messages);
    }
    assert!(
        remaining.len() >= 7,
        "Remaining messages should be retrievable, got {}",
        remaining.len()
    );

    adapter.close().await.expect("close");
}

// ============================================================================
// Auto-create topic via produce (auto.create.topics.enable is on by default)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_transport_auto_create_topic() {
    // Use a unique topic name that certainly does not exist until we produce.
    let topic = format!("auto-create-{}", std::process::id());
    let (_infra, bootstrap, config) = spin_up_kafka("auto-create-group", vec![topic.clone()]).await;

    // Producing to a nonexistent topic triggers broker-side auto-create on the
    // testcontainer default config. This covers the topic-discovery path.
    let producer = make_producer(&bootstrap);
    produce(&producer, &topic, &[b"first-message".to_vec()]).await;

    let adapter = TransportAdapter::new(&config, None)
        .await
        .expect("adapter build after topic auto-create");

    let messages = recv_until(&adapter, 1, Duration::from_secs(30)).await;
    assert_eq!(
        messages.len(),
        1,
        "Auto-created topic should deliver the produced message"
    );
    assert_eq!(messages[0].payload, b"first-message");
    assert_eq!(&*messages[0].topic, topic.as_str());

    adapter.close().await.expect("close");
}

// ============================================================================
// TransportBackend::from_config dispatches to Kafka variant
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_transport_backend_kafka_dispatch() {
    let (_infra, _bootstrap, kafka_config) =
        spin_up_kafka("backend-dispatch-group", vec!["backend-topic".to_string()]).await;

    // transport field defaults to "kafka" (see Config::default_transport).
    let config = Config {
        transport: "kafka".to_string(),
        kafka: kafka_config,
        ..Default::default()
    };

    let backend = TransportBackend::from_config(&config, None)
        .await
        .expect("from_config should build Kafka backend against live broker");

    // name() must route to the Kafka variant's underlying transport name.
    let name = backend.name();
    assert!(
        name.contains("kafka") || name == "kafka",
        "Kafka backend must report kafka-ish name, got {name}"
    );
    assert!(backend.is_healthy(), "Backend should be healthy");

    // commit with empty offsets is a no-op on Kafka — exercise that branch.
    backend
        .commit(&[])
        .await
        .expect("empty commit through backend should be no-op");

    backend.close().await.expect("backend close");
}

// ============================================================================
// Error handling: invalid brokers must not panic
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_invalid_brokers() {
    // Deliberately malformed broker list. scalo constructs the rdkafka
    // consumer eagerly, and the broker name resolution triggers immediately.
    let config = KafkaConfig {
        // Invalid port (65536 is out of u16 range when rdkafka parses it).
        brokers: vec!["not-a-real-broker-hostname.invalid:99999".to_string()],
        group: "invalid-broker-group".to_string(),
        topics: vec!["irrelevant".to_string()],
        topic_regex: None,
        client_id: "invalid-client".to_string(),
        sasl: None,
        tls: None,
        ..Default::default()
    };

    // Must return an error, not panic. rdkafka may accept a nonsense hostname
    // and only fail later on recv; both outcomes are acceptable.
    match TransportAdapter::new(&config, None).await {
        Ok(adapter) => {
            // Construction succeeded — recv should fail or return empty within
            // a reasonable window. We tolerate either, but it must NOT panic.
            let result = tokio::time::timeout(Duration::from_secs(5), adapter.recv(1)).await;
            // Either the timeout fires (outer Err), or recv returns (Ok/Err) —
            // we just require a clean return without panic.
            drop(result);
            let _ = adapter.close().await;
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.is_empty(),
                "Error from invalid brokers must carry a message"
            );
        }
    }

    // Also exercise TransportBackend::from_config's Kafka branch with the same
    // bogus config — ensures dispatch reaches the Kafka construction path and
    // fails cleanly.
    let full_config = Config {
        transport: "kafka".to_string(),
        kafka: config,
        ..Default::default()
    };
    match TransportBackend::from_config(&full_config, None).await {
        Ok(backend) => {
            let _ = backend.close().await;
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(!msg.is_empty(), "Backend error must carry a message");
        }
    }
}

// ============================================================================
// convert_config produces a sensible TransportKafkaConfig
// ============================================================================

/// `convert_config` is the pure-function mapper from `KafkaConfig` to
/// scalo's `TransportKafkaConfig`. It does not touch the network — so this
/// test does not need the container, but we keep it here for proximity.
#[tokio::test]
async fn test_kafka_transport_convert_config_auto_discover() {
    // Empty topics → auto_discover=true branch.
    let config = KafkaConfig {
        brokers: vec!["broker1:9092".to_string(), "broker2:9092".to_string()],
        group: "cfg-group".to_string(),
        topics: vec![],
        topic_regex: Some(r"events_.*".to_string()),
        client_id: "cfg-client".to_string(),
        sasl: None,
        tls: None,
        ..Default::default()
    };

    let out = TransportAdapter::convert_config(&config);
    assert_eq!(out.brokers.len(), 2);
    assert_eq!(out.group, "cfg-group");
    assert!(
        out.auto_discover,
        "auto_discover should be true when topics is empty"
    );
    assert_eq!(
        out.topic_include,
        vec![r"events_.*".to_string()],
        "topic_regex should map to topic_include"
    );
}
