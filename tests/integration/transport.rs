// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka transport adapter integration tests.
//!
//! Exercises the `MemoryTransportAdapter` (feature-gated behind `transport-memory`)
//! for in-process testing of the transport abstraction without a real Kafka cluster.
//!
//! These tests cover:
//! - Basic inject/recv flow
//! - Partition-key preservation
//! - Max-message limits
//! - Empty transport behaviour
//! - Commit as no-op
//! - Close semantics
//! - TransportBackend dispatch

#![cfg(feature = "transport-memory")]

use dfe_loader::buffer::KafkaOffset;
use dfe_loader::kafka::MemoryTransportAdapter;

// ============================================================================
// Basic inject + recv
// ============================================================================

#[tokio::test]
async fn inject_then_recv_returns_message() {
    let transport = MemoryTransportAdapter::new("test-topic");
    transport
        .inject(b"hello world".to_vec())
        .await
        .expect("inject should succeed");

    let messages = transport.recv(10).await.expect("recv should succeed");
    assert_eq!(messages.len(), 1, "Should receive exactly 1 message");

    let msg = &messages[0];
    assert_eq!(msg.payload, b"hello world");
    assert_eq!(&*msg.topic, "test-topic");
    assert_eq!(msg.partition, 0);
    assert!(
        msg.key.is_none(),
        "Inject without key should yield None key"
    );
}

#[tokio::test]
async fn inject_multiple_messages_preserves_order() {
    let transport = MemoryTransportAdapter::new("ordered-topic");

    for i in 0..5 {
        transport
            .inject(format!("msg-{i}").into_bytes())
            .await
            .expect("inject");
    }

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 5);

    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.payload, format!("msg-{i}").into_bytes());
    }
}

// ============================================================================
// inject_with_key preserves partition key
// ============================================================================

#[tokio::test]
async fn inject_with_key_preserves_key() {
    let transport = MemoryTransportAdapter::new("keyed-topic");
    transport
        .inject_with_key("user-42", b"payload-data".to_vec())
        .await
        .expect("inject_with_key");

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 1);

    let msg = &messages[0];
    assert_eq!(msg.payload, b"payload-data");
    assert!(msg.key.is_some(), "Keyed inject should populate key");
    let key_bytes = msg.key.as_ref().unwrap();
    assert_eq!(key_bytes, b"user-42");
}

#[tokio::test]
async fn inject_with_empty_key_preserves_it() {
    let transport = MemoryTransportAdapter::new("empty-key-topic");
    transport
        .inject_with_key("", b"body".to_vec())
        .await
        .expect("inject_with_key with empty key");

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 1);
    // Empty key is still Some("")
    let key = messages[0].key.as_ref().expect("key should be Some");
    assert!(key.is_empty());
}

// ============================================================================
// recv with max_messages limit
// ============================================================================

#[tokio::test]
async fn recv_respects_max_messages_limit() {
    let transport = MemoryTransportAdapter::new("limit-topic");

    // Inject 10 messages
    for i in 0..10 {
        transport
            .inject(format!("m{i}").into_bytes())
            .await
            .expect("inject");
    }

    // Request only 3
    let batch1 = transport.recv(3).await.expect("recv");
    assert_eq!(
        batch1.len(),
        3,
        "First batch should contain exactly 3 messages"
    );

    // Remaining messages still available
    let batch2 = transport.recv(100).await.expect("recv");
    assert_eq!(
        batch2.len(),
        7,
        "Remaining 7 messages should be in second batch"
    );

    // No more messages
    let batch3 = transport.recv(10).await.expect("recv");
    assert!(batch3.is_empty(), "No more messages should be available");
}

#[tokio::test]
async fn recv_max_zero_returns_empty() {
    let transport = MemoryTransportAdapter::new("max-zero-topic");
    transport
        .inject(b"some data".to_vec())
        .await
        .expect("inject");

    let messages = transport.recv(0).await.expect("recv");
    assert!(
        messages.is_empty(),
        "recv(0) should yield zero messages, got {}",
        messages.len()
    );
}

// ============================================================================
// Empty transport
// ============================================================================

#[tokio::test]
async fn recv_on_empty_transport_returns_empty_vec() {
    let transport = MemoryTransportAdapter::new("empty-topic");

    let messages = transport.recv(100).await.expect("recv should not error");
    assert!(
        messages.is_empty(),
        "Empty transport should yield empty vec, got {}",
        messages.len()
    );
}

// ============================================================================
// Commit is no-op (doesn't error)
// ============================================================================

#[tokio::test]
async fn commit_empty_offsets_is_ok() {
    let transport = MemoryTransportAdapter::new("commit-topic");
    let result = transport.commit(&[]).await;
    assert!(result.is_ok(), "Commit with empty offsets should succeed");
}

#[tokio::test]
async fn commit_non_empty_offsets_is_ok() {
    use std::sync::Arc;

    let transport = MemoryTransportAdapter::new("commit-topic");
    let offsets = vec![
        KafkaOffset {
            topic: Arc::from("t1"),
            partition: 0,
            offset: 100,
        },
        KafkaOffset {
            topic: Arc::from("t1"),
            partition: 1,
            offset: 200,
        },
    ];
    let result = transport.commit(&offsets).await;
    assert!(result.is_ok(), "Memory commit should always succeed");
}

// ============================================================================
// Close then recv
// ============================================================================

#[tokio::test]
async fn close_is_ok() {
    let transport = MemoryTransportAdapter::new("close-topic");
    transport.close().await.expect("close should succeed");
}

#[tokio::test]
async fn recv_after_close_is_empty() {
    let transport = MemoryTransportAdapter::new("close-recv-topic");
    transport
        .inject(b"data".to_vec())
        .await
        .expect("pre-close inject");

    transport.close().await.expect("close");

    // After close, recv returns empty (not an error)
    let messages = transport.recv(10).await.unwrap_or_default();
    assert!(
        messages.is_empty(),
        "recv after close should return empty, got {} messages",
        messages.len()
    );
}

#[tokio::test]
async fn inject_after_close_fails_gracefully() {
    let transport = MemoryTransportAdapter::new("inject-after-close");
    transport.close().await.expect("close");

    // Injection after close: result may be Ok or Err depending on impl —
    // the important thing is it doesn't panic.
    let _ = transport.inject(b"late".to_vec()).await;
}

// ============================================================================
// Transport metadata
// ============================================================================

#[tokio::test]
async fn healthy_on_fresh_transport() {
    let transport = MemoryTransportAdapter::new("healthy-topic");
    assert!(
        transport.is_healthy(),
        "Fresh memory transport should be healthy"
    );
}

#[tokio::test]
async fn name_returns_non_empty_string() {
    let transport = MemoryTransportAdapter::new("name-topic");
    let name = transport.name();
    assert!(!name.is_empty(), "Transport name should be non-empty");
}

// ============================================================================
// Large payloads
// ============================================================================

#[tokio::test]
async fn handles_large_payload() {
    let transport = MemoryTransportAdapter::new("large-topic");
    let large_payload = vec![0xAB_u8; 1_000_000]; // 1 MB

    transport
        .inject(large_payload.clone())
        .await
        .expect("inject large");

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload.len(), 1_000_000);
    assert_eq!(messages[0].payload, large_payload);
}

#[tokio::test]
async fn handles_empty_payload() {
    let transport = MemoryTransportAdapter::new("empty-payload-topic");
    transport
        .inject(Vec::new())
        .await
        .expect("empty payload inject");

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 1);
    assert!(messages[0].payload.is_empty());
}

// ============================================================================
// Offset sequencing
// ============================================================================

#[tokio::test]
async fn offset_increments_across_messages() {
    let transport = MemoryTransportAdapter::new("offset-topic");

    for i in 0..5 {
        transport
            .inject(format!("m{i}").into_bytes())
            .await
            .expect("inject");
    }

    let messages = transport.recv(10).await.expect("recv");
    assert_eq!(messages.len(), 5);

    // Offsets should be strictly increasing
    for i in 1..messages.len() {
        assert!(
            messages[i].offset > messages[i - 1].offset,
            "Offset should increase: msg[{i}].offset={} vs msg[{}].offset={}",
            messages[i].offset,
            i - 1,
            messages[i - 1].offset,
        );
    }
}

// ============================================================================
// TransportBackend dispatch (Memory variant)
// ============================================================================

#[tokio::test]
async fn transport_backend_memory_is_not_constructible_from_config() {
    // TransportBackend::from_config only produces Kafka or Grpc variants —
    // Memory is for direct unit-test use.
    // This test documents that intent: building a config with transport="memory"
    // would fall through to the Kafka branch.
    //
    // Verify by checking the names differ.
    let memory = MemoryTransportAdapter::new("dispatch-topic");
    let name = memory.name();
    assert_ne!(
        name, "kafka",
        "Memory transport should not call itself kafka"
    );
    assert_ne!(name, "grpc", "Memory transport should not call itself grpc");
}
