// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! End-to-end pipeline benchmarks
//!
//! Measures throughput and latency for the full message processing pipeline:
//! - JSON parsing (sonic-rs SIMD)
//! - Routing (db.table extraction)
//! - Transform (flatten, timestamp validation)
//! - Arrow conversion
//!
//! Run with: cargo bench --bench pipeline

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{Map, Value};

use dfe_loader::payload::parse::{
    extract_field_json, extract_field_json_cow, extract_nested_field_json,
    extract_nested_field_json_cow, parse_payload,
};
use dfe_loader::routing::Router;
use dfe_loader::transform::{flatten_value_owned, Transformer};

// Sample payloads of varying sizes
const SMALL_PAYLOAD: &[u8] = br#"{"org_id":"acme","event_category":"auth","user_id":123}"#;

const MEDIUM_PAYLOAD: &[u8] = br#"{"org_id":"acme","event_category":"auth","user_id":123,"timestamp":"2025-12-24T00:00:00Z","tags":{"level":"info","source":"api","collector":{"hostname":"collector-1","timestamp":"2025-12-24T00:00:00Z"}},"data":{"action":"login","ip":"192.168.1.1","user_agent":"Mozilla/5.0"}}"#;

const LARGE_PAYLOAD: &[u8] = br#"{"org_id":"acme","event_category":"auth","user_id":123,"timestamp":"2025-12-24T00:00:00Z","session_id":"sess_abc123def456","request_id":"req_789xyz","tags":{"level":"info","source":"api","environment":"production","region":"us-east-1","collector":{"hostname":"collector-1","version":"1.2.3","timestamp":"2025-12-24T00:00:00Z"}},"data":{"action":"login","ip":"192.168.1.1","user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","method":"POST","path":"/api/v1/auth/login","status_code":200,"latency_ms":45,"headers":{"content-type":"application/json","x-forwarded-for":"10.0.0.1"}},"metadata":{"version":"2.0","schema":"auth_event","partition":3}}"#;

/// Parse throughput benchmark - measures bytes/sec for JSON parsing
fn bench_parse_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_throughput");

    for (name, payload) in [
        ("small", SMALL_PAYLOAD),
        ("medium", MEDIUM_PAYLOAD),
        ("large", LARGE_PAYLOAD),
    ] {
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::new("parse_payload", name), payload, |b, p| {
            b.iter(|| parse_payload(black_box(p)))
        });
    }

    group.finish();
}

/// On-demand field extraction benchmark - allocating vs zero-copy
fn bench_field_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_extraction");

    // Simple field extraction - allocating (returns String)
    group.bench_function("extract_simple_alloc", |b| {
        b.iter(|| extract_field_json(black_box(MEDIUM_PAYLOAD), "org_id"))
    });

    // Simple field extraction - zero-copy (returns Cow)
    group.bench_function("extract_simple_cow", |b| {
        b.iter(|| extract_field_json_cow(black_box(MEDIUM_PAYLOAD), "org_id"))
    });

    // Nested field extraction - allocating
    group.bench_function("extract_nested_alloc", |b| {
        b.iter(|| extract_nested_field_json(black_box(MEDIUM_PAYLOAD), "tags.level"))
    });

    // Nested field extraction - zero-copy
    group.bench_function("extract_nested_cow", |b| {
        b.iter(|| extract_nested_field_json_cow(black_box(MEDIUM_PAYLOAD), "tags.level"))
    });

    // Deep nested field extraction - allocating
    group.bench_function("extract_deep_alloc", |b| {
        b.iter(|| extract_nested_field_json(black_box(MEDIUM_PAYLOAD), "tags.collector.hostname"))
    });

    // Deep nested field extraction - zero-copy
    group.bench_function("extract_deep_cow", |b| {
        b.iter(|| {
            extract_nested_field_json_cow(black_box(MEDIUM_PAYLOAD), "tags.collector.hostname")
        })
    });

    group.finish();
}

/// Routing benchmark - compares different routing methods
fn bench_routing(c: &mut Criterion) {
    let router = Router::default();
    let mut group = c.benchmark_group("routing");

    group.throughput(Throughput::Elements(1));

    // Route from raw bytes (allocating version)
    group.bench_function("route_bytes", |b| {
        b.iter(|| router.route(black_box(MEDIUM_PAYLOAD)))
    });

    // Route from raw bytes (zero-copy Cow version - fastest for raw bytes)
    group.bench_function("route_cow", |b| {
        b.iter(|| router.route_cow(black_box(MEDIUM_PAYLOAD)))
    });

    // Route from pre-parsed Value (avoids re-parsing - use when already parsed)
    let value: Value = serde_json::from_slice(MEDIUM_PAYLOAD).unwrap();
    group.bench_function("route_value", |b| {
        b.iter(|| router.route_value(black_box(&value)))
    });

    group.finish();
}

/// Flattening benchmark
fn bench_flatten(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten");

    for (name, payload) in [
        ("small", SMALL_PAYLOAD),
        ("medium", MEDIUM_PAYLOAD),
        ("large", LARGE_PAYLOAD),
    ] {
        let value: Value = serde_json::from_slice(payload).unwrap();

        group.bench_with_input(BenchmarkId::new("flatten_owned", name), &value, |b, v| {
            b.iter(|| flatten_value_owned(black_box(v.clone())))
        });
    }

    group.finish();
}

/// Transform benchmark - full transformation pipeline
fn bench_transform(c: &mut Criterion) {
    let transformer = Transformer::default();
    let mut group = c.benchmark_group("transform");

    for (name, payload) in [
        ("small", SMALL_PAYLOAD),
        ("medium", MEDIUM_PAYLOAD),
        ("large", LARGE_PAYLOAD),
    ] {
        let value: Value = serde_json::from_slice(payload).unwrap();

        group.bench_with_input(
            BenchmarkId::new("transform_with_raw", name),
            &(value.clone(), payload),
            |b, (v, p)| b.iter(|| transformer.transform_with_raw(black_box(v.clone()), None)),
        );
    }

    group.finish();
}

/// End-to-end benchmark - parse → route → transform
fn bench_end_to_end(c: &mut Criterion) {
    let router = Router::default();
    let transformer = Transformer::default();
    let mut group = c.benchmark_group("end_to_end");

    for (name, payload) in [
        ("small", SMALL_PAYLOAD),
        ("medium", MEDIUM_PAYLOAD),
        ("large", LARGE_PAYLOAD),
    ] {
        group.throughput(Throughput::Bytes(payload.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("parse_route_transform", name),
            payload,
            |b, p| {
                b.iter(|| {
                    // Step 1: Parse JSON
                    let value: Value = sonic_rs::from_slice(black_box(p)).unwrap();

                    // Step 2: Route (using pre-parsed value)
                    let _route = router.route_value(&value);

                    // Step 3: Transform
                    transformer.transform_with_raw(value, None)
                })
            },
        );
    }

    group.finish();
}

/// Batch processing benchmark - simulates processing multiple messages
fn bench_batch_processing(c: &mut Criterion) {
    let router = Router::default();
    let transformer = Transformer::default();

    // Create batch of 100 messages
    let batch: Vec<&[u8]> = (0..100).map(|_| MEDIUM_PAYLOAD).collect();

    c.bench_function("batch_100_messages", |b| {
        b.iter(|| {
            let mut results: Vec<Map<String, Value>> = Vec::with_capacity(batch.len());

            for payload in &batch {
                let value: Value = sonic_rs::from_slice(black_box(*payload)).unwrap();
                let _route = router.route_value(&value);
                if let Ok(result) = transformer.transform_with_raw(value, None) {
                    results.push(result.data);
                }
            }

            results
        })
    });
}

criterion_group!(
    benches,
    bench_parse_throughput,
    bench_field_extraction,
    bench_routing,
    bench_flatten,
    bench_transform,
    bench_end_to_end,
    bench_batch_processing,
);
criterion_main!(benches);
