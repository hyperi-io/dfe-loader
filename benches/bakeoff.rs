// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      benches/bakeoff.rs
// Purpose:   Pipeline throughput benchmark — parse, route, transform
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pipeline Throughput Benchmark
//!
//! Measures the CPU cost of the hot path:
//!
//! - **Input**: `Vec<&[u8]>` (raw JSON bytes from Kafka)
//! - **Output**: `Vec<Map<String, Value>>` (ready for JSONEachRow insertion)
//!
//! ## Test Matrix
//!
//! | Event Type        | Fields | Nesting |
//! |-------------------|--------|---------|
//! | Flat 10-field     | 10     | 0       |
//! | Flat 30-field     | 30     | 0       |
//! | Nested 2-level    | 15     | 2       |
//!
//! ## Benchmark Groups
//!
//! - `parse_route`: parse + routing field extraction only (no transform overhead)
//! - `full_pipeline`: parse + route + transform (complete hot path)
//!
//! Batch sizes: 100, 1000, 10000
//!
//! Run with: `cargo bench --bench bakeoff`

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use serde_json::{Map, Value};

use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::Transformer;

// =============================================================================
// Test Event Generators
// =============================================================================

fn flat_10_event() -> Vec<u8> {
    br#"{"org_id":"acme","event_category":"auth","timestamp":"2024-12-28T10:30:00.123Z","user_id":"user123","action":"login","success":true,"ip_address":"192.168.1.100","status_code":200,"response_time_ms":45,"user_agent":"Mozilla/5.0"}"#.to_vec()
}

fn flat_30_event() -> Vec<u8> {
    br#"{"org_id":"enterprise","event_category":"security","timestamp":"2024-12-28T10:30:00.123Z","user_id":"user123","session_id":"sess456","request_id":"req789","action":"api_call","method":"POST","path":"/api/v2/users","status_code":200,"response_time_ms":45,"bytes_sent":1024,"bytes_received":256,"user_agent":"Mozilla/5.0","client_ip":"203.0.113.50","server_ip":"10.0.0.5","datacenter":"us-east-1","service":"auth-service","version":"2.5.0","environment":"production","trace_id":"abc123def456","span_id":"def456ghi789","parent_span_id":"ghi789jkl012","risk_score":15,"threat_level":"low","authenticated":true,"protocol":"https","port":443,"country":"US","region":"VA"}"#.to_vec()
}

fn nested_2level_event() -> Vec<u8> {
    br#"{"org_id":"corp","event_category":"network","timestamp":"2024-12-28T10:30:00.123Z","source":{"ip":"10.0.0.1","port":54321,"geo":{"country":"US","city":"Seattle"}},"destination":{"ip":"10.0.0.2","port":443},"agent":{"name":"sensor-01","version":"2.5","hostname":"server-01.example.com"},"event":{"original":"connection established","severity":"info"},"user":{"name":"admin","id":"1001"},"message":"Connection from 10.0.0.1 to 10.0.0.2:443"}"#.to_vec()
}

// =============================================================================
// Pipeline implementations
// =============================================================================

/// Parse + route only — no transform overhead.
fn parse_and_route(messages: &[&[u8]], router: &Router) -> Vec<(String, Value)> {
    messages
        .iter()
        .map(|&payload| {
            let value: Value = sonic_rs::from_slice(payload).expect("parse");
            let route = router.route_value(&value);
            let destination = match &route {
                RouteResult::Table(t) => t.to_string(),
                RouteResult::Dlq(r) => r.to_string(),
            };
            (destination, value)
        })
        .collect()
}

/// Full pipeline: parse → route → transform → Map<String, Value>.
fn full_pipeline(
    messages: &[&[u8]],
    router: &Router,
    transformer: &Transformer,
) -> Vec<(String, Map<String, Value>)> {
    messages
        .iter()
        .map(|&payload| {
            let value: Value = sonic_rs::from_slice(payload).expect("parse");
            let route = router.route_value(&value);
            let destination = match &route {
                RouteResult::Table(t) => t.to_string(),
                RouteResult::Dlq(r) => r.to_string(),
            };
            let result = transformer
                .transform_with_raw(value, None, None)
                .expect("transform");
            (destination, result.data)
        })
        .collect()
}

// =============================================================================
// Benchmark Groups
// =============================================================================

fn bench_event_type(
    c: &mut Criterion,
    group_name: &str,
    event_fn: fn() -> Vec<u8>,
    batch_sizes: &[usize],
) {
    let event = event_fn();
    let router = Router::default();
    let transformer = Transformer::default();

    for &batch_size in batch_sizes {
        let batch: Vec<&[u8]> = (0..batch_size).map(|_| event.as_slice()).collect();

        let mut group = c.benchmark_group(format!("{}/batch_{}", group_name, batch_size));
        group.throughput(Throughput::Elements(batch_size as u64));
        group.sampling_mode(SamplingMode::Flat);
        if batch_size >= 10_000 {
            group.sample_size(20);
        } else if batch_size >= 1_000 {
            group.sample_size(50);
        }

        group.bench_function("parse_route", |b| {
            b.iter(|| parse_and_route(black_box(&batch), &router))
        });

        group.bench_function("full_pipeline", |b| {
            b.iter(|| full_pipeline(black_box(&batch), &router, &transformer))
        });

        group.finish();
    }
}

fn bench_flat_10(c: &mut Criterion) {
    bench_event_type(c, "bakeoff/flat_10", flat_10_event, &[100, 1_000, 10_000]);
}

fn bench_flat_30(c: &mut Criterion) {
    bench_event_type(c, "bakeoff/flat_30", flat_30_event, &[100, 1_000, 10_000]);
}

fn bench_nested(c: &mut Criterion) {
    bench_event_type(
        c,
        "bakeoff/nested_2level",
        nested_2level_event,
        &[100, 1_000, 10_000],
    );
}

fn bench_single_message(c: &mut Criterion) {
    let events: &[(&str, fn() -> Vec<u8>)] = &[
        ("flat_10", flat_10_event),
        ("flat_30", flat_30_event),
        ("nested_2level", nested_2level_event),
    ];

    let router = Router::default();
    let transformer = Transformer::default();

    for &(name, event_fn) in events {
        let event = event_fn();
        let batch: Vec<&[u8]> = vec![event.as_slice()];

        let mut group = c.benchmark_group(format!("bakeoff/single/{}", name));
        group.throughput(Throughput::Elements(1));

        group.bench_function("parse_route", |b| {
            b.iter(|| parse_and_route(black_box(&batch), &router))
        });

        group.bench_function("full_pipeline", |b| {
            b.iter(|| full_pipeline(black_box(&batch), &router, &transformer))
        });

        group.finish();
    }
}

fn bench_allocation_pressure(c: &mut Criterion) {
    let event = flat_10_event();
    let batch: Vec<&[u8]> = (0..1_000).map(|_| event.as_slice()).collect();
    let router = Router::default();
    let transformer = Transformer::default();

    let mut group = c.benchmark_group("bakeoff/alloc_pressure");
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("parse_route_1000", |b| {
        b.iter(|| parse_and_route(black_box(&batch), &router))
    });

    group.bench_function("full_pipeline_1000", |b| {
        b.iter(|| full_pipeline(black_box(&batch), &router, &transformer))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flat_10,
    bench_flat_30,
    bench_nested,
    bench_single_message,
    bench_allocation_pressure,
);
criterion_main!(benches);
