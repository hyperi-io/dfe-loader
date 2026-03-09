// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Transform benchmarks
//!
//! Detailed benchmarks for transformation operations:
//! - JSON flattening (nested to flat)
//! - Timestamp validation and correction
//! - Type coercion
//! - Map batch building (Vec<Map<String, Value>> accumulation)
//!
//! Run with: cargo bench --bench transform

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Map, Value};
use std::hint::black_box;

use dfe_loader::transform::{flatten_value_owned, BatchFlattener, TimestampValidator};

/// Sample nested JSON structures of varying depth
fn shallow_nested() -> Value {
    json!({
        "level1_a": "value_a",
        "level1_b": 123,
        "level1_c": true
    })
}

fn medium_nested() -> Value {
    json!({
        "level1": {
            "level2_a": "value_a",
            "level2_b": 456,
            "level2_c": {
                "level3_a": "deep_value",
                "level3_b": [1, 2, 3]
            }
        },
        "metadata": {
            "version": "1.0",
            "source": "test"
        }
    })
}

fn deep_nested() -> Value {
    json!({
        "a": {
            "b": {
                "c": {
                    "d": {
                        "e": {
                            "f": "deep_value"
                        }
                    }
                }
            }
        },
        "tags": {
            "env": "prod",
            "region": "us-east-1",
            "collector": {
                "hostname": "collector-1",
                "version": "2.0"
            }
        }
    })
}

fn wide_shallow() -> Value {
    // 50 top-level fields
    let mut map = serde_json::Map::new();
    for i in 0..50 {
        map.insert(format!("field_{}", i), json!(format!("value_{}", i)));
    }
    Value::Object(map)
}

/// Flattening benchmarks
fn bench_flatten_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_depth");

    let shallow = shallow_nested();
    let medium = medium_nested();
    let deep = deep_nested();
    let wide = wide_shallow();

    group.bench_function("shallow_3_fields", |b| {
        b.iter(|| flatten_value_owned(black_box(shallow.clone())))
    });

    group.bench_function("medium_nested", |b| {
        b.iter(|| flatten_value_owned(black_box(medium.clone())))
    });

    group.bench_function("deep_6_levels", |b| {
        b.iter(|| flatten_value_owned(black_box(deep.clone())))
    });

    group.bench_function("wide_50_fields", |b| {
        b.iter(|| flatten_value_owned(black_box(wide.clone())))
    });

    group.finish();
}

/// Timestamp validation benchmarks
fn bench_timestamp_validation(c: &mut Criterion) {
    let validator = TimestampValidator::default();
    let mut group = c.benchmark_group("timestamp");

    // Valid ISO 8601 timestamp
    let valid_iso = "2025-12-24T12:00:00Z";
    group.bench_function("validate_iso8601", |b| {
        b.iter(|| validator.validate(black_box(valid_iso)))
    });

    // Unix timestamp (seconds)
    let unix_secs: i64 = 1735041600;
    group.bench_function("validate_unix_secs", |b| {
        b.iter(|| validator.validate_unix(black_box(unix_secs)))
    });

    // Unix timestamp (milliseconds)
    let unix_ms: i64 = 1735041600000;
    group.bench_function("validate_unix_ms", |b| {
        b.iter(|| validator.validate_unix(black_box(unix_ms)))
    });

    // Invalid timestamp
    let invalid = "not a timestamp";
    group.bench_function("validate_invalid", |b| {
        b.iter(|| validator.validate(black_box(invalid)))
    });

    group.finish();
}

/// Map batch building benchmarks — measures Vec<Map<String, Value>> accumulation cost.
fn bench_map_batch_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_batch_builder");

    fn create_sample_row(i: usize) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("id".to_string(), json!(i));
        map.insert("name".to_string(), json!(format!("user_{}", i)));
        map.insert("active".to_string(), json!(i % 2 == 0));
        map.insert("score".to_string(), json!(i as f64 * 1.5));
        map.insert("timestamp".to_string(), json!("2025-12-24T12:00:00Z"));
        map
    }

    for batch_size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("collect_rows", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let mut batch: Vec<Map<String, Value>> = Vec::with_capacity(size);
                    for i in 0..size {
                        batch.push(create_sample_row(i));
                    }
                    black_box(batch)
                })
            },
        );
    }

    group.finish();
}

/// Batch flattening benchmarks - compare individual vs batch flattening
fn bench_batch_flatten(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_flatten");

    // Create a batch of uniform messages
    let sample = medium_nested();
    let flattener = BatchFlattener::from_sample(&sample);

    // Benchmark different batch sizes
    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        // Individual flattening (baseline)
        group.bench_with_input(
            BenchmarkId::new("individual", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let batch: Vec<Value> = (0..batch_size).map(|_| medium_nested()).collect();
                    batch
                        .into_iter()
                        .map(|v| flatten_value_owned(black_box(v)))
                        .collect::<Vec<_>>()
                })
            },
        );

        // Batch flattening with pre-computed schema
        group.bench_with_input(
            BenchmarkId::new("batch_flattener", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let batch: Vec<Value> = (0..batch_size).map(|_| medium_nested()).collect();
                    flattener.flatten_batch(black_box(batch))
                })
            },
        );
    }

    group.finish();
}

/// Memory allocation benchmarks (qualitative)
fn bench_allocation_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation");

    // Compare String allocation vs Cow/borrowed
    let payload = br#"{"org_id":"acme","event_category":"auth"}"#;

    group.bench_function("extract_with_alloc", |b| {
        b.iter(|| {
            use dfe_loader::payload::parse::extract_field_json;
            let _ = extract_field_json(black_box(payload), "org_id");
            let _ = extract_field_json(black_box(payload), "event_category");
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flatten_depth,
    bench_batch_flatten,
    bench_timestamp_validation,
    bench_map_batch_builder,
    bench_allocation_patterns,
);
criterion_main!(benches);
