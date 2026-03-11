// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      benches/simdjson_spike.rs
// Purpose:   simd-json vs sonic-rs targeted bake-off for the Phase 5.7 use-case
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # simd-json spike: targeted bake-off
//!
//! Compares three approaches for the two bounded operations in the Phase 5.7 hot path:
//!
//! ## Use-case 1: Schema-guided field extraction (N columns from one document)
//!
//! | Approach | Method | Buffer |
//! |---|---|---|
//! | `sonic_selective` | `get_from_slice` × N | `&[u8]` (zero-copy) |
//! | `sonic_dom` | `from_slice` × 1 + `.get()` × N | `&[u8]` (zero-copy) |
//! | `simd_dom` | `to_owned_value` × 1 + `.get()` × N | `Vec<u8>` clone ← **mandatory** |
//!
//! `simd_dom` always clones the payload into a `Vec<u8>` before parsing because
//! simd-json requires a mutable buffer (in-place string unescaping).
//! This clone cost is included in the measurement — it is unavoidable in production.
//!
//! ## Use-case 2: Full DOM parse for routing field extraction
//!
//! | Approach | Method |
//! |---|---|
//! | `sonic_full_parse` | `sonic_rs::from_slice::<serde_json::Value>` |
//! | `simd_full_parse` | `simd_json::to_owned_value` (with mandatory clone) |
//!
//! ## Schema sizes
//!
//! - 15 columns: typical DFE table (common header + ~10 promoted fields)
//! - 30 columns: large DFE table
//!
//! ## Decision threshold
//!
//! ≥5% improvement for simd-json warranted adoption (same bar as the earlier mison bake-off).
//! The mutable-buffer clone cost must be included — it is not optional in our architecture.
//!
//! Run with: `cargo bench --bench simdjson_spike`

use std::hint::black_box;

use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use serde_json::Value;

// =============================================================================
// Representative DFE payloads
// =============================================================================

/// Flat event with 30+ fields — common for security/network events.
fn flat_30_payload() -> Vec<u8> {
    br#"{
        "org_id": "acme",
        "event_category": "network",
        "timestamp": "2024-12-28T10:30:00.123Z",
        "severity": "medium",
        "src_ip": "192.168.1.100",
        "dst_ip": "10.0.0.5",
        "src_port": 54321,
        "dst_port": 443,
        "protocol": "tcp",
        "bytes_in": 1024,
        "bytes_out": 256,
        "duration_ms": 45,
        "user_id": "user123",
        "session_id": "sess456",
        "request_id": "req789",
        "action": "allow",
        "method": "POST",
        "path": "/api/v2/users",
        "status_code": 200,
        "user_agent": "Mozilla/5.0",
        "country": "AU",
        "region": "NSW",
        "datacenter": "ap-southeast-2",
        "service": "auth-service",
        "version": "2.5.0",
        "environment": "production",
        "trace_id": "abc123def456",
        "risk_score": 15,
        "threat_level": "low",
        "authenticated": true
    }"#
    .to_vec()
}

/// Nested 2-level event — typical agent/sensor events.
fn nested_payload() -> Vec<u8> {
    br#"{
        "org_id": "corp",
        "event_category": "endpoint",
        "timestamp": "2024-12-28T10:30:00.123Z",
        "source": {"ip": "10.0.0.1", "port": 54321, "hostname": "workstation-01"},
        "destination": {"ip": "10.0.0.2", "port": 443, "hostname": "server-01"},
        "agent": {"name": "sensor-01", "version": "2.5", "os": "linux"},
        "event": {"original": "connection", "severity": "info", "outcome": "success"},
        "user": {"name": "admin", "id": "1001", "domain": "corp.local"},
        "process": {"name": "curl", "pid": 12345, "ppid": 1000},
        "message": "Outbound connection established"
    }"#
    .to_vec()
}

// =============================================================================
// Schema column name sets
// =============================================================================

/// 15 schema columns — typical DFE table with common header + promoted fields.
const COLS_15: &[&str] = &[
    "org_id",
    "event_category",
    "timestamp",
    "severity",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "bytes_in",
    "bytes_out",
    "duration_ms",
    "user_id",
    "action",
    "status_code",
];

/// 30 schema columns — large DFE table.
const COLS_30: &[&str] = &[
    "org_id",
    "event_category",
    "timestamp",
    "severity",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "bytes_in",
    "bytes_out",
    "duration_ms",
    "user_id",
    "session_id",
    "request_id",
    "action",
    "method",
    "path",
    "status_code",
    "user_agent",
    "country",
    "region",
    "datacenter",
    "service",
    "version",
    "environment",
    "trace_id",
    "risk_score",
    "threat_level",
    "authenticated",
];

// =============================================================================
// Approach A: sonic_rs get_from_slice × N (current production path)
// =============================================================================

/// Current Phase 5.7 implementation: one SIMD scan per schema column.
///
/// Each `get_from_slice` navigates directly to the field via SIMD structural
/// indexing — no DOM built. Works on immutable `&[u8]`.
fn extract_sonic_selective(raw: &[u8], cols: &[&str]) -> usize {
    let mut found = 0usize;
    for &col in cols {
        if sonic_rs::get_from_slice(raw, &[col]).is_ok() {
            found += 1;
        }
    }
    found
}

// =============================================================================
// Approach B: sonic_rs full DOM + hash lookup × N
// =============================================================================

/// Alternative: parse full DOM once with sonic-rs, then field-access per column.
///
/// Trades N SIMD scans for 1 full parse + N hash lookups.
/// For large schemas (N≥15) this may be cheaper — amortises parse overhead.
/// Still works on immutable `&[u8]`.
fn extract_sonic_dom(raw: &[u8], cols: &[&str]) -> usize {
    let value: Value = sonic_rs::from_slice(raw).expect("sonic_rs parse");
    let mut found = 0usize;
    if let Some(obj) = value.as_object() {
        for &col in cols {
            if obj.contains_key(col) {
                found += 1;
            }
        }
    }
    found
}

// =============================================================================
// Approach C: simd-json full DOM + hash lookup × N (mandatory clone)
// =============================================================================

/// simd-json alternative: parse to OwnedValue, then field-access per column.
///
/// **MANDATORY CLONE**: simd-json requires `&mut [u8]` (in-place string unescaping).
/// `Arc<[u8]>` is immutable — must clone to `Vec<u8>` before every parse.
/// This clone cost (~200–500 bytes memcpy) is included in the measurement.
fn extract_simd_json_dom(raw: &[u8], cols: &[&str]) -> usize {
    let mut buf = raw.to_vec(); // mandatory clone — cannot avoid with Arc<[u8]>
    let value = simd_json::to_owned_value(&mut buf).expect("simd-json parse");
    let mut found = 0usize;
    if let simd_json::OwnedValue::Object(obj) = &value {
        for &col in cols {
            if obj.contains_key(col) {
                found += 1;
            }
        }
    }
    found
}

// =============================================================================
// Use-case 2: Routing — full DOM parse only
// =============================================================================

/// sonic-rs full DOM parse for routing (current production path).
fn parse_sonic(raw: &[u8]) -> Value {
    sonic_rs::from_slice(raw).expect("sonic_rs parse")
}

/// simd-json full DOM parse for routing (with mandatory clone).
fn parse_simd_json(raw: &[u8]) -> simd_json::OwnedValue {
    let mut buf = raw.to_vec();
    simd_json::to_owned_value(&mut buf).expect("simd-json parse")
}

// =============================================================================
// Benchmark groups
// =============================================================================

fn bench_extraction(c: &mut Criterion, group_name: &str, payload: &[u8], cols: &[&str]) {
    let batch_sizes: &[usize] = &[100, 1_000, 10_000];

    for &batch_size in batch_sizes {
        let batch: Vec<Vec<u8>> = (0..batch_size).map(|_| payload.to_vec()).collect();
        let batch_slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();

        let name = format!("{}/batch_{}", group_name, batch_size);
        let mut group = c.benchmark_group(&name);
        group.throughput(Throughput::Elements(batch_size as u64));
        group.sampling_mode(SamplingMode::Flat);
        if batch_size >= 10_000 {
            group.sample_size(20);
        } else if batch_size >= 1_000 {
            group.sample_size(50);
        }

        // Approach A: sonic get_from_slice × N (current)
        group.bench_function("sonic_selective", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &raw in black_box(&batch_slices) {
                    total += extract_sonic_selective(raw, cols);
                }
                black_box(total)
            })
        });

        // Approach B: sonic full DOM + lookup × N
        group.bench_function("sonic_dom", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &raw in black_box(&batch_slices) {
                    total += extract_sonic_dom(raw, cols);
                }
                black_box(total)
            })
        });

        // Approach C: simd-json DOM + lookup × N (with mandatory clone)
        group.bench_function("simd_dom_plus_clone", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &raw in black_box(&batch_slices) {
                    total += extract_simd_json_dom(raw, cols);
                }
                black_box(total)
            })
        });

        group.finish();
    }
}

fn bench_routing_parse(c: &mut Criterion, group_name: &str, payload: &[u8]) {
    let batch_sizes: &[usize] = &[100, 1_000, 10_000];

    for &batch_size in batch_sizes {
        let batch: Vec<Vec<u8>> = (0..batch_size).map(|_| payload.to_vec()).collect();
        let batch_slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();

        let name = format!("{}/batch_{}", group_name, batch_size);
        let mut group = c.benchmark_group(&name);
        group.throughput(Throughput::Elements(batch_size as u64));
        group.sampling_mode(SamplingMode::Flat);
        if batch_size >= 10_000 {
            group.sample_size(20);
        } else if batch_size >= 1_000 {
            group.sample_size(50);
        }

        group.bench_function("sonic_full_parse", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &raw in black_box(&batch_slices) {
                    let v = parse_sonic(raw);
                    total += v.is_object() as usize;
                }
                black_box(total)
            })
        });

        group.bench_function("simd_full_parse_plus_clone", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &raw in black_box(&batch_slices) {
                    let v = parse_simd_json(raw);
                    total += matches!(v, simd_json::OwnedValue::Object(_)) as usize;
                }
                black_box(total)
            })
        });

        group.finish();
    }
}

fn bench_extraction_flat30_cols15(c: &mut Criterion) {
    let payload = flat_30_payload();
    bench_extraction(c, "extraction/flat30_schema15", &payload, COLS_15);
}

fn bench_extraction_flat30_cols30(c: &mut Criterion) {
    let payload = flat_30_payload();
    bench_extraction(c, "extraction/flat30_schema30", &payload, COLS_30);
}

fn bench_extraction_nested_cols15(c: &mut Criterion) {
    let payload = nested_payload();
    bench_extraction(c, "extraction/nested_schema15", &payload, COLS_15);
}

fn bench_routing_flat30(c: &mut Criterion) {
    let payload = flat_30_payload();
    bench_routing_parse(c, "routing/flat30", &payload);
}

fn bench_routing_nested(c: &mut Criterion) {
    let payload = nested_payload();
    bench_routing_parse(c, "routing/nested", &payload);
}

criterion_group!(
    benches,
    bench_extraction_flat30_cols15,
    bench_extraction_flat30_cols30,
    bench_extraction_nested_cols15,
    bench_routing_flat30,
    bench_routing_nested,
);
criterion_main!(benches);
