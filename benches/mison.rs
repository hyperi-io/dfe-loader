// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Mison benchmarks
//!
//! Compares Mison structural index extraction against traditional parsing:
//! - Structural index building
//! - Field extraction (single and batch)
//! - Full pipeline comparison: Mison vs Traditional
//!
//! Run with: cargo bench --bench mison

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::Value;
use sonic_rs::JsonValueTrait;

use dfe_loader::mison::{FieldExtractor, MisonBatchProcessor, SchemaExtractor, StructuralIndex};

/// Simple event with top-level fields
fn simple_event() -> Vec<u8> {
    br#"{"org_id":"acme","event_category":"auth","timestamp":"2024-12-28T10:30:00Z","user_id":"user123","action":"login","success":true,"ip_address":"192.168.1.100"}"#.to_vec()
}

/// Nested event with metadata
fn nested_event() -> Vec<u8> {
    br#"{"org_id":"corp","event_category":"network","timestamp":"2024-12-28T10:30:00Z","data":{"src_ip":"10.0.0.1","dst_ip":"10.0.0.2","bytes":1500},"tags":{"env":"prod","region":"us-east"}}"#.to_vec()
}

/// Deep nested event
fn deep_event() -> Vec<u8> {
    br#"{"org_id":"deep","event_category":"complex","timestamp":"2024-12-28T10:30:00Z","level1":{"level2":{"level3":{"level4":{"value":"deep_value","count":42}}}}}"#.to_vec()
}

/// Realistic security event
fn security_event() -> Vec<u8> {
    br#"{"org_id":"security","event_category":"threat","timestamp":"2024-12-28T10:30:00.123Z","source":{"ip":"203.0.113.50","port":54321,"geo":{"country":"US","city":"Seattle"}},"destination":{"ip":"10.0.0.5","port":443},"alert":{"severity":"high","type":"intrusion","confidence":0.95},"metadata":{"collector":"sensor-01","version":"2.5"}}"#.to_vec()
}

/// Benchmark structural index building
fn bench_structural_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_index");

    for (name, data) in [
        ("simple", simple_event()),
        ("nested", nested_event()),
        ("deep", deep_event()),
        ("security", security_event()),
    ] {
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("build", name), &data, |b, data| {
            b.iter(|| {
                let index = StructuralIndex::build(black_box(data));
                black_box(index)
            })
        });
    }

    group.finish();
}

/// Benchmark single field extraction
fn bench_field_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_extract");

    let simple = simple_event();
    let simple_index = StructuralIndex::build(&simple);

    let nested = nested_event();
    let nested_index = StructuralIndex::build(&nested);

    // Single top-level field
    group.bench_function("single_top_level", |b| {
        b.iter(|| {
            FieldExtractor::extract_string(black_box(&simple_index), black_box(&simple), "org_id")
        })
    });

    // Multiple fields at once
    group.bench_function("multiple_fields", |b| {
        b.iter(|| {
            FieldExtractor::extract_strings(
                black_box(&simple_index),
                black_box(&simple),
                &["org_id", "event_category", "user_id"],
            )
        })
    });

    // Nested field extraction
    group.bench_function("nested_field", |b| {
        b.iter(|| {
            FieldExtractor::extract_nested_string(
                black_box(&nested_index),
                black_box(&nested),
                &["data", "src_ip"],
            )
        })
    });

    group.finish();
}

/// Benchmark schema-guided extraction
fn bench_schema_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_schema");

    // Simulate ClickHouse columns
    let columns = vec![
        ("org_id".to_string(), "String".to_string()),
        ("event_category".to_string(), "String".to_string()),
        ("timestamp".to_string(), "DateTime64(3)".to_string()),
        ("user_id".to_string(), "String".to_string()),
        ("action".to_string(), "String".to_string()),
        ("success".to_string(), "Bool".to_string()),
    ];

    let simple = simple_event();
    let simple_index = StructuralIndex::build(&simple);

    let mut extractor = SchemaExtractor::from_columns(&columns);

    group.bench_function("extract_all_fields", |b| {
        b.iter(|| extractor.extract_all(black_box(&simple_index), black_box(&simple)))
    });

    group.finish();
}

/// Benchmark full Mison pipeline vs traditional
fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_vs_traditional");

    // Columns to extract
    let columns = vec![
        ("org_id".to_string(), "String".to_string()),
        ("event_category".to_string(), "String".to_string()),
        ("timestamp".to_string(), "DateTime64(3)".to_string()),
        ("user_id".to_string(), "String".to_string()),
    ];

    let simple = simple_event();

    // Mison pipeline: bytes -> index -> extract -> Arrow
    group.bench_function("mison_single", |b| {
        let mut processor = MisonBatchProcessor::new(&columns);
        b.iter(|| {
            processor.clear();
            processor.process_single(black_box(&simple))
        })
    });

    // Traditional pipeline: bytes -> parse -> Value -> extract
    // Note: we use owned strings to avoid lifetime issues
    group.bench_function("traditional_single", |b| {
        b.iter(|| {
            // Parse JSON
            let value: Value = sonic_rs::from_slice(black_box(&simple)).unwrap();

            // Extract fields (simulating what transformer does)
            let org_id = value
                .get("org_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let category = value
                .get("event_category")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timestamp = value
                .get("timestamp")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let user_id = value
                .get("user_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            black_box((org_id, category, timestamp, user_id))
        })
    });

    // Batch comparison
    let batch: Vec<&[u8]> = (0..100).map(|_| simple.as_slice()).collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("mison_batch_100", |b| {
        let mut processor = MisonBatchProcessor::new(&columns);
        b.iter(|| {
            processor.clear();
            processor.process_batch(black_box(&batch))
        })
    });

    group.bench_function("traditional_batch_100", |b| {
        b.iter(|| {
            let mut results = Vec::with_capacity(100);
            for data in &batch {
                let value: Value = sonic_rs::from_slice(black_box(*data)).unwrap();
                let org_id = value
                    .get("org_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let category = value
                    .get("event_category")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                results.push((org_id, category));
            }
            black_box(results)
        })
    });

    group.finish();
}

/// Benchmark routing field extraction (the critical hot path)
fn bench_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_routing");

    let simple = simple_event();

    // Mison: build index + extract routing fields
    group.bench_function("mison_route", |b| {
        b.iter(|| {
            let index = StructuralIndex::build(black_box(&simple));
            let org_id = FieldExtractor::extract_string(&index, &simple, "org_id");
            let category = FieldExtractor::extract_string(&index, &simple, "event_category");
            black_box((org_id, category))
        })
    });

    // Traditional: parse + extract (with owned strings)
    group.bench_function("traditional_route", |b| {
        b.iter(|| {
            let value: Value = sonic_rs::from_slice(black_box(&simple)).unwrap();
            let org_id = value
                .get("org_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let category = value
                .get("event_category")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            black_box((org_id, category))
        })
    });

    // Existing sonic-rs get_from_slice approach
    group.bench_function("sonic_get_from_slice", |b| {
        b.iter(|| {
            let org_id: Option<String> = sonic_rs::get_from_slice(black_box(&simple), &["org_id"])
                .ok()
                .and_then(|v: sonic_rs::LazyValue| v.as_str().map(|s| s.to_string()));
            let category: Option<String> =
                sonic_rs::get_from_slice(black_box(&simple), &["event_category"])
                    .ok()
                    .and_then(|v: sonic_rs::LazyValue| v.as_str().map(|s| s.to_string()));
            black_box((org_id, category))
        })
    });

    group.finish();
}

/// Filebeat-style event with many fields (15 fields)
fn filebeat_event() -> Vec<u8> {
    br#"{"@timestamp":"2024-12-28T10:30:00.123Z","@metadata":{"beat":"filebeat","type":"_doc","version":"8.11.0"},"agent":{"name":"server-01","type":"filebeat","version":"8.11.0","hostname":"server-01.example.com","id":"abc123"},"ecs":{"version":"8.0.0"},"host":{"name":"server-01","hostname":"server-01.example.com","architecture":"x86_64","os":{"platform":"linux","version":"22.04","family":"debian","name":"Ubuntu","kernel":"5.15.0"},"ip":["192.168.1.100","10.0.0.50"],"mac":["00:11:22:33:44:55"]},"log":{"file":{"path":"/var/log/app.log"},"offset":12345},"message":"User login successful","event":{"original":"2024-12-28 10:30:00 INFO User login successful user=admin ip=192.168.1.100"},"source":{"ip":"192.168.1.100","port":54321},"user":{"name":"admin","id":"1001"}}"#.to_vec()
}

/// Large event with 30+ fields
fn large_event() -> Vec<u8> {
    br#"{"org_id":"enterprise","event_category":"security","timestamp":"2024-12-28T10:30:00.123Z","user_id":"user123","session_id":"sess456","request_id":"req789","action":"api_call","method":"POST","path":"/api/v2/users","status_code":200,"response_time_ms":45,"bytes_sent":1024,"bytes_received":256,"user_agent":"Mozilla/5.0","client_ip":"203.0.113.50","server_ip":"10.0.0.5","datacenter":"us-east-1","service":"auth-service","version":"2.5.0","environment":"production","trace_id":"abc123","span_id":"def456","parent_span_id":"ghi789","tags":["api","auth","user"],"metadata":{"retry_count":0,"cached":false},"geo":{"country":"US","region":"VA","city":"Ashburn"},"risk_score":15,"threat_level":"low","authenticated":true}"#.to_vec()
}

/// Benchmark many-field extraction (filebeat-style, 15+ fields)
fn bench_many_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("mison_many_fields");

    // Filebeat-style columns (15 fields)
    let filebeat_columns: Vec<(String, String)> = vec![
        ("@timestamp", "DateTime64(3)"),
        ("agent.name", "String"),
        ("agent.type", "String"),
        ("agent.version", "String"),
        ("agent.hostname", "String"),
        ("host.name", "String"),
        ("host.hostname", "String"),
        ("host.architecture", "String"),
        ("log.offset", "Int64"),
        ("message", "String"),
        ("source.ip", "String"),
        ("source.port", "Int32"),
        ("user.name", "String"),
        ("user.id", "String"),
        ("ecs.version", "String"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    // Large event columns (30 fields)
    let large_columns: Vec<(String, String)> = vec![
        ("org_id", "String"),
        ("event_category", "String"),
        ("timestamp", "DateTime64(3)"),
        ("user_id", "String"),
        ("session_id", "String"),
        ("request_id", "String"),
        ("action", "String"),
        ("method", "String"),
        ("path", "String"),
        ("status_code", "Int32"),
        ("response_time_ms", "Int64"),
        ("bytes_sent", "Int64"),
        ("bytes_received", "Int64"),
        ("user_agent", "String"),
        ("client_ip", "String"),
        ("server_ip", "String"),
        ("datacenter", "String"),
        ("service", "String"),
        ("version", "String"),
        ("environment", "String"),
        ("trace_id", "String"),
        ("span_id", "String"),
        ("parent_span_id", "String"),
        ("geo.country", "String"),
        ("geo.region", "String"),
        ("geo.city", "String"),
        ("risk_score", "Int32"),
        ("threat_level", "String"),
        ("authenticated", "Bool"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let filebeat = filebeat_event();
    let large = large_event();

    // Mison: 15 fields (original per-field extraction)
    group.bench_function("mison_15_fields", |b| {
        let mut processor = MisonBatchProcessor::new(&filebeat_columns);
        b.iter(|| {
            processor.clear();
            processor.process_single(black_box(&filebeat))
        })
    });

    // Mison: 15 fields (optimized batch extraction)
    let filebeat_batch_1: Vec<&[u8]> = vec![filebeat.as_slice()];
    group.bench_function("mison_15_fields_batch_opt", |b| {
        let mut processor = MisonBatchProcessor::new(&filebeat_columns);
        b.iter(|| {
            processor.clear();
            processor.process_batch_optimized(black_box(&filebeat_batch_1))
        })
    });

    // Traditional: 15 fields
    group.bench_function("traditional_15_fields", |b| {
        b.iter(|| {
            let value: Value = sonic_rs::from_slice(black_box(&filebeat)).unwrap();
            // Extract 15 fields
            let ts = value
                .get("@timestamp")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let agent = value
                .get("agent")
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let host = value
                .get("host")
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let msg = value
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let src_ip = value
                .get("source")
                .and_then(|v| v.get("ip"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let user = value
                .get("user")
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            // ... more fields
            black_box((ts, agent, host, msg, src_ip, user))
        })
    });

    // Mison: 30 fields (original per-field extraction)
    group.bench_function("mison_30_fields", |b| {
        let mut processor = MisonBatchProcessor::new(&large_columns);
        b.iter(|| {
            processor.clear();
            processor.process_single(black_box(&large))
        })
    });

    // Mison: 30 fields (optimized batch extraction)
    let large_batch_1: Vec<&[u8]> = vec![large.as_slice()];
    group.bench_function("mison_30_fields_batch_opt", |b| {
        let mut processor = MisonBatchProcessor::new(&large_columns);
        b.iter(|| {
            processor.clear();
            processor.process_batch_optimized(black_box(&large_batch_1))
        })
    });

    // Traditional: 30 fields
    group.bench_function("traditional_30_fields", |b| {
        b.iter(|| {
            let value: Value = sonic_rs::from_slice(black_box(&large)).unwrap();
            // Extract many fields
            let mut results = Vec::with_capacity(30);
            for field in &[
                "org_id",
                "event_category",
                "timestamp",
                "user_id",
                "session_id",
                "request_id",
                "action",
                "method",
                "path",
                "status_code",
                "response_time_ms",
                "bytes_sent",
                "bytes_received",
                "user_agent",
                "client_ip",
                "server_ip",
                "datacenter",
                "service",
                "version",
                "environment",
                "trace_id",
                "span_id",
                "parent_span_id",
                "risk_score",
                "threat_level",
                "authenticated",
            ] {
                results.push(
                    value
                        .get(*field)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                );
            }
            // Nested fields
            let country = value
                .get("geo")
                .and_then(|v| v.get("country"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let region = value
                .get("geo")
                .and_then(|v| v.get("region"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            results.push(country);
            results.push(region);
            black_box(results)
        })
    });

    // Batch of 100 filebeat events
    let filebeat_batch: Vec<&[u8]> = (0..100).map(|_| filebeat.as_slice()).collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("mison_15_fields_batch_100", |b| {
        let mut processor = MisonBatchProcessor::new(&filebeat_columns);
        b.iter(|| {
            processor.clear();
            processor.process_batch(black_box(&filebeat_batch))
        })
    });

    group.bench_function("mison_15_fields_batch_100_opt", |b| {
        let mut processor = MisonBatchProcessor::new(&filebeat_columns);
        b.iter(|| {
            processor.clear();
            processor.process_batch_optimized(black_box(&filebeat_batch))
        })
    });

    group.bench_function("traditional_15_fields_batch_100", |b| {
        b.iter(|| {
            let mut results = Vec::with_capacity(100);
            for data in &filebeat_batch {
                let value: Value = sonic_rs::from_slice(black_box(*data)).unwrap();
                let ts = value
                    .get("@timestamp")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let msg = value
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                results.push((ts, msg));
            }
            black_box(results)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_structural_index,
    bench_field_extraction,
    bench_schema_extraction,
    bench_full_pipeline,
    bench_routing,
    bench_many_fields,
);

criterion_main!(benches);
