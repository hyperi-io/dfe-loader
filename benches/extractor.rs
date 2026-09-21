// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Header extractor benchmarks
//!
//! Measures the per-row cost of `HeaderExtractor::extract` on the two shapes
//! the per-table plan distinguishes:
//! - `uncontended` -- no source field is read by more than one column, so every
//!   column keeps the move
//! - `contended` -- `_timestamp` and a meta `timestamp` column read one field,
//!   so that one field is copied
//!
//! Run with: cargo bench --bench extractor

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use dfe_loader::clickhouse::{ColumnInfo, ParsedType, TableSchema};
use dfe_loader::column_meta::{ColumnDirectivesConfig, ColumnMetaCache, parse_directives};
use dfe_loader::config::{MetadataConfig, RoutingConfig};
use dfe_loader::transform::HeaderExtractor;
use rustc_hash::FxHashMap;

/// A syslog-shaped table: the common header, then the typed columns.
fn schema(columns: &[(&str, &str)]) -> TableSchema {
    TableSchema {
        database: "dfe".to_string(),
        table: "syslog".to_string(),
        columns: columns
            .iter()
            .enumerate()
            .map(|(i, (name, type_name))| ColumnInfo {
                name: (*name).to_string(),
                type_name: (*type_name).to_string(),
                parsed_type: ParsedType::parse(type_name),
                position: (i as u64) + 1,
                default_kind: String::new(),
                default_expression: String::new(),
                comment: String::new(),
                is_in_primary_key: false,
                is_in_sorting_key: false,
            })
            .collect(),
        comment: String::new(),
    }
}

const COMMON_HEADER: [(&str, &str); 5] = [
    ("_uuid", "UUID"),
    ("_timestamp", "DateTime64(3)"),
    ("_timestamp_received", "DateTime64(3)"),
    ("_source", "LowCardinality(String)"),
    ("_org_id", "LowCardinality(String)"),
];

const TYPED_COLUMNS: [(&str, &str); 7] = [
    ("hostname", "String"),
    ("app_name", "LowCardinality(String)"),
    ("facility", "LowCardinality(String)"),
    ("severity", "LowCardinality(String)"),
    ("proc_id", "String"),
    ("source_ip", "String"),
    ("message", "String"),
];

/// Every column reads its own field, so nothing is shared.
fn uncontended_schema() -> TableSchema {
    let mut columns: Vec<(&str, &str)> = COMMON_HEADER.to_vec();
    columns.extend_from_slice(&TYPED_COLUMNS);
    schema(&columns)
}

/// `dfe.proofsyslog`: a meta `timestamp` column reads the field `_timestamp`
/// also reads.
fn contended_schema() -> TableSchema {
    let mut columns: Vec<(&str, &str)> = COMMON_HEADER.to_vec();
    columns.push(("timestamp", "DateTime64(3)"));
    columns.extend_from_slice(&TYPED_COLUMNS);
    schema(&columns)
}

fn col_meta(with_timestamp_source: bool) -> ColumnMetaCache {
    let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
    let mut ddl = FxHashMap::default();
    ddl.insert(
        "source_ip".to_string(),
        parse_directives("@source: first(source_ip/src_ip/sourceip) - Source address"),
    );
    if with_timestamp_source {
        ddl.insert(
            "timestamp".to_string(),
            parse_directives("@source: first(timestamp/@timestamp/time) - Event timestamp"),
        );
    }
    cache.apply_ddl("dfe.syslog", ddl);
    cache
}

const PAYLOAD: &[u8] = br#"{
    "timestamp": "2026-09-21T04:45:00.123Z",
    "hostname": "proof-host-01",
    "app_name": "sshd",
    "facility": "auth",
    "severity": "info",
    "proc_id": "4242",
    "source_ip": "10.0.0.9",
    "message": "Accepted password for svc-ingest",
    "unmapped": "ignored"
}"#;

fn bench_extract(c: &mut Criterion) {
    let extractor = HeaderExtractor::new(&MetadataConfig::default(), &RoutingConfig::default());

    let mut group = c.benchmark_group("header_extract");
    group.throughput(Throughput::Elements(1));

    let uncontended = uncontended_schema();
    let plain_meta = col_meta(false);
    group.bench_function("uncontended", |b| {
        b.iter(|| {
            black_box(extractor.extract(
                black_box(PAYLOAD),
                "dfe.syslog",
                &uncontended,
                &plain_meta,
            ))
        });
    });

    let contended = contended_schema();
    let sharing_meta = col_meta(true);
    group.bench_function("contended", |b| {
        b.iter(|| {
            black_box(extractor.extract(
                black_box(PAYLOAD),
                "dfe.syslog",
                &contended,
                &sharing_meta,
            ))
        });
    });

    group.finish();
}

criterion_group!(benches, bench_extract);
criterion_main!(benches);
