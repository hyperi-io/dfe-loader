// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      benches/insert_bakeoff.rs
// Purpose:   Real ClickHouse insert bakeoff — JSONEachRow (production) vs RowBinary
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Insert Bakeoff: `JSONEachRow` (`ClickHouseQueryClient`) vs `RowBinary` (clickhouse crate)
//!
//! **Requires a running `ClickHouse` instance.** Set env vars:
//!
//! ```bash
//! CLICKHOUSE_HOST=clickhouse.devex.hyperi.io
//! CLICKHOUSE_HTTP_PORT=8123
//! CLICKHOUSE_USER=default
//! CLICKHOUSE_PASSWORD=<password>
//! CLICKHOUSE_DATABASE=benchmark
//! ```
//!
//! ## What this measures
//!
//! Two insert paths plus parse-only benchmarks:
//!
//! 1. **`simd_jsoneachrow_http`**: sonic-rs → Map<String, Value> → `JSONEachRow` (production path)
//! 2. **`simd_rowbinary_http`**: structs → `RowBinary` → clickhouse crate HTTP (comparison)
//!
//! Batch sizes: 100, `1_000`, `10_000`, `20_000`
//!
//! Run with: `cargo bench --bench insert_bakeoff`

use std::env;
use std::sync::Arc;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use serde::Serialize;
use serde_json::{Map, Value};

use dfe_loader::clickhouse::{ClickHouseConfig, ClickHouseQueryClient, Transport};

// =============================================================================
// ClickHouse Row struct for official crate (RowBinary path)
// =============================================================================

#[derive(Debug, Clone, Serialize, clickhouse::Row)]
struct BenchRow {
    org_id: String,
    event_category: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    timestamp: time::OffsetDateTime,
    user_id: String,
    action: String,
    success: bool,
    ip_address: String,
    status_code: i32,
    response_time_ms: i64,
    user_agent: String,
}

// =============================================================================
// Test Event Data
// =============================================================================

fn make_raw_event(i: usize) -> Vec<u8> {
    format!(
        r#"{{"org_id":"acme","event_category":"auth","timestamp":1735382400{ms:03},"user_id":"user{idx}","action":"login","success":true,"ip_address":"192.168.1.{ip}","status_code":200,"response_time_ms":{rt},"user_agent":"Mozilla/5.0 bench/{idx}"}}"#,
        ms = i % 1000,
        idx = i % 1000,
        ip = i % 256,
        rt = 10 + (i % 500),
    )
    .into_bytes()
}

fn make_bench_row(i: usize) -> BenchRow {
    BenchRow {
        org_id: "acme".into(),
        event_category: "auth".into(),
        timestamp: time::OffsetDateTime::from_unix_timestamp(1_735_382_400)
            .expect("valid timestamp"),
        user_id: format!("user{}", i % 1000),
        action: "login".into(),
        success: true,
        ip_address: format!("192.168.1.{}", i % 256),
        status_code: 200,
        response_time_ms: (10 + (i % 500)) as i64,
        user_agent: format!("Mozilla/5.0 bench/{}", i % 1000),
    }
}

fn make_json_row(i: usize) -> Map<String, Value> {
    let raw = make_raw_event(i);
    sonic_rs::from_slice::<serde_json::Value>(&raw)
        .expect("valid json")
        .as_object()
        .expect("object")
        .clone()
}

// =============================================================================
// Connection helpers
// =============================================================================

struct BenchEnv {
    host: String,
    http_port: u16,
    tls: bool,
    user: String,
    password: String,
    database: String,
}

impl BenchEnv {
    fn from_env() -> Option<Self> {
        dotenvy::dotenv().ok();
        let host = env::var("CLICKHOUSE_HOST").ok()?;
        if host.is_empty() {
            return None;
        }
        Some(Self {
            host,
            http_port: env::var("CLICKHOUSE_HTTP_PORT")
                .unwrap_or_else(|_| "8123".into())
                .parse()
                .unwrap_or(8123),
            tls: env::var("CLICKHOUSE_TLS")
                .unwrap_or_default()
                .eq_ignore_ascii_case("true"),
            user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "benchmark".into()),
        })
    }

    /// Honours `CLICKHOUSE_TLS`: a plain request to a TLS port is reset by the
    /// server, which is how this read as a network fault rather than a scheme
    /// mismatch.
    fn http_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.http_port)
    }

    fn ch_client(&self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(self.http_url())
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database)
    }

    fn http_ch_client(&self) -> ClickHouseQueryClient {
        let config = ClickHouseConfig {
            hosts: vec![format!("{}:{}", self.host, self.http_port)],
            transport: Transport::Http,
            database: self.database.clone(),
            username: self.user.clone(),
            password: self.password.clone(),
            tls: self.tls,
            ..Default::default()
        };
        ClickHouseQueryClient::new(&config).expect("http client")
    }
}

async fn setup_table(env: &BenchEnv) -> String {
    let table = format!("bench_insert_{}", std::process::id());
    let client = env.ch_client();

    // ON CLUSTER creates independent MergeTree copies on all nodes.
    // The benchmark database is Atomic (not Replicated), so MergeTree() is correct here.
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {db}.{table} ON CLUSTER 'default' (
            org_id String,
            event_category String,
            timestamp DateTime64(3),
            user_id String,
            action String,
            success Bool,
            ip_address String,
            status_code Int32,
            response_time_ms Int64,
            user_agent String
        ) ENGINE = MergeTree()
        ORDER BY (org_id, timestamp)",
        db = env.database,
        table = table,
    );

    client.query(&ddl).execute().await.expect("create table");
    table
}

async fn drop_table(env: &BenchEnv, table: &str) {
    let client = env.ch_client();
    let ddl = format!(
        "DROP TABLE IF EXISTS {}.{} ON CLUSTER 'default'",
        env.database, table
    );
    let _ = client.query(&ddl).execute().await;
}

async fn truncate_table(env: &BenchEnv, table: &str) {
    let client = env.ch_client();
    let sql = format!(
        "TRUNCATE TABLE {}.{} ON CLUSTER 'default'",
        env.database, table
    );
    let _ = client.query(&sql).execute().await;
}

// =============================================================================
// Insert implementations
// =============================================================================

/// `JSONEachRow` insert — production path via `ClickHouseQueryClient`.
async fn insert_jsoneachrow(
    client: &ClickHouseQueryClient,
    db: &str,
    table: &str,
    rows: &[Map<String, Value>],
) -> usize {
    let full_table = format!("{db}.{table}");
    client
        .insert_json_rows(&full_table, rows, &[])
        .await
        .expect("jsoneachrow insert");
    rows.len()
}

/// `RowBinary` insert via the official clickhouse crate (comparison path).
async fn insert_rowbinary_http(env: &BenchEnv, table: &str, rows: &[BenchRow]) -> usize {
    let client = clickhouse::Client::default()
        .with_url(env.http_url())
        .with_user(&env.user)
        .with_password(&env.password)
        .with_database(&env.database);

    let mut insert = client.insert::<BenchRow>(table).await.expect("insert init");

    for row in rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("insert end");

    rows.len()
}

// =============================================================================
// Benchmark: Parse + Insert (full path)
// =============================================================================

fn bench_insert(c: &mut Criterion) {
    let bench_env = if let Some(env) = BenchEnv::from_env() {
        Arc::new(env)
    } else {
        eprintln!("Skipping insert_bakeoff: CLICKHOUSE_HOST not set");
        eprintln!("  Set CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER,");
        eprintln!("  CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let table = rt.block_on(setup_table(&bench_env));
    eprintln!("Benchmark table: {}.{}", bench_env.database, table);

    let http_client = Arc::new(bench_env.http_ch_client());
    let batch_sizes: Vec<usize> = vec![100, 1_000, 10_000, 20_000];

    let mut group = c.benchmark_group("insert_bakeoff");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for &size in &batch_sizes {
        let raw_events: Vec<Vec<u8>> = (0..size).map(make_raw_event).collect();
        let bench_rows: Vec<BenchRow> = (0..size).map(make_bench_row).collect();

        // =====================================================================
        // Path 1: sonic-rs parse → Map<String, Value> → JSONEachRow (production)
        // =====================================================================
        {
            let env = Arc::clone(&bench_env);
            let client = Arc::clone(&http_client);
            group.bench_with_input(
                BenchmarkId::new("simd_jsoneachrow_http", size),
                &size,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            rt.block_on(truncate_table(&env, &table));
                            let rows: Vec<Map<String, Value>> = raw_events
                                .iter()
                                .map(|b| {
                                    sonic_rs::from_slice::<serde_json::Value>(b)
                                        .expect("parse")
                                        .as_object()
                                        .expect("object")
                                        .clone()
                                })
                                .collect();
                            let start = Instant::now();
                            rt.block_on(insert_jsoneachrow(&client, &env.database, &table, &rows));
                            total += start.elapsed();
                        }
                        total
                    });
                },
            );
        }

        // =====================================================================
        // Path 2: struct → RowBinary → clickhouse crate HTTP (comparison)
        // =====================================================================
        {
            let env = Arc::clone(&bench_env);
            group.bench_with_input(
                BenchmarkId::new("simd_rowbinary_http", size),
                &size,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            rt.block_on(truncate_table(&env, &table));
                            let start = Instant::now();
                            rt.block_on(insert_rowbinary_http(&env, &table, &bench_rows));
                            total += start.elapsed();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();

    rt.block_on(drop_table(&bench_env, &table));
    eprintln!("Cleaned up benchmark table");
}

// =============================================================================
// Benchmark: Parse-only (isolate parsing cost from insert cost)
// =============================================================================

fn bench_parse_only(c: &mut Criterion) {
    let batch_sizes: Vec<usize> = vec![100, 1_000, 10_000, 20_000];

    let mut group = c.benchmark_group("parse_to_format");
    group.sample_size(50);

    for &size in &batch_sizes {
        let raw_events: Vec<Vec<u8>> = (0..size).map(make_raw_event).collect();

        // sonic-rs parse → Map<String, Value> (production parse path)
        group.bench_with_input(BenchmarkId::new("simd_to_map", size), &size, |b, _| {
            b.iter(|| {
                let rows: Vec<Map<String, Value>> = raw_events
                    .iter()
                    .map(|bytes| {
                        sonic_rs::from_slice::<serde_json::Value>(bytes)
                            .expect("parse")
                            .as_object()
                            .expect("object")
                            .clone()
                    })
                    .collect();
                std::hint::black_box(rows)
            });
        });

        // struct construction (RowBinary parse path)
        group.bench_with_input(BenchmarkId::new("simd_to_rows", size), &size, |b, _| {
            b.iter(|| {
                let rows: Vec<BenchRow> = (0..size).map(make_bench_row).collect();
                std::hint::black_box(rows)
            });
        });

        // NDJSON serialization cost (JSONEachRow wire format)
        group.bench_with_input(BenchmarkId::new("map_to_ndjson", size), &size, |b, _| {
            let rows: Vec<Map<String, Value>> = (0..size).map(make_json_row).collect();
            b.iter(|| {
                let mut buf = Vec::with_capacity(size * 256);
                for row in &rows {
                    serde_json::to_writer(&mut buf, row).expect("serialize");
                    buf.push(b'\n');
                }
                std::hint::black_box(buf)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parse_only, bench_insert);
criterion_main!(benches);
