// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Fair E2E Bakeoff Benchmark: Mison vs Decoder vs Current
//!
//! All three paths:
//! - **Input**: `Vec<&[u8]>` (raw JSON bytes from Kafka)
//! - **Output**: `RecordBatch` with identical schema + routing metadata (`db.table`)
//! - **Include**: routing field extraction, `_json` sidecar, `_destination` column
//!
//! This is the Phase 0 decision benchmark per REVIEW3.md Section 8.3.
//!
//! ## Test Matrix
//!
//! | Event Type        | Fields | Nesting |
//! |-------------------|--------|---------|
//! | Flat 10-field     | 10     | 0       |
//! | Flat 30-field     | 30     | 0       |
//! | Nested 2-level    | 15     | 2       |
//!
//! Batch sizes: 100, 1000, 10000
//!
//! ## Paths
//!
//! 1. **Current**: sonic-rs → Value DOM → route_value → flatten → ArrowBatchBuilder
//! 2. **Mison**: StructuralIndex → FieldExtractor (routing) → MisonBatchProcessor + _json sidecar
//! 3. **Decoder**: sonic-rs get_from_slice (routing) → arrow-json ReaderBuilder (schema) + _json sidecar
//!
//! Run with: `cargo bench --bench bakeoff`

use std::sync::Arc;

use std::hint::black_box;

use arrow::array::{ArrayRef, RecordBatch, StringArray, StringBuilder};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_json::ReaderBuilder;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use serde_json::Value;
use sonic_rs::JsonValueTrait;

use dfe_loader::mison::{FieldExtractor, MisonBatchProcessor, StructuralIndex};
use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::{ArrowBatchBuilder, Transformer};

// =============================================================================
// Test Event Generators
// =============================================================================

/// Flat event with 10 top-level fields (majority case for security/observability)
fn flat_10_event() -> Vec<u8> {
    br#"{"org_id":"acme","event_category":"auth","timestamp":"2024-12-28T10:30:00.123Z","user_id":"user123","action":"login","success":true,"ip_address":"192.168.1.100","status_code":200,"response_time_ms":45,"user_agent":"Mozilla/5.0"}"#.to_vec()
}

/// Flat event with 30 top-level fields (wide event from enriched pipelines)
fn flat_30_event() -> Vec<u8> {
    br#"{"org_id":"enterprise","event_category":"security","timestamp":"2024-12-28T10:30:00.123Z","user_id":"user123","session_id":"sess456","request_id":"req789","action":"api_call","method":"POST","path":"/api/v2/users","status_code":200,"response_time_ms":45,"bytes_sent":1024,"bytes_received":256,"user_agent":"Mozilla/5.0","client_ip":"203.0.113.50","server_ip":"10.0.0.5","datacenter":"us-east-1","service":"auth-service","version":"2.5.0","environment":"production","trace_id":"abc123def456","span_id":"def456ghi789","parent_span_id":"ghi789jkl012","risk_score":15,"threat_level":"low","authenticated":true,"protocol":"https","port":443,"country":"US","region":"VA"}"#.to_vec()
}

/// Nested 2-level event (typical filebeat/agent output with metadata)
fn nested_2level_event() -> Vec<u8> {
    br#"{"org_id":"corp","event_category":"network","timestamp":"2024-12-28T10:30:00.123Z","source":{"ip":"10.0.0.1","port":54321,"geo":{"country":"US","city":"Seattle"}},"destination":{"ip":"10.0.0.2","port":443},"agent":{"name":"sensor-01","version":"2.5","hostname":"server-01.example.com"},"event":{"original":"connection established","severity":"info"},"user":{"name":"admin","id":"1001"},"message":"Connection from 10.0.0.1 to 10.0.0.2:443"}"#.to_vec()
}

// =============================================================================
// Schema Definitions (shared across all paths for identical output)
// =============================================================================

/// ClickHouse-style column definitions for the 10-field event
fn flat_10_columns() -> Vec<(String, String)> {
    vec![
        ("org_id", "String"),
        ("event_category", "String"),
        ("timestamp", "DateTime64(3)"),
        ("user_id", "String"),
        ("action", "String"),
        ("success", "Bool"),
        ("ip_address", "String"),
        ("status_code", "Int32"),
        ("response_time_ms", "Int64"),
        ("user_agent", "String"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// ClickHouse-style column definitions for the 30-field event
fn flat_30_columns() -> Vec<(String, String)> {
    vec![
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
        ("risk_score", "Int32"),
        ("threat_level", "String"),
        ("authenticated", "Bool"),
        ("protocol", "String"),
        ("port", "Int32"),
        ("country", "String"),
        ("region", "String"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// ClickHouse-style column definitions for the nested 2-level event
/// Note: Mison extracts nested fields by path, Current flattens then extracts.
/// The schema uses flattened dot-notation names (matching post-flatten output).
fn nested_2level_columns() -> Vec<(String, String)> {
    vec![
        ("org_id", "String"),
        ("event_category", "String"),
        ("timestamp", "DateTime64(3)"),
        ("source.ip", "String"),
        ("source.port", "Int32"),
        ("source.geo.country", "String"),
        ("source.geo.city", "String"),
        ("destination.ip", "String"),
        ("destination.port", "Int32"),
        ("agent.name", "String"),
        ("agent.version", "String"),
        ("agent.hostname", "String"),
        ("event.original", "String"),
        ("event.severity", "String"),
        ("user.name", "String"),
        ("user.id", "String"),
        ("message", "String"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Build Arrow schema from column definitions (for Decoder path)
fn columns_to_arrow_schema(columns: &[(String, String)]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, type_str)| {
            let dt = match type_str.as_str() {
                "Bool" => DataType::Boolean,
                "Int32" => DataType::Int64, // arrow-json infers ints as Int64
                "Int64" => DataType::Int64,
                _ => DataType::Utf8, // String, DateTime64, etc. all parsed as Utf8
            };
            Field::new(name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

// =============================================================================
// Path 1: CURRENT (sonic-rs DOM → route → flatten → ArrowBatchBuilder)
// =============================================================================
// This replicates the actual hot path from orchestrator.rs:
// 1. sonic_rs::from_slice → serde_json::Value
// 2. router.route_value(&value) → db.table
// 3. transformer.transform_with_raw(value, None) → flatten + timestamp + sanitize
// 4. ArrowBatchBuilder::push(data, dest, raw_payload)
// 5. ArrowBatchBuilder::build() → RecordBatch

/// Process a batch through the Current pipeline, producing a RecordBatch
fn current_pipeline(messages: &[&[u8]], router: &Router, transformer: &Transformer) -> RecordBatch {
    let mut builder = ArrowBatchBuilder::new(messages.len());

    for &payload in messages {
        // Step 1: Parse JSON to DOM
        let value: Value = sonic_rs::from_slice(payload).unwrap();

        // Step 2: Route using parsed value
        let route = router.route_value(&value);
        let destination = match &route {
            RouteResult::Table(t) => t.as_str(),
            RouteResult::Dlq(r) => r.as_str(),
        };

        // Step 3: Transform (flatten, timestamp, field sanitization)
        let result = transformer.transform_with_raw(value, None, None).unwrap();

        // Step 4: Push to builder with _json sidecar (raw bytes)
        builder.push(result.data, destination, Some(payload));
    }

    // Step 5: Build RecordBatch
    builder.build().unwrap().unwrap()
}

// =============================================================================
// Path 2: MISON (StructuralIndex → extract routing → batch extract → Arrow)
// =============================================================================
// Mison-native path:
// 1. StructuralIndex::build(bytes) for routing extraction
// 2. FieldExtractor::extract_string for org_id, event_category → db.table
// 3. MisonBatchProcessor::process_batch → RecordBatch (schema-guided)
// 4. Append _json sidecar + _destination column to final batch

/// Process a batch through the Mison pipeline, producing a RecordBatch
fn mison_pipeline(
    messages: &[&[u8]],
    processor: &mut MisonBatchProcessor,
    default_db: &str,
    default_table: &str,
) -> RecordBatch {
    // Phase 1: Extract routing fields for all messages using Mison
    // We need db.table before we can batch-process (per-table buffers in production)
    // For the benchmark, all messages go to the same table.
    let mut destinations: Vec<String> = Vec::with_capacity(messages.len());

    for &payload in messages {
        let index = StructuralIndex::build(payload);
        let db = FieldExtractor::extract_string(&index, payload, "org_id").unwrap_or(default_db);
        let table = FieldExtractor::extract_string(&index, payload, "event_category")
            .unwrap_or(default_table);

        let mut dest = String::with_capacity(db.len() + 1 + table.len());
        dest.push_str(db);
        dest.push('.');
        dest.push_str(table);
        destinations.push(dest);
    }

    // Phase 2: Schema-guided extraction → Arrow (re-builds index internally)
    processor.clear();
    let data_batch = processor.process_batch(messages).unwrap();

    // Phase 3: Append _destination and _json columns
    let row_count = data_batch.num_rows();
    append_sidecar_columns(data_batch, messages, &destinations, row_count)
}

/// Mison optimized path using single-pass batch extraction
fn mison_pipeline_optimized(
    messages: &[&[u8]],
    processor: &mut MisonBatchProcessor,
    default_db: &str,
    default_table: &str,
) -> RecordBatch {
    let mut destinations: Vec<String> = Vec::with_capacity(messages.len());

    for &payload in messages {
        let index = StructuralIndex::build(payload);
        let db = FieldExtractor::extract_string(&index, payload, "org_id").unwrap_or(default_db);
        let table = FieldExtractor::extract_string(&index, payload, "event_category")
            .unwrap_or(default_table);

        let mut dest = String::with_capacity(db.len() + 1 + table.len());
        dest.push_str(db);
        dest.push('.');
        dest.push_str(table);
        destinations.push(dest);
    }

    processor.clear();
    let data_batch = processor.process_batch_optimized(messages).unwrap();

    let row_count = data_batch.num_rows();
    append_sidecar_columns(data_batch, messages, &destinations, row_count)
}

// =============================================================================
// Path 3: DECODER (sonic get_from_slice routing → arrow-json ReaderBuilder)
// =============================================================================
// Decoder-based path:
// 1. sonic_rs::get_from_slice for org_id, event_category → db.table (no DOM)
// 2. Accumulate raw JSON bytes as newline-delimited
// 3. arrow-json ReaderBuilder with known schema → RecordBatch
// 4. Append _json sidecar + _destination column

/// Process a batch through the Decoder pipeline, producing a RecordBatch
fn decoder_pipeline(
    messages: &[&[u8]],
    schema: SchemaRef,
    default_db: &str,
    default_table: &str,
) -> RecordBatch {
    // Phase 1: Extract routing fields using sonic-rs on-demand (no DOM)
    let mut destinations: Vec<String> = Vec::with_capacity(messages.len());

    for &payload in messages {
        let db_str: Option<String> = sonic_rs::get_from_slice(payload, &["org_id"])
            .ok()
            .and_then(|v: sonic_rs::LazyValue| v.as_str().map(|s| s.to_string()));
        let db = db_str.as_deref().unwrap_or(default_db);

        let table_str: Option<String> = sonic_rs::get_from_slice(payload, &["event_category"])
            .ok()
            .and_then(|v: sonic_rs::LazyValue| v.as_str().map(|s| s.to_string()));
        let table = table_str.as_deref().unwrap_or(default_table);

        let mut dest = String::with_capacity(db.len() + 1 + table.len());
        dest.push_str(db);
        dest.push('.');
        dest.push_str(table);
        destinations.push(dest);
    }

    // Phase 2: Concatenate raw JSON and parse via arrow-json ReaderBuilder
    // Estimate total size and pre-allocate
    let total_bytes: usize = messages.iter().map(|m| m.len() + 1).sum();
    let mut json_buf = Vec::with_capacity(total_bytes);
    for (i, &payload) in messages.iter().enumerate() {
        if i > 0 {
            json_buf.push(b'\n');
        }
        json_buf.extend_from_slice(payload);
    }

    let cursor = std::io::Cursor::new(&json_buf);
    let reader = ReaderBuilder::new(schema)
        .with_batch_size(messages.len().max(1))
        .build(cursor)
        .unwrap();

    let batches: Vec<RecordBatch> = reader.into_iter().map(|b| b.unwrap()).collect();
    let data_batch = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else if batches.is_empty() {
        panic!("Decoder produced no batches");
    } else {
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    };

    // Phase 3: Append _destination and _json columns
    let row_count = data_batch.num_rows();
    append_sidecar_columns(data_batch, messages, &destinations, row_count)
}

// =============================================================================
// Shared: _destination + _json sidecar column construction
// =============================================================================
// Both Mison and Decoder paths need this. Current path does it inside ArrowBatchBuilder.

/// Append _destination and _json sidecar columns to a RecordBatch
fn append_sidecar_columns(
    data_batch: RecordBatch,
    messages: &[&[u8]],
    destinations: &[String],
    row_count: usize,
) -> RecordBatch {
    // Build _destination column
    let mut dest_builder = StringBuilder::with_capacity(row_count, row_count * 32);
    for dest in destinations {
        dest_builder.append_value(dest);
    }
    let dest_array: ArrayRef = Arc::new(dest_builder.finish());

    // Build _json sidecar column (zero-copy-ish: accumulate bytes, build StringArray)
    let mut json_values_buf: Vec<u8> = Vec::with_capacity(row_count * 512);
    let mut json_offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    json_offsets.push(0);
    for &payload in messages {
        json_values_buf.extend_from_slice(payload);
        json_offsets.push(json_values_buf.len() as i32);
    }

    let values_buffer = Buffer::from_vec(json_values_buf);
    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(json_offsets));
    let json_array: ArrayRef = Arc::new(StringArray::new(offsets_buffer, values_buffer, None));

    // Combine: _destination + data columns + _json
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(data_batch.num_columns() + 2);
    fields.push(Arc::new(Field::new("_destination", DataType::Utf8, false)));
    fields.extend(data_batch.schema().fields().iter().cloned());
    fields.push(Arc::new(Field::new("_json", DataType::Utf8, true)));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_batch.num_columns() + 2);
    columns.push(dest_array);
    columns.extend(data_batch.columns().iter().cloned());
    columns.push(json_array);

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}

// =============================================================================
// Decoder path for nested events (needs flat field names → nested JSON field names)
// =============================================================================
// The Decoder path uses arrow-json ReaderBuilder which reads top-level JSON keys.
// For nested events, we need a schema with the TOP-LEVEL field names (not flattened).
// Then the Decoder naturally reads nested objects as JSON strings.

/// Arrow schema for the nested event's TOP-LEVEL fields (for Decoder path)
/// Nested objects use Struct types so arrow-json ReaderBuilder can parse them.
/// This matches what ReaderBuilder actually needs — it cannot parse objects as Utf8.
fn nested_2level_decoder_schema() -> SchemaRef {
    use arrow::datatypes::Fields;

    let geo_fields = Fields::from(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
    ]);

    let source_fields = Fields::from(vec![
        Field::new("ip", DataType::Utf8, true),
        Field::new("port", DataType::Int64, true),
        Field::new("geo", DataType::Struct(geo_fields), true),
    ]);

    let dest_fields = Fields::from(vec![
        Field::new("ip", DataType::Utf8, true),
        Field::new("port", DataType::Int64, true),
    ]);

    let agent_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("hostname", DataType::Utf8, true),
    ]);

    let event_fields = Fields::from(vec![
        Field::new("original", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
    ]);

    let user_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
    ]);

    Arc::new(Schema::new(vec![
        Field::new("org_id", DataType::Utf8, true),
        Field::new("event_category", DataType::Utf8, true),
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("source", DataType::Struct(source_fields), true),
        Field::new("destination", DataType::Struct(dest_fields), true),
        Field::new("agent", DataType::Struct(agent_fields), true),
        Field::new("event", DataType::Struct(event_fields), true),
        Field::new("user", DataType::Struct(user_fields), true),
        Field::new("message", DataType::Utf8, true),
    ]))
}

// =============================================================================
// Benchmark Groups
// =============================================================================

/// Benchmark all 3 paths on a single event type at multiple batch sizes
fn bench_event_type(
    c: &mut Criterion,
    group_name: &str,
    event_fn: fn() -> Vec<u8>,
    columns: Vec<(String, String)>,
    decoder_schema: SchemaRef,
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
        // Reduce sample size for large batches to keep benchmark time reasonable
        if batch_size >= 10000 {
            group.sample_size(20);
        } else if batch_size >= 1000 {
            group.sample_size(50);
        }

        // --- Path 1: Current ---
        group.bench_function("current", |b| {
            b.iter(|| current_pipeline(black_box(&batch), &router, &transformer))
        });

        // --- Path 2a: Mison (standard) ---
        {
            let cols = columns.clone();
            group.bench_function("mison", |b| {
                let mut processor = MisonBatchProcessor::new(&cols);
                b.iter(|| mison_pipeline(black_box(&batch), &mut processor, "common", "common"))
            });
        }

        // --- Path 2b: Mison (optimized single-pass) ---
        {
            let cols = columns.clone();
            group.bench_function("mison_opt", |b| {
                let mut processor = MisonBatchProcessor::new(&cols);
                b.iter(|| {
                    mison_pipeline_optimized(black_box(&batch), &mut processor, "common", "common")
                })
            });
        }

        // --- Path 3: Decoder ---
        {
            let schema = decoder_schema.clone();
            group.bench_function("decoder", |b| {
                b.iter(|| decoder_pipeline(black_box(&batch), schema.clone(), "common", "common"))
            });
        }

        group.finish();
    }
}

/// Flat 10-field events
fn bench_flat_10(c: &mut Criterion) {
    let columns = flat_10_columns();
    let schema = columns_to_arrow_schema(&columns);
    bench_event_type(
        c,
        "bakeoff/flat_10",
        flat_10_event,
        columns,
        schema,
        &[100, 1000, 10000],
    );
}

/// Flat 30-field events
fn bench_flat_30(c: &mut Criterion) {
    let columns = flat_30_columns();
    let schema = columns_to_arrow_schema(&columns);
    bench_event_type(
        c,
        "bakeoff/flat_30",
        flat_30_event,
        columns,
        schema,
        &[100, 1000, 10000],
    );
}

/// Nested 2-level events
fn bench_nested(c: &mut Criterion) {
    let columns = nested_2level_columns();
    // Decoder uses top-level schema (nested objects become Utf8 strings)
    let decoder_schema = nested_2level_decoder_schema();
    bench_event_type(
        c,
        "bakeoff/nested_2level",
        nested_2level_event,
        columns,
        decoder_schema,
        &[100, 1000, 10000],
    );
}

/// Single-message latency (p50/p99 sensitivity)
fn bench_single_message(c: &mut Criterion) {
    let events: Vec<(&str, fn() -> Vec<u8>, Vec<(String, String)>)> = vec![
        ("flat_10", flat_10_event, flat_10_columns()),
        ("flat_30", flat_30_event, flat_30_columns()),
        (
            "nested_2level",
            nested_2level_event,
            nested_2level_columns(),
        ),
    ];

    let router = Router::default();
    let transformer = Transformer::default();

    for (name, event_fn, columns) in &events {
        let event = event_fn();
        let batch: Vec<&[u8]> = vec![event.as_slice()];
        let decoder_schema = if *name == "nested_2level" {
            nested_2level_decoder_schema()
        } else {
            columns_to_arrow_schema(columns)
        };

        let mut group = c.benchmark_group(format!("bakeoff/single/{}", name));
        group.throughput(Throughput::Elements(1));

        group.bench_function("current", |b| {
            b.iter(|| current_pipeline(black_box(&batch), &router, &transformer))
        });

        {
            let cols = columns.clone();
            group.bench_function("mison", |b| {
                let mut processor = MisonBatchProcessor::new(&cols);
                b.iter(|| mison_pipeline(black_box(&batch), &mut processor, "common", "common"))
            });
        }

        {
            let cols = columns.clone();
            group.bench_function("mison_opt", |b| {
                let mut processor = MisonBatchProcessor::new(&cols);
                b.iter(|| {
                    mison_pipeline_optimized(black_box(&batch), &mut processor, "common", "common")
                })
            });
        }

        {
            let schema = decoder_schema.clone();
            group.bench_function("decoder", |b| {
                b.iter(|| decoder_pipeline(black_box(&batch), schema.clone(), "common", "common"))
            });
        }

        group.finish();
    }
}

/// Memory allocation comparison (batch of 1000)
/// Uses a single iteration to measure peak allocation difference
fn bench_allocation_pressure(c: &mut Criterion) {
    let event = flat_10_event();
    let batch: Vec<&[u8]> = (0..1000).map(|_| event.as_slice()).collect();
    let columns = flat_10_columns();
    let schema = columns_to_arrow_schema(&columns);
    let router = Router::default();
    let transformer = Transformer::default();

    let mut group = c.benchmark_group("bakeoff/alloc_pressure");
    group.throughput(Throughput::Elements(1000));

    // Current: allocates Value DOM per message + Map + flatten result
    group.bench_function("current_1000", |b| {
        b.iter(|| current_pipeline(black_box(&batch), &router, &transformer))
    });

    // Mison: allocates StructuralIndex per message (bitmaps) but no DOM
    group.bench_function("mison_1000", |b| {
        let mut processor = MisonBatchProcessor::new(&columns);
        b.iter(|| mison_pipeline(black_box(&batch), &mut processor, "common", "common"))
    });

    // Decoder: allocates NDJSON buffer + single ReaderBuilder parse
    group.bench_function("decoder_1000", |b| {
        b.iter(|| decoder_pipeline(black_box(&batch), schema.clone(), "common", "common"))
    });

    group.finish();
}

// =============================================================================
// Output Validation (not benchmarked — run once to ensure fairness)
// =============================================================================

/// Validate that all three paths produce batches with the same row count
/// and the same _destination + _json columns.
/// Schema may differ in data column types (Mison uses Utf8 for everything,
/// Decoder infers types, Current infers from serde Value).
/// The important thing is: same row count, same routing, same raw payload.
#[cfg(test)]
mod validation {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn validate_flat_10_output_equivalence() {
        let event = flat_10_event();
        let batch: Vec<&[u8]> = vec![event.as_slice(); 3];
        let columns = flat_10_columns();
        let schema = columns_to_arrow_schema(&columns);
        let router = Router::default();
        let transformer = Transformer::default();

        let current = current_pipeline(&batch, &router, &transformer);
        let mut processor = MisonBatchProcessor::new(&columns);
        let mison = mison_pipeline(&batch, &mut processor, "common", "common");
        let decoder = decoder_pipeline(&batch, schema, "common", "common");

        // Same row count
        assert_eq!(current.num_rows(), 3, "current row count");
        assert_eq!(mison.num_rows(), 3, "mison row count");
        assert_eq!(decoder.num_rows(), 3, "decoder row count");

        // All have _destination column
        assert!(
            current.schema().field_with_name("_destination").is_ok(),
            "current missing _destination"
        );
        assert!(
            mison.schema().field_with_name("_destination").is_ok(),
            "mison missing _destination"
        );
        assert!(
            decoder.schema().field_with_name("_destination").is_ok(),
            "decoder missing _destination"
        );

        // All have _json column
        assert!(
            current.schema().field_with_name("_json").is_ok(),
            "current missing _json"
        );
        assert!(
            mison.schema().field_with_name("_json").is_ok(),
            "mison missing _json"
        );
        assert!(
            decoder.schema().field_with_name("_json").is_ok(),
            "decoder missing _json"
        );

        // _json values are identical raw payloads
        let current_json = get_string_column(&current, "_json");
        let mison_json = get_string_column(&mison, "_json");
        let decoder_json = get_string_column(&decoder, "_json");

        for i in 0..3 {
            assert_eq!(
                current_json.value(i),
                mison_json.value(i),
                "row {} _json mismatch current vs mison",
                i
            );
            assert_eq!(
                current_json.value(i),
                decoder_json.value(i),
                "row {} _json mismatch current vs decoder",
                i
            );
        }

        // _destination values match (all should route to common.auth)
        let current_dest = get_string_column(&current, "_destination");
        let mison_dest = get_string_column(&mison, "_destination");
        let decoder_dest = get_string_column(&decoder, "_destination");

        for i in 0..3 {
            assert_eq!(
                current_dest.value(i),
                mison_dest.value(i),
                "row {} _destination mismatch current vs mison",
                i
            );
            assert_eq!(
                current_dest.value(i),
                decoder_dest.value(i),
                "row {} _destination mismatch current vs decoder",
                i
            );
        }
    }

    #[test]
    fn validate_flat_30_output_equivalence() {
        let event = flat_30_event();
        let batch: Vec<&[u8]> = vec![event.as_slice(); 3];
        let columns = flat_30_columns();
        let schema = columns_to_arrow_schema(&columns);
        let router = Router::default();
        let transformer = Transformer::default();

        let current = current_pipeline(&batch, &router, &transformer);
        let mut processor = MisonBatchProcessor::new(&columns);
        let mison = mison_pipeline(&batch, &mut processor, "common", "common");
        let decoder = decoder_pipeline(&batch, schema, "common", "common");

        assert_eq!(current.num_rows(), 3);
        assert_eq!(mison.num_rows(), 3);
        assert_eq!(decoder.num_rows(), 3);
    }

    #[test]
    fn validate_nested_output_equivalence() {
        let event = nested_2level_event();
        let batch: Vec<&[u8]> = vec![event.as_slice(); 3];
        let columns = nested_2level_columns();
        let decoder_schema = nested_2level_decoder_schema();
        let router = Router::default();
        let transformer = Transformer::default();

        let current = current_pipeline(&batch, &router, &transformer);
        let mut processor = MisonBatchProcessor::new(&columns);
        let mison = mison_pipeline(&batch, &mut processor, "common", "common");
        let decoder = decoder_pipeline(&batch, decoder_schema, "common", "common");

        assert_eq!(current.num_rows(), 3);
        assert_eq!(mison.num_rows(), 3);
        assert_eq!(decoder.num_rows(), 3);
    }

    fn get_string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }
}

// =============================================================================
// Criterion Setup
// =============================================================================

criterion_group!(
    benches,
    bench_flat_10,
    bench_flat_30,
    bench_nested,
    bench_single_message,
    bench_allocation_pressure,
);
criterion_main!(benches);
