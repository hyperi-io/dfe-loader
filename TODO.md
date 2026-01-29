# TODO - dfe-loader

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table Arrow buffers with clickhouse-arrow native protocol

---

## Current: Mison Benchmark Validation

- [ ] **Run Mison Benchmarks** - Awaiting dedicated host (CPU currently busy)
  - `cargo bench --bench mison`
  - Compare: `mison_15_fields` vs `mison_15_fields_batch_opt`
  - Compare: `mison_30_fields` vs `mison_30_fields_batch_opt`
  - Compare: `mison_15_fields_batch_100` vs `mison_15_fields_batch_100_opt`
  - Expected: Single-pass batch extraction should outperform per-field for 10+ fields

---

## Next: Performance Validation

- [ ] **Full Rust async optimisation review** - Hot path + operations branches
  - Review async/await patterns for unnecessary overhead
  - Check for blocking operations in async contexts
  - Audit tokio spawn vs direct await decisions

- [ ] **Benchmark transport integration** - Verify no performance regression
  - Compare before/after throughput with Kafka
  - Test with MemoryTransport for baseline

- [ ] **Production load testing** - Real-world validation
  - Test against k8s.tyrell.com.au environment
  - Monitor memory usage under sustained load

---

## Code TODOs (from source)

None currently - all critical TODOs completed.

---

## Deferred

- [ ] **WAL** - Kafka has consumer groups, Zenoh acceptable for dev/test
- [ ] **Chunking** - Kafka handles 1MB, Zenoh fragments internally
- [ ] **Envelope format** - Not needed, raw JSON/MsgPack works
- [ ] **simd-json integration** - sonic-rs benchmarks show it's already faster
- [ ] **TLS Configuration** - Use when needed
- [ ] **Memory Size Tracking** - Per-buffer memory accounting
- [ ] **OIDC Token Fetch** - `src/kafka/consumer.rs:100` - OAuth Bearer token refresh callback (awaiting requirement)

---

## Completed

### 2026-01-19: Auto-Initialization & Schema Optimization

- [x] **Auto-initialize mode** - Ensure happy path always works
  - Kafka topic creation with graceful permission failure
  - ClickHouse database/table creation from embedded DDL
  - Engine auto-detection: SharedMergeTree → ReplicatedMergeTree → MergeTree
  - Text search index: full_text (25.1+) or ngrambf bloom filter fallback
  - Config: `auto_init.enabled`, `create_topics`, `create_database`, `create_table`, `create_text_index`

- [x] **Schema module with compile-time embedding**
  - `schemas/common_table.sql` - DDL template with `{db}`, `{table}`, `{engine}`
  - `schemas/common_header.csv` - Field definitions
  - `src/schema/mod.rs` - `render_ddl_with_engine()`, `ClusterCapabilities`, `TableEngine`

- [x] **Optimized ClickHouse schema** (based on query pattern analysis)
  - `ORDER BY (_org_id, timestamp_load, _uuid)` - org first for RLS
  - `PARTITION BY (toYYYYMM(timestamp_load), _org_id)` - monthly + org (<100 orgs)
  - `LowCardinality(String)` for `_org_id` - dictionary encoding
  - `timestamp` with minmax index for event time queries
  - `timestamp_load` as primary query filter (not `timestamp`)

- [x] **Fixed hs-rustlib async_trait dependency** - Added to transport feature
  - Commit `bbf1ea1` in hs-rustlib
  - All 283 unit + 124 integration tests passing

### 2026-01-13: Project Rename & WBS Tier 1

- [x] **Project Rename Complete** - dfe-loader-clickhouse → dfe-loader
  - All 45 files updated (Cargo.toml, imports, docs, configs)
  - Backup branch: `pre-rename-backup`
  - All 280 library + 124 integration tests passing

- [x] **WBS Tier 1 Assessment Complete**
  - 1.1 Payload Detection → Migrated to hs-rustlib (366 lines)
  - 1.2-1.6 → Stay in dfe-loader (ClickHouse/app-specific)

- [x] **hs-rustlib v0.2.0 Published to Artifactory**
  - Added stateful FormatDetector with FormatMode
  - dfe-loader using registry dependency (not local path)

### 2026-01-13: Tier 2 Test Infrastructure Enhancement

- [x] Fixture Builder Library - EventBuilder, ConfigBuilder, SchemaBuilder, DdlBuilder (1,191 lines)
- [x] Query-Back Verification - All integration tests verify data after INSERT
- [x] Property-Based Tests - 11 new proptest tests (routing, transform, buffer, timestamp)
- [x] Performance Metrics - MetricsSnapshot system with auto-detect improvements/regressions
- [x] Testcontainers Infrastructure - Ready for CI/CD (not yet integrated)
- [x] Fixed RLS Test - Explicit Arrow schemas, removed `#[ignore]` marker
- [x] Testing Documentation - TESTING.md (547 lines) + PERFORMANCE_TESTING.md (147 lines)
- [x] All 421 tests passing (294 unit + 124 integration + 3 performance)

### 2026-01-07: Code TODOs Cleanup

- [x] DLQ Send integration (already implemented in orchestrator.rs:194-218)
- [x] Field Projection - `Projector` struct with schema-based field filtering

### 2025-12-29: Transport Abstraction (hs-rustlib)

- [x] Transport module structure in hs-rustlib
- [x] `Transport` trait with async send/recv/commit methods
- [x] `CommitToken` trait for transport-specific tokens
- [x] `Message<T>` struct (key, payload, token, timestamp, format)
- [x] `SendResult` enum (Ok, Backpressured, Fatal)
- [x] `TransportError` and `TransportResult` types
- [x] Feature flags: `transport-memory`, `transport-kafka`, `transport-zenoh`, `transport-all`
- [x] `MemoryTransport` using tokio::mpsc (5 unit tests)
- [x] `KafkaTransport` wrapping rdkafka with SASL/SSL support
- [x] `ZenohTransport` with Zenoh 1.x API and SHM support
- [x] `PayloadFormat` auto-detection (JSON/MsgPack by first byte)
- [x] Payload utilities: parse, serialize, extract_field, extract_nested_field
- [x] All 52 transport tests passing

### 2025-12-28: Mison Structural Index

- [x] Structural index builder (SIMD bitmaps)
- [x] Leveled colon/comma bitmap generation
- [x] Schema-guided field extractor
- [x] Pattern tree for speculation
- [x] Direct-to-Arrow column builders
- [x] Mison benchmarks
- [x] SIMD leveled bitmap optimization
- [x] Single-pass batch field extraction (O(colons + fields))
- [x] Runtime SIMD detection (AVX2/SSE4.2/NEON)

### 2025-12-28: Enrichment Modules

- [x] GeoIP enrichment with MaxMind MMDB + LRU cache
- [x] Reputation enrichment with threat types/sources + CIDR matching
- [x] Risk scoring with component weights and presets

### 2025-12-28: Performance Sprint

- [x] Created `benches/pipeline.rs` with throughput benchmarks
- [x] Updated `benches/transform.rs` with real benchmarks
- [x] Added `extract_field_json_cow()` for true zero-copy extraction
- [x] Buffer Pool with generic `ObjectPool<T>`, RAII `Pooled<T>` wrapper
- [x] Config Hot-Reload with polling-based watcher
- [x] BatchFlattener with pre-computed keys (benchmarked - existing impl optimal)

### 2025-12-25: Resilience Features

- [x] Batch Salvage - Binary-split retry on insert failure
- [x] Circuit Breaker - Per-table failure detection
- [x] Schema Cache Enhancement - Periodic refresh, error invalidation
- [x] Concurrent Insert Semaphore - Configurable parallel limit

### 2025-12-25: Full Type Support & Arrow-Only Pipeline

- [x] BFloat16, Time, Time64, AggregateFunction, SimpleAggregateFunction types
- [x] Arrow-only inserts (removed JSON fallback)
- [x] Removed klickhouse dependency
- [x] Hot path optimizations (4 rounds)
- [x] DLQ routing with per-table topics
- [x] Kafka offset commit on successful insert

### 2025-12-24: clickhouse-arrow Integration

- [x] clickhouse-arrow fork with Variant/Dynamic/Nested types
- [x] ArrowClickHouseClient wrapper
- [x] Native Arrow inserts
- [x] Configurable db.table routing
- [x] Per-table ArrowBatchBuilder with offset tracking

---

## Notes

- Test environment: k8s.tyrell.com.au (see .env for credentials)
- clickhouse-arrow fork: `crates/clickhouse-arrow/`
- Run benchmarks: `cargo bench --bench mison`
- Transport spec: `reference/transport_abstraction_spec.md`

---

**Last Updated:** 2026-01-21
