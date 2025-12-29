# TODO - dfe-loader-clickhouse

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

## Next: Transport Integration (dfe-loader-clickhouse)

**Transport Layer:** Implemented in `hs-rustlib` (52 tests passing).

**Specs:**

- [reference/transport_abstraction_spec.md](reference/transport_abstraction_spec.md) - Full transport design
- [reference/devtest_transport_analysis.md](reference/devtest_transport_analysis.md) - Dev/test options analysis

**Decision:** Three transports (Memory, Zenoh, Kafka) confirmed. Zenoh for dev/test, Kafka for production.

**Related (out of scope):**

- [reference/RECEIVER-WBS.md](reference/RECEIVER-WBS.md) - HTTP ingestion service (separate project)
- [reference/DFE-LOADER-S3-WBS.md](reference/DFE-LOADER-S3-WBS.md) - S3 archival loader (separate project)

### Integration Tasks

- [ ] Add transport config section to Config
- [ ] Replace direct Consumer with Transport trait
- [ ] Update Orchestrator to use generic transport
- [ ] Maintain existing KafkaOffset for buffer tracking
- [ ] Test with all three transports
- [ ] Benchmark to ensure no performance regression

---

## Code TODOs (from source)

### Critical Path

- [ ] **DLQ Send** - `src/pipeline/orchestrator.rs:170`
  - Currently just logs, needs actual DLQ producer integration

### Auth (Medium Priority)

- [ ] **OIDC Token Fetch** - `src/kafka/consumer.rs:100`
  - Implement OAuth Bearer token refresh callback

### Transform (Low Priority)

- [ ] **Projection** - `src/transform/project.rs:5`
  - Field projection/selection (stub, not currently used)

---

## Deferred

- [ ] **WAL** - Kafka has consumer groups, Zenoh acceptable for dev/test
- [ ] **Chunking** - Kafka handles 1MB, Zenoh fragments internally
- [ ] **Envelope format** - Not needed, raw JSON/MsgPack works
- [ ] **simd-json integration** - sonic-rs benchmarks show it's already faster
- [ ] **TLS Configuration** - Use when needed
- [ ] **Memory Size Tracking** - Per-buffer memory accounting

---

## Completed

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

**Last Updated:** 2025-12-29
