# TODO - dfe-loader

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table Arrow buffers with clickhouse-arrow native protocol

---

## Current: Zenoh → gRPC Transport Migration

**Goal:** Replace `ZenohTransport` with `GrpcTransport` (tonic) across all DFE projects.

**Design:** `reference/grpc_transport_design.md`
**Analysis:** `reference/devtest_transport_analysis.md`

### Phase 1: rustlib (hyperi-rustlib)

1. [ ] Create `proto/dfe_transport.proto` — PushEvents RPC definition
2. [ ] Add `tonic-build` to build deps, write `build.rs` for proto codegen
3. [ ] Create `src/transport/grpc/` module (config, token, client, server)
4. [ ] Implement `GrpcTransport` with existing `Transport` trait
5. [ ] Update `TransportType` enum — Zenoh → Grpc
6. [ ] Update `TransportConfig` — zenoh field → grpc field
7. [ ] Update feature flags — `transport-zenoh` → `transport-grpc` (tonic + prost)
8. [ ] Remove `src/transport/zenoh/` directory
9. [ ] Remove `zenoh` dependency from Cargo.toml
10. [ ] Write unit tests (config, token, round-trip, backpressure, shutdown)
11. [ ] Publish to Artifactory (major bump — v2.0.0)

### Phase 2: dfe-loader

1. [ ] Update Cargo.toml — `transport-zenoh` → `transport-grpc`, rustlib `>=2.0`
2. [ ] Remove `ZenohConfig` from `src/config/loader.rs`
3. [ ] Add `GrpcConfig` to `src/config/loader.rs`
4. [ ] Remove `ZenohTransportAdapter` from `src/kafka/transport.rs`
5. [ ] Add `GrpcTransportAdapter` to `src/kafka/transport.rs`
6. [ ] Update `TransportBackend` enum — Zenoh → Grpc
7. [ ] Update config examples and docs
8. [ ] Run tests, verify CI passes

### Phase 3: dfe-archiver

1. [ ] Update Cargo.toml — swap `transport-zenoh` for `transport-grpc`, rustlib `>=2.0`
2. [ ] No code changes needed (uses rustlib transport directly)

### Phase 4: dfe-receiver

1. [ ] Add `transport-grpc` to Cargo.toml features
2. [ ] Add gRPC client config to `LoaderConfig`
3. [ ] Wire up gRPC client for direct-to-loader delivery
4. [ ] Test: receiver → gRPC → loader → ClickHouse

---

## Next: Defaults & Per-Table _raw Config

- [x] **Change `default_table` from `"dfe"` to `"default"`** — destination becomes `dfe.default`
  - Updated `RoutingConfig::default()` in `src/config/loader.rs` and `src/routing/router.rs`
  - Updated config test assertions and route result assertions
  - Auto-init creates `dfe.default` on startup

- [ ] **Per-table `_raw` drop** — configurable `include_raw` with per-table overrides
  1. [ ] Add `include_raw: bool` and `raw_overrides: HashMap<String, bool>` to `MetadataConfig`
  2. [ ] Transformer checks table name against overrides before injecting `_raw`
  3. [ ] Default: `include_raw = true`, `default` table override to `false`
  4. [ ] Unit tests for global default, per-table override, and precedence logic
  5. [ ] Integration test: verify `_raw` NULL when dropped, present when included

---

## Next: GHCR Container Image Publishing

- [ ] **Container image publishing** (see `docs/CONTAINER-PUBLISHING.md`)
  1. [ ] Create `Dockerfile` in repo root (wraps pre-built binary, Option B)
  2. [ ] Add `publish.container` section to `.hyperi-ci.yaml`
  3. [ ] Update ci submodule to v1.59.0+
  4. [ ] Update publish workflow for container inputs
  5. [ ] Test: trigger release, verify `ghcr.io/hyperi-io/dfe-loader`

---

## Next: Startup Version Check (rustlib)

- [ ] **Add `version-check` feature to hyperi-rustlib**
  - Calls `POST /api/v1/check` on hyperi-telemetry (Cloudflare Worker) on startup
  - Sends: product, current_version, instance_id, os, arch, deployment
  - Logs result: info if update available, debug if current, warn if check failed
  - Graceful failure — never blocks or crashes on network error
  - Feature flag: `version-check` (requires `reqwest`, `tokio`, `serde_json`)
  - NOT named telemetry — exposed as version check only

- [ ] **Wire version check into dfe-loader startup**
  - Call `version_check::check_on_startup()` in main.rs
  - Pass product="dfe-loader", version from Cargo.toml

---

## Next: Performance Validation & CI Stabilisation

- [ ] **Run Mison Benchmarks** - Awaiting dedicated host (CPU currently busy)

- [ ] **Full Rust async optimisation review** - Hot path + operations branches

- [ ] **Benchmark transport integration** - Verify no performance regression

- [ ] **Production load testing** - Real-world validation

---

## Code TODOs (from source)

- None currently

---

## Deferred

- [ ] **Receiver WAL** - Required for at-least-once with gRPC mesh (separate from transport)
- [ ] **Chunking** - Kafka handles 1MB, gRPC has configurable max_message_size (16MB default)
- [ ] **Envelope format** - Not needed, raw JSON/MsgPack works
- [ ] **simd-json integration** - sonic-rs benchmarks show it's already faster
- [ ] **TLS Configuration** - Use when needed
- [ ] **Memory Size Tracking** - Per-buffer memory accounting
- [ ] **OIDC Token Fetch** - `src/kafka/consumer.rs:100` - OAuth Bearer token refresh callback (awaiting requirement)

---

## Completed

### 2026-03-02: Config Reload & Registry Publishing

- [x] **Publish hyperi-rustlib with config-reload feature** to Artifactory
- [x] **Switch dfe-loader back to registry dep** — `>=1.7.0` from Artifactory
- [x] **CI pipeline validated** — Detect Config, Quality, Test all green
- [x] **Fixed cargo fmt** — 9 files formatted
- [x] **Fixed auto-commit race** — `continue-on-error: true` on markdown auto-fix step
- [x] All CI passing, v1.9.3 published

### 2026-02-25: Config Cascade & Config-Reload Migration

- [x] **Figment config cascade** - Replaced `config` crate with figment (CLI > ENV > .env > config file > defaults)
- [x] **DFE_LOADER_ env prefix** - All config via `DFE_LOADER__KAFKA__BROKERS` nesting + flat overrides
- [x] **SharedConfig<T> type alias** - Replaced local SharedConfig with hyperi-rustlib generic
- [x] **ConfigReloader<T> wrapper** - Replaced local ConfigWatcher with hyperi-rustlib ConfigReloader
- [x] **Config hot-reload wired** - SIGHUP + file polling + periodic timer in pipeline
- [x] **_source field + common header** - Optional _source injection, configurable common header fields
- [x] **Arrow version pin** - `<58.0` for clickhouse-arrow 0.4.2 compatibility
- [x] **dfe-archiver migrated** - SharedConfig type alias + ConfigReloader wiring
- [x] **dfe-receiver migrated** - SharedConfig type alias + ConfigReloader + subscriber rebuild pattern
- [x] All 389 unit tests passing after merge to main

### 2026-02-19: CI Cross-Compilation & Binary Publish Pipeline

- [x] **Fixed pipe-delimited RUST_FEATURES parsing** in build.sh (extract first set for binary builds)
- [x] **Fixed arch-specific OpenSSL headers** for aarch64 cross-compilation (`CFLAGS_aarch64_unknown_linux_gnu`)
- [x] **Fixed GNU ld script absolute paths** via sed patching + usrmerge handling in sysroot
- [x] **Fixed transitive dependency resolution** in sysroot (two-level deep for libc6:arm64)
- [x] **Installed libc6-dev:arm64 system-wide** for dynamic linker availability (Multi-Arch: same)
- [x] **Fixed binary publish permission loss** — `actions/upload-artifact@v4` strips Unix permissions; added `chmod +x` restore step
- [x] **Reviewed macOS BSD compatibility changes** — `sed -i.bak`, `printf "%b"`, `grep -oE` all safe on Linux CI
- [x] v1.6.13 published successfully with both amd64 (26M) and arm64 (22M) binaries to Artifactory

### 2026-02-18: Field Mapping Integration Tests

- [x] Integration tests for field mapping feature (33 tests in `tests/integration/field_mapping.rs`)
  - Apply semantics (rename, copy, first-match, priority, skip existing, missing source, empty, value types)
  - MappingBuilder schema filtering (column matching, empty schema, comment overrides, config override)
  - Builtin preset smoke tests (ECS, CIM, Beats loading and apply)
  - FieldMappingCache lifecycle (get, build, invalidate, dedup, drain)
  - ClickHouse `@renamed` column comment parsing (with skip guard)
  - End-to-end preset apply with schema filtering

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

- [x] **Fixed hyperi-rustlib async_trait dependency** - Added to transport feature
  - Commit `bbf1ea1` in hyperi-rustlib
  - All 283 unit + 124 integration tests passing

### 2026-01-13: Project Rename & WBS Tier 1

- [x] **Project Rename Complete** - dfe-loader-clickhouse → dfe-loader
  - All 45 files updated (Cargo.toml, imports, docs, configs)
  - Backup branch: `pre-rename-backup`
  - All 280 library + 124 integration tests passing

- [x] **WBS Tier 1 Assessment Complete**
  - 1.1 Payload Detection → Migrated to hyperi-rustlib (366 lines)
  - 1.2-1.6 → Stay in dfe-loader (ClickHouse/app-specific)

- [x] **hyperi-rustlib v0.2.0 Published to Artifactory**
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

### 2025-12-29: Transport Abstraction (hyperi-rustlib)

- [x] Transport module structure in hyperi-rustlib
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

**Last Updated:** 2026-03-02
