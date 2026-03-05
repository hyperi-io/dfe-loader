# TODO - dfe-loader

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table Arrow buffers with clickhouse-arrow native protocol

---

## CI Infrastructure Overhaul `[PLANNED]`

**Goal:** Fix registry routing, migrate bash→Python, add Claude CI helpers, fast test project.

### Phase A: Claude Code CI Helpers
- [ ] ci/scripts/claude/ci-watch.py — poll run to completion, structured output
- [ ] ci/scripts/claude/ci-logs.py — fetch/filter logs, --grep, --failed, --tail
- [ ] ci/scripts/claude/ci-trigger.py — dispatch + optional watch

### Phase B: Registry Routing Fix
- [ ] ci/scripts/core/detect_build_config.py — explicit VALID_REGISTRIES, fail on unknown
- [ ] ci/actions/setup/detect-build-config/action.yml — replace inline bash with Python call
- [ ] Verify publish action dispatch has no wildcard fallthrough

### Phase C: Bash → Python Migration
- [ ] detect_build_config.py (replaces inline bash, 25+ jq calls) — covered in Phase B
- [ ] ci_common.py: add YAML key extraction (replaces sed/grep in common.sh)
- [ ] publish-binary.sh: migrate artifact logic to Python, keep bash wrapper
- [ ] rust/build.sh: migrate config logic, keep cross-compile bash

### Phase D: Fast CI Test Project
- [ ] ci/.tmp/ci-test-rust-minimal/ — zero-dep Rust binary, full .hyperi-ci.yaml
- [ ] Attach CI, confirm builds in <5 min
- [ ] Use for all future CI script iteration

### Phase E: Documentation
- [ ] ci/docs/CONFIGURATION.md — document Claude helper scripts (usage, args, exit codes)
- [ ] ci/docs/CONFIGURATION.md — document Python migration rationale and bash/Python split rule
- [ ] ci/README.md — update with new script locations

---

## Current: Helm + Dockerfile Automation `[IN PROGRESS]`

**Goal:** Maximise in rustlib — canonical Helm chart templates + Dockerfile generation so each
dfe-app provides its `DeploymentContract` values and gets artefacts generated as standard.

See Container Image + Helm Chart Publishing section below for full WBS.

---

## Current: gRPC Transport Migration

**Goal:** Add `GrpcTransport` (tonic) with Vector wire-protocol compatibility across DFE projects.

**Design:** `reference/grpc_transport_design.md`
**Analysis:** `reference/devtest_transport_analysis.md`

### Phase 1: rustlib (hyperi-rustlib) — COMPLETE

- [x] Proto files vendored (DFE lean + Vector wire compat)
- [x] tonic/prost dependencies + feature flags (`transport-grpc`, `transport-grpc-vector-compat`)
- [x] `GrpcTransport` implementing `Transport` trait (client + server modes)
- [x] `VectorCompatService` + `VectorCompatClient` for Vector agent interop
- [x] `EventWrapper` ↔ `serde_json::Value` bidirectional conversion
- [x] Zenoh transport removed from rustlib
- [x] Published to JFrog Artifactory (v1.8.1+)

### Phase 2: dfe-loader — Zenoh Cleanup COMPLETE, gRPC Wiring Pending

- [x] Removed `transport-zenoh` feature from Cargo.toml
- [x] Removed `ZenohConfig` from config
- [x] Removed `ZenohTransportAdapter` from transport
- [x] Updated `TransportBackend` enum (Kafka only for now)
- [x] Updated rustlib to v1.8.1 from JFrog
- [x] All 433 tests passing, CI green on ARC runner
- [ ] Add `transport-grpc` feature to Cargo.toml
- [ ] Add `GrpcConfig` to `src/config/loader.rs`
- [ ] Add `GrpcTransportAdapter` to `src/kafka/transport.rs`
- [ ] Update `TransportBackend` enum — add Grpc variant
- [ ] Update config examples and docs

### Phase 3: dfe-archiver

1. [ ] Update Cargo.toml — add `transport-grpc`, update rustlib
2. [ ] No code changes needed (uses rustlib transport directly)

### Phase 4: dfe-receiver

1. [ ] Add `transport-grpc` to Cargo.toml features
2. [ ] Add gRPC client config to `LoaderConfig`
3. [ ] Wire up gRPC client for direct-to-loader delivery
4. [ ] Test: receiver → gRPC → loader → ClickHouse

---

## Current: ScalingPressure Integration (IN PROGRESS)

**rustlib v1.9.0** — `scaling` module published (ScalingPressure, RateWindow, config)

- [x] **rustlib `scaling` module** — ScalingPressure engine, RateWindow, 29 tests (v1.9.0)
- [x] **dfe-loader `ScalingConfig`** — weight/saturation config in loader.rs, wired to ServerState
- [x] **Switched Cargo.toml back to registry dep** — >=1.10.0 from Artifactory
- [ ] **Wire orchestrator component updates** — set_component calls in pipeline loop
- [ ] **dfe-receiver migration** — replace hard-coded keda_scaling_metric() with rustlib ScalingPressure
- [ ] **dfe-archiver integration** — add scaling config and wiring
- [ ] **Config documentation** — scaling.* config keys with defaults for all services

---

## Current: Dependency Updates

- [x] **Update hyperi-rustlib** to v1.10.0 across DFE projects
  - dfe-loader: >=1.10.0 (dlq-kafka, scaling features)
  - dfe-receiver: >=1.10.0 (dlq-kafka, scaling features)
  - dfe-archiver: 1.4.3 → >=1.10.0 (add scaling, dlq features) — pending

- [ ] **Update all dependency crates** to latest versions in dfe-loader, dfe-receiver, dfe-archiver
  - Use `cargo outdated` and web search to identify updates
  - Check for breaking changes before upgrading
  - Run full test suite after each project update

---

## Current: Wire Enrichment into Pipeline `[IN PROGRESS]`

**Goal:** Connect existing enrichment modules (GeoIP, reputation, risk) into the pipeline
orchestrator. All three modules are implemented as library code but NOT wired into the hot path.

**Current state:** Code written, NOT yet compiled or tested. Three files modified (uncommitted).

- [x] `EnrichmentConfig` added to main `Config` (with `ip_fields`, `reputation`, `risk_scoring` sub-structs)
- [x] `ReputationEnrichmentConfig`, `RiskScoringConfig` structs added to `src/config/loader.rs`
- [x] Config re-exports added to `src/config/mod.rs`
- [x] `EnrichmentPipeline` struct added to orchestrator (GeoIP + reputation + risk, all optional)
- [x] `EnrichmentPipeline::init()` async fn — inits from config, non-fatal failures
- [x] Enrichment step 4.9 added to `process_message()` between computed columns and buffer push
- [x] `inject_geo()`, `inject_reputation()`, `inject_risk()` helpers added to orchestrator
- [x] `extract_enrich_ip()` helper added — checks ip_fields list, first match wins
- [ ] **Next: `cargo build`** — verify compile (not yet done, host restart pending)
- [ ] `cargo clippy` — clean warnings
- [ ] `cargo test --lib` — all tests pass
- [ ] Commit enrichment pipeline wiring
- [ ] Add enrichment metrics (lookup latency, cache hit rate, enriched count)

---

## Next: DFE Shared Schemas (dfe-schemas submodule)

**Design:** `docs/DFE-SCHEMAS.md`

- [ ] Create `hyperi-io/dfe-schemas` repo with common header YAML definitions
  - `common-header/timeseries.yaml` (9 columns — default for event ingestion)
  - `common-header/minimal.yaml` (4 columns — high-volume structured data)
  - `common-header/passthrough.yaml` (4 columns — transparent bridge mode)
  - `hunt-results/detection.yaml` (6 hunt detection output columns)
- [ ] Add `dfe-schemas` as git submodule at `schemas/`
- [ ] Implement profile loader with resolution order: env var → submodule → bundled fallback
- [ ] Wire profile selection into schema module (`src/schema/mod.rs`)
- [ ] Unit tests for profile loading and column type mapping
- [ ] Keep bundled `schemas/profiles/` in sync as fallback

---

## Next: KEDA Scaling Metrics — MOSTLY DONE (see ScalingPressure above)

**Implemented:** `loader_scaling_pressure` gauge (0-100) via rustlib `ScalingPressure` engine.
Remaining: wire orchestrator component updates (kafka_lag, buffer_depth, etc.)

---

## Completed: Unified DLQ Module (rustlib + DFE Services)

- [x] **rustlib `dlq` module** — `DlqBackend` trait, `Dlq` orchestrator, cascade/fan-out modes
  - File backend (NDJSON + file-rotate), Kafka backend, pluggable custom backends
  - `DlqEntry` shared envelope with base64 payload, source tracking, builder pattern
  - Feature flags: `dlq` (file), `dlq-kafka` (Kafka + file), 22 unit tests
  - Published as hyperi-rustlib v1.10.0
- [x] **dfe-loader** — Replaced bespoke `DlqProducer`/`DlqMessage` with rustlib `Dlq`/`DlqEntry`
  - Deleted `src/kafka/dlq.rs` (340 lines), all DLQ via rustlib cascade (Kafka → file fallback)
  - 367 lib + 156 integration tests passing
- [x] **dfe-receiver** — Replaced hard-coded `dlq_land` topic routing with rustlib `Dlq`
  - Cascade mode: Kafka primary, file fallback. Legacy routing preserved as fallback.
  - 200 tests passing

### DLQ Future Backends (Backlog)

- [ ] **dfe-archiver DLQ integration** — add `dlq-kafka` feature, wire `Dlq` into archive pipeline
  - Discuss at integration time whether archiver needs DLQ (failed archive writes → DLQ?)
- [ ] **S3/MinIO backend** — long-term DLQ archive, cross-region replication
- [ ] **ClickHouse backend** — query DLQ entries with SQL, dashboards
- [ ] **HTTP webhook backend** — alert on DLQ writes (PagerDuty, Slack, OpsGenie)
- [ ] **Spool (yaque) backend** — high-throughput binary DLQ for replay pipelines

Each is a new file implementing `DlqBackend` trait + feature flag. No changes to existing code.

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

## Current: Container Image + Helm Chart Publishing (All DFE Services)

**Status:** NOT IMPLEMENTED — Dockerfile and Helm chart do not exist for any dfe-<service> yet.

**Approach:** Maximise in rustlib — canonical Helm chart templates + Dockerfile generation so each
dfe-app provides its `DeploymentContract` values and gets artefacts generated as standard.

- [ ] **rustlib `deployment` module enhancements**
  1. [ ] Add Helm chart template generation (canonical templates in rustlib, app values injected)
  2. [ ] Add Dockerfile generation from `DeploymentContract` (port, healthcheck, entrypoint)
  3. [ ] `generate_helm_chart(contract, output_dir)` — writes chart/ with values.yaml, templates/
  4. [ ] `generate_dockerfile(contract, output_path)` — writes Dockerfile (Option B: COPY binary)
  5. [ ] CLI flag: `--emit-helm` / `--emit-dockerfile` for CI integration
  6. [ ] Apps only define ~10-20% (app name, port, config mount, components)

- [ ] **dfe-loader** — Generate chart/ + Dockerfile from DeploymentContract
- [ ] **dfe-receiver** — Same pattern
- [ ] **dfe-archiver** — Same pattern
- [ ] **CI integration** — Container + Helm config in `.hyperi-ci.yaml` (jfrog registry)

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

### 2026-03-04: Multi-Provider GeoIP with Auto-Download

- [x] **Config structs** — `GeoIpConfig`, `GeoIpProvider` (6 variants), `AutoDownloadConfig`
- [x] **Auto-download module** — `src/enrich/geoip_download.rs` with `ensure_databases()`
  - DB-IP Lite (anonymous, gzip), MaxMind GeoLite2 (Basic Auth, tar.gz)
  - IPLocate (anonymous, raw), IPinfo Lite (token, raw), sapics (anonymous, raw), Custom (user paths)
  - Freshness checking, atomic writes, graceful failure (non-fatal)
- [x] **GeoIP enricher updates** — `continent_code`/`continent_name` fields, `from_config()` async constructor
- [x] **Risk scorer wiring** — `continent_code` from `GeoIpResult` into `RiskInput` (was dead field)
- [x] **CI bundling** — `scripts/download-geoip.sh` downloads DB-IP Lite MMDB (CC BY 4.0)
- [x] **Dockerfile** — `COPY geoip/ /var/lib/dfe/geoip/` for bundled databases
- [x] **Dependencies** — `reqwest`, `flate2`, `tar` added to Cargo.toml
- [x] GeoIP is off by default (`enabled: false`), 407 lib tests pass, clippy clean

### 2026-03-04: CEL Expressions + CLI Migration + Ubuntu 24.04

- [x] **CEL routing rules** — `when` field on `RoutingRule` with compiled `cel_interpreter::Program`
- [x] **Computed columns** — CEL expressions in ClickHouse column comments (`@computed` directive)
- [x] **CLI migration** — `DfeApp` trait + `run_app()` lifecycle (replaced hand-rolled Args)
- [x] **Ubuntu 24.04** — Dockerfile base image updated from debian:bookworm-slim
- [x] **rustlib v1.13.0** — Published with `expression` feature (OnceLock fix for flaky test)

### 2026-03-02: Unified DLQ Module

- [x] **rustlib v1.10.0** — `dlq` module with DlqBackend trait, file + Kafka backends, cascade/fan-out orchestrator
- [x] **dfe-loader** — Replaced bespoke DlqProducer with rustlib Dlq (deleted 340 lines)
- [x] **dfe-receiver** — Replaced hard-coded dlq_land routing with rustlib Dlq cascade
- [x] **env_compat test fix** — Serialised parallel env var tests with Mutex
- [x] All tests passing: rustlib (22 DLQ), loader (367+156), receiver (200)

### 2026-03-02: gRPC Transport, Zenoh Removal, CI Modernisation

- [x] **rustlib v1.8.1 gRPC transport** — Published to JFrog with Vector wire-compat
- [x] **Removed zenoh transport** — ZenohConfig, ZenohTransportAdapter, transport-zenoh feature
- [x] **Updated rustlib to v1.8.1** from JFrog Artifactory
- [x] **CI on ARC runner** — `GH_RUNNER_DEFAULT=arc-runner-16cpu`, regenerated workflows
- [x] **Registry fix** — Container/Helm publishing switched from ghcr to jfrog
- [x] **Updated ci/ai submodules** to latest
- [x] All 433 tests passing, CI green (Quality + Test) on ARC runner

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
- [x] Feature flags: `transport-memory`, `transport-kafka`, `transport-grpc`, `transport-all`
- [x] `MemoryTransport` using tokio::mpsc (5 unit tests)
- [x] `KafkaTransport` wrapping rdkafka with SASL/SSL support
- [x] `GrpcTransport` with tonic + Vector wire-protocol compatibility
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

**Last Updated:** 2026-03-05
