# TODO - dfe-loader

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table row buffers with JSONEachRow inserts via `clickhouse` crate (HTTP)

---

## Active: Completed Migrations

The Arrow → HTTP/JSONEachRow migration is complete (Phases 0–5). See completed section below.

### **CRITICAL: Type Coercion Gap Analysis**

The old `clickhouse-arrow` fork performed explicit client-side type coercions before
inserting. The new JSONEachRow path relies entirely on ClickHouse server-side coercion.

**Gap analysis required:** Identify which coercions the Arrow fork did that ClickHouse
JSONEachRow does NOT do automatically, and implement them in the Rust transformer layer.

Known areas to audit:
- [ ] DateTime64 precision handling — does CH parse `"2024-01-15T10:30:00.123Z"` reliably?
- [ ] Epoch timestamp auto-detection (seconds vs millis vs micros) — was done client-side
- [ ] LowCardinality string trimming/normalisation — Arrow had explicit handling
- [ ] Nullable vs non-nullable coercion — Arrow had schema-driven null filling
- [ ] UUID format normalisation — Arrow used FixedSizeBinary(16), JSON uses string
- [ ] Int/Float overflow behaviour — Arrow enforced bounds, JSONEachRow may silently truncate
- [ ] Array(T) inner type coercion — e.g. `Array(DateTime64)` with epoch timestamps
- [ ] JSON type insertion — does CH accept arbitrary JSON string for a JSON-typed column?
- [ ] Bool coercion — `"true"` string vs `true` boolean vs `1` integer

**Deliverable:** Document which coercions are missing and implement them in
`src/transform/transformer.rs` or a new `src/clickhouse/coerce.rs` module.
Add integration tests for each coercion that ClickHouse would otherwise fail silently.

### Phase 5.5: Migrate to clickhouse-rs Feature Branch

Our feature branch at `/projects/clickhouse-rs` adds capabilities not in upstream:
- Native protocol option (upstream HTTP-only for v0.12+)
- Full ClickHouse type support (upstream missing: JSON, Variant, Dynamic, Nested, BFloat16, etc.)

Tasks:
- [ ] Review `/projects/clickhouse-rs` branch — document exact patches vs upstream
- [ ] Publish feature branch to Artifactory (`hyperi-cargo-local`) as a versioned crate
- [ ] Update `Cargo.toml` to use Artifactory version instead of crates.io upstream
- [ ] Confirm JSON type (GA v25.3) insert/query works end-to-end via both protocols
- [ ] Confirm full type support: Variant, Dynamic, Nested, BFloat16, Time, AggregateFunction

### Phase 6: Dependency Audit + Version Bumps

- [ ] Web search ALL external crate versions for latest
- [ ] Update `Cargo.toml` with verified latest versions
- [ ] Remove stale/unused dependencies
- [ ] `cargo update` + full test suite

### Phase 7: Crates Workspace Extraction (Post-Migration)

Extract reusable modules into workspace crates (following `dfe-transform-wasm/crates/` pattern):

- [ ] Create workspace root `Cargo.toml` with `[workspace]` section
- [ ] Extract `crates/clickhouse` — HTTP client, types, schema cache, inserter, circuit breaker
- [ ] Extract `crates/buffer` — Row buffer management, pool
- [ ] Main binary stays at workspace root or `crates/loader`
- [ ] Shared workspace dependencies in `[workspace.dependencies]`
- [ ] Compile + test

**Benefits:** Cleaner dep boundaries, faster incremental compilation, reusable by other DFE services.

---

## Completed: DFE Shared Schemas (dfe-schemas submodule)

- [x] `hyperi-io/dfe-schemas` repo exists with full schema set
  - `common-header/timeseries.yaml`, `minimal.yaml`, `passthrough.yaml`
  - `hunt-results/detection.yaml`
  - `meta/aws/`, `meta/azure/`, `meta/gcp/`, `meta/m365/` cloud source schemas
- [x] Attached as git submodule at `schemas/`

**Architecture note:** dfe-loader does NOT load these YAML files. Schema flow is:
`schemas/ YAML → dfe-engine (SchemaBuilderV2) → ClickHouse DDL → system.columns`
Rust services read `system.columns` at runtime only. The submodule is reference/documentation.

---

## Completed: Helm + Dockerfile Generation from DeploymentContract

- [x] `generate_chart(contract, output_dir)` in rustlib — fully implemented
- [x] `generate_dockerfile(contract)` in rustlib — fully implemented
- [x] `--emit-helm [DIR]` CLI flag — writes chart/ from contract (default: ./chart)
- [x] `--emit-dockerfile [FILE]` CLI flag — writes Dockerfile from contract (default: ./Dockerfile)
- [x] chart/ regenerated from contract — in sync

---

## Completed: Dependency Updates

- [x] `cargo update` — rustlib bumped 1.13.3 → 1.13.5, all other deps at latest compatible versions
- [x] 407 lib tests pass after update

---

## Backlog: KEDA ScalingPressure — Other Services

ScalingPressure is fully wired in dfe-loader (memory, buffer_depth, errors, insert_latency).
Remaining work is in other repos:

- [ ] **dfe-receiver** — replace hard-coded `keda_scaling_metric()` with rustlib ScalingPressure
- [ ] **dfe-archiver** — add scaling config and wiring
- [ ] **Config documentation** — scaling.* config keys with defaults for all services

---

## Backlog: gRPC Transport — Other Services

gRPC is fully wired in dfe-loader (GrpcConfig, GrpcTransportAdapter, TransportBackend::Grpc).
Remaining work is in other repos:

- [ ] **dfe-archiver** — add `transport-grpc` feature, update rustlib
- [ ] **dfe-receiver** — add gRPC client config, wire direct-to-loader delivery, test e2e
- [ ] Config docs for gRPC in all services

---

## Backlog: DLQ Future Backends

Each is a new file implementing `DlqBackend` trait + feature flag. No changes to existing code.

- [ ] S3/MinIO backend — long-term DLQ archive, cross-region replication
- [ ] ClickHouse backend — query DLQ entries with SQL, dashboards
- [ ] HTTP webhook backend — alert on DLQ writes (PagerDuty, Slack, OpsGenie)
- [ ] Spool (yaque) backend — high-throughput binary DLQ for replay pipelines
- [ ] dfe-archiver DLQ integration — `dlq-kafka` feature + wire `Dlq` into archive pipeline

---

## Backlog: Kafka Transport Consolidation

The project has TWO Kafka consumers:
- `src/kafka/transport.rs` — uses rustlib `KafkaTransport` (via `TransportAdapter`)
- `src/kafka/consumer.rs` — uses `rdkafka` directly (legacy)

Both are compiled. The orchestrator uses `TransportBackend` (rustlib transport).
The legacy `consumer.rs` should be removed once transport adapter coverage is confirmed complete.
Direct `rdkafka` dependency in `Cargo.toml` can be dropped after removal.

- [ ] Verify `TransportAdapter` covers all consumer.rs functionality (commit, seek, pause/resume)
- [ ] Remove `src/kafka/consumer.rs` (legacy direct rdkafka)
- [ ] Remove `rdkafka` from `Cargo.toml` dependencies

---

## Deferred

- [ ] Receiver WAL — required for at-least-once with gRPC mesh
- [ ] TLS configuration — use when needed
- [ ] Memory size tracking — per-buffer accounting
- [ ] OIDC token fetch — OAuth Bearer refresh callback (awaiting requirement)

---

## Completed

### 2026-03-09: Arrow → HTTP/JSONEachRow Migration (Phases 0–5, Complete)

- [x] Insert bakeoff benchmark (`benches/insert_bakeoff.rs`) — 3 paths × 4 batch sizes
- [x] Fixed clustered ClickHouse table creation (ON CLUSTER + ReplicatedMergeTree/MergeTree)
- [x] Decision: Drop Mison (zero advantage over sonic-rs)
- [x] Decision: Drop Arrow/clickhouse-arrow, use `clickhouse` crate (HTTP, JSONEachRow)
- [x] Decision: JSONEachRow via reqwest (no DynamicRow — serde overhead negligible)
- [x] Phase 0: Mison deleted (3,436 lines), archive branch created
- [x] Phase 1: `HttpClickHouseClient` created (clickhouse + reqwest dual-client)
- [x] Phase 2: Buffer layer migrated to `Vec<Map<String, Value>>` — `FlushBatch`, `BufferManager`
- [x] Phase 3: Arrow dependencies removed (`clickhouse-arrow`, `arrow`, `arrow-json`, `futures-util`)
  - Deleted `src/clickhouse/client.rs` (ArrowClickHouseClient, 644 lines)
  - Deleted `src/transform/arrow.rs`, `src/buffer/arrow.rs`, `src/mison/` (6 files)
- [x] Phase 4: Inserter updated for `Vec<Map<String, Value>>` + `HttpClickHouseClient`
- [x] Phase 5: Pipeline wired, integration tests migrated, benchmarks rewritten
  - 520 tests passing, all bench files compile clean

### 2026-03-08: OCSF Remap Preset + Default Topic

- [x] OCSF builtin field mapping preset (`mappings/ocsf.yaml`, ~50 mappings, v1.3.0)
- [x] `BuiltinPreset::Ocsf` variant wired in `remap_loader.rs`
- [x] Default Kafka input topic changed: `events` → `default_land` (aligns with `_land`/`_load` suffix convention)
- [x] 408 lib tests pass

### 2026-03-06: CI Infrastructure Overhaul

- [x] `ci/scripts/claude/ci-watch.py` — poll run to completion, exponential backoff
- [x] `ci/scripts/claude/ci-logs.py` — fetch/filter logs, --grep, --failed, --tail
- [x] `ci/scripts/claude/ci-trigger.py` — trigger workflow + optional watch
- [x] `ci/scripts/core/detect_build_config.py` — replaced 190-line bash/25+ jq calls
- [x] `ci/actions/setup/detect-build-config/action.yml` — now calls Python
- [x] `ci/hooks/pre-push` — Python rewrite, version-bumping commits → enforce local build
- [x] Registry routing explicit (`VALID_CONTAINER_REGISTRIES`, `VALID_HELM_REGISTRIES`) — no wildcard
- [x] JFrog helm URL derivation fixed (no longer reads defaults.yaml, no GHCR override)

### 2026-03-05: Startup Version Check

- [x] `version-check` feature in hyperi-rustlib
- [x] `check_on_startup()` wired into `src/main.rs`

### 2026-03-05: Multi-Provider GeoIP with Auto-Download

- [x] `GeoIpConfig`, `GeoIpProvider` (6 variants), `AutoDownloadConfig`
- [x] Auto-download module (`src/enrich/geoip_download.rs`) — DB-IP, MaxMind, IPLocate, IPinfo, sapics, Custom
- [x] GeoIP enricher: `continent_code`/`continent_name`, `from_config()` async constructor
- [x] Risk scorer: `continent_code` wired into `RiskInput`
- [x] CI bundling: `scripts/download-geoip.sh` for DB-IP Lite MMDB
- [x] Dockerfile: `COPY geoip/ /var/lib/dfe/geoip/`
- [x] 407 lib tests pass, clippy clean

### 2026-03-04: CEL Expressions + CLI Migration + Ubuntu 24.04

- [x] CEL routing rules — `when` field on `RoutingRule` with compiled `cel_interpreter::Program`
- [x] Computed columns — CEL expressions in ClickHouse column comments (`@computed` directive)
- [x] CLI migration — `DfeApp` trait + `run_app()` lifecycle
- [x] Ubuntu 24.04 — Dockerfile base image
- [x] rustlib v1.13.0 published with `expression` feature

### 2026-03-04: Enrichment Pipeline Wiring

- [x] `EnrichmentConfig`, `ReputationEnrichmentConfig`, `RiskScoringConfig` in config
- [x] `EnrichmentPipeline` in orchestrator — GeoIP + reputation + risk, all optional
- [x] `EnrichmentPipeline::init()` — inits from config, non-fatal failures
- [x] Enrichment step wired into `process_message()` between computed columns and buffer push
- [x] `inject_geo()`, `inject_reputation()`, `inject_risk()`, `extract_enrich_ip()` helpers

### 2026-03-02: ScalingPressure — dfe-loader Fully Wired

- [x] rustlib `scaling` module (ScalingPressure, RateWindow) — v1.9.0
- [x] `ScalingConfig` in loader.rs, wired to ServerState
- [x] `set_component` calls in orchestrator: memory, buffer_depth, errors, insert_latency

### 2026-03-02: Unified DLQ Module

- [x] rustlib v1.10.0 — `dlq` module with DlqBackend trait, file + Kafka backends, cascade/fan-out
- [x] dfe-loader — replaced bespoke DlqProducer/DlqMessage with rustlib Dlq/DlqEntry (deleted 340 lines)
- [x] dfe-receiver — replaced hard-coded dlq_land routing with rustlib Dlq cascade
- [x] 367 lib + 156 integration tests passing

### 2026-03-02: gRPC Transport — dfe-loader Fully Wired

- [x] rustlib v1.8.1 gRPC transport published (Vector wire-compat)
- [x] `transport-grpc` feature in Cargo.toml
- [x] `GrpcConfig` in `src/config/loader.rs`
- [x] `GrpcTransportAdapter` in `src/kafka/transport.rs`
- [x] `TransportBackend::Grpc` variant — full match coverage
- [x] Zenoh transport removed entirely
- [x] All 433 tests passing

### 2026-02-25: Config Cascade & Config-Reload

- [x] Figment config cascade — CLI > ENV > .env > config file > defaults
- [x] `DFE_LOADER_` env prefix with `__` nesting
- [x] `SharedConfig<T>` and `ConfigReloader<T>` from rustlib
- [x] Config hot-reload: SIGHUP + file polling + periodic timer
- [x] `_source` field + common header configurable fields

### 2026-02-19: CI Cross-Compilation & Binary Publish

- [x] aarch64 cross-compilation sysroot approach
- [x] Binary publish permission fix (`chmod +x` after artifact download)
- [x] v1.6.13 published with amd64 + arm64 binaries

### 2026-01-19: Auto-Initialization & Schema

- [x] Auto-init: topic creation, database/table creation from embedded DDL
- [x] Engine auto-detection: SharedMergeTree → ReplicatedMergeTree → MergeTree
- [x] Schema module with compile-time embedding (`schemas/common_table.sql`)
- [x] Optimised ClickHouse schema (ORDER BY, PARTITION BY, LowCardinality)

### 2026-01-13: Project Rename & WBS

- [x] Project rename: dfe-loader-clickhouse → dfe-loader
- [x] hyperi-rustlib v0.2.0 published to Artifactory

### 2025-12-28: Mison Structural Index (ARCHIVED — dropped after benchmarks)

- [x] SIMD structural index, leveled bitmap, schema-guided extractor
- [x] Direct-to-Arrow column builders
- [x] Mison benchmarks written (`benches/mison.rs`)

### 2025-12-28: Enrichment Modules

- [x] GeoIP enrichment (MaxMind MMDB + LRU cache)
- [x] Reputation enrichment (threat types/sources + CIDR matching)
- [x] Risk scoring (component weights + presets)

### 2025-12-25: Resilience + Arrow-Only Pipeline

- [x] Batch salvage, circuit breaker, schema cache, concurrent insert semaphore
- [x] Arrow-only inserts, klickhouse removed, DLQ routing, Kafka offset commit

---

## Notes

- Test environment: k8s.tyrell.com.au (see .env for credentials)
- Benchmark results: `benches/insert_bakeoff.rs` (run with `cargo bench --bench insert_bakeoff`)
- clickhouse crate: HTTP + JSONEachRow (official, v0.14.x) for DDL/queries
- reqwest: HTTP POST with JSONEachRow for data inserts (dynamic schemas)

---

**Last Updated:** 2026-03-09
