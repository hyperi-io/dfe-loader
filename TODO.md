# TODO - dfe-loader

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table row buffers with RowBinary inserts via `clickhouse-rs` fork (default), JSONEachRow fallback

---

## Implementation Stages

The architecture is deliberately staged:

```
Stage 1 (COMPLETE): JSONEachRow + upstream clickhouse crate (HTTP) + coercion fixes
Stage 2 (Phase 5.5): Native protocol via /projects/clickhouse-rs fork (lower CPU)
```

Stage 1 ships first, Stage 2 is a drop-in upgrade — same API, better protocol.

---

## Active

### Phase 5.7: Schema-Guided Extraction + Zero-Copy _json ✓ COMPLETE

Complete architectural overhaul of the hot path for SIMD efficiency and CPU reduction.

**Canonical pipeline:**
```
msg bytes → JSON normalise (auto-sense JSON/MsgPack) → Arc<[u8]>
                │
                ├─ route (sonic-rs get_from_slice, no full parse)
                ├─ extract schema cols (get_from_slice per col, O(schema_cols))
                ├─ delta coerce promoted cols only (4 cases, O(schema_cols × 1 branch))
                ├─ enrich promoted cols (GeoIP/rep/risk flat injection)
                └─ buffer (promoted Map + Arc<[u8]> for _json splice)

At flush:
  sonic_rs::to_string(promoted_map) + splice Arc<[u8]> as "_json" → NDJSON
```

Common header fields (`_timestamp`, `_org_id`, `_source`, `_timestamp_received`)
are ALWAYS extracted to top-level columns — required for DFE operational correctness.

Implementation changes:
- [x] **B**: `buffer/manager.rs` — `FlushBatch` carries `raw_payloads: Vec<Arc<[u8]>>`,
      `push()` accepts `raw: Option<Arc<[u8]>>`, single-pass `get_ready_for_flush()`
- [x] **I**: `clickhouse/inserter.rs` — `#[derive(Clone)]`, simplify `insert_batches*`
- [x] **J**: `pipeline/orchestrator.rs` — single-pass `get_ready_for_flush()`, no `should_flush()` guard
- [x] **C**: `clickhouse/client_http.rs` — zero-copy `_json` splice via `write_row_with_json`;
      strips trailing `}` from promoted map, appends `,"_json":<raw bytes>}`; no re-parse
- [x] **D/E**: `transform/coerce.rs` — `CoercionMode` enum (`Full` / `Delta`),
      Delta dispatch (pass-through for most types, O(1) branch per col)
- [x] **F**: `orchestrator.rs` — per-batch offset commit; `per_batch_offsets` extracted before
      insert, Table A failure does not block Table B commit
- [x] **G**: `orchestrator.rs` — bounded DLQ channel `mpsc::channel(1_000)` + background drain
      task; `try_send` in hot path drops on `Full` with a warn (never blocks)
- [x] **H**: `orchestrator.rs` — schema resolution via `resolve_tx` background task; results
      arrive in `schema_result_rx` select! arm; event loop never blocks on ClickHouse
- [x] **A**: `transform/extractor.rs` — `HeaderExtractor` with sonic-rs SIMD extraction
      (`get_from_slice` per schema col); `orchestrator.rs` — `json_primary` mode gate,
      `ColumnMetaCache` wired into mapping builder + computed cols + extractor

**Hot-path optimisation review:** After every major change (each sub-item above and after
Phase 5.5 Step C), perform a CPU-first review of the hot path:
- Priority: CPU 80%, Memory 20%
- Profile with `cargo flamegraph` or `perf record` on a representative workload
- Check for: unnecessary allocations, clone()s, map iterations, bounds checks
- Document findings in `docs/DESIGN.md` (Future Optimisations section)

**Next:** Phase 5.5 clickhouse-rs fork.

### Spike: simdjson vs sonic-rs targeted bake-off ✓ COMPLETE — REJECTED

sonic_dom (single full parse + O(1) lookups) beats sonic_selective (get_from_slice×N) by 2.7–4.2×.
simd-json rejected: requires `&mut [u8]` incompatible with `Arc<[u8]>` zero-copy `_json` model.
Net pipeline improvement only ~2–3%. Bench file removed; full rationale in `docs/DESIGN.md`.

### Phase 5.6: Type Coercion Completeness ✓ COMPLETE

- [x] `coerce_row(&mut Map<String, Value>)` added to `Coercer`
- [x] `Inserter` wired with optional `schema_cache` + `coercer` via `with_schema_coercion()`
- [x] DateTime64 from epoch ms, ISO string, Bool, UUID, IPv4, Null defaults, Array(DateTime64)
- [x] Integration tests in `tests/integration/datatypes.rs` (9 tests)

### Phase 5.5: clickhouse-rs Fork — Native Protocol + Batching

dfe-loader is the **test harness** for the HyperI `clickhouse-rs` fork at
`/projects/clickhouse-rs` (GitHub: `hyperi-io/clickhouse-rs`).

**Fork branches and merge order:**
```
feature/batching      → main (independent, merge anytime)
feature/native-transport → main
feature/connection-pooling → main (after native-transport)
feature/lc-insert     → main (after connection-pooling)
```

| Branch | Commits | Purpose |
|---|---|---|
| `feature/batching` | 1 from main | HTTP `TableBatcher<T>`, independent of native |
| `feature/native-transport` | 4 from main | Native TCP: types, Bool/sparse, INSERT, schema cache |
| `feature/connection-pooling` | +1 on native | Deadpool pool, cursor drain, connection health |
| `feature/lc-insert` | +1 on pooling | LowCardinality INSERT encoder + LC(Nullable(T)) fix |

Fork adds over upstream `clickhouse` crate:
- Native TCP protocol (upstream is HTTP-only as of v0.12+)
- HTTP `TableBatcher<T>` (server-side buffering)
- Connection pooling (Deadpool)
- LowCardinality INSERT + LC(Nullable(T)) reader fix
- JSON, Variant, Dynamic, Nested, BFloat16, Time, AggregateFunction types

**Swap mechanism** (zero change to main `[dependencies]`):
```toml
# Cargo.toml — add at end to activate fork
[patch.crates-io]
clickhouse = { path = "../clickhouse-rs" }
```
Remove or comment out the `[patch]` section to revert to upstream.

**WBS Breakdown (sequential — do NOT skip steps):**

**Step A ✓ COMPLETE:** Upstream `clickhouse` crate (crates.io), 564 tests passing.
  Phase 5.7 (schema-guided extraction) complete. Dead code cleanup done (v1.14.4 GA released).

**Step A.5 (code review) ✓ COMPLETE:** Full simplification pass run (2026-03-11).
  - `fmt_ts` made `pub(crate)`, shared between transformer + extractor (no duplication)
  - `HeaderExtractor` switched from `get_from_slice×N` to `from_slice`+O(1) lookups (2.7–4.2×)
  - `coerce_row` dead code removed; Delta mode early-skip added (avoids clone for pass-through types)
  - `calc_backoff` free-function extracted (deduplicates identical logic in InserterConfig/Inserter)
  - `pending_bytes` in `BufferStats::stats()` was always 0 — fixed to populate inline
  - Orchestrator double schema-cache lookup collapsed to single
  - `hyperi-ai` submodule updated to v2.7.0, re-attached with `--force --agent claude`

**Step B ✓ COMPLETE:** v1.14.4 GA released (GH Release + R2 binaries, JFrog container + helm).
  amd64 + arm64 binaries published. Ready for internal testing.

**Step C:** Migrate to clickhouse-rs fork — done LOCK-STEP with dfe-loader.

Phases 1-3 complete (fork activated, InsertFormat dispatch wired, ParsedType deduplicated).
Fork `hyperi/optimise-1` branch has `src/dynamic/` module: ParsedType, DynamicSchema,
SchemaCache, RowBinary encoder, DynamicInsert, DynamicBatcher.

Remaining tasks:
- [x] Activate fork via `[patch.crates-io]`
- [x] Wire `InsertFormat` dispatch (RowBinary default, JSONEachRow fallback)
- [x] Replace loader `ParsedType` with re-export from fork
- [ ] Integration test DynamicInsert RowBinary path against devex cluster
- [ ] Merge fork branch chain to main (batching → native → pooling → lc-insert → optimise-1)
- [ ] Publish merged fork to crates.io as a pre-release (`0.14.x-hyperi.1`)
- [ ] Update `Cargo.toml` to use published pre-release (remove `[patch]`)
- [ ] Confirm JSON type (GA v25.3) insert/query works end-to-end
- [ ] Confirm full type support: Variant, Dynamic, Nested, BFloat16, Time, AggregateFunction
- [ ] Once stable, open PR to upstream `clickhouse-rs`

### Consume hyperi-rustlib v1.16.0 (Dynamic Linking) ✓ COMPLETE

rustlib v1.14.0+ switches rdkafka to dynamic-linking against system librdkafka
(was compiling C++ from source — 30min build eliminated). v1.15.0 added
`NativeDepsContract` for auto-generating Dockerfile native deps. v1.16.0 added
`ImageProfile` (production vs development container profiles).

- [x] Bump hyperi-rustlib from `1.13.2` to `>=1.16.0`
- [x] Switch rdkafka from `cmake-build` to `dynamic-linking` features
- [x] `cargo update` + 387 lib tests pass
- [x] Add `native_deps` + `image_profile` to deployment contract
- [x] Regenerate Dockerfile from contract (+ Ubuntu 24.04 UID fix + GeoIP COPY)
- [ ] Test container image starts and connects to Kafka (deferred to CI)

### rustlib v1.16.3 Remediation (DFE Observability) ✓ COMPLETE

- [x] Bump hyperi-rustlib to >=1.16.3 with `metrics` feature
- [x] Migrate `apply_env_overrides()` to rustlib `ApplyFlatEnv` trait
- [x] Add `DfeMetrics` dual-emit alongside existing prometheus metrics
- [x] Wire security events at config reload, DLQ, and validation sites
- [x] Fix log spam sites: sampled coercion, debounced DLQ/consumer errors

### Code Review Remediation ✓ COMPLETE

- [x] Add `[lints]` section with pedantic + unwrap/expect warnings
- [x] Add `deny.toml`, `rustfmt.toml`, `clippy.toml`, `rust-toolchain.toml`
- [x] Switch GeoIP + reputation to `parking_lot::RwLock` (no poison panics)
- [x] Remove blanket `From<String> for Error` (masks error category)
- [x] Split `config/loader.rs` (2,362 → 1,041 lines) into `kafka.rs` + `pipeline.rs`
- [x] Hot-reload safety: warn on restart-required config changes (transport, clickhouse URL, format)
- [x] Security fix: `lz4_flex` 0.11.5 → 0.11.6 (GHSA-vvp9-7p8x-rfvv)

### Security Fixes ✓ COMPLETE

- [x] `tar` 0.4.44 → 0.4.45 (GHSA-gchp-q4r4-x4ff, GHSA-j4xf-2g29-59ph)
- [x] `aws-lc-sys` 0.38.0 → 0.39.0 (GHSA-9f94-5g5w-gf6r, GHSA-394x-vwmw-crm3)
- [x] `astral-tokio-tar` GHSA-6gx3-4362-rf54 dismissed (low, transitive dev-dep, no upstream fix)

### v1.15.0 GA Released ✓

Released with: MemoryGuard, DFE metrics groups, Kafka transport consolidation,
clickhouse-rs fork activation, security events, dual-mode test infra, security fixes.

### Renovate Remediation

- [ ] Verify Renovate is running (check Dependency Dashboard issue on GitHub)
- [ ] If still broken: deep web research on Mend Renovate GitHub App resolution issues
- [ ] Confirm at least one automated dependency PR has been created
- [ ] Remove `schedule` workarounds once confirmed working

### Test Coverage Review

- [ ] Review existing tests for variety and gaps (unit, integration, e2e)
- [ ] Add full startup smoke test to catch init panics (config load, transport init, schema cache, metrics bind)
- [ ] Identify untested error paths (bad config, unreachable ClickHouse, schema mismatch, DLQ overflow)
- [ ] Add negative tests for hot-reload (invalid config, restart-required field change)
- [ ] Verify MemoryGuard triggers consumer pause under simulated pressure
- [ ] Add circuit breaker state transition integration test (closed → open → half-open → closed)

### Documentation Review

- [ ] Update hyperi-ai submodule
- [ ] Run full documentation review using `/review` skill
- [ ] Fix all stale references, paths, and outdated examples

### CI Rebuild with Updated hyperi-ci

- [ ] Update hyperi-ci submodule — prod/test separation changes
- [ ] Re-attach CI (`ci/attach.sh`)
- [ ] Full rebuild and test cycle with new CI pipeline
- [ ] Verify container build, helm chart, binary publish all pass

### Phase 6: Dependency Audit + Version Bumps

- [ ] Web search ALL external crate versions for latest
- [ ] Update `Cargo.toml` with verified latest versions
- [ ] Remove stale/unused dependencies
- [ ] `cargo update` + full test suite

### DFE Metrics Standard Migration

Standard: `hyperi-ai/standards/rules/dfe-metrics.md`
rustlib v1.17.0 provides `metrics-dfe` feature with composable metric groups.

- [x] Implement `metrics-dfe` feature in rustlib (8 metric groups)
- [x] Wire auto-emit: ConfigReloader `config_reloads_total`, StatsContext `rdkafka_*`
- [x] Adopt all 8 groups in dfe-loader (dual-emit with legacy `loader_*` names)
- [x] Write DFE metrics standard in hyperi-ai
- [x] Write per-app migration prompts (`~/DFE-METRICS-MIGRATION-*.md`)
- [ ] Change MetricsManager namespace from `loader` to `dfe_loader` (breaks dashboards — coordinate)
- [ ] Remove legacy `loader_*` metric names after dashboard migration

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

## Completed: Kafka Transport Consolidation

- [x] Verified `TransportAdapter` covers all consumer.rs functionality
- [x] Removed `src/kafka/consumer.rs` (352 lines, legacy direct rdkafka)
- [x] Removed `rdkafka` direct dependency from `Cargo.toml`
- [x] Removed `Error::KafkaLib(rdkafka::error::KafkaError)` variant
- [x] Moved `KafkaMessage` struct to `kafka/mod.rs`

---

## Backlog: Migrate `paste` → `pastey` in clickhouse-rs fork

RUSTSEC-2024-0436: `paste` crate unmaintained. Transitive dependency via
`polonius-the-crab` → `higher-kinded-types` → `macro_rules_attribute`.
`pastey` is the recommended drop-in replacement fork.

Not a direct dep of dfe-loader — lives in the clickhouse-rs fork's dependency chain.
`cel-interpreter` also depends on `paste` (via hyperi-rustlib) — upstream fix needed.

- [ ] Replace `paste` with `pastey` in clickhouse-rs fork (if `polonius-the-crab` migrates)
- [ ] Track `cel-interpreter` upstream migration
- [ ] Remove `RUSTSEC-2024-0436` ignore from `deny.toml` once resolved

---

## Deferred

- [ ] Receiver WAL — required for at-least-once with gRPC mesh
- [ ] TLS configuration — use when needed
- [ ] Memory size tracking — per-buffer accounting

---

## Completed

### 2026-03-16: Dead Code Cleanup + v1.14.4 GA Release

- [x] Deleted `tests/fixtures/arrow_schema.rs` (246 lines, zero callers, arrow crate removed)
- [x] Deleted `benches/simdjson_spike.rs` (379 lines, completed spike, decision documented)
- [x] Removed `simd-json` and `mockall` dev-dependencies from Cargo.toml
- [x] Removed `simdjson_spike` bench entry from Cargo.toml
- [x] Fixed stale ArrowStream comment in `src/clickhouse/config.rs`
- [x] Removed OIDC TODO comment from `src/kafka/consumer.rs`
- [x] Updated CI references (CONTRIBUTING.md → hyperi-ci)
- [x] v1.14.4 GA released: GH Release + R2 binaries + JFrog container + helm
- [x] GitHub issue #3 (`_source` routing) closed as by-design

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

- Test environment: clickhouse.devex.hyperi.io (see .env for credentials)
- Benchmark results: `benches/insert_bakeoff.rs` (run with `cargo bench --bench insert_bakeoff`)
- clickhouse-rs fork: RowBinary via DynamicInsert (default), JSONEachRow fallback via reqwest
- clickhouse crate: HTTP for DDL/queries (system.columns, health checks)

---

**Last Updated:** 2026-03-19
