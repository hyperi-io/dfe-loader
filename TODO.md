# TODO - dfe-loader-clickhouse

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table Arrow buffers with clickhouse-arrow native protocol

---

## Remaining Work

### High Priority - Production Readiness

- [ ] **Batch Salvage (6.4)** - Binary-split on insert failure
  - When batch insert fails, split batch in half and retry
  - Recursively split until single-row failure identified
  - Send failed rows to DLQ

- [ ] **Circuit Breaker (7.4)** - Per-table failure detection
  - Track failure rate per destination table
  - Open circuit after N consecutive failures
  - Half-open state for recovery testing
  - Route to DLQ when circuit open

### Medium Priority

- [ ] **Schema periodic refresh** - Cache invalidation
  - Configurable refresh interval
  - Invalidate on schema mismatch error
  - Re-fetch from ClickHouse system.columns

- [ ] **Concurrent multi-table insert**
  - Parallel inserts to different tables
  - Respect per-table rate limits

### Low Priority

- [ ] **Buffer Pool (5.3)** - Object pool for buffer reuse
- [ ] **Hot-reload (1.2.4)** - Config file watcher

### Future Enhancements (NOT NOW)

- [ ] simd-json integration (pending benchmark)
- [ ] Zero-copy routing field extraction
- [ ] Vectorised JSON flattening
- [ ] Production load testing and benchmarking

---

## Completed

### 2025-12-25: Full Type Support & Arrow-Only Pipeline

- [x] BFloat16, Time, Time64, AggregateFunction, SimpleAggregateFunction types
- [x] Variant/Dynamic/Nested serializers in clickhouse-arrow fork
- [x] Arrow-only inserts (removed JSON fallback)
- [x] Removed klickhouse dependency (moved to separate repo)
- [x] Hot path optimisations (ownership flattening, lazy allocs, cached time)
- [x] DLQ routing with per-table topics
- [x] Kafka offset commit on successful insert
- [x] arrow-json SIMD conversion
- [x] Integration tests rewritten for Arrow inserts

### 2025-12-24: clickhouse-arrow Integration

- [x] Added clickhouse-arrow dependency to Cargo.toml
- [x] Created ArrowClickHouseClient wrapper
- [x] Updated Inserter with native Arrow inserts
- [x] Configurable db.table routing
- [x] Per-table ArrowBatchBuilder with offset tracking
- [x] Schema introspection from ClickHouse

### Previous Work

- [x] Cargo.toml with all dependencies
- [x] Config module with 7-layer cascade
- [x] Kafka consumer with extended auth (SCRAM, OAuth, mTLS, IAM)
- [x] Router with category extraction and dot-notation nested access
- [x] JSON flattening transform (owned and borrowed variants)
- [x] Timestamp validation/correction
- [x] Type coercion (all ClickHouse types)
- [x] Metrics and health endpoints
- [x] Integration tests

---

## Notes

- Test environment: k8s.tyrell.com.au (see .env for credentials)
- clickhouse-arrow fork: `crates/clickhouse-arrow/`

---

**Last Updated:** 2025-12-25
