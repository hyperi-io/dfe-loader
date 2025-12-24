# TODO - dfe-loader-clickhouse

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Arrow-based chunked buffer with clickhouse-arrow native protocol

---

## Current Sprint: Arrow Migration

### In Progress

- [ ] **Complete pipeline refactor for Arrow**
  - ArrowBuffer with chunked lifecycle
  - Schema introspection on-demand with refresh
  - Partition by _destination at flush time
  - clickhouse-arrow for native protocol inserts

### High Priority

- [ ] **Schema introspection on-demand with refresh**
  - Fetch schema on first table write
  - Refresh every N seconds (configurable)
  - Build Arrow schema from ClickHouse system.columns

- [ ] **Wire up clickhouse-arrow client**
  - Replace klickhouse JSON insert with native Arrow insert
  - Use local fork with Variant/Dynamic/Nested types

- [ ] **Compare serialization with ClickHouse C++ source**
  - Verify Variant serialization format
  - Verify Dynamic serialization format
  - Verify Nested serialization format

---

## Arrow Architecture TODO

### Core Buffer (ArrowBuffer)

- [x] ArrowChunk with lifecycle (Pending → InFlight → Acked/Failed)
- [x] KafkaOffset tracking per chunk
- [x] Partition by _destination column
- [x] Chunk-level ack/fail (no row-level removal)
- [x] ArrowBufferStats for monitoring
- [ ] JSON → Arrow deserialization (arrow-json)
- [ ] MessagePack → Arrow deserialization

### Schema Registry

- [x] TableSchema with Arrow schema + ClickHouse types
- [x] ch_type_to_arrow conversion
- [ ] On-demand fetch from ClickHouse system.columns
- [ ] Periodic refresh (configurable interval)
- [ ] Cache invalidation on schema change error

### Inserter

- [x] Arrow RecordBatch insert interface
- [x] Retry logic with exponential backoff
- [ ] clickhouse-arrow native protocol (replace JSON bridge)
- [ ] Concurrent multi-table insert

### Pipeline

- [ ] Update orchestrator for Arrow-based BufferManager
- [ ] Update process_message for Arrow batches
- [ ] Update flush_batches for Arrow FlushBatch
- [ ] Wire chunk ack back to Kafka offset commit

---

## Remaining from Original Plan

### Medium Priority

- [ ] **DLQ Producer (2.2)** - Route bad messages to dead letter queue
- [ ] **Batch Salvage (6.4)** - Binary-split on insert failure
- [ ] **Circuit Breaker (7.4)** - Per-table failure detection

### Low Priority

- [ ] **Buffer Pool (5.3)** - Object pool for buffer reuse
- [ ] **Hot-reload (1.2.4)** - Config file watcher

---

## Completed

### 2025-12-24: Arrow Migration Started

- [x] Fork clickhouse-arrow with Variant/Dynamic/Nested types
- [x] Fork klickhouse with new types (parked)
- [x] Design ArrowBuffer chunked architecture
- [x] Implement ArrowBuffer with partition-by-destination
- [x] Implement BufferManager with schema introspection
- [x] Update Inserter for Arrow RecordBatch
- [x] Update STATE.md/TODO.md/WBS.md for Arrow architecture

### Previous Work

- [x] Cargo.toml with all dependencies
- [x] Config module with 7-layer cascade
- [x] Kafka consumer with extended auth (SCRAM, OAuth, mTLS, IAM)
- [x] Router with category extraction
- [x] JSON flattening transform
- [x] Timestamp validation/correction
- [x] Type coercion (all ClickHouse types)
- [x] Metrics and health endpoints
- [x] Integration tests
- [x] Schema introspection (Phase 6.2)

---

## Notes

- Build with `CARGO_BUILD_JOBS=2` to limit resource usage
- Test environment: k8s.tyrell.com.au (see .env for credentials)
- clickhouse-arrow fork: `crates/clickhouse-arrow/`
- klickhouse fork (parked): `crates/klickhouse/`

---

**Last Updated:** 2025-12-24
