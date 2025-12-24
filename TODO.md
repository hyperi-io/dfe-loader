# TODO - dfe-loader-clickhouse

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

**Architecture:** Per-table Arrow buffers with clickhouse-arrow native protocol

---

## Current Sprint: Variant/Dynamic/Nested Serialization

### High Priority - REQUIRED FOR PRODUCTION

- [ ] **Implement Variant type serialization** (clickhouse-arrow fork)
  - Location: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs`
  - Reference: ClickHouse `src/DataTypes/DataTypeVariant.cpp`
  - Discriminator encoding
  - Type dispatch per variant

- [ ] **Implement Dynamic type serialization** (clickhouse-arrow fork)
  - Location: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs`
  - Reference: ClickHouse `src/DataTypes/DataTypeDynamic.cpp`
  - max_types parameter handling
  - Runtime type discovery

- [ ] **Implement Nested type serialization** (clickhouse-arrow fork)
  - Location: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs`
  - Reference: ClickHouse `src/DataTypes/DataTypeNested.cpp`
  - Parallel arrays structure
  - Column ordering

### Completed This Sprint

- [x] **Dynamic db.table routing from event data**
- [x] **clickhouse-arrow client integration**
- [x] **Schema introspection on-demand with refresh**

---

## Architecture

### Per-Table Buffer Design (Implemented)

Each destination `db.table` has its own ArrowBatchBuilder:

```
Kafka message → Parse → Route to db.table → Push to per-table buffer
                                          → Build Arrow RecordBatch on threshold
                                          → Insert to ClickHouse (native Arrow)
                                          → Ack Kafka offsets
```

**Why per-table?**

- Schema uniformity: Arrow RecordBatch requires all rows have same schema
- Schema introspection: Arrow schema derived from ClickHouse `system.columns`
- Independent flush: High-volume tables flush more often

### Routing Logic (Implemented)

```
db = first_present(event, config.db_fields) ?? config.default_db ?? "common"
table = first_present(event, config.table_fields) ?? config.default_table ?? "common"
destination = "{db}.{table}"
```

---

## Core Buffer (Completed)

- [x] Per-table ArrowBatchBuilder
- [x] KafkaOffset tracking per batch
- [x] Independent flush thresholds per table
- [x] ArrowBufferStats for monitoring
- [x] JSON → Arrow conversion (manual)
- [ ] JSON → Arrow conversion (arrow-json SIMD) - see Future Work
- [ ] MessagePack → Arrow conversion

### Schema Registry

- [x] TableSchema with Arrow schema + ClickHouse types
- [x] ch_type_to_arrow conversion
- [ ] On-demand fetch from ClickHouse system.columns
- [ ] Periodic refresh (configurable interval)
- [ ] Cache invalidation on schema change error

### Inserter (Completed)

- [x] Arrow RecordBatch insert interface
- [x] Retry logic with exponential backoff
- [x] clickhouse-arrow native protocol with JSON fallback
- [ ] Concurrent multi-table insert

### Pipeline (Completed)

- [x] Per-table BufferManager integration
- [x] process_message pushes to correct table buffer
- [x] flush_batches handles per-table FlushBatch
- [ ] Wire Kafka offset commit on successful insert

---

## Remaining from Original Plan

### Medium Priority

- [ ] **DLQ Producer (2.2)** - Route bad messages to dead letter queue
  - Option 1 (default): Topic per db.table using routing logic
  - Option 2: Common DLQ topic for all failures
- [ ] **Batch Salvage (6.4)** - Binary-split on insert failure
- [ ] **Circuit Breaker (7.4)** - Per-table failure detection

### Low Priority

- [ ] **Buffer Pool (5.3)** - Object pool for buffer reuse
- [ ] **Hot-reload (1.2.4)** - Config file watcher

---

## Future Work (NOT NOW)

### SIMD Optimizations Investigation

- [ ] Investigate where further SIMD optimizations can be applied
  - arrow-json for JSON → Arrow conversion
  - sonic-rs SIMD parsing tuning
  - Potential batch-level SIMD operations

---

## Completed

### 2025-12-24: clickhouse-arrow Integration

- [x] Added clickhouse-arrow dependency to Cargo.toml
- [x] Created ArrowClickHouseClient wrapper
- [x] Updated Inserter with native Arrow inserts + JSON fallback
- [x] All tests passing

### 2025-12-24: Dynamic db.table Routing

- [x] Updated RoutingConfig with db_fields, table_fields, default_db, default_table
- [x] Updated Router to extract db.table pre-flattening with dot notation
- [x] Updated config files (config.dev.yaml, config.example.yaml)
- [x] Updated tests for new routing format

### 2025-12-24: Per-Table Buffer Architecture

- [x] Redesign from single buffer to per-table buffers
- [x] Implement ArrowBatchBuilder in transform/arrow.rs
- [x] Implement BufferManager with per-table HashMap
- [x] Update orchestrator for new BufferManager API
- [x] Fix all compilation errors and tests
- [x] Update DESIGN.md with new architecture

### 2025-12-24: Arrow Migration Started

- [x] Fork clickhouse-arrow with Variant/Dynamic/Nested types
- [x] Fork klickhouse with new types (parked)
- [x] Design ArrowBuffer chunked architecture
- [x] Implement ArrowBuffer with partition-by-destination
- [x] Update Inserter for Arrow RecordBatch
- [x] Update documentation (STATE/TODO/WBS/DESIGN)

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
