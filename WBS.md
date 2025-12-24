# Work Breakdown Structure: dfe-loader-clickhouse

**Project:** Rust port of clickhouse-loader (Go)
**Created:** 2024-12-24
**Status:** Arrow Pipeline - Native Inserts Working

---

## Architecture (2025-12-24)

**Per-table Arrow buffers with clickhouse-arrow native protocol**

```text
Kafka → Parse JSON/MsgPack → Route to db.table → Per-table buffer → Arrow RecordBatch → ClickHouse (native)
```

### Key Design Decisions

| Decision             | Choice                 | Rationale                                     |
| -------------------- | ---------------------- | --------------------------------------------- |
| Buffer Format        | **Per-table Arrow**    | Schema uniformity per RecordBatch             |
| ClickHouse Client    | **clickhouse-arrow**   | Native protocol with Arrow integration        |
| Routing              | **Pre-flatten**        | Extract db.table before flattening            |
| Schema Source        | **On-demand introspect** | Fetch from CH, cache with TTL refresh       |

---

## Phase 0: Foundation ✅ COMPLETE

### 0.1 Technical Decisions ✅ COMPLETE

| Decision             | Choice               | Rationale                          |
| -------------------- | -------------------- | ---------------------------------- |
| JSON Library         | sonic-rs + arrow-json | SIMD parsing + Arrow conversion   |
| ClickHouse Library   | clickhouse-arrow     | Arrow native protocol              |
| Buffer Strategy      | Per-table buffers    | Schema uniformity per batch        |
| Memory Control       | Chunk lifecycle      | Drop on ack, no row removal        |

### 0.2 Project Setup ✅ COMPLETE

- [x] **0.2.1** Create Cargo.toml with Arrow dependencies
- [x] **0.2.2** Fork clickhouse-arrow with new types
- [x] **0.2.3** Park klickhouse fork (complete but unused)

---

## Phase 5: Buffer Management ✅ COMPLETE

### 5.1 Per-Table Arrow Buffer

- [x] **5.1.1** ArrowBatchBuilder per table
- [x] **5.1.2** KafkaOffset tracking per batch
- [x] **5.1.3** Independent flush per table
- [x] **5.1.4** ArrowBufferStats for monitoring
- [ ] **5.1.5** Memory size tracking (future)

### 5.2 Buffer Manager

- [x] **5.2.1** HashMap<db.table, TableBuffer>
- [x] **5.2.2** TableSchema registry with Arrow/CH type mapping
- [x] **5.2.3** ch_type_to_arrow conversion function
- [x] **5.2.4** On-demand schema fetch from ClickHouse
- [x] **5.2.5** Periodic schema refresh (configurable interval)
- [ ] **5.2.6** Schema cache invalidation on error (future)

---

## Phase 3: Routing ✅ COMPLETE

### 3.1 Dynamic db.table Routing

- [x] **3.1.1** db_fields priority list (configurable)
- [x] **3.1.2** table_fields priority list (configurable)
- [x] **3.1.3** Dot notation for nested field access
- [x] **3.1.4** Default db/table fallbacks
- [x] **3.1.5** Legacy category_to_table mapping
- [x] **3.1.6** Pre-flatten routing (before JSON flattening)

---

## Phase 6: ClickHouse Integration ✅ COMPLETE

### 6.1 clickhouse-arrow Client

- [x] **6.1.1** Fork clickhouse-arrow with Variant/Dynamic/Nested types
- [x] **6.1.2** ArrowClickHouseClient wrapper
- [x] **6.1.3** Inserter with native Arrow inserts + JSON fallback
- [ ] **6.1.4** TLS configuration (use when needed)

### 6.2 Schema Introspection

- [x] **6.2.1** TableSchema struct with Arrow + CH type mapping
- [x] **6.2.2** ch_type_to_arrow conversion
- [x] **6.2.3** On-demand fetch from system.columns
- [x] **6.2.4** Periodic refresh (configurable, default 60s)
- [ ] **6.2.5** Cache invalidation on schema mismatch error

### 6.3 Inserter

- [x] **6.3.1** Arrow RecordBatch insert interface
- [x] **6.3.2** Retry with exponential backoff
- [x] **6.3.3** clickhouse-arrow native protocol with JSON fallback
- [ ] **6.3.4** Concurrent multi-table insert (future)

---

## Phase 7: Pipeline Orchestration ✅ COMPLETE

### 7.1 Pipeline Core

- [x] **7.1.1** Updated for per-table BufferManager
- [x] **7.1.2** process_message → route → buffer with offset
- [x] **7.1.3** flush_batches handles per-table FlushBatch
- [ ] **7.1.4** Kafka offset commit on successful insert (future)

---

## HIGH PRIORITY: Variant/Dynamic/Nested Serialization

**Status:** PLACEHOLDER CODE - Must implement before production use

The clickhouse-arrow fork has placeholder serialization for new ClickHouse types. These must be implemented with real serialization logic.

### Implementation Tasks

| Type    | File to Edit                                          | CH Reference                        | Status       |
| ------- | ----------------------------------------------------- | ----------------------------------- | ------------ |
| Variant | `crates/clickhouse-arrow/.../types/serialize.rs`      | `DataTypeVariant.cpp`               | ⚠️ Placeholder |
| Dynamic | `crates/clickhouse-arrow/.../types/serialize.rs`      | `DataTypeDynamic.cpp`               | ⚠️ Placeholder |
| Nested  | `crates/clickhouse-arrow/.../types/serialize.rs`      | `DataTypeNested.cpp`                | ⚠️ Placeholder |

### Key Implementation Details

**Variant:**
- Discriminator byte/varint indicates active type
- Type-specific data follows discriminator
- Null variant has special discriminator value

**Dynamic:**
- max_types parameter limits stored type variations
- Type discovery at runtime
- Shared dictionary for type names

**Nested:**
- Stored as parallel arrays (one array per nested column)
- All arrays must have same length per row
- Flattened column names: `nested_name.column_name`

### ClickHouse Source References

- Variant: `ClickHouse/src/DataTypes/DataTypeVariant.cpp`
- Dynamic: `ClickHouse/src/DataTypes/DataTypeDynamic.cpp`
- Nested: `ClickHouse/src/DataTypes/DataTypeNested.cpp`
- Serialization: `ClickHouse/src/DataTypes/Serializations/`

---

## Future Work

### SIMD Optimizations (NOT NOW)

- [ ] Investigate arrow-json SIMD for JSON → Arrow
- [ ] sonic-rs SIMD tuning
- [ ] Batch-level SIMD operations

### DLQ Routing

- [ ] Topic per db.table option
- [ ] Common DLQ topic option
- [ ] Configurable via ENV/config cascade

---

## Completed Phases

| Phase    | Description          | Status         |
| -------- | -------------------- | -------------- |
| Phase 0  | Foundation           | ✅ Complete    |
| Phase 1  | Core Infrastructure  | ✅ Complete    |
| Phase 2  | Kafka Integration    | ✅ Complete    |
| Phase 3  | Routing              | ✅ Complete    |
| Phase 5  | Buffer Management    | ✅ Complete    |
| Phase 6  | ClickHouse           | ✅ Complete    |
| Phase 7  | Pipeline             | ✅ Complete    |
| Phase 8  | CLI & Commands       | ✅ Complete    |

---

**Last Updated:** 2025-12-24
