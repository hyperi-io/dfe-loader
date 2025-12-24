# Work Breakdown Structure: dfe-loader-clickhouse

**Project:** Rust port of clickhouse-loader (Go)
**Created:** 2024-12-24
**Status:** Major Refactor - Arrow-based Architecture

---

## Architecture Change (2025-12-24)

**BREAKING CHANGE:** Migrating from JSON-based buffers to Arrow-based chunked buffers.

```text
OLD: Kafka → JSON parse → Transform (mutate JSON) → ColumnarBuffer → JSONEachRow
NEW: Kafka → Arrow parse → Transform (build new batch) → ArrowBuffer → Native protocol
```

### Key Design Decisions

| Decision           | Choice               | Rationale                                   |
| ------------------ | -------------------- | ------------------------------------------- |
| Buffer Format      | **Arrow**            | Columnar, vectorized ops, zero-copy to CH   |
| ClickHouse Client  | **clickhouse-arrow** | Native protocol with Arrow integration      |
| Transform Strategy | **Build new batch**  | Immutable batches, Arc-share unchanged cols |
| Schema Source      | **On-demand introspect** | Fetch from CH, refresh periodically     |

---

## Phase 0: Foundation & Decisions ✅ COMPLETE

### 0.1 Technical Decisions ✅ COMPLETE (REVISED)

| Decision           | Original Choice     | New Choice         | Rationale                    |
| ------------------ | ------------------- | ------------------ | ---------------------------- |
| JSON Library       | sonic-rs            | **arrow-json**     | JSON → Arrow direct          |
| ClickHouse Library | klickhouse          | **clickhouse-arrow** | Arrow native protocol      |
| Buffer Strategy    | Custom Columnar     | **Arrow chunks**   | Immutable, vectorized        |
| Memory Control     | MemoryController    | **Chunk lifecycle**| Drop on ack, no row removal  |

### 0.2 Project Setup ✅ COMPLETE

- [x] **0.2.1** Create Cargo.toml with Arrow dependencies
- [x] **0.2.2** Fork clickhouse-arrow with new types
- [x] **0.2.3** Park klickhouse fork (complete but unused)

---

## Phase 5: Buffer Management ⚠️ REWRITE

### 5.1 Arrow Buffer (NEW)

- [x] **5.1.1** ArrowChunk with lifecycle states (Pending/InFlight/Acked/Failed)
- [x] **5.1.2** KafkaOffset tracking per chunk for at-least-once
- [x] **5.1.3** Partition by _destination column at flush
- [x] **5.1.4** Chunk-level ack (drop entire chunk, O(1))
- [x] **5.1.5** ArrowBufferStats for monitoring
- [ ] **5.1.6** Memory size tracking

### 5.2 Buffer Manager (NEW)

- [x] **5.2.1** Single ArrowBuffer for all destinations
- [x] **5.2.2** TableSchema registry with Arrow/CH type mapping
- [x] **5.2.3** ch_type_to_arrow conversion function
- [ ] **5.2.4** On-demand schema fetch from ClickHouse
- [ ] **5.2.5** Periodic schema refresh (configurable interval)
- [ ] **5.2.6** Schema cache invalidation on error

### 5.3 REMOVED

- ~~Buffer Pool~~ - Not needed with chunk lifecycle

---

## Phase 4: Transformation ⚠️ REWRITE

### 4.1 Arrow Transforms (NEW)

Arrow batches are immutable - transforms build NEW batches with changed columns.

- [ ] **4.1.1** JSON → Arrow batch builder (arrow-json)
- [ ] **4.1.2** MessagePack → Arrow batch builder
- [ ] **4.1.3** Column addition (build new batch with extra columns)
- [ ] **4.1.4** Zero-copy unchanged columns (Arc sharing)

### 4.2 Enrichment with Arrow

- [ ] **4.2.1** GeoIP enrichment (vectorized column append)
- [ ] **4.2.2** Risk scoring (vectorized)
- [ ] **4.2.3** Field derivation (compute new columns from existing)

### 4.3 Transform Pipeline

```rust
// Transforms build new batches, chain with Arc-sharing
fn transform_pipeline(batch: RecordBatch) -> Result<RecordBatch> {
    let batch = add_destination_column(batch, &router)?;   // New col
    let batch = enrich_geoip(batch, "src_ip")?;            // New cols
    let batch = add_load_timestamp(batch)?;                // New col
    Ok(batch)
}
```

---

## Phase 6: ClickHouse Integration ⚠️ REWRITE

### 6.1 clickhouse-arrow Client (NEW)

- [x] **6.1.1** Fork clickhouse-arrow with Variant/Dynamic/Nested types
- [ ] **6.1.2** Arrow → ClickHouse native protocol serialization
- [ ] **6.1.3** Connection management
- [ ] **6.1.4** TLS configuration

### 6.2 Schema Introspection (REVISED)

- [x] **6.2.1** TableSchema struct with Arrow + CH type mapping
- [x] **6.2.2** ch_type_to_arrow conversion
- [ ] **6.2.3** On-demand fetch from system.columns
- [ ] **6.2.4** Periodic refresh (configurable, e.g., 60s)
- [ ] **6.2.5** Cache invalidation on schema mismatch error

### 6.3 Inserter (REVISED)

- [x] **6.3.1** Arrow RecordBatch insert interface
- [x] **6.3.2** Retry with exponential backoff
- [ ] **6.3.3** clickhouse-arrow native protocol (replace JSON bridge)
- [ ] **6.3.4** Concurrent multi-table insert

---

## Phase 7: Pipeline Orchestration ⚠️ UPDATE

### 7.1 Pipeline Core (REVISED)

- [ ] **7.1.1** Update for Arrow-based BufferManager
- [ ] **7.1.2** Process message → build Arrow batch with _destination
- [ ] **7.1.3** Flush → partition by destination, insert, ack chunks
- [ ] **7.1.4** Kafka offset commit on chunk ack

---

## Summary

| Phase    | Description          | Status         | Notes                        |
| -------- | -------------------- | -------------- | ---------------------------- |
| Phase 0  | Foundation           | ✅ Complete    | Revised for Arrow            |
| Phase 1  | Core Infrastructure  | ✅ Complete    | No changes needed            |
| Phase 2  | Kafka Integration    | ✅ Complete    | No changes needed            |
| Phase 3  | Routing              | ✅ Complete    | No changes needed            |
| Phase 4  | Transformation       | ⚠️ Rewrite     | Arrow-based transforms       |
| Phase 5  | Buffer Management    | ⚠️ Rewrite     | ArrowBuffer chunks           |
| Phase 6  | ClickHouse           | ⚠️ Rewrite     | clickhouse-arrow client      |
| Phase 7  | Pipeline             | ⚠️ Update      | Wire Arrow components        |
| Phase 8  | CLI & Commands       | ✅ Complete    | No changes needed            |
| Phase 9  | Enrichment           | ⏳ Post-MVP    | Arrow-native enrichment      |

---

**Last Updated:** 2025-12-24
