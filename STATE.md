# Project State

**Project:** dfe-loader-clickhouse
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** Arrow Pipeline - clickhouse-arrow Integration Complete
**Reference:** Feature parity (or better) with `/projects/clickhouse-loader` (Go version)

---

## Build Settings

```bash
# Limit Cargo resource consumption
export CARGO_BUILD_JOBS=2
```

---

## Development Principles

1. **Clean Arrow approach**: Don't munge existing code patterns into new Arrow approach if a clean rewrite or restructure is better
2. **Batch everywhere**: No row-by-row processing - batch everything for columnar efficiency
3. **Per-table buffers**: Each db.table gets its own ArrowBatchBuilder for schema uniformity
4. **Pre-flatten routing**: Extract db.table BEFORE flattening (dot notation for nested access)

---

## Current Status (2025-12-24)

### Architecture: Per-Table Arrow Buffers with Native Protocol

```text
Kafka → Parse JSON/MsgPack → Route to db.table → Per-table buffer → Arrow RecordBatch → ClickHouse (native)
```

### Key Design Decisions

1. **Per-table buffers**: `HashMap<db.table, ArrowBatchBuilder>` - schema uniformity per batch
2. **Pre-flatten routing**: Extract db.table from event BEFORE flattening (dot notation for nested)
3. **Schema introspection**: Fetch from ClickHouse on first write, refresh periodically
4. **No backwards compatibility**: Clean break from old JSON approach
5. **Kafka offset tracking**: Per-batch offset list for at-least-once delivery
6. **Native Arrow inserts**: clickhouse-arrow for direct Arrow RecordBatch inserts (fallback to JSON bridge)

### DLQ Routing

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Send to topic matching db.table name (using previous routing logic)
- **Option 2**: Send everything to common DLQ topic

### Missing db.table Handling

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Send to DLQ
- **Option 2**: Send to "common" (for either db or table)

### Libraries

| Purpose        | Library          | Status      | Notes                                    |
|----------------|------------------|-------------|------------------------------------------|
| ClickHouse     | clickhouse-arrow | Active      | Native Arrow inserts (forked with Variant/Dynamic/Nested) |
| ClickHouse     | klickhouse       | Fallback    | JSON bridge for legacy support           |
| Arrow          | arrow            | Active      | Columnar data format                     |
| JSON→Arrow     | arrow-json       | Added       | SIMD JSON to Arrow (dependency added)    |
| JSON (SIMD)    | sonic-rs         | Active      | Fast JSON parsing                        |
| MsgPack        | rmp-serde        | Active      | MessagePack to serde_json::Value         |

---

## Dynamic db.table Routing

Routing happens **PRE-flattening** using dot notation for nested field access.

```rust
// Config (from ENV/config cascade)
db_fields: ["org_id"]                               // First matching field = database
table_fields: ["event_category", "tags.event_category"]  // First matching = table
default_db: "common"                                // Fallback if no field found
default_table: "common"                             // Fallback if no field found
```

**Example event:**
```json
{"org_id": "acme", "event_category": "auth", "tags": {"event_category": "login"}}
```

**Result:** `acme.auth` (org_id found, event_category found first)

---

## Per-Table Buffer Architecture

```rust
struct BufferManager {
    buffers: HashMap<String, TableBuffer>,  // Key: "db.table"
    schemas: HashMap<String, TableSchema>,  // Cached from ClickHouse
}

struct TableBuffer {
    builder: ArrowBatchBuilder,    // Accumulates JSON objects
    offsets: Vec<KafkaOffset>,     // For at-least-once ack
    created_at: Instant,           // For time-based flush
}
```

### Flow

1. **Parse**: Kafka message → JSON/MsgPack → `serde_json::Value`
2. **Route**: Extract db.table from event data (pre-flatten)
3. **Buffer**: Push to per-table `ArrowBatchBuilder` with Kafka offset
4. **Flush**: When threshold reached, build `RecordBatch`, insert to ClickHouse
5. **Ack**: On success, commit Kafka offsets from batch

### Benefits

- **Schema uniformity**: Each RecordBatch has consistent schema (same table)
- **Independent flush**: High-volume tables flush more often
- **Efficient batching**: Accumulate N messages before Arrow conversion
- **Memory efficient**: Data stays as JSON until batch build time

---

## Work Completed

### clickhouse-arrow Integration (2025-12-24)

- [x] Added clickhouse-arrow dependency to Cargo.toml
- [x] Created `ArrowClickHouseClient` wrapper
- [x] Updated `Inserter` to support native Arrow inserts with JSON fallback
- [x] Configurable db.table routing implemented
- [x] All 171+ tests passing

### Per-Table Buffer (2025-12-24)

- [x] `BufferManager` with `HashMap<table, TableBuffer>`
- [x] `ArrowBatchBuilder` in `transform/arrow.rs`
- [x] `TableBuffer` with offset tracking
- [x] Orchestrator updated for new API
- [x] All tests passing

### clickhouse-arrow Fork

Added new ClickHouse types to local fork:

- `Type::Variant(Vec<Type>)` - Discriminated union
- `Type::Dynamic { max_types: Option<usize> }` - Runtime-typed
- `Type::Nested(Vec<(String, Type)>)` - Parallel arrays

Location: `crates/clickhouse-arrow/`

### klickhouse Fork (Parked)

Complete fork with Variant/Dynamic/JSON/Nested types. Parked in favor of clickhouse-arrow.

Location: `crates/klickhouse/`

---

## TODO

### HIGH PRIORITY - Required for Production

1. [ ] **Implement Variant serialization** - Replace placeholder in clickhouse-arrow fork
2. [ ] **Implement Dynamic serialization** - Replace placeholder in clickhouse-arrow fork
3. [ ] **Implement Nested serialization** - Replace placeholder in clickhouse-arrow fork

### Completed

1. [x] Implement configurable db.table field routing
2. [x] Wire up clickhouse-arrow for native protocol inserts
3. [x] Schema introspection on-demand with refresh

### Future

1. [ ] Use arrow-json for SIMD JSON → Arrow conversion
2. [ ] Kafka offset commit on successful insert
3. [ ] DLQ routing with db.table topic naming
4. [ ] Investigate further SIMD optimizations

---

## Test Environment

Located at k8s.tyrell.com.au with:

- ClickHouse: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

## Current Session (2025-12-25)

### In Progress

- **Variant/Dynamic/Nested Serialization**: Started researching serialization format
  - Read through `crates/clickhouse-arrow/clickhouse-arrow/src/native/types.rs` - Type definitions complete
  - Read through `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs` - Found placeholder code
  - Read existing serializers (nullable.rs, array.rs, tuple.rs) as patterns to follow
  - Was about to research ClickHouse binary format when session interrupted

### Accomplished

- Session resumed from previous context summary
- Reviewed codebase structure for serialization implementation
- Identified exactly where placeholders need replacement:
  - `serialize_prefix_async`: Lines 92-95 in serialize.rs (placeholder comment)
  - `serialize_column`: Lines 556-561 in types.rs (returns `Unimplemented` error)
  - `serialize_column_sync`: Lines 632-637 in types.rs (returns `Unimplemented` error)

### Key Files to Modify

- `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs` - Main serialization dispatch
- `crates/clickhouse-arrow/clickhouse-arrow/src/native/types.rs` - serialize_column methods
- New files needed: `serialize/variant.rs`, `serialize/dynamic.rs`, `serialize/nested.rs`

### Decisions Made

- Follow existing serializer patterns (Serializer trait with write_prefix, write, write_sync)
- Nested type can reuse tuple serialization logic (parallel arrays pattern)
- Need ClickHouse C++ source reference for exact binary format

### Next Steps

1. Research ClickHouse native protocol format for Variant type
2. Create `serialize/variant.rs` with VariantSerializer
3. Create `serialize/dynamic.rs` with DynamicSerializer
4. Create `serialize/nested.rs` with NestedSerializer (similar to tuple)
5. Wire up serializers in serialize.rs and types.rs
6. Test against real ClickHouse 24.x+ instance

### Blockers/Issues

- Need to understand exact binary format for Variant discriminator encoding
- ClickHouse source files referenced in WBS.md:
  - `ClickHouse/src/DataTypes/DataTypeVariant.cpp`
  - `ClickHouse/src/DataTypes/DataTypeDynamic.cpp`
  - `ClickHouse/src/DataTypes/DataTypeNested.cpp`
  - `ClickHouse/src/DataTypes/Serializations/`

### Git State

- **Branch:** main
- **Upstream:** ahead of origin/main by 3 commits
- **Uncommitted:** 18 files modified, 2 new files
- **Staged:** none

### Session Context Summary

This session continued work on implementing Variant/Dynamic/Nested type serialization
for the clickhouse-arrow fork. The previous session completed the Arrow pipeline
integration (routing, schema introspection, native inserts). Current focus is
replacing placeholder serialization code with real implementations. All existing
tests pass (171+). The serialization work requires understanding ClickHouse's
native binary protocol format for these newer types (introduced in ClickHouse 24.x).

---

**Last Updated:** 2025-12-25
**Version:** 0.3.0-arrow
**Status:** Arrow Pipeline Complete - Variant/Dynamic/Nested Serialization In Progress
