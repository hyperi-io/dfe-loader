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

1. [x] **Implement Variant serialization** - VariantSerializer complete
2. [x] **Implement Dynamic serialization** - DynamicSerializer complete
3. [x] **Implement Nested serialization** - NestedSerializer complete (delegates to Array+Tuple)
4. [x] **Integration test against ClickHouse 24.x+** - Validated with ClickHouse 25.12.1

### Completed

1. [x] Implement configurable db.table field routing
2. [x] Wire up clickhouse-arrow for native protocol inserts
3. [x] Schema introspection on-demand with refresh
4. [x] Variant/Dynamic/Nested serializers in clickhouse-arrow fork

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

### Accomplished

- **Variant/Dynamic/Nested Serialization COMPLETE**:
  - Researched ClickHouse native protocol format using local source at `/projects/ClickHouse`
  - Created `serialize/variant.rs` with VariantSerializer (discriminator + per-variant columns)
  - Created `serialize/dynamic.rs` with DynamicSerializer (runtime type discovery)
  - Created `serialize/nested.rs` with NestedSerializer (delegates to Array+Tuple)
  - Added `Value::Variant(u8, Box<Value>)` and `Value::Dynamic(String, Box<Value>)` types
  - Wired up all serializers in serialize.rs and types.rs
  - All 173 tests passing (160 unit + 13 integration)

- **Integration Testing COMPLETE**:
  - Verified ClickHouse 25.12.1 at k8s.tyrell.com.au supports Variant type
  - Added `test_clickhouse_variant_type_support` integration test
  - Successfully created Variant table and inserted data via JSON bridge

### Key Findings from ClickHouse Source

- **Variant**: Discriminator (u8, 0-254 for types, 255=NULL), then per-variant column data
- **Dynamic**: Similar to Variant but types discovered at runtime, stored in structure prefix
- **Nested**: Just `Array(Tuple(...))` - no special serialization needed

### Files Created/Modified

- NEW: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize/variant.rs`
- NEW: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize/dynamic.rs`
- NEW: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize/nested.rs`
- MODIFIED: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/serialize.rs`
- MODIFIED: `crates/clickhouse-arrow/clickhouse-arrow/src/native/types.rs`
- MODIFIED: `crates/clickhouse-arrow/clickhouse-arrow/src/native/values.rs`
- MODIFIED: `tests/integration/clickhouse.rs` (added Variant test)

### Reference

- ClickHouse server source available at `/projects/ClickHouse`
- Key files: `SerializationVariant.cpp`, `SerializationDynamic.cpp`, `DataTypeNested.cpp`

### Git State

- **Branch:** main
- **Working tree:** Modified (new serializers + value types + integration test)

---

**Last Updated:** 2025-12-25
**Version:** 0.3.0-arrow
**Status:** Arrow Pipeline Complete - Variant/Dynamic/Nested Serialization Complete
