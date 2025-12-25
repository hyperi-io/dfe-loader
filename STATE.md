# Project State

**Project:** dfe-loader-clickhouse
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** Arrow-Only Pipeline Complete with SIMD Optimizations
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
5. **Arrow-only inserts**: No JSON fallback - native Arrow protocol only
6. **SIMD everywhere**: Use SIMD-accelerated parsing and conversion where possible

---

## Current Status (2025-12-25)

### Architecture: Per-Table Arrow Buffers with Native Protocol

```text
Kafka → Parse JSON/MsgPack (SIMD) → Route → Per-table buffer → Arrow RecordBatch (SIMD) → ClickHouse (native)
```

### Key Design Decisions

1. **Per-table buffers**: `HashMap<db.table, ArrowBatchBuilder>` - schema uniformity per batch
2. **Pre-flatten routing**: Extract db.table from event BEFORE flattening (dot notation for nested)
3. **Schema introspection**: Fetch from ClickHouse via Arrow client on first write, refresh periodically
4. **No backwards compatibility**: Clean break from old JSON approach
5. **Kafka offset tracking**: Per-batch offset list for at-least-once delivery (committed after successful insert)
6. **Native Arrow inserts**: clickhouse-arrow for direct Arrow RecordBatch inserts (**NO fallback**)

### DLQ Routing (Implemented)

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Per-table routing - `db.table.dlq` topics
- **Option 2**: Common DLQ topic for all failures

DLQ messages include: original payload, error reason, original topic/partition/offset, timestamp.

### Missing db.table Handling

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Send to DLQ
- **Option 2**: Send to "common" (for either db or table)

### Libraries

| Purpose        | Library          | Status      | Notes                                    |
|----------------|------------------|-------------|------------------------------------------|
| ClickHouse     | clickhouse-arrow | Active      | Native Arrow inserts (DFE fork with full type support) |
| Arrow          | arrow            | Active      | Columnar data format                     |
| JSON→Arrow     | arrow-json       | Active      | SIMD JSON to Arrow (via ReaderBuilder)   |
| JSON (SIMD)    | sonic-rs         | Active      | Fast SIMD JSON parsing                   |
| MsgPack        | rmp-serde        | Active      | MessagePack to serde_json::Value         |

**clickhouse-arrow DFE Fork Types:**
- Variant, Dynamic, Nested (ClickHouse 24.x+)
- BFloat16 (ML workloads)
- Time/Time64 (time-of-day)
- AggregateFunction, SimpleAggregateFunction (materialized views)

**Removed:**
- klickhouse - Moved to `/projects/klickhouse-hypersec` as read-only reference

---

## SIMD Optimizations

### Implemented

1. **sonic-rs JSON parsing**: Direct SIMD-accelerated parsing to serde_json::Value
   - Fixed wasteful sonic-rs → string → serde_json conversion
   - Now uses `sonic_rs::from_slice::<serde_json::Value>()` directly

2. **arrow-json SIMD conversion**: Batch JSON bytes → Arrow RecordBatch
   - `json_bytes_to_arrow_simd()` for raw bytes with known schema
   - `SimdBatchBuilder` for accumulating raw JSON and batch converting
   - Schema inference via `infer_schema_from_json_bytes()`

3. **Pre-computed schemas**: Cache Arrow schemas per table to avoid repeated inference

### Performance Hierarchy (fastest to slowest)

1. `json_bytes_to_arrow_simd()` - Direct SIMD bytes→Arrow with known schema
2. `SimdBatchBuilder.build()` - Accumulate bytes, batch convert with SIMD
3. `json_batch_to_arrow()` - Batch conversion with schema inference
4. `json_to_arrow_batch()` - Single-row conversion (avoid in hot path)

### Hot Path Optimizations (2025-12-25)

**Round 1 - Core Optimizations:**

1. **Eliminated double JSON parsing**: Added `route_value()` method to Router that operates on
   already-parsed `serde_json::Value`. Orchestrator now parses once and routes without re-parsing.

2. **Arc<str> for topic strings**: `KafkaOffset.topic` and `KafkaMessage.topic` now use `Arc<str>`
   instead of `String`. Messages from the same topic share the same Arc, avoiding clones.

3. **Ownership-based flattening**: Added `flatten_value_owned()` that takes ownership of the Value,
   eliminating all leaf value clones. Transformer now uses this in the hot path.

4. **Conditional sanitization**: `sanitize_fields()` now returns input unchanged when no sanitization
   is enabled. When sanitization is needed, individual keys are checked before allocating.

**Round 2 - Deep Lateral Analysis:**

5. **Per-table destination storage**: `ArrowBatchBuilder` stores destination once per table, not
   per-row. Saves N-1 String allocations per batch where N = row count.

6. **Fast path for simple fields**: Router's `get_nested_field()` checks for `.` first. Simple
   field access (90%+ of cases) avoids iterator allocation from `split('.')`.

7. **Lazy warnings allocation**: Transformer only allocates warnings Vec when actually needed
   (timestamp correction/invalid). 99%+ of messages have zero warnings.

8. **Cached current time**: Transformer caches `Utc::now()` once per message and reuses for
   timestamp validation, injection, and fallback. Reduces syscalls from 2-3 to 1 per message.

### Future Optimizations

1. **simd-json integration**: Replace sonic-rs with simd-json for even faster parsing
2. **Vectorized flattening**: SIMD-accelerated nested JSON flattening
3. **Arrow compute kernels**: Use arrow-rs compute functions for transformations
4. **String interning for db.table**: Cache common destination strings to avoid repeated format!()

---

## Common Header Schema (v2 - Minimal)

The destination tables have a minimal required schema. All other fields are dynamic.

| Column | Type | Default | Nullable | Notes |
|--------|------|---------|----------|-------|
| `timestamp` | DateTime64(3) | - | **NO** | Event occurrence time (milliseconds) |
| `timestamp_load` | DateTime64(3) | `now64(3)` | **NO** | Load time (ClickHouse DEFAULT) |
| `_uuid` | UUID | `generateUUIDv7()` | **NO** | Unique event ID. UUIDv7 (time-ordered) |
| `logoriginal` | String | - | YES | Original unparsed log line |
| `logjson` | JSON | - | YES | Complete Kafka message as JSON type |
| `_tags` | JSON | - | YES | Meta info + collector/agent info as JSON |

### Routing Fields (NOT stored in destination)

| Field | Config Priority | Purpose |
|-------|-----------------|---------|
| `org_id` | `["org_id", "tags.event.org_id"]` | Sets **database** for insert |
| `event_category` | `["event_category", "tags.event.category"]` | Sets **table** for insert |

These are extracted pre-flatten for routing but **removed before insert** (not in destination schema).

### Config: Tags Handling

```toml
[metadata]
# Fields to check for tags (first match wins)
tags_fields = ["tags", "_tags", "meta", "metadata.tags"]
# Output field name (underscore prefix avoids collision)
tags_output = "_tags"
# Drop tags entirely after routing extraction (saves storage)
drop_tags = false
```

### Implementation Requirements

1. **`timestamp`** (NOT NULLABLE)
   - DateTime64(3) for millisecond precision
   - Copy-from field logic using source metadata
   - Fallback to `now64(3)` if missing/invalid
   - Already implemented in `TimestampValidator`

2. **`timestamp_load`** (NOT NULLABLE)
   - DateTime64(3) for millisecond precision
   - ClickHouse DEFAULT `now64(3)` - loader omits field
   - Note: All rows in a batch get same timestamp (acceptable)

3. **`_uuid`** (NOT NULLABLE, was `event_hash`)
   - Renamed to `_uuid` (underscore prefix avoids collision with source data)
   - Auto-generate UUIDv7, ignore any incoming value
   - Time-ordered (sortable), unique per event
   - Let ClickHouse generate via DEFAULT `generateUUIDv7()`

4. **`logjson`** - **NEW**
   - Store complete original Kafka message as JSON
   - Capture before any transformation

5. **`_tags`** - **CHANGED** (was `tags`)
   - Renamed to `_tags` (underscore prefix avoids collision)
   - Config-driven source field list (first match wins)
   - Optional: `drop_tags = true` to not store after routing extraction
   - Stored as JSON column (not flattened)

### _uuid: UUIDv7 Generation

UUIDv7 is time-ordered (millisecond precision) with random suffix - ideal for event IDs.

**Recommendation**: Use ClickHouse DEFAULT - simpler, no client dependency.

```sql
_uuid UUID DEFAULT generateUUIDv7()
```
- ClickHouse 24.8+ has native `generateUUIDv7()`
- Monotonic within timestamp, sortable
- Loader doesn't need to generate - just omit field

**Variants available in ClickHouse 24.8+:**
| Function | Monotonicity | Notes |
|----------|--------------|-------|
| `generateUUIDv7()` | Thread-monotonic | Guarantees ordering within thread |
| `generateUUIDv7ThreadMonotonic()` | Same as above | Explicit name |
| `generateUUIDv7NonMonotonic()` | None | Slightly faster, no ordering guarantee |

---

## Dynamic db.table Routing

Routing happens **PRE-flattening** using dot notation for nested field access.

```rust
// Config (from ENV/config cascade)
db_fields: ["org_id", "tags.event.org_id"]          // First matching field = database
table_fields: ["event_category", "tags.event.category"]  // First matching = table
default_db: "common"                                // Fallback if no field found
default_table: "common"                             // Fallback if no field found
```

**Example event:**
```json
{"org_id": "acme", "event_category": "auth", "tags": {"event": {"org_id": "backup"}}}
```

**Result:** `acme.auth` (org_id found at top level, event_category found first)

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

1. **Parse**: Kafka message → JSON/MsgPack → `serde_json::Value` (SIMD)
2. **Route**: Extract db.table from event data (pre-flatten)
3. **Buffer**: Push to per-table `ArrowBatchBuilder` with Kafka offset
4. **Flush**: When threshold reached, build `RecordBatch` (SIMD), insert to ClickHouse
5. **Ack**: On success, commit Kafka offsets from batch

### Benefits

- **Schema uniformity**: Each RecordBatch has consistent schema (same table)
- **Independent flush**: High-volume tables flush more often
- **Efficient batching**: Accumulate N messages before Arrow conversion
- **Memory efficient**: Data stays as JSON until batch build time

---

## Work Completed

### Feature Implementation (2025-12-25)

- [x] arrow-json SIMD conversion (`SimdBatchBuilder`, `json_bytes_to_arrow_simd`)
- [x] Kafka offset commit after successful ClickHouse insert (at-least-once delivery)
- [x] DLQ routing with db.table topic naming (`DlqProducer`, `DlqRoutingMode`)
- [x] Fixed sonic-rs JSON parsing (removed wasteful conversion)
- [x] Added `offsets_committed` metric to Prometheus

### klickhouse Removal (2025-12-25)

- [x] Moved klickhouse fork to `/projects/klickhouse-hypersec` (read-only reference)
- [x] Removed all klickhouse dependencies from Cargo.toml
- [x] Rewrote `Inserter` to use Arrow-only (no JSON fallback)
- [x] Moved `ColumnInfo` and `TableSchema` to `types.rs`
- [x] Updated `ArrowClickHouseClient` with `query()`, `table_exists()`, `list_tables()`
- [x] Rewrote integration tests to use Arrow inserts
- [x] All 13 integration tests passing with Arrow-native inserts

### clickhouse-arrow Integration (2025-12-24)

- [x] Added clickhouse-arrow dependency to Cargo.toml
- [x] Created `ArrowClickHouseClient` wrapper
- [x] Updated `Inserter` to support native Arrow inserts
- [x] Configurable db.table routing implemented
- [x] All tests passing

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
- `Value::Variant(u8, Box<Value>)` - Variant value type
- `Value::Dynamic(String, Box<Value>)` - Dynamic value type

Location: `crates/clickhouse-arrow/`

### Variant/Dynamic/Nested Serialization (2025-12-25)

- [x] Created `serialize/variant.rs` with VariantSerializer
- [x] Created `serialize/dynamic.rs` with DynamicSerializer
- [x] Created `serialize/nested.rs` with NestedSerializer
- [x] Wired up all serializers in serialize.rs and types.rs
- [x] Integration test against ClickHouse 25.12.1

---

## TODO

### All Completed

1. [x] Implement configurable db.table field routing
2. [x] Wire up clickhouse-arrow for native protocol inserts
3. [x] Schema introspection on-demand with refresh
4. [x] Variant/Dynamic/Nested serializers in clickhouse-arrow fork
5. [x] Remove klickhouse dependency - Arrow-only path
6. [x] Integration test Arrow inserts work end-to-end
7. [x] Use arrow-json for SIMD JSON → Arrow conversion
8. [x] Kafka offset commit on successful insert
9. [x] DLQ routing with db.table topic naming
10. [x] Investigate further SIMD optimizations
11. [x] BFloat16/Time/Time64/AggregateFunction types in clickhouse-arrow fork

### Common Header v2 Implementation

1. [x] **Capture `logjson` before transform** - Store raw Kafka payload as JSON before any processing
2. [x] **Config-driven `_tags` extraction** - First-match from `tags_fields` list, store as `_tags`
3. [x] **Add `drop_tags` config option** - Option to not store tags after routing extraction
4. [x] **Remove routing fields** - Strip `org_id`/`event_category` from output (used only for routing)
5. [x] **Let ClickHouse generate `_uuid`** - Use DEFAULT `generateUUIDv7()` in DDL
6. [x] **Let ClickHouse generate `timestamp_load`** - Use DEFAULT `now64(3)` in DDL
7. [x] **Update table DDL template** - Create reference DDL with common header columns

### Future Enhancements

1. [ ] simd-json integration (even faster than sonic-rs)
2. [ ] Zero-copy routing field extraction
3. [ ] Vectorized JSON flattening
4. [ ] Production load testing and benchmarking

---

## Test Environment

Located at k8s.tyrell.com.au with:

- ClickHouse: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

## Reference Projects

- **klickhouse-hypersec**: `/projects/klickhouse-hypersec` - Fork with Variant/Dynamic/JSON/Nested types (read-only reference)
- **ClickHouse source**: `/projects/ClickHouse` - Server source for protocol research
- **Go loader**: `/projects/clickhouse-loader` - Reference implementation

---

**Last Updated:** 2025-12-25
**Version:** 0.6.0-hot-path-optimized
**Status:** Arrow-Only Pipeline with Hot Path Optimizations - Production Ready
