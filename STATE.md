# Project State

**Project:** dfe-loader-clickhouse
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** Major Refactor - Arrow-based Architecture
**Reference:** Feature parity (or better) with `/projects/clickhouse-loader` (Go version)

---

## Build Settings

```bash
# Limit Cargo resource consumption
export CARGO_BUILD_JOBS=2
```

---

## Current Status (2025-12-24)

### Architecture Migration: Arrow-Based Pipeline

**BREAKING CHANGE:** Migrating from JSON-based buffers to Arrow-based chunked buffers.

```text
OLD: Kafka → Consumer → JSON parse → Transform → ColumnarBuffer (JSON) → JSONEachRow insert
NEW: Kafka → Consumer → Arrow parse → Transform → ArrowBuffer (chunks) → Native protocol insert
```

### New Design Principles

1. **Single partitioned buffer**: One ArrowBuffer for all tables, destination is a column
2. **Chunked lifecycle**: Each Kafka batch → immutable Arrow chunk → drop on ack
3. **Schema introspection on-demand**: Fetch schema on first write, refresh periodically
4. **No backwards compatibility code**: Clean break from JSON-based approach

### Libraries

| Purpose        | Library          | Status      | Notes                                    |
|----------------|------------------|-------------|------------------------------------------|
| ClickHouse     | clickhouse-arrow | In Progress | Forked with Variant/Dynamic/Nested types |
| ClickHouse     | klickhouse       | Parked      | Fork complete, not used for main path    |
| Arrow          | arrow            | Active      | Columnar data format                     |
| JSON→Arrow     | arrow-json       | Planned     | SIMD JSON to Arrow deserialization       |
| MsgPack→Arrow  | TBD              | Planned     | MessagePack to Arrow                     |

---

## Chunked Arrow Buffer Design

### Core Concepts

```rust
/// Kafka batch → Arrow chunk (immutable)
struct ArrowChunk {
    id: u64,
    batch: RecordBatch,        // Includes _destination column
    offset: Option<KafkaOffset>, // For at-least-once ack
    state: ChunkState,         // Pending → InFlight → Acked/Failed
}

/// Single buffer for all destinations
struct ArrowBuffer {
    chunks: HashMap<u64, ArrowChunk>,
    // Partition by _destination at flush time
}
```

### Flow

1. **Ingest**: Kafka batch → deserialize (JSON/MsgPack) → Arrow RecordBatch
2. **Route**: Add `_destination` column based on category extraction
3. **Buffer**: Push chunk to ArrowBuffer with Kafka offset
4. **Flush**: Partition pending chunks by `_destination`, write to ClickHouse
5. **Ack**: On success, drop chunk (instant memory free via Arc)
6. **Retry**: On failure, mark chunk as failed, retry later

### Benefits

- **O(1) memory free**: Drop whole chunks, no row-level cleanup
- **Efficient partitioning**: Arrow's columnar format for fast group-by
- **Schema introspection**: Build Arrow schema from ClickHouse `system.columns`
- **Zero-copy potential**: Arrow → ClickHouse native via clickhouse-arrow

---

## Schema Introspection

### On-Demand with Refresh

```rust
struct SchemaRegistry {
    schemas: HashMap<String, TableSchema>,
    last_refresh: HashMap<String, Instant>,
    refresh_interval: Duration,  // e.g., 60 seconds
    client: Arc<ClickHouseClient>,
}

impl SchemaRegistry {
    async fn get_or_fetch(&mut self, table: &str) -> Result<&TableSchema> {
        if self.needs_refresh(table) {
            self.fetch_schema(table).await?;
        }
        Ok(self.schemas.get(table).unwrap())
    }
}
```

### ClickHouse Type → Arrow Type

| ClickHouse      | Arrow               | Notes                        |
|-----------------|---------------------|------------------------------|
| Int8-64         | Int8-64             | Direct mapping               |
| UInt8-64        | UInt8-64            | Direct mapping               |
| Float32/64      | Float32/64          | Direct mapping               |
| String          | Binary              | UTF-8 not guaranteed         |
| UUID            | FixedSizeBinary(16) | Raw bytes                    |
| DateTime64      | Int64               | Epoch with precision         |
| Array(T)        | List(T)             | Recursive                    |
| Variant         | Binary              | Serialized bytes             |
| Dynamic         | Binary              | Serialized bytes             |
| JSON            | Binary              | Serialized bytes             |

---

## Work Completed

### clickhouse-arrow Fork

Added new ClickHouse types to local fork:

- `Type::Variant(Vec<Type>)` - Discriminated union
- `Type::Dynamic { max_types: Option<usize> }` - Runtime-typed
- `Type::Nested(Vec<(String, Type)>)` - Parallel arrays

Files modified:
- `crates/clickhouse-arrow/clickhouse-arrow/src/native/types.rs`
- `crates/clickhouse-arrow/clickhouse-arrow/src/native/types/deserialize.rs`
- `crates/clickhouse-arrow/clickhouse-arrow/src/arrow/types.rs`
- `crates/clickhouse-arrow/clickhouse-arrow/src/errors.rs`

### klickhouse Fork (Parked)

Complete fork with Variant/Dynamic/JSON/Nested types. Parked in favor of clickhouse-arrow.

Location: `crates/klickhouse/`

---

## TODO

### Immediate

1. [ ] Complete pipeline refactor to Arrow-based approach
2. [ ] Implement schema introspection with on-demand refresh
3. [ ] Wire up clickhouse-arrow for native protocol inserts

### Pending

1. [ ] Compare serialization with ClickHouse C++ source
2. [ ] Add arrow-json for SIMD JSON → Arrow
3. [ ] Implement MessagePack → Arrow

---

## Test Environment

Located at k8s.tyrell.com.au with:

- ClickHouse: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

**Last Updated:** 2025-12-24
**Version:** 0.2.0-arrow
**Status:** Major Refactor In Progress
