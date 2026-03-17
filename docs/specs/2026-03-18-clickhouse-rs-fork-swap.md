# clickhouse-rs Fork Swap and Dynamic RowBinary Insert

**Date:** 2026-03-18
**Status:** Approved design
**Scope:** hyperi-io/clickhouse-rs fork + dfe-loader integration

---

## Problem

dfe-loader currently inserts to ClickHouse via JSONEachRow over HTTP using
`reqwest`. This works but has two costs:

1. **Loader CPU** — serialise `Map<String, Value>` to JSON text (quote escaping,
   UTF-8 validation for every number/boolean)
2. **ClickHouse CPU** — parse JSON text back into columnar binary on every row

At PB-scale ingest, the ClickHouse-side JSON parse is significant. RowBinary
skips it entirely — binary arrives pre-columnarised, zero server-side parsing.

The HyperI clickhouse-rs fork already has native TCP transport, connection
pooling, LowCardinality INSERT, and AsyncInserter. What's missing is a
**runtime schema-driven RowBinary insert** for dynamic schemas — the same
ease-of-use as JSONEachRow but with binary performance.

---

## Decision

1. Add `DynamicInsert` API to the clickhouse-rs fork — accepts `Map<String, Value>`,
   encodes to RowBinary using schema fetched from `system.columns` at runtime
2. Lift `ParsedType` (type parser) from dfe-loader into the fork — generic, any
   user benefits
3. Add automatic schema recovery — pause on schema mismatch, re-fetch, retry
4. Swap dfe-loader to use the fork via `[patch.crates-io]`
5. Drop `reqwest` for data inserts in dfe-loader — single `clickhouse::Client`

---

## Insert Tier Architecture (Fork)

```
Tier 1: Insert<T>          — Compile-time schema, #[derive(Row)], fastest
Tier 2: DynamicInsert       — Runtime schema, Map<String,Value> -> RowBinary (NEW, default for dynamic)
Tier 3: InsertFormatted     — Raw bytes, any format (JSONEachRow, CSV, etc.)
```

Tier 2 is the new default for anyone without compile-time schema knowledge.
Same ergonomics as Tier 3 but binary wire format.

### User-Facing API (Tier 2)

```rust
// As simple as JSONEachRow — user pushes Map<String, Value>, fork does the rest
let client = Client::default().with_url("http://localhost:8123");

let mut insert = client.dynamic_insert("db.table").await?;
// Schema fetched automatically from system.columns, cached with TTL

insert.write_map(&row).await?;   // Map<String, Value> -> RowBinary
insert.write_map(&row2).await?;
insert.end().await?;
```

Under the hood:
1. `dynamic_insert("db.table")` fetches schema from `system.columns` (cached)
2. Schema parsed into `Vec<ColumnSchema>` with full type info
3. Each `write_map()` encodes `Map<String, Value>` to RowBinary using cached schema
4. Missing columns get default values, extra columns silently dropped

### Async Variant (Background Task + Backpressure)

```rust
// Multi-producer batching — same pattern as AsyncInserter<T>
let batcher = client.dynamic_batcher("db.table", BatchConfig::default()).await?;
let handle = batcher.handle();  // cheap clone for concurrent tasks

handle.write_map(&row).await?;  // bounded channel backpressure
handle.write_map(&row2).await?;
batcher.send().await?;           // flush + shutdown
```

### Formatted Variant (Existing, Unchanged)

```rust
// Raw bytes — JSONEachRow, CSV, etc.
let mut insert = client
    .insert_formatted_with("INSERT INTO db.table FORMAT JSONEachRow")
    .buffered();
insert.write(ndjson_bytes).await?;
insert.end().await?;
```

---

## Schema Reflection (Fork)

### ParsedType — Lift from dfe-loader

The loader's `ParsedType` (558 lines) is a full ClickHouse type parser. It handles:

- Wrapper types: `Nullable(T)`, `LowCardinality(T)`, nested combinations
- Parametric types: `DateTime64(3, 'UTC')`, `Decimal(18, 6)`, `FixedString(32)`
- Container types: `Array(T)`, `Map(K, V)`
- Enum types: `Enum8(...)`, `Enum16(...)`
- All scalar types: Int8-256, UInt8-256, Float32/64, UUID, IPv4/IPv6, Bool, Date, etc.

This moves into the fork as a public API. Any clickhouse-rs user with dynamic
schemas benefits — not DFE-specific.

### ColumnSchema (New in Fork)

```rust
pub struct ColumnSchema {
    pub name: String,
    pub parsed_type: ParsedType,
    pub default_kind: DefaultKind,   // None, Default, Materialized, Alias
    pub has_default: bool,           // Can be omitted from INSERT
}
```

Fetched from `system.columns` (name, type, default_kind, default_expression).

### SchemaCache (Upgraded in Fork)

| Current | Proposed |
|---|---|
| `Vec<(String, String)>` name + raw type | `Vec<ColumnSchema>` name + parsed type + default |
| TTL only | TTL + background refresh + error-triggered invalidation |
| `pub(crate)` | `pub` — users can inspect for custom logic |
| Native transport only | Shared across HTTP and native transports |

---

## Schema Recovery Flow

RowBinary fails hard on schema mismatch (wrong column count, wrong type).
JSONEachRow is self-describing so ClickHouse handles drift. The fork must
handle schema drift transparently for DynamicInsert.

```
write_map(&row)
    |
    v
encode with cached schema
    |
    +-- OK -> continue
    |
    +-- FAIL (ClickHouse returns schema mismatch error)
            |
            v
        PAUSE inserts (hold channel, don't drop rows)
            |
            v
        Accumulate failed rows in retry buffer
            |
            v
        Invalidate schema cache for this table
            |
            v
        Re-fetch schema from system.columns
            |
            v
        Re-encode failed rows with new schema
            |
            v
        RESUME inserts
            |
            +-- OK -> drain retry buffer, continue normal flow
            |
            +-- FAIL again -> surface error to caller
                (genuine data problem, not schema drift)
```

Properties:
- **Zero data loss** — failed rows buffered, not dropped
- **Automatic recovery** — no user intervention for ADD COLUMN, type changes
- **Bounded retry** — one re-fetch per mismatch, prevents infinite loops
- **Observable** — metrics for schema refreshes, recovery count, retry buffer depth

---

## Runtime RowBinary Encoder

The fork's `src/native/columns.rs` (1,494 lines) and `src/native/encode.rs`
(531 lines) already encode every ClickHouse type for the typed `Insert<T>` path.

The work is adding a `Value`-dispatch layer that routes `&serde_json::Value` to
the correct column encoder based on `ParsedType`:

```rust
fn encode_dynamic_value(value: &Value, col: &ColumnSchema, buf: &mut Vec<u8>) -> Result<()> {
    // Nullable: write 0/1 prefix byte
    // LowCardinality: dictionary encode
    // Then type-dispatch:
    //   String/FixedString   -> length-prefixed bytes
    //   Int8..Int256         -> little-endian from Value::Number
    //   Float32/64           -> IEEE 754
    //   DateTime64(p, tz)    -> epoch with precision scaling
    //   UUID                 -> 16 bytes, ClickHouse byte order
    //   IPv4/IPv6            -> network byte order
    //   Decimal(p,s)         -> scaled integer
    //   Array(T)             -> varint length + recursive elements
    //   Map(K,V)             -> varint length + key/value pairs
    //   JSON                 -> as String (ClickHouse parses internally)
    //   Bool                 -> u8
}
```

This reuses the existing encoders — no duplication. The new layer is a dispatcher
from `Value` + `ParsedType` to the existing binary writers.

---

## What Lives Where

### In the fork (clickhouse-rs)

| Component | Reason |
|---|---|
| `ParsedType` (type parser) | Generic — any dynamic-schema user benefits |
| `ColumnSchema` (name + parsed type + default) | Schema reflection is library concern |
| `SchemaCache` (TTL + background refresh + invalidation) | Shared across transports |
| Runtime RowBinary encoder (Value -> binary) | Performance-critical, couples to wire format |
| Schema recovery (pause/re-fetch/retry) | Transparent to user, library responsibility |
| `DynamicInsert` / `DynamicBatcher` API | User-facing, matches existing `Insert<T>` pattern |
| `AsyncFormattedInserter` (JSONEachRow async) | Completes the Tier 3 async story |

### In dfe-loader (application-specific)

| Component | Reason |
|---|---|
| `Coercer` (epoch -> DateTime, UUID normalisation) | Application-specific coercion policy |
| DDL comment parsing (`@source`, `@renamed`, `@computed`) | DFE expression language |
| `CircuitBreaker` (per-table failure tracking) | Application-specific resilience |
| Binary-split salvage (recursive half-split for bad rows) | Application-specific error recovery |
| Kafka offset tracking (`KafkaOffset`, `FlushBatch`) | Application-specific |
| Object pools (`BufferPools`) | Application-specific memory management |
| `BufferManager` (per-table row accumulation) | Wraps fork's `DynamicBatcher`, adds Kafka offsets |

---

## Fork Branch Strategy

### Current branch structure

```
main (upstream v0.14.2)
+-- hyperi/native-transport
|   +-- hyperi/connection-pooling
|       +-- hyperi/lc-insert
|           +-- hyperi/async-inserter
+-- hyperi/batching (independent)
```

### New work

Add to `hyperi/async-inserter` (tip of the chain):

1. `ParsedType` + `ColumnSchema` + upgraded `SchemaCache`
2. Runtime RowBinary encoder (`Value` dispatch)
3. `DynamicInsert` + `DynamicBatcher` API
4. Schema recovery flow
5. `AsyncFormattedInserter` (JSONEachRow async variant)

### Merge order to upstream

Sequential, each tested in dfe-loader before merging next:

```
native-transport -> connection-pooling -> lc-insert -> async-inserter
batching is independent (mergeable to upstream anytime)
```

---

## dfe-loader Migration Path

### Phase 1: Swap to fork (patch.crates-io)

Uncomment `[patch.crates-io]` in Cargo.toml. Zero change to `[dependencies]`.
Continue using JSONEachRow via `InsertFormatted` — validates the fork compiles
and passes existing tests.

### Phase 2: Switch to DynamicInsert

Replace `HttpClickHouseClient` insert methods with `client.dynamic_insert()`.
Drop `reqwest` as a direct dependency for data inserts. Keep for health checks
if needed, or migrate those to the fork's `Client::query()`.

### Phase 3: Remove duplication

- Drop `ParsedType` from dfe-loader (use fork's)
- Drop loader's `SchemaCache` (wrap fork's, add comment parsing on top)
- Switch `Coercer` to Delta mode (only coercions RowBinary can't handle)
- `BufferManager` flush target changes from "build NDJSON + reqwest POST"
  to `dynamic_batcher.write_map()`

### Phase 4: PR to upstream (later)

Open PRs from hyperi/* branches to upstream clickhouse-rs. Each branch
is a self-contained feature with tests and docs.

### Phase 5: Swap back to upstream (if merged)

Remove `[patch.crates-io]`, bump `clickhouse` version in `[dependencies]`
to the upstream release containing our changes. Zero code changes in
dfe-loader — the API is identical.

---

## Swap Mechanism (Cargo.toml)

```toml
# In [dependencies] — always points to crates.io version
clickhouse = { version = ">=0.14", features = ["lz4", "time", "rustls-tls"] }

# Activate fork: uncomment this section
[patch.crates-io]
clickhouse = { path = "../clickhouse-rs" }

# To revert: comment out or remove [patch.crates-io]
# Zero change to [dependencies] needed
```

For iterating on fork fixes: edit code in `/projects/clickhouse-rs`,
`cargo build` in dfe-loader picks up changes immediately (path dependency).

---

## Performance Expectations

| Metric | JSONEachRow (current) | DynamicInsert RowBinary (proposed) |
|---|---|---|
| Loader CPU (serialisation) | `serde_json::to_string` per row | Binary encode per column (no quote escaping, no UTF-8 overhead for numbers) |
| Wire bytes | ~2x larger (JSON text) | Compact binary |
| ClickHouse CPU (ingest) | Full JSON parser per row | Zero — binary already columnar |
| Schema flexibility | Self-describing, CH coerces | Schema-driven, fork coerces client-side |
| Schema change handling | Transparent | Automatic recovery (pause, re-fetch, retry) |

**Expected improvement:** 30-50% reduction in ClickHouse ingest CPU. Wire bytes
roughly halved. Loader CPU roughly neutral (JSON serialisation replaced by binary
encoding — different work, similar cost).

---

## Open Questions

1. **LowCardinality in DynamicInsert** — the fork's `hyperi/lc-insert` branch
   handles LC for typed inserts. Need to verify LC dictionary encoding works
   with the `Value` dispatch path.

2. **Compression** — LZ4 compression on RowBinary is standard. Need to confirm
   the fork's native transport compression plays well with DynamicInsert.

3. **HTTP vs Native transport for DynamicInsert** — start with HTTP (existing
   infrastructure), add native TCP as optimisation later. Both use RowBinary
   wire format.
