# ClickHouse Data Types Reference

**Purpose:** Complete enumeration of ClickHouse data types for reference during schema design and when extending RowBinary encoding in the clickhouse-rs fork.

**Last Updated:** 2026-03-09

**Sources:**

- [ClickHouse Data Types Documentation](https://clickhouse.com/docs/en/sql-reference/data-types)
- [ClickHouse Native Protocol - Columns](https://clickhouse.com/docs/native-protocol/columns)
- [clickhouse-cpp GitHub](https://github.com/ClickHouse/clickhouse-cpp)
- [JSON Type Blog Post](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
- [ClickHouse 24.8 Release](https://clickhouse.com/blog/clickhouse-release-24-08)
- [JSON/Dynamic/Variant Production Ready PR](https://github.com/ClickHouse/ClickHouse/pull/77785)

---

## Type Status Legend

| Status | Meaning |
|--------|---------|
| ✅ GA | Generally Available, production-ready |
| 🔷 Beta | Beta, stable but may change |
| 🧪 Experimental | Experimental, requires setting flag |
| ⚠️ Deprecated | Deprecated, avoid in new code |

---

## Complete Type Enumeration

### Integer Types (✅ GA)

| Type | Bytes | Range | Native Protocol |
|------|-------|-------|-----------------|
| `Int8` | 1 | -128 to 127 | Little-endian |
| `Int16` | 2 | -32768 to 32767 | Little-endian |
| `Int32` | 4 | -2³¹ to 2³¹-1 | Little-endian |
| `Int64` | 8 | -2⁶³ to 2⁶³-1 | Little-endian |
| `Int128` | 16 | -2¹²⁷ to 2¹²⁷-1 | Little-endian |
| `Int256` | 32 | -2²⁵⁵ to 2²⁵⁵-1 | Little-endian |
| `UInt8` | 1 | 0 to 255 | Little-endian |
| `UInt16` | 2 | 0 to 65535 | Little-endian |
| `UInt32` | 4 | 0 to 2³²-1 | Little-endian |
| `UInt64` | 8 | 0 to 2⁶⁴-1 | Little-endian |
| `UInt128` | 16 | 0 to 2¹²⁸-1 | Little-endian |
| `UInt256` | 32 | 0 to 2²⁵⁶-1 | Little-endian |

### Floating Point Types (✅ GA)

| Type | Bytes | Description | Native Protocol |
|------|-------|-------------|-----------------|
| `Float32` | 4 | IEEE 754 single precision | IEEE 754 binary |
| `Float64` | 8 | IEEE 754 double precision | IEEE 754 binary |
| `BFloat16` | 2 | Brain floating point (ML) | IEEE 754 variant |

### Decimal Types (✅ GA)

| Type | Precision | Scale | Underlying |
|------|-----------|-------|------------|
| `Decimal(P, S)` | 1-76 | 0-P | Auto-selected |
| `Decimal32(S)` | 1-9 | 0-9 | Int32 |
| `Decimal64(S)` | 10-18 | 0-18 | Int64 |
| `Decimal128(S)` | 19-38 | 0-38 | Int128 |
| `Decimal256(S)` | 39-76 | 0-76 | Int256 |

### Boolean Type (✅ GA)

| Type | Underlying | Values |
|------|------------|--------|
| `Bool` | UInt8 | 0=false, 1=true |

### String Types (✅ GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `String` | Variable-length, any bytes | (len, value) varint + bytes |
| `FixedString(N)` | Fixed N bytes, zero-padded | N bytes exactly |

### Date and Time Types (✅ GA)

| Type | Range | Resolution | Underlying |
|------|-------|------------|------------|
| `Date` | 1970-01-01 to 2149-06-06 | 1 day | UInt16 (days since epoch) |
| `Date32` | 1900-01-01 to 2299-12-31 | 1 day | Int32 (days since epoch) |
| `DateTime` | 1970-01-01 to 2106-02-07 | 1 second | UInt32 (seconds since epoch) |
| `DateTime(tz)` | Same + timezone | 1 second | UInt32 + timezone metadata |
| `DateTime64(P)` | Wide range | 10⁻ᴾ seconds | Int64 (scaled) |
| `DateTime64(P, tz)` | Same + timezone | 10⁻ᴾ seconds | Int64 + timezone metadata |
| `Time` | 00:00:00 to 23:59:59 | 1 second | UInt32 |
| `Time64(P)` | High precision time | 10⁻ᴾ seconds | Int64 |

**DateTime64 Precision:**

- P=0: seconds
- P=3: milliseconds
- P=6: microseconds
- P=9: nanoseconds

### UUID Type (✅ GA)

| Type | Bytes | Format |
|------|-------|--------|
| `UUID` | 16 | RFC 4122, stored as FixedString(16) |

### Network Types (✅ GA)

| Type | Bytes | Format |
|------|-------|--------|
| `IPv4` | 4 | UInt32 alias, big-endian |
| `IPv6` | 16 | FixedString(16), network byte order |

### Enum Types (✅ GA)

| Type | Underlying | Max Values |
|------|------------|------------|
| `Enum8('a'=1, 'b'=2, ...)` | Int8 | 256 |
| `Enum16('a'=1, 'b'=2, ...)` | Int16 | 65536 |

### Container Types (✅ GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `Array(T)` | Variable-length array | Offsets (UInt64[]) + Data (T[]) |
| `Tuple(T1, T2, ...)` | Fixed heterogeneous tuple | Concatenated columns |
| `Map(K, V)` | Key-value pairs | Offsets + Keys (K[]) + Values (V[]) |
| `Nested(name1 T1, ...)` | Nested structure | Flattened to arrays |

### Nullable Wrapper (✅ GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `Nullable(T)` | T or NULL | Nulls (UInt8[]) + Values (T[]) |

### LowCardinality Wrapper (✅ GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `LowCardinality(T)` | Dictionary encoding | Index + Keys columns |

Supported inner types: String, FixedString, Date, DateTime, numbers

### Geo Types (✅ GA)

| Type | Underlying | Description |
|------|------------|-------------|
| `Point` | Tuple(Float64, Float64) | (x, y) coordinate |
| `Ring` | Array(Point) | Closed polygon ring |
| `Polygon` | Array(Ring) | Polygon with holes |
| `MultiPolygon` | Array(Polygon) | Multiple polygons |

---

## Semi-Structured Types (NEW - 2024/2025)

### Variant Type (✅ GA as of late 2024)

```sql
Variant(T1, T2, T3, ...)
```

**Description:** Discriminated union of types. Each value is exactly one of the specified types.

**Example:**

```sql
Variant(String, UInt64, Array(String))
```

**Native Protocol:**

- Discriminator column (UInt8) indicating which type
- Separate column for each type variant
- Only the active variant has data for each row

### Dynamic Type (✅ GA as of late 2024)

```sql
Dynamic
Dynamic(max_types=N)
```

**Description:** Can store ANY type without pre-specification. Like Variant but types discovered at runtime.

**Features:**

- No need to declare types upfront
- `max_types` limits separate storage columns (default 32)
- Types beyond limit stored as String

**Native Protocol:**

- Type descriptor column
- Multiple data columns
- Overflow column for excess types

### JSON Type (✅ GA as of late 2024)

```sql
JSON
JSON(max_dynamic_paths=N, max_dynamic_types=M)
```

**Description:** Native columnar JSON storage. NOT the old Object('json') type.

**Features:**

- Paths flattened to subcolumns
- Dynamic types per path via Variant
- Typed paths extracted for efficient queries
- Subpath access: `json_column.path.to.field`

**Parameters:**

- `max_dynamic_paths`: Limit dynamic path columns (default 1024)
- `max_dynamic_types`: Limit types per path (default 32)

**Native Protocol:**

- Serialized as dynamic column structure
- Each path becomes separate column
- Uses Dynamic type internally for varying types

**Example:**

```sql
CREATE TABLE events (
    data JSON
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO events VALUES ('{"user": "alice", "score": 42, "tags": ["a", "b"]}');

SELECT data.user, data.score FROM events;
```

---

## Aggregate Function Types (✅ GA)

| Type | Description |
|------|-------------|
| `AggregateFunction(name, T1, ...)` | Intermediate state of aggregate |
| `SimpleAggregateFunction(name, T)` | Simplified aggregate state |

**Common aggregates:**

- `AggregateFunction(sum, UInt64)`
- `AggregateFunction(avg, Float64)`
- `AggregateFunction(uniq, String)`
- `AggregateFunction(quantile(0.5), Float64)`
- `SimpleAggregateFunction(sum, UInt64)`
- `SimpleAggregateFunction(max, DateTime)`

---

## Deprecated/Legacy Types

| Type | Status | Replacement |
|------|--------|-------------|
| `Object('json')` | ⚠️ Deprecated | Use `JSON` type |

---

## Library Support

### Current Stack (RowBinary via clickhouse-rs fork)

dfe-loader writes to ClickHouse in one of two formats, dispatched by
`InsertFormat` configuration:

| Format | Client | Purpose | Default |
|--------|--------|---------|---------|
| **RowBinary** | `clickhouse` (fork) via `DynamicInsert` | Schema-reflected typed binary encoding | ✅ Yes |
| **JSONEachRow** | `reqwest` HTTP POST | Fallback, server-side type coercion | No |

The fork's `DynamicInsert` reflects the target schema from `system.columns`,
encodes a `Map<String, Value>` to RowBinary column-by-column, and sets
`input_format_binary_read_json_as_string=1` for JSON-typed columns. The
loader never calls `clickhouse::Row` — everything is dynamic.

DDL and schema queries use the same fork via `ClickHouseQueryClient`.

### Fork Capabilities Beyond Upstream

| Type | Status |
|------|--------|
| All standard types | ✅ Upstream |
| **JSON** (GA v25.3) | ✅ Fork |
| **Variant** | ✅ Fork |
| **Dynamic** | ✅ Fork |
| **Nested** | ✅ Fork |
| **BFloat16** | ✅ Fork |
| **Time/Time64** | ✅ Fork |
| **AggregateFunction** | ✅ Fork |
| **SimpleAggregateFunction** | ✅ Fork |
| Native TCP protocol | ✅ Fork (upstream removed in v0.12+) |

The fork lives at `/projects/clickhouse-rs` (GitHub: `hyperi-io/clickhouse-rs`)
and is pulled in via `[patch.crates-io]` in `Cargo.toml`. See
[DESIGN.md](./DESIGN.md) for the branch chain and upstream PR plan.

**Historical note:** clickhouse-arrow (DFE fork) was trialled in early
development and dropped — RowBinary insert throughput matched Arrow within
noise (network-dominated) while avoiding the complexity of building Arrow
RecordBatches. See [DESIGN.md](./DESIGN.md) § Parser Selection History.

---

## Native Protocol Serialization Notes

### Numeric Types

All numeric types use **little-endian** byte order, matching x86/x64 memory layout for zero-copy operations.

### String/FixedString

- String: Varint length prefix + raw bytes
- FixedString(N): Exactly N bytes, zero-padded

### Arrays

```
Offsets: [UInt64 offsets array]
Data: [T elements array]
```

Offset[i] = end index of array[i] in data

### Nullable

```
Nulls: [UInt8 mask, 1=null]
Values: [T values, null positions have default]
```

### LowCardinality

```
Index type + Keys + Dictionary
```

### DateTime with Timezone

Timezone stored in column metadata, not per-value. All values in column share timezone.

---

## Implementation Status for dfe-loader

**RowBinary (default):** encoded by the fork's `DynamicInsert` from the
reflected schema. All standard types plus JSON, Variant, Dynamic, Nested,
BFloat16, Time/Time64, AggregateFunction, SimpleAggregateFunction. Decimal(P,S)
resolves to the concrete Decimal32/64/128/256 at encode time based on precision.

**JSONEachRow (fallback):** all types work via server-side coercion. Set
`insert_format = "json_each_row"` to opt in.

---

## Decision: Library Choice

**Current:** `/projects/clickhouse-rs` fork (MIT/Apache-2.0) used for DDL,
queries, and inserts. RowBinary via `DynamicInsert` is the default; JSONEachRow
via `reqwest` is the fallback.

**Rationale:** Schema-reflected RowBinary skips the server-side JSON parse, cuts
ClickHouse CPU, and gives full type support (JSON, Variant, Dynamic, Nested)
without requiring compile-time schemas. The `Map<String, Value>` model is
preserved end-to-end — encoding happens against `system.columns` at runtime.

See [DESIGN.md](./DESIGN.md) § Parser Selection History for the full decision
trail including Arrow, Mison, and simd-json evaluation results.
