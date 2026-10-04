<!--
  Project:      dfe-loader
  File:         docs/clickhouse/TYPES.md
  Purpose:      ClickHouse data type reference and native-protocol serialisation notes
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# ClickHouse data types

This is the type catalogue the dynamic encoder works against. When dfe-loader
reflects a table from `system.columns`, every column comes back as one of these
types (possibly wrapped in `Nullable` or `LowCardinality`), and the encoder in
`src/clickhouse_ext/` has to turn a `Value` into the right bytes for it. Use
this page when you are designing a schema, or when you are extending the encoder
to cover a type it does not yet handle.

Nothing here is loader-invented: it is the ClickHouse type system as of the
24.8+ line, annotated with how each type lands on the native wire. For how those
bytes are actually shipped (HTTP RowBinary vs native/TCP), see
[INSERT-FORMATS.md](INSERT-FORMATS.md); for the encoder seam itself, see
[CLICKHOUSE-EXT.md](CLICKHOUSE-EXT.md).

Sources:

- [ClickHouse Data Types Documentation](https://clickhouse.com/docs/en/sql-reference/data-types)
- [ClickHouse Native Protocol - Columns](https://clickhouse.com/docs/native-protocol/columns)
- [clickhouse-cpp GitHub](https://github.com/ClickHouse/clickhouse-cpp)
- [JSON Type Blog Post](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
- [ClickHouse 24.8 Release](https://clickhouse.com/blog/clickhouse-release-24-08)
- [JSON/Dynamic/Variant Production Ready PR](https://github.com/ClickHouse/ClickHouse/pull/77785)

---

## Type status legend

| Status | Meaning |
|--------|---------|
| GA | Generally Available, production-ready |
| Beta | Beta, stable but may change |
| Experimental | Experimental, requires setting flag |
| Deprecated | Deprecated, avoid in new code |

---

## Complete type enumeration

### Integer types (GA)

| Type | Bytes | Range | Native Protocol |
|------|-------|-------|-----------------|
| `Int8` | 1 | -128 to 127 | Little-endian |
| `Int16` | 2 | -32768 to 32767 | Little-endian |
| `Int32` | 4 | -2^31 to 2^31-1 | Little-endian |
| `Int64` | 8 | -2^63 to 2^63-1 | Little-endian |
| `Int128` | 16 | -2^127 to 2^127-1 | Little-endian |
| `Int256` | 32 | -2^255 to 2^255-1 | Little-endian |
| `UInt8` | 1 | 0 to 255 | Little-endian |
| `UInt16` | 2 | 0 to 65535 | Little-endian |
| `UInt32` | 4 | 0 to 2^32-1 | Little-endian |
| `UInt64` | 8 | 0 to 2^64-1 | Little-endian |
| `UInt128` | 16 | 0 to 2^128-1 | Little-endian |
| `UInt256` | 32 | 0 to 2^256-1 | Little-endian |

### Floating point types (GA)

| Type | Bytes | Description | Native Protocol |
|------|-------|-------------|-----------------|
| `Float32` | 4 | IEEE 754 single precision | IEEE 754 binary |
| `Float64` | 8 | IEEE 754 double precision | IEEE 754 binary |
| `BFloat16` | 2 | Brain floating point (ML) | IEEE 754 variant |

### Decimal types (GA)

| Type | Precision | Scale | Underlying |
|------|-----------|-------|------------|
| `Decimal(P, S)` | 1-76 | 0-P | Auto-selected |
| `Decimal32(S)` | 1-9 | 0-9 | Int32 |
| `Decimal64(S)` | 10-18 | 0-18 | Int64 |
| `Decimal128(S)` | 19-38 | 0-38 | Int128 |
| `Decimal256(S)` | 39-76 | 0-76 | Int256 |

### Boolean type (GA)

| Type | Underlying | Values |
|------|------------|--------|
| `Bool` | UInt8 | 0=false, 1=true |

### String types (GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `String` | Variable-length, any bytes | (len, value) varint + bytes |
| `FixedString(N)` | Fixed N bytes, zero-padded | N bytes exactly |

### Date and time types (GA)

| Type | Range | Resolution | Underlying |
|------|-------|------------|------------|
| `Date` | 1970-01-01 to 2149-06-06 | 1 day | UInt16 (days since epoch) |
| `Date32` | 1900-01-01 to 2299-12-31 | 1 day | Int32 (days since epoch) |
| `DateTime` | 1970-01-01 to 2106-02-07 | 1 second | UInt32 (seconds since epoch) |
| `DateTime(tz)` | Same + timezone | 1 second | UInt32 + timezone metadata |
| `DateTime64(P)` | Wide range | 10^-P seconds | Int64 (scaled) |
| `DateTime64(P, tz)` | Same + timezone | 10^-P seconds | Int64 + timezone metadata |
| `Time` | 00:00:00 to 23:59:59 | 1 second | UInt32 |
| `Time64(P)` | High precision time | 10^-P seconds | Int64 |

**DateTime64 precision:**

- P=0: seconds
- P=3: milliseconds
- P=6: microseconds
- P=9: nanoseconds

### UUID type (GA)

| Type | Bytes | Format |
|------|-------|--------|
| `UUID` | 16 | RFC 4122, stored as FixedString(16) |

### Network types (GA)

| Type | Bytes | Format |
|------|-------|--------|
| `IPv4` | 4 | UInt32 alias, big-endian |
| `IPv6` | 16 | FixedString(16), network byte order |

### Enum types (GA)

| Type | Underlying | Max Values |
|------|------------|------------|
| `Enum8('a'=1, 'b'=2, ...)` | Int8 | 256 |
| `Enum16('a'=1, 'b'=2, ...)` | Int16 | 65536 |

### Container types (GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `Array(T)` | Variable-length array | Offsets (UInt64[]) + Data (T[]) |
| `Tuple(T1, T2, ...)` | Fixed heterogeneous tuple | Concatenated columns |
| `Map(K, V)` | Key-value pairs | Offsets + Keys (K[]) + Values (V[]) |
| `Nested(name1 T1, ...)` | Nested structure | Flattened to arrays |

### Nullable wrapper (GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `Nullable(T)` | T or NULL | Nulls (UInt8[]) + Values (T[]) |

### LowCardinality wrapper (GA)

| Type | Description | Native Protocol |
|------|-------------|-----------------|
| `LowCardinality(T)` | Dictionary encoding | Index + Keys columns |

Supported inner types: String, FixedString, Date, DateTime, numbers

### Geo types (GA)

| Type | Underlying | Description |
|------|------------|-------------|
| `Point` | Tuple(Float64, Float64) | (x, y) coordinate: longitude, latitude |
| `Ring` | Array(Point) | Closed polygon ring |
| `LineString` | Array(Point) | Open line |
| `Polygon` | Array(Ring) | Polygon with holes |
| `MultiLineString` | Array(LineString) | Multiple lines |
| `MultiPolygon` | Array(Polygon) | Multiple polygons |

---

## Semi-structured types (new -- 2024/2025)

### Variant type (GA as of late 2024)

```sql
Variant(T1, T2, T3, ...)
```

**Description:** Discriminated union of types. Each value is exactly one of the
specified types.

**Example:**

```sql
Variant(String, UInt64, Array(String))
```

**Native protocol:**

- Discriminator column (UInt8) indicating which type
- Separate column for each type variant
- Only the active variant has data for each row

### Dynamic type (GA as of late 2024)

```sql
Dynamic
Dynamic(max_types=N)
```

**Description:** Can store ANY type without pre-specification. Like Variant but
types discovered at runtime.

**Features:**

- No need to declare types upfront
- `max_types` limits separate storage columns (default 32)
- Types beyond limit stored as String

**Native protocol:**

- Type descriptor column
- Multiple data columns
- Overflow column for excess types

### JSON type (GA as of late 2024)

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

**Native protocol:**

- Serialised as dynamic column structure
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

## Aggregate function types (GA)

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

## Deprecated/legacy types

| Type | Status | Replacement |
|------|--------|-------------|
| `Object('json')` | Deprecated | Use `JSON` type |

---

## How dfe-loader writes these types

dfe-loader inserts through a HyperI fork of `clickhouse-rs` (the `hyperi-port/*`
chain), pinned via `[patch.crates-io]` to an immutable git tag (never a branch).
The fork keeps insertion typed -- compile-time `T: Row` schemas -- which is the
right upstream default. The loader's rows are not typed at compile time: their
columns come from `system.columns` at runtime, so the dynamic encoding layer
lives in this repo at `src/clickhouse_ext/`, on top of the fork's stable public
surface. See [CLICKHOUSE-EXT.md](CLICKHOUSE-EXT.md) for that seam.

The loader writes in one of two formats, dispatched by `clickhouse.insert_format`:

| Format | Path | Purpose | Default |
|--------|------|---------|---------|
| RowBinary | `clickhouse_ext::DynamicInsert` over `Client` | Schema-reflected typed binary encoding | Yes |
| JSONEachRow | `Client::insert_formatted_with` (FORMAT JSONEachRow) | Fallback, server-side type coercion | No |

`DynamicInsert` reflects the target schema from `system.columns`, encodes a
`Map<String, Value>` against that schema column-by-column, and sets
`input_format_binary_read_json_as_string=1` for JSON-typed columns. The loader
never calls `clickhouse::Row` -- everything is dynamic. DDL and schema queries go
through the same fork via `ClickHouseQueryClient`. The full transport x format
matrix (HTTP vs native/TCP) is in [INSERT-FORMATS.md](INSERT-FORMATS.md).

### Type coverage beyond upstream

| Type | Status |
|------|--------|
| All standard types | Upstream |
| JSON (GA v25.3) | Fork |
| Variant | Fork |
| Dynamic | Fork |
| Nested | Fork |
| BFloat16 | Fork |
| Time/Time64 | Fork |
| AggregateFunction | Fork |
| SimpleAggregateFunction | Fork |
| Native TCP protocol | Fork (upstream removed in v0.12+) |

**Historical note:** clickhouse-arrow (a DFE fork) was trialled in early
development and dropped -- RowBinary insert throughput matched Arrow within noise
(the path is network-dominated) while avoiding the complexity of building Arrow
RecordBatches.

---

## Native protocol serialisation notes

### Numeric types

All numeric types use **little-endian** byte order, matching x86/x64 memory
layout for zero-copy operations.

### String/FixedString

- String: Varint length prefix + raw bytes
- FixedString(N): Exactly N bytes, zero-padded

### Arrays

```text
Offsets: [UInt64 offsets array]
Data: [T elements array]
```

Offset[i] = end index of array[i] in data

### Nullable

```text
Nulls: [UInt8 mask, 1=null]
Values: [T values, null positions have default]
```

### LowCardinality

```text
Index type + Keys + Dictionary
```

### DateTime with timezone

Timezone stored in column metadata, not per-value. All values in a column share
the same timezone.

### JSON columns on the wire

A JSON-typed column (including `_json`) is written as a length-prefixed string.
On the RowBinary path the loader sets `input_format_binary_read_json_as_string=1`
on the insert so the server reads that string back as a JSON value rather than a
literal `String`. The setting is applied automatically whenever the resolved
schema contains a JSON column -- no configuration needed. See
[INSERT-FORMATS.md](INSERT-FORMATS.md).

---

## Encoder implementation status

**RowBinary (default):** encoded by `clickhouse_ext::DynamicInsert` from the reflected schema. It covers the integer, float, decimal, bool, string, date and time, UUID, IP, enum, `Array`, `Map`, JSON and geo types, inside `Nullable` and `LowCardinality`. `Decimal(P, S)` resolves to the concrete Decimal32/64/128/256 at encode time based on precision. A value bound for a type the encoder has no arm for (`Tuple`, `Variant`, `Dynamic`, `Nested`, ...) is refused with `UnsupportedType`, which holds the batch for retry rather than writing wrong bytes.

**JSONEachRow (fallback):** the server parses each type itself. Before it does, the loader shapes the values it would refuse by the same rules as RowBinary: a JSON column's non-object, every geo_point form (sent as `[lon, lat]`), and a single value bound for an `Array`. Set `insert_format = "json_each_row"` to opt in (HTTP transport only -- see the config guard in [INSERT-FORMATS.md](INSERT-FORMATS.md)).

### Geo points

A `Point` is written as two little-endian `Float64`, x then y, where x is the longitude and y the latitude. The value can arrive in any form Elasticsearch takes for a `geo_point`:

| Form | Example | Coordinate order |
|------|---------|------------------|
| Object | `{"lat": 41.12, "lon": -71.34}` | named |
| GeoJSON | `{"type": "Point", "coordinates": [-71.34, 41.12]}` | lon, lat |
| String | `"41.12,-71.34"` | lat, lon |
| Array | `[-71.34, 41.12]` | lon, lat |
| WKT | `"POINT (-71.34 41.12)"` | lon, lat |

A third coordinate is read and ignored. A geohash, a latitude outside -90 to 90, a longitude outside -180 to 180, or a value no form reads is an encoding error for that row alone: salvage isolates it and it goes to the DLQ with the reason. An absent `Point` is `(0, 0)`, the type's own default. ClickHouse refuses `Nullable(Point)`, so there is no null to write instead.

`Ring`, `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` are arrays of points on the wire, written from nested arrays whose innermost elements take any form above. An absent one is the empty array.

### A single value bound for an Array

Elasticsearch lets any field carry one value or many, so a scalar bound for an `Array(T)` column is written as a one-element array, and T's own rules then decide whether it is valid. `"related_ip": "52.108.0.3"` lands in an `Array(IPv6)` column as `['::ffff:52.108.0.3']`. An object is wrapped only where the element is read from one (`JSON`, `Map`, `Point`); bound for an array of scalars it stays an encoding error. A null or absent array is the empty array. For an `Array(Point)` column, a bare `[lon, lat]` is one point, as Elasticsearch reads it.

The rationale for RowBinary as the default: schema-reflected RowBinary skips the
server-side JSON parse, cuts ClickHouse CPU, and gives full type support without
requiring compile-time schemas. The `Map<String, Value>` model is preserved
end-to-end -- encoding happens against `system.columns` at runtime.
