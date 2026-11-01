<!--
  Project:      dfe-loader
  File:         docs/clickhouse/clickhouse-ext.md
  Purpose:      The dynamic RowBinary insert the loader takes from dfe-clickhouse
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# The dynamic insert

The official `clickhouse` crate keeps insertion typed (`T: Row`). dfe-loader inserts runtime-shaped `Map<String,Value>` rows whose columns come from `system.columns`, so it takes the dynamic layer from `dfe-clickhouse`'s `dynamic` module (feature `dynamic`). The loader holds no encoder of its own.

## What the crate does

| Piece | Does |
|-------|------|
| `ParsedType` | Parses a `system.columns` type string (Nullable, LowCardinality, Array, Map, DateTime64, Decimal, Enum, the geo shapes, SimpleAggregateFunction) |
| `DynamicRow` | Encodes one `Map<String,Value>` against its `ColumnDef`s to RowBinary |
| `DynamicSchema`, `DynamicSchemaCache` | A table's columns, cached with a TTL and capped at 10,000 tables |
| `fetch_dynamic_schema`, `fetch_dynamic_schema_tcp` | Read a schema over HTTP or the native protocol |
| `DynamicInsert` | `write_map` / `write_map_with_raw`, then `end` |
| `shape_for_type` | The same value rules applied to a row map, for the JSONEachRow path |

Over HTTP an insert is one `INSERT ... FORMAT RowBinaryWithDefaults`: a row that omits a defaulted column takes the server's default, so one INSERT carries rows of any shape, and rows go out in 1 MiB chunks as they are written. Over the native protocol rows go out as `FORMAT Native` blocks, and a row that supplies a different set of defaulted columns starts a new INSERT. JSON columns are written as text with `input_format_binary_read_json_as_string=1`.

The coercions a source value gets on the way: a zoned timestamp lands at the instant it names, a numeric DateTime64 epoch is read by magnitude and rescaled to the column's precision, an IPv4 literal lands in an IPv6 column as `::ffff:a.b.c.d`, a JSON column wraps a root array under `list` and a scalar or non-JSON text under `value`, a `Point` takes every Elasticsearch geo_point form inside its coordinate ranges, and one value lands in an `Array` as a one-element array. A value the column cannot hold (an integer past its width, a decimal past its backing, an undeclared enum value) fails the row rather than being written wrong. `tests/integration/insert_coercions.rs` runs each case through the loader's `Inserter` on every protocol the shared ClickHouse offers.

## How the loader drives it

`Inserter` reads the schema through its own widening read bound and the shared cache, opens the insert with `InsertTimeouts` from `clickhouse.request_timeout_ms` and `clickhouse.insert_timeout_ms`, and sets `SchemaRefresh` from `schema.refresh_on_error`. A failure is read in `Inserter::dynamic_verdict`:

- A timeout or a dropped connection returns the batch to be held.
- A server refusal is read by its code. A retryable code holds the batch. Any other code is the rows' own only once a fresh read shows the table unchanged since they were encoded (`DynamicSchema::same_columns`), and then salvage dead-letters them row by row. Otherwise the table changed under them, and the retry encodes against the current schema.
- A stale, missing or unreadable schema drops the cached schemas and retries.
- An encoder refusal (`EncodingError` naming its column) is the payload's, and goes to salvage.
- A column type the crate cannot encode is a loader gap, so the rows are held.

Both client crates come from crates.io, with no `[patch]` and no git source:

```toml
clickhouse = { version = ">=0.15.3, <0.16", features = ["lz4", "time", "rustls-tls", "rustls-tls-native-roots", "inserter"] }
dfe-clickhouse = { version = ">=0.1.9, <0.2", default-features = false, features = ["ext", "tls", "dynamic"] }
```

From `dfe-clickhouse` the loader also takes `ServerException::parse` (`ext`), which reads the code and message out of the `Code: N. DB::Exception: ...` text the HTTP client returns as `Error::BadResponse`, and `TcpClient` with its rustls trust (`tls`, which implies `tcp`) for `clickhouse.protocol: native`. `src/clickhouse/error.rs` classifies every insert rejection by the server's code and keeps at most 1 MiB of any error text.

The HTTP client is the loader's own. `build_client` in `src/clickhouse/client_http.rs` hands `clickhouse::Client` a hyper client through `Client::with_http_client`, carrying the pool timer that closes idle connections after 2 s, the connect timeout and HTTPS verified against the system trust store.
