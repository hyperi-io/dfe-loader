<!--
  Project:      dfe-loader
  File:         docs/performance/history.md
  Purpose:      Decision and parser-selection history (the rationale trail)
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Decision and parser-selection history

This is the rationale trail behind the loader's hot-path choices -- which JSON
parser, which insert encoding, and why. Each entry records a targeted bake-off against the DFE use-case, not a general benchmark.

For the current insert path and the dynamic encoder, see
[../clickhouse/insert-formats.md](../clickhouse/insert-formats.md) and
[../clickhouse/clickhouse-ext.md](../clickhouse/clickhouse-ext.md). This doc is
the "why we landed here", not the "how it works".

## Why RowBinary is the default

RowBinary ships pre-typed bytes. The server parses no row text and infers no types, reading each value off the wire in the order the INSERT names the columns. Only a JSON column's value travels as text for the server to parse. For a high-throughput loader that is the difference between ClickHouse spending CPU on ingest formatting and spending it on merges.

The dynamic encoder turns one `Map<String, Value>` plus its reflected schema into RowBinary bytes client-side. A defaulted column the row leaves out takes its server-side default, and a field with no column is dropped.

JSONEachRow is retained as a fallback, not a throughput choice. It is
self-describing and forgiving -- the server coerces types and tolerates schema
drift -- which makes it the right tool for diagnostics ("does the row land at
all if the server does the typing?") and for the rare table with types the
dynamic encoder does not yet cover.

### clickhouse-arrow -- trialled, dropped

An early trial used `clickhouse-arrow` (Arrow IPC columnar inserts) as a
candidate fast path. It was dropped: RowBinary matched Arrow throughput for this
workload because insert time from flush to answer is dominated by the network, not the encode,
so the columnar advantage did not show up at the pipeline level.
RowBinary kept the encoder a single row-wise/per-column path off the same
reflected schema, with no extra Arrow dependency or schema-mapping layer.

## JSON parser selection

All JSON parser decisions are recorded here as permanent rationale -- each
choice was the result of a targeted bake-off against the DFE hot path.

### sonic-rs -- retained

sonic-rs is the production JSON parser for all DFE hot-path operations:

- `from_slice::<Value>` -- full DOM parse for routing and schema-guided
  extraction.
- `get_from_slice` -- zero-copy lazy field access for routing-only paths.

Bench: `benches/bakeoff.rs`.

### Mison -- evaluated, rejected (2025-Q4)

Mison gave a 4-7% throughput improvement over sonic-rs in targeted benchmarks, not enough to justify maintaining a separate codebase: it needed a fork with custom Rust bindings. All mison code was deleted in commit `2a7a635`. The bench lived in a separate bake-off repo, not kept, and these results are its only record.

### simd-json (0.17) -- evaluated, rejected (2026-03-11)

**Bench:** a `simdjson_spike` bench, not retained -- flat30 and nested
payloads, 15/30 schema columns, batch sizes 100/1K/10K.

**Results (batch=10K).** Extraction:

| Approach | flat/30col | flat/15col | nested/15col | Notes |
|---|---|---|---|---|
| `sonic_selective` -- `get_from_slice` x N | 196 ms | 59.7 ms | 179 ms | Was original impl |
| `sonic_dom` -- `from_slice` x 1 + `.get()` x N | **71.7 ms** | **58.1 ms** | **42.8 ms** | **Current impl** |
| `simd_dom+clone` -- simd-json + mandatory clone | 48.2 ms | 41.8 ms | 37.6 ms | Needs a `Vec<u8>` clone |

Routing (full DOM parse, batch=10K):

| Approach | flat30 | nested |
|---|---|---|
| `sonic_full_parse` | 50.1 ms | 37.9 ms |
| `simd_full_parse+clone` | 38.5 ms | 37.1 ms |

Key observations:

- At 30 schema columns `sonic_dom` is **2.7x faster** than `sonic_selective`.
- At 15 schema columns with a nested payload, **4.2x faster**
  (`get_from_slice` scans the whole doc per miss).
- At 15 schema columns with a flat payload, tied (59.7 ms vs
  58.1 ms).
- Routing: simd-json 23% faster for flat30, ~2% for nested (within noise).

**Rejection reasons:**

1. A mandatory clone. simd-json needs `&mut [u8]` (in-place string
   unescaping), and the DFE pipeline holds payloads as `Arc<[u8]>` for the
   `_json` splice, so every parse would need `raw.to_vec()`, a full memcpy per
   message.
2. A net gain below the bar. Against `sonic_dom` (single full parse,
   same dependency), simd-json was ~28-33% faster for flat extraction and
   ~12% for nested. Extraction was <10% of total pipeline time (dominated by
   40-75 ms network I/O per batch), so the net pipeline improvement was ~2-3%,
   below the 5% mison rejection threshold.
3. No lazy field access. sonic-rs `get_from_slice` navigates to a field
   without building any DOM, and simd-json always builds a full tape first.

**Action taken:** `HeaderExtractor` switched from `get_from_slice` x N to
`sonic_rs::from_slice` + O(1) hash lookups -- a 2.7-4.2x extraction speedup
(payload/schema dependent), no added dependencies (commit `7d17a8d`).

## Durable design decisions

The choices below predate the current module layout but remain the reasons the
hot path is built the way it is.

Each destination `db.table` has a separate row buffer. An INSERT goes to one table, so a batch holds one table's rows. High-volume tables flush more often, and low-volume tables wait for the age trigger. A table's failed batch holds back the commit of only the partitions its rows came from. Each row is a `Map<String, Value>`, so the buffer layer needs no fixed schema, and rows accumulate only until a flush threshold (rows, bytes or age).

`_json` is never inserted into the promoted `Map`. The payload bytes are copied once into a reference-counted `Arc<[u8]>` and spliced as `_json` at serialise time, never re-serialised. That is why the extractor promotes only the schema's columns instead of flattening the whole payload.

A payload must open a JSON object or array (`{` or `[` after any leading whitespace). Anything else, MessagePack included, is dead-lettered as not JSON before a parser reads it.

With RowBinary the server takes the bytes as typed, so the encoder in `dfe-clickhouse`'s `dynamic` module converts each value to its column's type itself: an epoch read by magnitude at the column's precision, zoned text to UTC, hyphen-less UUID hex, an integer IPv4, the geo forms and a single value bound for an `Array`. A value it cannot convert fails that row, which salvage dead-letters.
