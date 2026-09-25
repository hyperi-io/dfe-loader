<!--
  Project:      dfe-loader
  File:         docs/performance/HISTORY.md
  Purpose:      Decision and parser-selection history (the rationale trail)
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Decision and parser-selection history

This is the rationale trail behind the loader's hot-path choices -- which JSON
parser, which insert encoding, and why. Each entry is a permanent record of a
targeted bake-off against the specific DFE use-case, not a general benchmark.
Keep it when you re-evaluate: the bake-offs cost real time, and the conclusions
are still load-bearing.

For the current insert path and the dynamic encoder seam, see
[../clickhouse/INSERT-FORMATS.md](../clickhouse/INSERT-FORMATS.md) and
[../clickhouse/CLICKHOUSE-EXT.md](../clickhouse/CLICKHOUSE-EXT.md). This doc is
the "why we landed here", not the "how it works now".

## Why RowBinary is the default

RowBinary ships pre-typed bytes. The server does no JSON parse and no type
inference -- it reads column values straight off the wire in `system.columns`
order. For a high-throughput loader that is the difference between ClickHouse
spending CPU on ingest formatting and spending it on merges.

The dynamic encoder turns one `Map<String, Value>` plus its reflected schema
into RowBinary bytes client-side. ClickHouse receives pre-typed binary and skips
parsing entirely. Missing columns take their server-side default; extra columns
are dropped.

JSONEachRow is retained as a fallback, not a throughput choice. It is
self-describing and forgiving -- the server coerces types and tolerates shape
drift -- which makes it the right tool for diagnostics ("does the row land at
all if the server does the typing?") and for the rare table whose types the
dynamic encoder does not yet cover.

### clickhouse-arrow -- trialled, dropped

An early trial used `clickhouse-arrow` (Arrow IPC columnar inserts) as a
candidate fast path. It was dropped: RowBinary matched Arrow throughput for this
workload because end-to-end insert time is network-dominated, not encode-
dominated, so the columnar advantage did not show up at the pipeline level.
RowBinary also kept the encoder a single row-wise/per-column path off the same
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

- **Result:** 4-7% throughput improvement over sonic-rs in targeted
  benchmarks.
- **Rejection reason:** the improvement was insufficient to justify maintaining
  a separate codebase (mison required a fork with custom Rust bindings). The
  cost/benefit was negative. All mison code was deleted in commit
  `2a7a635`.
- **Bench:** separate bake-off repo (not retained -- results documented here
  only).

### simd-json (0.17) -- evaluated, rejected (2026-03-11)

**Bench:** a `simdjson_spike` bench, not retained -- flat30 and nested
payloads, 15/30 schema columns, batch sizes 100/1K/10K.

**Results (batch=10K, measured).** Extraction:

| Approach | flat/30col | flat/15col | nested/15col | Notes |
|---|---|---|---|---|
| `sonic_selective` -- `get_from_slice` x N | 196 ms | 59.7 ms | 179 ms | Was original impl |
| `sonic_dom` -- `from_slice` x 1 + `.get()` x N | **71.7 ms** | **58.1 ms** | **42.8 ms** | **Current impl** |
| `simd_dom+clone` -- simd-json + mandatory clone | 48.2 ms | 41.8 ms | 37.6 ms | Requires `Vec<u8>` clone |

Routing (full DOM parse, batch=10K):

| Approach | flat30 | nested |
|---|---|---|
| `sonic_full_parse` | 50.1 ms | 37.9 ms |
| `simd_full_parse+clone` | 38.5 ms | 37.1 ms |

Key observations:

- At 30 schema columns `sonic_dom` is **2.7x faster** than `sonic_selective`.
- At 15 schema columns with a nested payload, **4.2x faster**
  (`get_from_slice` scans the whole doc per miss).
- At 15 schema columns with a flat payload, essentially tied (59.7 ms vs
  58.1 ms).
- Routing: simd-json 23% faster for flat30, ~2% for nested (within noise).

**Rejection reasons:**

1. **Mandatory clone.** simd-json requires `&mut [u8]` (in-place string
   unescaping). The DFE pipeline holds payloads as `Arc<[u8]>` for zero-copy
   `_json` splice. Every parse would need `raw.to_vec()` -- a full memcpy per
   message. This is architecturally incompatible.
2. **Net gain too small.** After implementing `sonic_dom` (single full parse,
   same dependency), simd-json is only ~28-33% faster for flat extraction and
   ~12% for nested. Extraction is <10% of total pipeline time (dominated by
   40-75 ms network I/O per batch). Net pipeline improvement: ~2-3% -- below
   the 5% mison rejection threshold.
3. **No lazy field access.** sonic-rs `get_from_slice` navigates to a field
   without building any DOM. simd-json has no equivalent -- it always builds a
   full tape first.

**Action taken:** `HeaderExtractor` switched from `get_from_slice` x N to
`sonic_rs::from_slice` + O(1) hash lookups -- a 2.7-4.2x extraction speedup
(payload/schema dependent), zero new dependencies (commit `7d17a8d`).

## Durable design decisions

The choices below predate the current module layout but remain the reasons the
hot path is shaped the way it is.

### Per-table buffering

Each destination `db.table` has its own row buffer, because:

- **ClickHouse target.** Inserts are per-table; all rows in a batch go to one
  table.
- **Independent flush.** High-volume tables flush more often; low-volume tables
  wait for the age trigger. One table's failure does not block another's
  offset commit.
- **Schema flexibility.** Each row is a `Map<String, Value>` -- no fixed schema
  required at the buffer layer.
- **Bounded memory.** Rows accumulate until a flush threshold (rows, bytes, or
  age).

### Zero-copy `_json`

`_json` is never inserted into the promoted `Map`. The full payload is held once
as a reference-counted `Arc<[u8]>` and spliced as `_json` only at serialise
time -- no heap allocation until the final insert body is assembled. This is why
the extractor promotes only schema-matched columns (~10-30 entries) instead of
flattening the whole payload (200+ fields).

### Single-byte format auto-detect

JSON object/array starts (`{`, `[`) are always `< 0x80`; MsgPack map/array
starts are always `>= 0x80`. The ranges are mutually exclusive for top-level
structured event data, so format detection is one byte read and one branch --
O(1), sub-nanosecond. JSON and MsgPack can be mixed on the same topic; after
detection the `Arc` bytes are always valid JSON. (Edge case: bare MsgPack
primitives at top level would misdetect as JSON, but event pipelines do not
emit those.)

### Delta coercion only

Coercion runs only on the promoted columns and only for the four cases the wire
format cannot fix server-side. Everything else (String<->numeric, 1/0->Bool,
null->DEFAULT) is left to ClickHouse:

| Delta coercion | Why needed |
|---|---|
| Epoch ms/us/ns -> DateTime64 ISO string | CH takes the integer at face value at column precision |
| ISO 8601 `T` separator -> space | Default `basic` parser rejects `T` |
| UUID without hyphens -> RFC 4122 | CH UUID rejects bare hex |
| IPv4 integer -> dotted-decimal | CH IPv4 rejects integers |

See `src/transform/coerce.rs` (`CoercionMode::Delta`).

## Future optimisations

- **SIMD GeoIP** -- vectorised IP lookups across a batch.
- **Workspace crate extraction** -- split `crates/buffer` and friends out of
  the binary crate.
