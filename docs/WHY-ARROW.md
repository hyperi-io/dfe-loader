# Insert Format Decision: Arrow → JSONEachRow

**Status:** SUPERSEDED — Arrow dropped in favour of JSONEachRow (2026-03-09)
**Original Date:** 2026-02-06
**Decision Date:** 2026-03-09
**Scope:** Documents the full lifecycle of the insert-format decision:
(1) Arrow chosen, (2) Arrow built, (3) Arrow benchmarked, (4) Arrow dropped.

---

## Current Architecture (2026-03-09)

```text
Kafka → Parse (SIMD) → Route → Transform → Vec<Map<String, Value>> → JSONEachRow → reqwest HTTP POST → ClickHouse
```

- **Client:** `reqwest` HTTP POST with NDJSON body
- **Format:** JSONEachRow (one JSON object per line)
- **DDL/queries:** `clickhouse` crate (official ClickHouse Inc, HTTP)
- **Why:** Simpler, schema-flexible, sufficient throughput for our scale

---

## Original Decision: Arrow via clickhouse-arrow (2026-02-06)

The original decision chose Arrow + clickhouse-arrow (native TCP) as the insert path.
That reasoning is preserved below as historical context.

### Why Arrow was chosen

1. **CPU efficiency:** Native TCP at 5.5% CPU vs 17% for JSONEachRow (209% difference)
2. **Columnar alignment:** Arrow's memory layout matches MergeTree columnar storage
3. **Zero-copy primitives:** Primitives serialised via `bytemuck::cast_slice`, no copy
4. **Type coverage:** Fork handled 48 types including Variant, Dynamic, Nested, BFloat16

### Why Arrow was dropped (2026-03-09)

After building the full Arrow pipeline, the approach was reversed in favour of
JSONEachRow for the following reasons:

1. **Dynamic schema problem.** Our events are schemaless — field names vary per event.
   Arrow requires a fixed schema per RecordBatch. ArrowBatchBuilder had to infer schema
   from each batch, or use a schema fetched from ClickHouse `system.columns`. This added
   significant complexity that the JSON approach avoids entirely.

2. **JSONEachRow handles unknown columns.** ClickHouse silently ignores unknown fields
   in JSONEachRow by default — the correct behaviour for a schemaless loader. Arrow's
   strict schema enforcement required either querying the schema on every flush or
   dropping unknown columns.

3. **Mison was dropped.** The planned zero-copy pipeline (JSON → Mison structural index
   → Arrow RecordBatch) was benchmarked and found to have no meaningful advantage over
   sonic-rs → serde_json::Value → JSONEachRow. The intermediate Arrow step was the
   bottleneck, not the JSON parsing.

4. **clickhouse-arrow fork maintenance.** 863 lines of custom DFE-specific code for
   Variant/Dynamic/Nested types. With the official `clickhouse` crate handling DDL and
   queries, the fork was pure maintenance burden with no benefit at current scale.

5. **Throughput is sufficient.** At our k8s cluster scale, JSONEachRow via HTTP is not
   the throughput bottleneck. Kafka partition consumption and ClickHouse part merging
   dominate. The 209% CPU difference in server-side insert cost is real but irrelevant
   when we are not insert-CPU-bound.

6. **Simpler code.** The JSONEachRow path is ~400 lines vs the Arrow path's ~1,200 lines
   in the inserter + buffer + client layers. Fewer moving parts, easier to reason about.

---

## Benchmark Results

See `benches/insert_bakeoff.rs` for the bakeoff comparing:

- `simd_jsoneachrow_http` — `reqwest` HTTP POST with JSONEachRow (production path)
- `simd_rowbinary_http` — `clickhouse` crate RowBinary (reference)

Run with: `cargo bench --bench insert_bakeoff`

---

## Current Library Stack

| Purpose | Crate | Protocol |
|---------|-------|----------|
| DDL, schema queries | `clickhouse` v0.14+ | HTTP, RowBinary |
| Data inserts | `reqwest` v0.12+ | HTTP, JSONEachRow |

---

## Historical Format Comparison (Server-Side CPU Cost)

From [ClickHouse Input Format Benchmarks](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient):

| Format | CPU (p99/insert) | vs Native |
|--------|-----------------|-----------|
| Native + LZ4 | 5.5% | baseline |
| RowBinary (HTTP) | ~9% | +64% |
| JSONEachRow (HTTP) | 17% | +209% |

These numbers are real but context-dependent. At our scale, the server is not
insert-CPU-bound. The schema flexibility of JSONEachRow avoids schema-inference
overhead that more than compensates for the higher insert CPU cost.

---

## Phase 5.5: Revisiting Native Protocol

The native protocol is still on the roadmap via `/projects/clickhouse-rs` — a fork
of the official `clickhouse` crate that adds:

- Native TCP protocol option (upstream is HTTP-only as of v0.12+)
- Full type support: JSON, Variant, Dynamic, Nested, BFloat16

This would give us the CPU efficiency of native protocol WITH the schema flexibility
we need. See `TODO.md` Phase 5.5 for the implementation plan.

The move to JSONEachRow is not a permanent architectural limit — it is the pragmatic
path that ships sooner and is correct for current scale.

---

**Last Updated:** 2026-03-09
