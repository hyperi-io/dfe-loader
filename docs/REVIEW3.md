# Architectural Review 3: Zero-Copy Pipeline & CPU Efficiency

**Date:** 2026-02-06
**Status:** Planning (no code changes)
**Scope:** End-to-end pipeline optimization from Kafka receive to ClickHouse insert
**Priority:** CPU efficiency > latency > memory

---

## 1. Problem Statement

dfe-loader ingests JSON (or MsgPack) messages from Kafka, routes them to per-table Arrow buffers, transforms them, and inserts batches of ~10K rows into ClickHouse via native protocol.

**CPU is the scaling bottleneck.** In a k8s pod, every unnecessary copy, allocation, and serde step consumes CPU cycles that could be processing more messages. Memory must be capped to avoid OOMs.

The core question: **how close can we get to a single-copy pipeline from Kafka wire bytes to ClickHouse native blocks?**

---

## 2. Current Pipeline: Copy & Allocation Audit

### 2.1 Data Flow (Current)

```
Kafka wire bytes (zero-copy in librdkafka)
    |
    | [COPY #1: to_vec() -- rdkafka lifetime requires owned bytes]
    v
Vec<u8> owned payload
    |
    | [DESER: sonic_rs::from_slice -- full DOM parse]
    v
serde_json::Value::Object(Map<String, Value>)     <-- N heap allocs for keys+values
    |
    | [BORROW: route_value() -- Cow<str> from Value]
    v
RouteResult::Table(String)                         <-- 1 String alloc
    |
    | [CLONE: extract_tags value.clone()]           <-- deep clone if tags present
    | [MOVE+ALLOC: flatten_value_owned()]           <-- new BTreeMap, nested key Strings
    | [ALLOC: timestamp/raw/tags/org_id keys]       <-- ~5 small String allocs per msg
    | [ALLOC+MAP: sanitize_fields()]                <-- new Map if sanitization on
    v
Map<String, Value> (transformed, flattened)
    |
    | [MOVE: push to ArrowBatchBuilder.pending]
    | [COPY #2: extend_from_slice raw payload -> _json sidecar]
    v
ArrowBatchBuilder { pending: Vec<Map>, json_values_buf: Vec<u8> }
    |
    | [BUILD: iterate Maps, copy values into columnar Arrow arrays]
    | [COPY #3: string values -> Arrow StringBuilder buffers]
    | [SER: complex values -> serde_json::to_vec -> Binary columns]
    | [ZERO-COPY: json_values_buf -> Buffer::from_vec -> StringArray]
    v
RecordBatch (columnar Arrow)
    |
    | [SERIALIZE: clickhouse-arrow -> ClickHouse native blocks]
    v
ClickHouse wire format (LZ4 compressed)
```

### 2.2 Per-Message Cost (500-byte JSON, 10 fields, 2 nested levels)

| Metric | Count | Detail |
|--------|-------|--------|
| Raw payload copies | 2 | rdkafka `to_vec()` + `_json` sidecar `extend_from_slice` |
| Full deserializations | 1 | `sonic_rs::from_slice` -> `serde_json::Value` DOM |
| Re-serializations | 1-2 | `serde_json::to_string` per array field during flatten, `serde_json::to_vec` per complex Binary column |
| Heap allocations | 30-50 | BTreeMap nodes, String keys, String values, nested keys |
| BTreeMap allocations | 2-3 | Parse result, flatten result, sanitization result |
| String field copies | 3 | Parse -> flatten move -> Arrow columnarize |
| org_id copies | 4 | Parse -> `to_string()` -> `to_string()` again -> Arrow |

### 2.3 Top Allocation Hotspots

| Rank | Location | Description |
|------|----------|-------------|
| 1 | `orchestrator.rs:440` | Full JSON DOM parse (`sonic_rs::from_slice -> serde_json::Value`) -- every key and string value heap-allocated |
| 2 | `flatten.rs:48` | New `Map<String, Value>` for flattened result -- BTreeMap nodes |
| 3 | `flatten.rs:130,138` | `prefix_buf.clone()` for nested field dot-notation keys |
| 4 | `arrow.rs:756` | String data copied into Arrow columnar buffers |
| 5 | `arrow.rs:120` | `extend_from_slice` of raw payload into `_json` sidecar |
| 6 | `transformer.rs:294` | `value.clone()` -- deep clone of tags subtree |
| 7 | Various | Static field name `.clone()`/`.into()` for map keys (`_timestamp`, `_raw`, `_tags`, `_org_id`) -- ~5 per message |

---

## 3. Research Findings

### 3.1 ClickHouse Native Protocol

**Key benchmark (ClickHouse FastFormats blog):**

| Format | Wall Clock (10k batch) | CPU (p99/insert) | Data Size |
|--------|----------------------|-------------------|-----------|
| **Native + LZ4** | **131s** | **5.5%** | 2.55 GiB |
| ArrowStream | 146s (+10%) | ~7% | 2.93 GiB |
| RowBinary | 161s (+18%) | ~9% | 3.27 GiB |
| TSV | 190s (+31%) | 11% | 4.22 GiB |
| JSONEachRow | 266s (+51%) | 17% (209% higher) | 5.39 GiB |

Native format directly matches MergeTree columnar storage = 1-copy at server side. This is the lowest CPU option for ClickHouse. **Our use of clickhouse-arrow over native protocol is correct.**

**Native protocol wire format for INSERT:**
- Block header: column count (UVarInt), row count (UVarInt)
- Per column: name (String), type (String), data (binary -- type-specific serialization)
- Optional LZ4 block compression
- Column data is already columnar -- matches Arrow's layout conceptually

**Async inserts** (ClickHouse 24.x+):
- Now supported over native protocol (PR #54730, fixed in #71312)
- Server-side batching with configurable flush: size, time, query count
- `wait_for_async_insert=1` recommended for durability
- Deduplication disabled by default for async mode
- **Not recommended for dfe-loader** -- we already batch client-side at 10K rows, which is optimal. Async inserts add server-side buffering overhead and lose deduplication. Our client-side batching gives us control over memory and flush timing.

**JSON type native serialization:**
- PR #70312 (Oct 2024): `output_format_native_write_json_as_string` -- serialize JSON columns as plain String in native format. Simplifies client integration.
- PR #80499 (Jun 2025): Flattened serialization for Dynamic/JSON in native format -- no shared variant structures needed.
- **For dfe-loader**: We send `_json` as a String column containing JSON text. ClickHouse's JSON type ingests the String and decomposes into sub-columns server-side. This is correct -- the server handles the Variant/Dynamic decomposition.

### 3.2 JSON Parsing: sonic-rs vs Alternatives

| Library | Version | SIMD | Partial Parse | vs serde_json |
|---------|---------|------|---------------|---------------|
| **sonic-rs** | 0.5.5 | AVX2/SSE4.2/NEON | `get_from_slice()` | 2-3x faster |
| simd-json | 0.17.0 | AVX2/SSE4.2/NEON | Tape only (full parse first) | 1.5-2x faster |
| jiter | 0.13.0 | None | Pull-based `Jiter` | 8x faster (iter mode, no DOM) |
| arrow-json | 58.x | Partial (string/utf8) | Decoder API | 2.5x faster than serde-json path |

**sonic-rs is 1.3-2x faster than simd-json.** simd-json uses a two-pass tape approach; sonic-rs parses directly without an intermediate tape. simd-json also requires `&mut [u8]` (modifies input in-place), which is an ergonomic problem.

**sonic-rs `get_from_slice()`** uses SIMD to skip JSON containers via bracket counting. Navigates directly to the target field without building a DOM. Returns `LazyValue` borrowing from input bytes -- zero-copy. This is already used in dfe-loader for routing field extraction.

**No generic parser can output Arrow buffers directly.** Neither sonic-rs nor simd-json can target Arrow memory layout. However, there are two purpose-built approaches that can:
1. **arrow-json Decoder**: Tape-based JSON-to-Arrow, schema-driven, maintained by the Arrow project
2. **Mison** (custom, in-tree): Structural index → schema-guided extract → direct Arrow builders (see Section 3.5)

**Conclusion: Keep sonic-rs for routing extraction.** simd-json would be a downgrade. The real optimization is eliminating the `serde_json::Value` DOM, not switching parsers. For the DOM-elimination step, both Mison and arrow-json Decoder are viable -- see Section 3.5 and Section 8 for the comparison.

### 3.3 arrow-json Tape Decoder

The arrow-json `Decoder` (stable since arrow-rs v40+, latest v58.x) parses JSON directly into Arrow columnar format **without going through `serde_json::Value`**:

1. **Tape pass**: JSON bytes parsed into a flat tape (`Vec<TapeElement>`) + raw string/number bytes
2. **Columnar pass**: With known schema, each column decoded in a tight loop

**Performance improvements in recent versions:**
- PR #7157 (arrow 54.3.0): 30% faster deserialization, "Tweets" benchmark 229% speedup
- PR #9091: 1.8x faster hex decoding for binary data
- PR #9086: 1.7x faster field indexing in StructArrayDecoder

**Streaming API** -- `Decoder::decode(&mut self, buf: &[u8])` + `flush()`:
- No framing required -- handles partial JSON documents
- No `BufRead` wrapper needed when data is already in memory
- Batch size control via `with_batch_size()`
- Designed for integration with async IO

**Key insight**: Our current `SimdBatchBuilder` uses `ReaderBuilder` which wraps bytes in `Cursor` + `BufRead`. The `Decoder` API is more efficient when data is already in memory.

### 3.4 Arrow Zero-Copy Construction

**`Buffer::from_vec()` is truly zero-copy** -- takes ownership of `Vec`'s allocation without memcpy. Our `build_json_column()` already uses this correctly for the `_json` sidecar.

**Alignment requirements:**
- `Buffer` itself: no alignment requirement
- `ScalarBuffer<i32>` (for offsets): requires 4-byte alignment -- will **panic** if not aligned
- `MutableBuffer::new()`: guarantees 64-byte (cache-line) alignment
- `Buffer::from_vec(Vec<u8>)`: inherits Vec's 1-byte alignment -- safe for StringArray values, **not** for ScalarBuffer

Our code correctly uses `Vec<i32>` for offsets (4-byte aligned) and `Vec<u8>` for values.

### 3.5 Mison Structural Index (In-Tree Custom Module)

dfe-loader includes a custom SIMD JSON processor based on the Mison paper (VLDB 2017): 3,428 lines in `src/mison/` with full AVX2/SSE4.2/NEON runtime detection.

**Architecture:**

```
Raw JSON bytes
    |
    | [SIMD scan: build_character_bitmaps (AVX2/SSE4.2/NEON)]
    v
StructuralIndex { quotes, colons, commas, braces, brackets -- per nesting level }
    |
    | [SchemaExtractor::extract_all_batch -- single pass through level-0 colons]
    | [FxHashMap<field_name, column_index> lookup per colon position]
    | [PatternTree speculation for repeated message shapes]
    v
Vec<ExtractedValue<'a>> -- borrows from input bytes, zero allocation
    |
    | [MisonArrowBuilder::append_from_extracted -- per-column typed builders]
    v
RecordBatch (columnar Arrow)
```

**Key properties:**

1. **Single SIMD pass**: One scan through input bytes builds all structural bitmaps simultaneously. The same index is reused for routing extraction AND full-field extraction -- no second scan.

2. **Zero DOM allocation**: `ExtractedValue<'a>` borrows from input bytes. Strings are `&'a [u8]` slices (no String allocation). Objects and arrays are `&'a [u8]` slices of the raw JSON.

3. **Schema-guided extraction**: Only extracts fields present in the destination ClickHouse schema. Traditional pipeline extracts ALL fields, flattens, then discards unused. Mison skips fields not in the schema entirely.

4. **Single-pass batch extraction**: `extract_all_batch()` walks level-0 colons once with FxHashMap lookup per position. Complexity is O(colons + fields), NOT O(colons × fields). Early-exits when all target fields found.

5. **Pattern tree speculation**: For repeated message shapes (common in log pipelines), the pattern tree predicts field positions. Verification is O(1) per field. Falls back to full scan on miss.

6. **Direct Arrow builders**: `MisonArrowBuilder` has per-column typed builders (Bool, Int64, Float64, String, Json). `ColumnBuilder::append_value()` converts `ExtractedValue` directly to Arrow arrays. No intermediate `serde_json::Value`.

**Module structure:**

| File | Lines | Purpose |
|------|-------|---------|
| `simd.rs` | 498 | AVX2/SSE4.2/NEON bitmap generation, runtime detection |
| `index.rs` | ~400 | StructuralIndex, LeveledBitmaps, string masking |
| `extract.rs` | 1,219 | SchemaExtractor, FieldExtractor, ExtractedValue<'a>, batch extraction |
| `pattern.rs` | ~300 | PatternTree speculation, TablePatternRegistry |
| `arrow.rs` | 601 | MisonArrowBuilder, MisonBatchProcessor, ColumnBuilder |

**What Mison does NOT handle today:**

- Not integrated into the production pipeline (orchestrator uses sonic-rs DOM path)
- No routing field pre-extraction (would need to extract org_id/db/table from same index)
- No `_json` sidecar / `_raw` injection in the Mison Arrow path
- No `_org_id` / `_timestamp` injection post-extraction
- No nested field flattening (extracts top-level only, nested returned as raw `&[u8]`)

These are integration gaps, not fundamental limitations. The core extraction and Arrow building work.

### 3.6 ClickHouse Rust Client Landscape

| Crate | Protocol | Format | Arrow | Status |
|-------|----------|--------|-------|--------|
| **clickhouse-arrow** | Native TCP | Native blocks | First-class | Active (v0.1.6) -- **our choice** |
| clickhouse-rs (official) | HTTP | RowBinary | None | Active (v0.14.2) -- no native protocol |
| klickhouse | Native TCP | Native blocks | None | Active |

**clickhouse-arrow is the correct choice.** It communicates over native TCP protocol and converts Arrow RecordBatch to ClickHouse native columnar blocks. The conversion is column-at-a-time (not row-by-row), which is efficient.

Arrow IPC cannot be sent over the native protocol. Sending ArrowStream over HTTP is ~10% slower than native format. Native protocol with LZ4 compression is the fastest path.

**Netflix reference**: 5PB/day, switched from JDBC to custom native protocol encoding for CPU savings. Their Rust implementation achieved 1.21M events/sec -- 91% improvement over Go.

---

## 4. Optimization Opportunities

### 4.1 Tier 1: Eliminate serde_json::Value DOM (HIGH IMPACT)

**The single largest optimization.** Currently every message is parsed into a full `serde_json::Value` DOM tree, which allocates N Strings for keys and values, multiple BTreeMap nodes, and then gets thrown away after transformation. This is the #1 CPU consumer.

There are **two candidate approaches** for eliminating the DOM. Both produce Arrow RecordBatches without `serde_json::Value`, but they differ in architecture.

#### Approach A: Mison Pipeline (Custom, In-Tree)

```
Kafka message bytes
    |
    | [SIMD scan #1: StructuralIndex::build -- all structural bitmaps]
    v
StructuralIndex
    |
    |-- [extract routing fields from index -- reuse scan, no re-scan]
    |-- [extend_from_slice] --> _json sidecar buffer                [1 copy]
    |
    | [SchemaExtractor::extract_all_batch -- single pass through colons]
    | [ExtractedValue<'a> borrows from input bytes -- zero allocation]
    v
Vec<ExtractedValue<'a>>
    |
    | [MisonArrowBuilder -- per-column typed builders]
    v
RecordBatch (columnar Arrow)
    |
    | [Post-hoc: inject _org_id, _timestamp, _raw, _json columns]
    v
Final RecordBatch --> clickhouse-arrow --> ClickHouse native blocks
```

**Advantages:**
- **1 SIMD pass** -- structural index built once, reused for both routing and full extraction
- **Zero DOM allocation** -- `ExtractedValue<'a>` borrows from input bytes
- **Schema-guided** -- only extracts fields in destination schema (skips unused fields entirely)
- **Pattern tree speculation** -- O(1) field position prediction for repeated message shapes
- **Already written** -- 3,428 lines of tested code in `src/mison/`

**Gaps (integration work):**
- Not wired into orchestrator (routing, sidecar injection, offset tracking)
- No nested field flattening (top-level only, nested returned as raw `&[u8]`)
- Post-Arrow transforms (`_org_id`, `_timestamp`) not yet implemented

#### Approach B: arrow-json Decoder Pipeline

```
Kafka message bytes
    |
    |-- [sonic_rs::get_from_slice] --> routing fields             [SIMD scan #1]
    |-- [sonic_rs::get_from_slice] --> _raw source field          [SIMD scan #1 cont.]
    |-- [extend_from_slice] --> _json sidecar buffer              [1 copy]
    |
    v  (raw bytes, grouped by table)
Per-table Decoder buffer
    |
    | [arrow_json::Decoder::decode()] --> tape-based parse        [SIMD scan #2]
    v
RecordBatch (columnar Arrow)
    |
    | [Arrow compute] --> _org_id, _timestamp injection, field rename
    v
Final RecordBatch --> clickhouse-arrow --> ClickHouse native blocks
```

**Advantages:**
- **Maintained by Arrow project** -- battle-tested in DataFusion, Arroyo, etc.
- **Streaming API** -- handles partial documents, built-in batch size control
- **Handles nested JSON** -- Decoder can parse nested structures into Arrow StructArrays
- **Less integration work** -- standard Decoder API, well-documented

**Gaps:**
- **2 SIMD passes** -- sonic for routing, then Decoder tape for Arrow build
- Decoder schema must match JSON shape exactly (rigid)
- No schema-guided skip -- Decoder processes all fields in the JSON, filters post-hoc
- New dependency (arrow-json Decoder API, though arrow-json is already in Cargo.toml)

#### Comparison

| Dimension | Mison | arrow-json Decoder |
|-----------|-------|--------------------|
| SIMD passes per message | 1 | 2 |
| DOM allocation | Zero (`ExtractedValue<'a>`) | Zero (tape) |
| Schema-guided skip | Yes (only extracts needed fields) | No (processes all fields) |
| Nested JSON | Top-level only (nested as raw bytes) | Full support |
| Pattern speculation | Yes (O(1) for repeated shapes) | No |
| Integration work needed | Medium (routing, sidecars, transforms) | Medium (schema mapping, transforms) |
| Maintenance burden | Custom code, we own it | Apache Arrow project |
| Production-tested | Not yet (benchmarks only) | Widely deployed |
| Lines of code | 3,428 (existing) | ~200 (wrapper, new) |

**The honest answer: we need benchmarks to decide.** Both approaches eliminate the serde_json::Value DOM -- that's the key win either way. The theoretical advantage of Mison (1 SIMD pass, schema-guided skip) needs to be validated against the Decoder's tape-based approach on real workloads with real message shapes and field counts.

**Estimated impact (either approach):** 40-60% CPU reduction for flat messages.

### 4.2 Tier 2: Pre-Extract Routing Fields (MEDIUM IMPACT)

Before routing bytes to the Arrow build path, we need 2-3 fields extracted (org_id, db, table). Currently we call `sonic_rs::get_from_slice()` separately for each field, scanning the JSON up to 3 times.

**If Mison is chosen (Approach A):** This tier is free. The structural index built in the first SIMD pass already supports multi-field extraction. Routing fields are extracted from the same index as data fields -- zero additional scans.

**If Decoder is chosen (Approach B):** This tier matters. Options:

- **Option B1: Multi-field `get_from_slice()`** -- Extract org_id + table + timestamp in a single sonic-rs call. Would require sonic-rs API extension or wrapper.
- **Option B2: Single-pass jiter extraction** -- Use jiter's `Jiter` pull-based iterator to walk top-level keys once, extracting all routing fields. One pass vs three SIMD scans.
- **Option B3: Use Mison just for routing** -- Build structural index, extract routing fields, then hand off raw bytes to Decoder for Arrow build. Hybrid approach, 2 passes but routing is faster.

**Estimated impact:** 5-15% CPU reduction for the Decoder path. Zero additional cost for the Mison path.

### 4.3 Tier 3: Reduce Per-Message Allocations (MEDIUM IMPACT)

Even with the Decoder path, some allocations remain in the routing/transform layer:

| Fix | Current | Proposed | Savings |
|-----|---------|----------|---------|
| Intern static keys | `"_timestamp".to_string()` every message | `&'static str` or `string_cache` | ~5 allocs/msg |
| Transfer org_id ownership | `to_string()` x2 | `remove()` from Value, pass owned | 2 allocs/msg |
| Use `remove()` for tags | `value.clone()` deep clone | `obj.remove()` before flatten | 1 deep clone/msg |
| DictionaryArray for `_destination` | StringBuilder repeats same string N times | DictionaryArray with 1 entry | N-1 copies/batch |
| CompactString for routing | `String` for "db.table" | `CompactString` (stack <=24 bytes) | 1 alloc/msg |

**Estimated impact:** 10-20% allocation reduction. Less GC pressure, better cache locality.

### 4.4 Tier 4: Decoder Streaming Integration (MEDIUM IMPACT)

Replace the current `ArrowBatchBuilder` accumulation pattern with `arrow_json::Decoder` streaming:

```rust
// Current: accumulate serde_json::Value objects, build manually
let mut builder = ArrowBatchBuilder::new(batch_size);
for msg in batch {
    let value: Value = sonic_rs::from_slice(&msg.payload)?;
    // ... transform ...
    builder.push(value, table, raw_bytes);
}
let batch = builder.build(schema)?;

// Proposed: feed raw bytes to Decoder, get RecordBatch directly
let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
for msg in batch {
    // routing + raw/json extraction via get_from_slice (no DOM)
    decoder.decode(&msg.payload)?;
}
let batch = decoder.flush()?;
// ... post-hoc Arrow compute for transforms ...
```

The Decoder handles partial documents, schema alignment, and columnar construction internally. No `Cursor` or `BufRead` wrapper needed.

**Challenge:** The Decoder expects the JSON shape to match the Arrow schema. Our transform step modifies the data (adds `_org_id`, `_timestamp`, renames fields). These transforms would need to happen either:
1. **Pre-Decoder**: Modify the raw JSON bytes before feeding to Decoder (fragile, error-prone)
2. **Post-Decoder**: Apply transforms as Arrow compute operations on the RecordBatch (cleaner)

Post-Decoder transforms using Arrow compute kernels:
- `_org_id` injection: Build a separate `StringArray` filled with the org_id value, append as column
- `_timestamp` injection: `arrow_cast::cast()` String -> Timestamp, or build from extracted value
- Field rename: Schema-level rename (no data movement)
- `_raw` / `_json`: Build as separate columns from pre-extracted data, append to RecordBatch

### 4.5 Tier 5: Memory Capping (ESSENTIAL for k8s)

Current memory is unbounded per buffer. For k8s pods with fixed memory limits:

| Control | Mechanism | Config |
|---------|-----------|--------|
| Per-buffer byte limit | Track `json_values_buf.len()` + estimated pending Value memory | `buffer.max_memory_bytes` |
| Global memory budget | Sum all buffer memory, force flush when threshold reached | `buffer.global_memory_limit` |
| Decoder batch size | Controls internal Decoder memory | `decoder.batch_size` |
| Concurrent insert limit | Already implemented (semaphore) | `inserter.max_concurrent_inserts` |

**Estimated pending Value memory**: For a `serde_json::Value` with N string fields of average length L: `N * (24 + L) + overhead`. With the Decoder path, memory is bounded by `batch_size * avg_message_size` in the Decoder's internal buffers.

---

## 5. Theoretical Minimum Copy Count

### 5.1 Absolute Minimum (Flat JSON, No Transform)

| Step | Copies | Why |
|------|--------|-----|
| Kafka receive | 1 | rdkafka lifetime requires owned bytes |
| Routing extraction | 0 | `get_from_slice()` borrows from input |
| `_json` sidecar | 0 | Could reference original bytes if lifetime allows |
| JSON -> Arrow (Decoder) | 1 | Tape pass + columnar construction into Arrow buffers |
| Arrow -> ClickHouse native | 1 | Column-at-a-time serialization |
| **Total** | **3** | 1 unavoidable (Kafka), 1 for Arrow, 1 for CH native |

### 5.2 Current Count (Flat JSON)

| Step | Copies | Avoidable? |
|------|--------|-----------|
| Kafka receive | 1 | No |
| sonic_rs DOM parse | 1 (semantic) | **Yes** -- use Decoder |
| `_json` sidecar extend | 1 | Partially -- could use offset+ref |
| Flatten + transform | 0 (moves) | N/A |
| Arrow manual build | 1 | **Yes** -- Decoder does this |
| Arrow -> ClickHouse native | 1 | No |
| **Total** | **5** | 2 avoidable |

### 5.3 Optimized Target

From 5 copies to 3 copies = **40% copy reduction** for the hot path.

---

## 6. ClickHouse JSON Type Considerations

### 6.1 Insert Performance

The JSON type has higher insert CPU cost than plain String:
- Server-side parsing of JSON text into sub-columns
- Type inference per path
- Dynamic/Variant discriminator management
- Sub-column file management (separate files per type per path)

There are no published insert benchmarks for the JSON type specifically, but the architecture implies O(paths * rows) work per batch beyond what a plain String column requires.

### 6.2 Read Performance (Why We Still Want It)

- v25.8: **58x faster reads** for specific path queries vs String + JSONExtract
- v25.8: **3300x less memory** for analytical queries on JSON data
- Sub-column pushdown: only reads accessed paths from disk
- Native vectorized operations on sub-column types

### 6.3 Recommendation

Keep `_json` as JSON type in the DDL. The read-side benefits massively outweigh the insert cost. The insert cost is borne by ClickHouse (server CPU), not by dfe-loader. We send the data as a String column over native protocol; ClickHouse handles the JSON decomposition.

For tables where `_json` is not needed (high-volume, write-heavy, no ad-hoc queries), provide the per-table disable mechanism already implemented (`disable_json_tables` config + `@no_capture_json` DDL tag).

---

## 7. Async Insert Analysis

### 7.1 When Async Inserts Help

Async inserts buffer small writes server-side, reducing part creation overhead. Designed for many small, continuous data streams that cannot batch client-side.

### 7.2 Why NOT for dfe-loader

dfe-loader already batches client-side at 10K rows. We already control:
- Batch size (configurable)
- Flush timing (time-based + size-based)
- Memory usage (per-buffer tracking)
- Error handling (synchronous response per batch)

Adding async inserts would:
- Add a second layer of batching (unnecessary)
- Lose synchronous error feedback (DLQ routing depends on knowing which batch failed)
- Disable deduplication by default
- Add latency (server-side buffer wait)
- Reduce control over memory (server owns the buffer)

### 7.3 Verdict

**Do not enable async inserts.** Our client-side batching at 10K rows is already in the optimal range (ClickHouse recommends 10K-100K). Synchronous inserts with native protocol give us the best CPU efficiency + error handling combination.

---

## 8. Architecture Decision: Benchmark First, Then Commit

### 8.1 Decision

**Do NOT commit to either Mison or arrow-json Decoder until fair benchmarks are run on a dedicated host.**

Both approaches eliminate the `serde_json::Value` DOM (the #1 CPU hotspot). Both produce Arrow RecordBatches for clickhouse-arrow native inserts. The theoretical arguments favour Mison (1 SIMD pass vs 2, schema-guided skip), but the existing benchmarks are misleading -- they compare different things (Mison full pipeline including Arrow build vs sonic-rs partial pipeline without Arrow build).

### 8.2 What We Know

1. **Arrow as intermediate format is correct.** clickhouse-arrow over native protocol is the lowest-CPU insert path (5.5% vs 17% for JSONEachRow). There is no viable alternative in Rust that skips Arrow -- we'd have to reimplement both the JSON-to-columnar conversion AND the ClickHouse native protocol serialisation.

2. **The DOM must go.** Both candidates eliminate it. This is the high-impact change regardless of which approach wins.

3. **Mison has architectural advantages on paper:**
   - 1 SIMD pass (vs Decoder's 2 -- sonic routing + tape parse)
   - Schema-guided extraction (skip fields not in destination schema)
   - Pattern tree speculation (O(1) for repeated message shapes)
   - Zero allocation (`ExtractedValue<'a>` borrows from input)
   - Already written and unit-tested (3,428 lines)

4. **Decoder has practical advantages on paper:**
   - Maintained by Apache Arrow (battle-tested in DataFusion, Arroyo)
   - Full nested JSON support
   - Streaming API with partial document handling
   - Smaller integration surface

5. **The existing benchmarks don't answer the question.** The `benches/mison.rs` benchmarks compare:
   - `mison_route` (1.1µs) includes structural index + extract + Arrow build
   - `traditional_route` (543ns) includes sonic DOM parse + field get, but NO Arrow build
   - `sonic_get_from_slice` (141ns) includes SIMD field extraction only

   This is apples-to-oranges. We need end-to-end benchmarks where both paths produce the same Arrow RecordBatch output.

### 8.3 Required Benchmark Design

**Fair comparison: same input, same output, end-to-end.**

| Benchmark | Input | Output | What's Measured |
|-----------|-------|--------|-----------------|
| **Mison E2E** | `Vec<&[u8]>` (raw JSON batch) | `RecordBatch` + routing metadata | Index + extract + Arrow build |
| **Decoder E2E** | `Vec<&[u8]>` (raw JSON batch) | `RecordBatch` + routing metadata | sonic routing + Decoder tape + Arrow |
| **Current E2E** | `Vec<&[u8]>` (raw JSON batch) | `RecordBatch` + routing metadata | sonic DOM + flatten + manual Arrow |

All three must:
- Use the same 10-field flat JSON messages (representative of production)
- Include routing field extraction (org_id, table)
- Include `_json` sidecar accumulation
- Produce identical RecordBatch schemas
- Measure: throughput (msg/sec), CPU cycles per message (`perf stat`), peak memory

**Test matrix:**
- Flat JSON, 10 fields (majority case)
- Flat JSON, 30 fields (wide events)
- Nested JSON, 2 levels (Decoder can handle, Mison returns raw bytes)
- Batch sizes: 100, 1000, 10000

### 8.4 Decision Framework

After benchmarks:

| If Mison wins by >15% | If within 15% | If Decoder wins by >15% |
|------------------------|---------------|-------------------------|
| Integrate Mison into orchestrator. Add routing extraction, sidecar injection, post-Arrow transforms. Keep Decoder as fallback for nested JSON. | Choose based on maintenance burden. Decoder requires less custom code but Mison is already written. | Adopt Decoder pipeline. Keep Mison module for potential future use (multi-field extraction, routing). |

**Nested JSON handling (either path):** Fall back to the existing DOM path (sonic-rs parse → flatten → manual Arrow build). This is the slow path and only needed for messages with nested structures that must be flattened.

### 8.5 Arrow as Intermediate Format (Settled)

Arrow via clickhouse-arrow over native TCP is the correct insert architecture. This decision is documented in detail in [WHY-ARROW.md](WHY-ARROW.md) and is not part of the WBS. The short version: native protocol is 209% less CPU than JSONEachRow, clickhouse-arrow is the only Rust crate that does native + Arrow, and the Arrow → native conversion cost (~2-3ms per 10K rows) is negligible compared to the JSON parsing hotspot we're addressing.

The question this review answers is how we get FROM raw JSON bytes TO Arrow, not whether Arrow is the right intermediate.

### 8.6 Risks

| Risk | Mitigation |
|------|-----------|
| Benchmark results are inconclusive | Fall back to maintenance burden as tiebreaker (Decoder = Apache-maintained, Mison = custom) |
| Mison wins but has integration gaps | Gaps are well-defined (Section 3.5) and bounded. Routing, sidecar injection, post-Arrow transforms. |
| Decoder wins but needs 2 SIMD passes | The 2nd pass (tape) may be fast enough that the theoretical 1-pass advantage doesn't matter in practice |
| Neither matches current pipeline for nested JSON | Keep the current DOM path as slow path. Split-path routing is needed regardless of which fast path wins |

---

## 9. Implementation WBS (Week of 2026-02-10)

See [TODO.md](../TODO.md) for the detailed work breakdown structure.

### Summary

| Phase | Description | Gate |
|-------|-------------|------|
| **Phase 0** | Fair E2E benchmark: Mison vs Decoder vs Current | Benchmark results determine path |
| **Phase 1** | Integrate winning approach into orchestrator | Passes existing integration tests |
| **Phase 2** | Post-Arrow transforms (_org_id, _timestamp, sidecars) | Parity with current pipeline output |
| **Phase 3** | Memory capping + allocation reduction | Production hardening |
| **Phase 4** | Production load test + validation | CPU/memory targets met |

**Phase 0 is the prerequisite.** No implementation work on either path begins until the benchmarks are run and the results are clear.

---

## 10. References

### ClickHouse
- [ClickHouse Input Format Benchmarks (FastFormats)](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)
- [Async Inserts Documentation](https://clickhouse.com/docs/optimize/asynchronous-inserts)
- [Insert Strategy Best Practices](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)
- [New JSON Type Architecture](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
- [Native Protocol Client Packets](https://clickhouse.com/docs/native-protocol/client)
- [PR #70312: JSON as String in Native Format](https://github.com/ClickHouse/ClickHouse/pull/70312)
- [PR #80499: Flattened Dynamic/JSON Serialization](https://github.com/ClickHouse/ClickHouse/pull/80499)
- [PR #54730: Async Inserts over Native Protocol](https://github.com/ClickHouse/ClickHouse/pull/54730)

### Arrow / Rust
- [arrow-json Tape Decoder PR #3479 (2.5x faster)](https://github.com/apache/arrow-rs/pull/3479)
- [arrow-json 30% Performance Improvement PR #7157](https://github.com/apache/arrow-rs/pull/7157)
- [Arroyo: Fast Columnar JSON Decoding](https://www.arroyo.dev/blog/fast-arrow-json-decoding/)
- [arrow_json::reader Documentation](https://arrow.apache.org/rust/arrow_json/reader/index.html)
- [Buffer::from_vec Zero-Copy](https://docs.rs/arrow/latest/arrow/buffer/struct.Buffer.html)
- [ScalarBuffer Alignment Issue #4431](https://github.com/apache/arrow-rs/issues/4431)
- [clickhouse-arrow Crate](https://docs.rs/clickhouse-arrow)

### JSON Parsers
- [sonic-rs GitHub (1.3-2x faster than simd-json)](https://github.com/cloudwego/sonic-rs)
- [simd-json GitHub](https://github.com/simd-lite/simd-json)
- [jiter GitHub (pull-based, 8x faster than serde_json in iter mode)](https://github.com/pydantic/jiter)

### Mison / Structural Index
- [Mison: A Fast JSON Parser for Data Analytics (VLDB 2017)](http://www.vldb.org/pvldb/vol10/p1118-li.pdf)
- [dfe-loader Mison implementation](../src/mison/) -- 3,428 lines, AVX2/SSE4.2/NEON

### Industry
- [Netflix: 5PB/day ClickHouse Architecture](https://clickhouse.com/blog)
- [LanceDB: SIMD Performance Analysis](https://blog.lancedb.com/my-simd-is-faster-than-yours-fb2989bf25e7)

---

**Last Updated:** 2026-02-06
