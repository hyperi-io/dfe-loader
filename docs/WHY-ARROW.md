# Why Arrow? Bulk Insert Format Decision

**Date:** 2026-02-06
**Status:** Decided -- Arrow via clickhouse-arrow (native TCP protocol)
**Scope:** This document evaluates all viable paths from dfe-loader to ClickHouse bulk inserts, concluding that Arrow as an intermediate columnar format via our clickhouse-arrow fork is the correct architecture. This decision is settled and not part of the ongoing WBS.

---

## 1. The Question

dfe-loader produces batches of ~10K transformed messages that must be inserted into ClickHouse as efficiently as possible. CPU is the primary constraint (k8s pod scaling bottleneck).

The architecture uses Arrow RecordBatch as the intermediate columnar format between JSON parsing and ClickHouse insertion. Is this the right choice? Could we skip Arrow entirely and get better performance?

---

## 2. Candidate Insert Paths

There are four viable paths from Rust to ClickHouse bulk inserts:

### Path A: Arrow → clickhouse-arrow → Native TCP (Current)

```
JSON bytes → [parse] → Arrow RecordBatch → [clickhouse-arrow] → Native blocks → TCP:9000
```

- **Protocol:** Native TCP binary (port 9000)
- **Wire format:** Columnar blocks with LZ4 compression
- **Client:** clickhouse-arrow v0.4.0 (our fork with Variant/Dynamic/Nested/JSON)
- **Serialization:** ~12,841 lines of type-specific code (48 types)
- **Key feature:** Column-at-a-time serialization with zero-copy for primitives

### Path B: Rust structs → klickhouse → Native TCP (Removed)

```
JSON bytes → [parse] → serde_json::Value → [klickhouse] → Native blocks → TCP:9000
```

- **Protocol:** Native TCP binary (port 9000)
- **Wire format:** Same native blocks as Path A
- **Client:** klickhouse v0.15.0-dfe (our fork, now at `/projects/klickhouse-hypersec/`)
- **Serialization:** Row-at-a-time via `#[derive(Row)]` trait
- **Removed:** 2025-12-25 (commit 889cf81)

### Path C: Rust structs → clickhouse-rs → HTTP RowBinary (Not Used)

```
JSON bytes → [parse] → Rust structs → [clickhouse-rs] → RowBinary → HTTP:8123
```

- **Protocol:** HTTP (port 8123)
- **Wire format:** RowBinary (row-oriented binary)
- **Client:** clickhouse-rs v0.14.2 (official, ClickHouse Inc maintained)
- **Serialization:** serde-based, row-at-a-time

### Path D: Mison/Custom → Direct native blocks (Hypothetical)

```
JSON bytes → [Mison SIMD] → ExtractedValue<'a> → [custom serializer] → Native blocks → TCP:9000
```

- **Protocol:** Native TCP binary (port 9000)
- **Wire format:** Same native blocks
- **Client:** Would require writing a new native protocol client
- **Serialization:** Custom, skipping Arrow entirely

---

## 3. Performance Benchmarks

### 3.1 ClickHouse Server-Side Insert Cost

From the [ClickHouse FastFormats benchmark](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient):

| Format | Wall Clock | CPU (p99/insert) | Data Size | vs Native |
|--------|-----------|-------------------|-----------|-----------|
| **Native + LZ4** | **131s** | **5.5%** | 2.55 GiB | baseline |
| ArrowStream (HTTP) | 146s | ~7% | 2.93 GiB | +11% wall, +27% CPU |
| RowBinary (HTTP) | 161s | ~9% | 3.27 GiB | +23% wall, +64% CPU |
| TSV (HTTP) | 190s | 11% | 4.22 GiB | +45% wall, +100% CPU |
| JSONEachRow (HTTP) | 266s | 17% | 5.39 GiB | +103% wall, +209% CPU |

Native protocol with LZ4 is the lowest-CPU option. The wire format directly matches MergeTree's columnar storage, so the server does approximately one copy per column.

### 3.2 Client-Side Serialization Cost (Arrow → Native)

From clickhouse-arrow benchmarks and architecture analysis:

| Arrow column type | Serialization cost | Notes |
|-------------------|-------------------|-------|
| Primitives (Int, Float, Date) | ~Zero (buffer reference) | `bytemuck::cast_slice` + single `write_all` |
| Nullable primitives | ~Zero (vectored I/O) | Null bitmap + values combined in one syscall |
| String/Binary | Low (offset iteration) | varint(len) + buffer slice per value |
| LowCardinality | Low | Dictionary + indices |
| Array/Map/Tuple | Medium | Structural conversion required |
| Variant/Dynamic | Medium | Discriminator + per-type columns |

For a typical 10K-row batch with 8 columns (our common header):

- Serialization: ~2-3ms uncompressed
- LZ4 compression: ~5-10ms
- Total client-side overhead: ~7-13ms per 10K rows

This is not the bottleneck. JSON parsing and DOM allocation consume 10-50x more CPU than Arrow → native serialization.

---

## 4. Detailed Path Evaluation

### 4.1 Path A: Arrow via clickhouse-arrow (CHOSEN)

**Advantages:**

1. **Lowest server-side CPU** -- Native protocol at 5.5% CPU vs 17% for JSON (209% difference)
2. **Columnar alignment** -- Arrow's memory layout matches ClickHouse's MergeTree storage. Primitive columns are essentially memcpy.
3. **Zero-copy primitives** -- Int, Float, Date columns use `bytemuck::cast_slice` with no data copy. Nullable primitives use vectored I/O combining bitmap + values in one syscall (2.2x faster with SIMD).
4. **Ecosystem integration** -- Arrow is the standard columnar format in Rust. arrow-json Decoder and our Mison module both produce RecordBatch directly. Choosing a different insert format would require converting FROM Arrow TO that format, adding a copy.
5. **Type coverage** -- Our fork handles 48 types including Variant, Dynamic, Nested, JSON, BFloat16, Time64, AggregateFunction. This represents ~12,841 lines of working, tested serialization code.
6. **Batch operations** -- Arrow compute kernels enable post-hoc transforms (cast, filter, project) on RecordBatch before insert. Used for `_timestamp` type coercion and `_org_id` injection.

**Costs:**

1. **Fork maintenance** -- 863 lines of DFE-specific code (Variant/Dynamic/Nested serializers). The remaining 93% is upstream-compatible.
2. **Arrow overhead for simple types** -- For String columns, Arrow adds an offsets array that ClickHouse native format doesn't need. Cost is ~4 bytes per string value for the offset, negligible.
3. **Arrow intermediate** -- Data passes through Arrow on its way to native blocks. For primitives this is zero-copy. For strings it's one copy (into Arrow buffers) + serialization (to native format).

### 4.2 Path B: klickhouse (REMOVED)

klickhouse was our original ClickHouse client. It speaks native TCP protocol and has decent type support, including our fork additions for Variant/Dynamic/JSON.

**Why it was removed (2025-12-25):**

1. **Row-at-a-time serialization** -- klickhouse serializes each row independently via `Row::serialize_row() -> Vec<(column_name, Value)>`. This means N heap allocations per row for the column-value pairs, then a second pass to collect into columnar format. Our pipeline already has data in columnar format (Arrow RecordBatch). Converting Arrow → row tuples → columnar wire format is wasteful.

2. **No Arrow integration** -- klickhouse operates on its own `Value` type. Using it would require: `Arrow RecordBatch → iterate rows → klickhouse::Value per field → serialize`. This adds a full row-major traversal of the columnar data, destroying cache locality.

3. **Double conversion** -- With klickhouse, the pipeline would be: `JSON → serde_json::Value (DOM) → klickhouse::Value → native blocks`. With clickhouse-arrow: `JSON → Arrow RecordBatch → native blocks`. One fewer conversion step.

4. **Architectural mismatch** -- dfe-loader's entire pipeline is columnar (per-table Arrow buffers, batch transforms, schema-guided extraction). klickhouse's row-oriented API fights this at every step.

**klickhouse remains at `/projects/klickhouse-hypersec/` as read-only reference.**

### 4.3 Path C: clickhouse-rs via HTTP RowBinary (REJECTED)

clickhouse-rs is the official Rust client maintained by ClickHouse Inc.

**Why it was rejected:**

1. **HTTP protocol only** -- No native TCP. Every insert requires HTTP request/response overhead (headers, connection management, potentially TLS handshake if not keep-alive).

2. **RowBinary format** -- 23% slower wall clock and 64% more CPU than native format (Section 3.1). RowBinary is row-oriented; ClickHouse must transpose to columnar on ingest.

3. **No Arrow integration** -- Expects serde-serializable Rust structs. Same mismatch as klickhouse: would require converting Arrow RecordBatch → Rust structs → RowBinary.

4. **Vendor support is the only advantage** -- clickhouse-rs is maintained by ClickHouse Inc. But our clickhouse-arrow fork is 93% upstream-compatible code, with only 863 lines of DFE additions. The maintenance burden is bounded.

### 4.4 Path D: Direct native serialization from Mison (EVALUATED, REJECTED)

The most interesting alternative: what if Mison's `ExtractedValue<'a>` could be serialized directly to ClickHouse native blocks, skipping Arrow entirely?

**What this would save:**

- Arrow buffer allocation (~1 copy for strings, 0 for primitives)
- Arrow → native serialization pass

**What this would cost:**

1. **Reimplementing clickhouse-arrow's serialization** -- We'd need ~12,841 lines of type-specific serialization code that converts typed values to ClickHouse native wire format. This covers 48 types with LZ4 compression, null handling, LowCardinality dictionary encoding, and the native protocol framing (block headers, column metadata, protocol versioning).

2. **Reimplementing the native protocol client** -- TCP connection management, authentication (SASL/TLS), protocol negotiation, compression, error handling. clickhouse-arrow's `protocol.rs` alone is 506 lines.

3. **Losing Arrow compute** -- Post-hoc transforms (timestamp casting, column injection, schema validation) currently use Arrow compute kernels. Without Arrow, we'd need custom implementations for each.

4. **Losing ecosystem compatibility** -- Arrow RecordBatch is the interchange format for columnar data in Rust. Tools like DataFusion, Polars, and our own test infrastructure (`query_back` verification) all speak Arrow. A custom columnar format is an island.

5. **The conversion cost is not the bottleneck** -- Arrow → ClickHouse native serialization takes ~2-3ms per 10K-row batch. JSON parsing + DOM allocation takes 10-50x more CPU. Eliminating Arrow saves microseconds while the parsing step wastes milliseconds.

**The math doesn't work.** We'd write ~15,000 lines of new serialization code to save ~2ms per 10K-row batch. The JSON parsing hotspot we're addressing in the WBS is 10-50x larger.

---

## 5. The Arrow → Native Conversion in Detail

To be concrete about what clickhouse-arrow does when converting a RecordBatch to native wire bytes:

### 5.1 Block Structure

```
Block = BlockInfo + ColumnCount (varint) + RowCount (varint) + Column[]

Column = Name (string) + TypeName (string) + [CustomSerFlag] + [Prefix] + Data
```

### 5.2 Per-Type Serialization

**Primitives (Int8-UInt64, Float32/64, Date, DateTime):**

```
Arrow buffer bytes → bytemuck::cast_slice → write_all (single syscall)
```

Cost: effectively zero. The Arrow buffer IS the wire data (both little-endian).

**Nullable primitives:**

```
Arrow null bitmap (packed bits) → expand to byte-per-row → vectored I/O with values
```

Cost: bitmap expansion (SIMD-accelerated, 2.2x faster than scalar).

**String/Binary:**

```
For each value: write varint(length) + write raw bytes from Arrow buffer
```

Cost: varint encoding overhead. Data bytes are referenced from Arrow's buffer, not copied.

**LowCardinality (used for `_org_id`):**

```
Write dictionary values + write index array (UInt8/16/32 based on cardinality)
```

Cost: dictionary + indices. Efficient for repeated values (org_id is typically 1 value per batch).

**Variant/Dynamic (used for `_json` type internally by ClickHouse):**

```
Write discriminator bytes + per-variant column data
```

Cost: discriminator array + type-specific serialization. Note: we send `_json` as String; ClickHouse decomposes to JSON type server-side.

### 5.3 Compression

After column serialization, the block is optionally LZ4-compressed. Typical compression ratio ~3:1 for JSON-derived data, saving network bandwidth at the cost of ~5-10ms CPU per batch.

---

## 6. Fork Maintenance Assessment

### 6.1 Code Breakdown

| Category | Lines | % of Total | Upstream Compatible |
|----------|-------|------------|-------------------|
| Arrow serialization (standard types) | 10,192 | 79% | Yes |
| Native protocol (standard types) | 1,056 | 8% | Yes |
| Block/protocol layer | 930 | 7% | Yes |
| **DFE fork: Variant serializer** | **301** | **2.3%** | No (custom) |
| **DFE fork: Dynamic serializer** | **434** | **3.4%** | No (custom) |
| **DFE fork: Nested serializer** | **128** | **1.0%** | No (custom) |
| **Total** | **~12,841** | | **93.3% upstream** |

### 6.2 Maintenance Burden

- **Upstream tracking:** The 93.3% upstream-compatible code can be synced with upstream clickhouse-arrow releases. Breaking changes are rare (stable API since v0.3.x).
- **DFE additions:** 863 lines of custom serializers for Variant/Dynamic/Nested. These types are ClickHouse 24.1+ features that upstream clickhouse-arrow hasn't added yet. If upstream adds them, we can drop our fork.
- **Risk:** If upstream clickhouse-arrow is abandoned, we own 12,841 lines. However, the codebase is well-structured with clear type dispatch, and the native protocol spec is stable (ClickHouse maintains backwards compatibility).

### 6.3 Published to Artifactory

All three crates are published to our private registry:

| Crate | Version | Registry |
|-------|---------|----------|
| clickhouse-arrow | 0.3.0 | hyperi (Artifactory) |
| clickhouse-arrow-derive | 0.3.0 | hyperi (Artifactory) |
| hyperi-rustlib | 0.3.0 | hyperi (Artifactory) |

CI builds and publishes on every release. dfe-loader consumes from registry, not local path.

---

## 7. Decision

**Arrow via clickhouse-arrow over native TCP protocol is the correct architecture.**

### Reasons (ranked by importance)

1. **Native protocol is non-negotiable for CPU efficiency.** 5.5% CPU vs 17% for JSONEachRow (209% difference). At scale, this is the difference between 1 pod and 3 pods.

2. **clickhouse-arrow is the only Rust crate that does native protocol + Arrow.** There is no alternative that accepts Arrow RecordBatch and speaks native TCP. The alternatives (klickhouse, clickhouse-rs) require converting away from Arrow, adding copies.

3. **The pipeline is already columnar.** JSON → Arrow → ClickHouse is 2 conversions. JSON → Arrow → row structs → ClickHouse would be 3. JSON → custom columnar → ClickHouse would require reimplementing 12,841 lines of serialization.

4. **Arrow → native conversion is cheap.** ~2-3ms per 10K rows. The JSON parsing hotspot being addressed in the WBS is 10-50x larger. Optimizing the insert format is not where the CPU savings are.

5. **The fork is manageable.** 863 lines of custom code (6.7%) on top of a stable, well-structured upstream. Published to Artifactory with CI.

### What this means for the WBS

Arrow as intermediate format is settled. The WBS (Phase 0 benchmarks, Phase 1 integration) focuses on how we get FROM raw JSON bytes TO Arrow -- comparing Mison vs arrow-json Decoder. Both paths produce Arrow RecordBatch as output; the insert side doesn't change.

---

## 8. References

- [ClickHouse Input Format Benchmarks (FastFormats)](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)
- [ClickHouse Native Protocol Spec](https://clickhouse.com/docs/native-protocol/client)
- [clickhouse-arrow Crate](https://docs.rs/clickhouse-arrow)
- [clickhouse-rs (official)](https://github.com/ClickHouse/clickhouse-rs)
- [klickhouse](https://github.com/Protryon/klickhouse)
- [Netflix: 5PB/day ClickHouse Architecture](https://clickhouse.com/blog)
- [Arrow Buffer::from_vec Zero-Copy](https://docs.rs/arrow/latest/arrow/buffer/struct.Buffer.html)
- [PR #70312: JSON as String in Native Format](https://github.com/ClickHouse/ClickHouse/pull/70312)

---

**Last Updated:** 2026-02-06
