# CPU Cost Guide

**Baseline benchmarks for dfe-loader performance optimization.**

Last updated: 2025-12-28
Platform: AMD64 Linux (Fedora 42)
Rust: 1.83.0 (release profile)

---

## Summary: Per-Message Pipeline Cost

| Stage | Cost/msg | Notes |
|-------|----------|-------|
| **JSON Parse (sonic-rs)** | ~150-200 ns | SIMD-accelerated |
| **Routing** | ~45-170 ns | route_value fastest (45ns), bytes slower (170ns) |
| **Flatten** | ~130-825 ns | Depth-dependent (3 fields: 130ns, 6 levels: 825ns) |
| **Transform** | ~600-900 ns | Full transform with logjson capture |
| **GeoIP (cache hit)** | ~30 ns/IP | Private IP fast path: ~27 ns |
| **Reputation (cache hit)** | ~36 ns/IP | O(1) hash lookup |
| **Risk Scoring** | ~33-100 ns | Integer-only math |
| **Full Enrichment** | ~100 ns/IP | GeoIP + Reputation + Risk combined |

**Estimated total per message: ~1.5-2.5 µs** (without ClickHouse I/O)

---

## Detailed Benchmarks

### 1. JSON Parsing (sonic-rs)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Parse small payload (52 bytes) | 177-195 ns | 255-282 MiB/s |
| Parse medium payload (262 bytes) | 784-798 ns | 329-335 MiB/s |
| Parse large payload (636 bytes) | 1.73-1.82 µs | 350-368 MiB/s |
| Field extraction (top-level) | 61-65 ns | - |
| Field extraction (nested) | 158-167 ns | - |
| Field extraction (3 levels deep) | 193-206 ns | - |

### 2. Routing

| Benchmark | Time | Throughput |
|-----------|------|------------|
| route_bytes (raw JSON) | 159-179 ns | 5.6-6.3 M/s |
| route_cow (Cow<str> return) | 147-159 ns | 6.3-6.8 M/s |
| **route_value (pre-parsed)** | **44-46 ns** | **21.9-22.9 M/s** |

**Recommendation**: Parse once, route with `route_value()` for 3-4x speedup.

### 3. Flattening

| Benchmark | Time | Notes |
|-----------|------|-------|
| Shallow (3 fields) | 127-137 ns | Simple objects |
| Medium nested | 591-624 ns | 2-3 levels |
| Deep (6 levels) | 814-837 ns | Deep nesting |
| Wide (50 fields) | 4.45-4.97 µs | Many top-level fields |

**Cost model**: ~100-150 ns base + ~100 ns per nesting level + ~80 ns per field.

### 4. Transform Pipeline

| Benchmark | Time | Notes |
|-----------|------|-------|
| Transform small | 594-650 ns | With logjson capture |
| Transform medium | 2.24-2.49 µs | Typical event |
| Transform large | 5.85-6.54 µs | Complex event |

### 5. Full E2E Pipeline (no I/O)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Parse + Route + Flatten + Transform (medium) | 2.66-2.74 µs | 95-99 MiB/s |

---

## Enrichment Benchmarks

### GeoIP Lookup

| Benchmark | Time (10 IPs) | Per-IP | Notes |
|-----------|---------------|--------|-------|
| Private IP fast path | 269-279 ns | **~27 ns** | RFC1918/loopback detection |
| Cache hit | 295-350 ns | **~30-35 ns** | RwLock read |
| Cache miss (no DB) | 358-1,140 ns | ~36-114 ns | Varies with contention |

### Reputation Lookup

| Benchmark | Time (10 IPs) | Per-IP | Notes |
|-----------|---------------|--------|-------|
| Empty blocklist | 349-405 ns | **~35-40 ns** | Cache + lock overhead |
| Cache hit | 354-402 ns | **~35-40 ns** | O(1) FxHashMap |
| 1000 IPs + 100 CIDRs | 359-426 ns | **~36-43 ns** | Hash still O(1) |
| Prefix match | 356-3,500 ns | ~36-350 ns | CIDR scan cost varies |

### Risk Scoring

| Benchmark | Time | Notes |
|-----------|------|-------|
| Minimal input | 32-42 ns | Empty risk factors |
| Full input (all threats) | 81-104 ns | Botnet + malicious + abuse score |
| High security preset | 40-42 ns | Preset lookup O(1) |
| From enrichment results | 47-52 ns | Struct copy overhead |

### Full Enrichment Pipeline

| Benchmark | Time (10 IPs) | Per-IP |
|-----------|---------------|--------|
| GeoIP + Reputation + Risk | 950-1,020 ns | **~95-102 ns** |

---

## Optimization Priorities

### Hot Path (optimize first)

1. **JSON parsing** - Already SIMD (sonic-rs), ~200 ns/msg
2. **Routing** - Use `route_value()` after parsing, ~45 ns
3. **Transform** - ~600-900 ns, most time in flatten

### Cold Path (defer)

1. **Schema fetch** - Once per table, cached
2. **Blocklist reload** - Background task
3. **MMDB reload** - On file change

### Memory Allocation

| Operation | Allocations | Notes |
|-----------|-------------|-------|
| Flatten shallow | 0 | Reuses existing Map |
| Flatten deep | ~N | One per nested level |
| Transform | 1-2 | logjson + _tags |
| Routing | 0 | Returns borrowed str |

---

## Scaling Estimates

### Single-threaded throughput

| Scenario | Est. msgs/sec | Notes |
|----------|---------------|-------|
| Parse only | ~5-6 M/s | Just JSON parsing |
| Parse + Route | ~4-5 M/s | With routing |
| Full transform | ~400-600 K/s | All processing |
| With enrichment | ~350-500 K/s | +GeoIP/Reputation/Risk |

### Bottleneck analysis

1. **CPU-bound**: Transform dominates at ~2.5 µs/msg
2. **Network I/O**: ClickHouse insert batches amortize
3. **Memory**: ~1-2 KB per in-flight message

---

## Benchmark Commands

```bash
# Run all benchmarks
cargo bench

# Specific benchmark suites
cargo bench --bench json_parsing
cargo bench --bench transform
cargo bench --bench pipeline
cargo bench --bench enrichment

# HTML reports
cargo bench -- --noplot  # faster, no gnuplot
open target/criterion/report/index.html
```

---

## Mison Structural Index (Experimental)

Mison-style structural indexing for schema-guided extraction. Based on the VLDB 2017 paper.

### Current Benchmarks (v0.11.0)

| Benchmark | Mison | Traditional | Ratio |
|-----------|-------|-------------|-------|
| Index build (simple) | ~165 ns | - | - |
| Single field extract | ~70-165 ns | - | - |
| Route (2 fields) | 1.1 µs | 543 ns | 2x slower |
| Schema extract (6 fields) | 834 ns | - | - |
| Full single | 3.1 µs | 653 ns | 5x slower |
| Batch 100 | 179 µs | 57 µs | 3x slower |
| sonic_get_from_slice (2 fields) | - | 141 ns | **Fastest** |

### Analysis

Current implementation is **slower** than sonic-rs for these reasons:

1. **Scalar fallback**: AVX2/SSE4.2 not enabled at compile-time (needs runtime detection)
2. **Per-bit scanning**: Leveled bitmap uses O(n) bit-by-bit loop vs O(1) SIMD ops
3. **Index overhead**: Building full index for 2 fields is wasteful
4. **String comparisons**: Key matching not optimized

### When Mison Would Win

Mison approach is designed for:
- **Many field extractions** (10+ fields per message)
- **Same schema for many messages** (index can be partially reused)
- **Avoiding DOM allocation** (current impl still has overhead)

### Next Steps

1. Enable runtime SIMD detection (is_x86_feature_detected!)
2. Use popcount + bit manipulation instead of per-bit loops
3. Add "fast path" for schema-only extraction (skip full index)
4. Consider simdjson integration for better baseline

**Recommendation**: Use `sonic_rs::get_from_slice()` for routing (141 ns/2 fields).

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.11.0 | 2025-12-28 | Mison structural index implementation (experimental) |
| 0.10.0 | 2025-12-28 | Initial enrichment benchmarks |
| 0.9.0 | 2025-12-25 | Hot path round 4 (FxHashMap, CompactString) |
