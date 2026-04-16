# Performance Optimization Guide

This document covers performance optimization techniques for dfe-loader, including
PGO/BOLT build optimizations and runtime tuning.

## Build Optimizations

### Profile-Guided Optimization (PGO)

PGO can provide **10-20% performance improvement** by using runtime profiling data
to guide compiler optimizations. See [cargo-pgo](https://github.com/Kobzol/cargo-pgo).

#### Setup

```bash
# Install the tool
cargo install cargo-pgo
rustup component add llvm-tools-preview

# Step 1: Build instrumented binary
cargo pgo build

# Step 2: Run with representative workload
./target/x86_64-unknown-linux-gnu/release/dfe-loader \
    --config test-config.yaml \
    # Run for sufficient time to gather profiles

# Step 3: Build optimized binary
cargo pgo optimize
```

#### Best Practices

- Run the instrumented binary with a **representative workload**
- Include all common code paths (various message types, error handling)
- Profile for at least 5-10 minutes of sustained load
- Re-profile when making significant code changes

### BOLT Post-Link Optimization

BOLT provides **additional 5-15% improvement** on top of PGO by optimizing
code layout in the final binary. See [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt).

#### Setup

```bash
# Build with BOLT (requires BOLT installed)
cargo pgo bolt build

# Run with perf profiling
./target/x86_64-unknown-linux-gnu/release/dfe-loader-bolt-instrumented \
    --config test-config.yaml
# BOLT uses perf data, so run: perf record -e cycles:u -j any,u -- ./binary

# Optimize with BOLT
cargo pgo bolt optimize

# Combined PGO + BOLT
cargo pgo optimize
cargo pgo bolt optimize --with-pgo
```

#### Requirements

- Linux only (ELF binaries)
- x86_64 or AArch64
- LLVM BOLT tools installed

### Link-Time Optimization (LTO)

Already configured in `Cargo.toml`:

```toml
[profile.release]
lto = "thin"           # Use "fat" for max perf, slower compile
codegen-units = 1      # Better optimization
```

For maximum performance:

```toml
lto = "fat"            # More aggressive, 2-3x longer compile
```

## Memory Allocators

dfe-loader supports alternative allocators that can provide **10-25% improvement**
for OLAP-style workloads.

### jemalloc (Recommended for Production)

Best for long-running servers with large allocations.

```bash
cargo build --release --features jemalloc
```

### mimalloc

Good for mixed workloads, better security hardening.

```bash
cargo build --release --features mimalloc
```

### Benchmarks

| Allocator | Throughput | Memory | Notes |
|-----------|------------|--------|-------|
| System    | Baseline   | Baseline | Default glibc allocator |
| jemalloc  | +15-25%    | +5-10% | Best for large batches |
| mimalloc  | +10-20%    | Similar | Better for mixed sizes |

## Insert Throughput

dfe-loader writes to ClickHouse via the `/projects/clickhouse-rs` fork.
The default path is RowBinary via `DynamicInsert` (schema-reflected typed
encoding — ClickHouse skips JSON parsing). Set `insert_format = "json_each_row"`
to fall back to `reqwest` HTTP POST with JSONEachRow. Tuning below applies to
both formats.

### Batch Size

Larger batches amortise HTTP overhead. Default flush thresholds:

- `flush_rows = 10000` rows
- `flush_bytes = 1048576` (1MB)
- `flush_age_secs = 5`

Increase `flush_rows` and `flush_bytes` for higher throughput at the cost of latency.

### Allocator Feature Propagation

jemalloc/mimalloc features are declared in dfe-loader's `Cargo.toml` and apply globally:

```toml
[features]
jemalloc = ["dep:tikv-jemallocator", "dep:tikv-jemalloc-ctl"]
mimalloc = ["dep:mimalloc"]
```

## Runtime Tuning

### Batch Sizes

| Setting | Default | Tuning Guidance |
|---------|---------|-----------------|
| `flush_rows` | 10,000 | Increase for throughput, decrease for latency |
| `flush_bytes` | 1MB | Match to typical batch memory size |
| `flush_timeout` | 5s | Lower for real-time, higher for batch |

### Concurrent Inserts

```yaml
clickhouse:
  max_concurrent_inserts: 8  # Default, good for most workloads
```

- Increase for high-latency ClickHouse connections
- Decrease if ClickHouse is CPU-bound

## Profiling

### CPU Profiling with perf

```bash
perf record -g ./target/release/dfe-loader --config config.yaml
perf report
```

### Memory Profiling with jemalloc

```bash
# Enable jemalloc profiling
export MALLOC_CONF="prof:true,prof_prefix:jeprof.out"
./target/release/dfe-loader --config config.yaml

# Analyze
jeprof --svg ./target/release/dfe-loader jeprof.out.*.heap > heap.svg
```

### Flame Graphs

```bash
# Install
cargo install flamegraph

# Generate
cargo flamegraph --bin dfe-loader -- --config config.yaml
```

## Recommended Production Build

```bash
# Full optimization build
cargo build --release --features jemalloc

# With PGO (if you have profile data)
cargo pgo optimize --features jemalloc

# With PGO + BOLT (maximum optimization)
cargo pgo optimize --features jemalloc
cargo pgo bolt optimize --with-pgo
```

## References

- [cargo-pgo documentation](https://github.com/Kobzol/cargo-pgo)
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt)
- [jemalloc tuning](https://github.com/jemalloc/jemalloc/wiki/Getting-Started)
- [mimalloc benchmarks](https://github.com/microsoft/mimalloc#benchmark-results)
