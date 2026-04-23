# Performance Optimization Guide

This document covers performance optimization techniques for dfe-loader, including
PGO/BOLT build optimizations and runtime tuning.

> **CI Integration Notice (2026-04+):**
> hyperi-ci now applies **Tier 1** build optimisations (jemalloc allocator + fat
> LTO) automatically on `beta` and `release` channels. **Tier 2** (PGO + BOLT)
> is opt-in per project via `.hyperi-ci.yaml`. The manual `cargo pgo` / `cargo
> pgo bolt` commands documented below are now for **local development and
> one-off profiling only** — CI handles them automatically when configured.
>
> See:
> - `hyperi-ai/standards/languages/RUST.md` — *Release-Track Build
>   Optimisation (hyperi-ci)* for the full contract
> - `hyperi-ai/standards/infrastructure/CI.md` — *Channel-Tiered Build
>   Optimisation* for the channel × tier table
> - `TODO.md` — *Rust Release-Track Optimisation* for dfe-loader's specific
>   Tier 2 opt-in steps
>
> The rest of this document remains relevant for:
> - Understanding *why* each optimisation matters (the CI just automates them)
> - Local profiling / one-off performance investigation
> - Runtime tuning (batch sizes, concurrent inserts — NOT build-time concerns)

## Build Optimizations

### Profile-Guided Optimization (PGO)

PGO can provide **10-20% performance improvement** by using runtime profiling data
to guide compiler optimizations. See [cargo-pgo](https://github.com/Kobzol/cargo-pgo).

> **CI:** hyperi-ci runs this automatically on `release` channel when
> configured. See `TODO.md` → *Rust Release-Track Optimisation* for the
> opt-in steps. The commands below are for **local profiling only**.

#### Local Setup (Manual)

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

#### Best Practices for PGO Workloads

- Run the instrumented binary with a **representative workload**
- Include all common code paths (various message types, error handling)
- Profile for at least 5-10 minutes of sustained load
- Re-profile when making significant code changes
- **CRITICAL:** The workload must exercise REAL data-processing paths —
  parse, transform, serialise, insert. **Do NOT use port checks, health
  probes, or "does the service start" tests.** Profile data from startup /
  readiness paths misleads the compiler and produces NEGATIVE PGO gains.

### BOLT Post-Link Optimization

BOLT provides **additional 5-15% improvement** on top of PGO by optimizing
code layout in the final binary. See [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt).

> **CI:** hyperi-ci runs BOLT automatically on `release` channel when
> `optimize.bolt.enabled: true` is set in `.hyperi-ci.yaml` AND `pgo.enabled`
> is also true (BOLT requires PGO profile data). Linux-only. The commands
> below are for **local profiling only**.

#### Local Setup (Manual)

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

## Memory Allocator

DFE policy (2026-04-17): **jemalloc at every channel, no mimalloc**. One
allocator across the fleet means one profiling story (`jeprof`), one set of
perf-trace symbols, one debugging playbook. See
`hyperi-ai/standards/languages/RUST.md` → *Allocator Policy*.

hyperi-ci adds `--features jemalloc` automatically on every channel
(spike/alpha/beta/release). For local builds, opt in with:

```bash
cargo build --release --features jemalloc
```

### Verification on stripped release binaries

Release builds set `strip = true`, so `nm` won't see allocator symbols.
Use `strings`:

```bash
strings target/release/dfe-loader | grep -ciE 'jemalloc|je_mallctl'
# Expect > 0
```

Expect a ~3.5% binary-size increase from the static jemalloc link
(per dfe-receiver canary 1, 2026-04-17).

### Benchmark (jemalloc vs system glibc, OLAP-style workload)

| Metric | System (glibc) | jemalloc |
|--------|---------------|----------|
| Throughput | Baseline | +15–25% |
| RSS | Baseline | +5–10% |
| Large-batch p99 latency | Baseline | -10–20% |

## Insert Throughput

dfe-loader writes to ClickHouse via the `/projects/clickhouse-rs` fork.
The default path is RowBinary via `DynamicInsert` (schema-reflected typed
encoding — ClickHouse skips JSON parsing). Set `insert_format = "json_each_row"`
to fall back to `reqwest` HTTP POST with JSONEachRow. Tuning below applies to
both formats.

### Batch Size

Larger batches amortise HTTP overhead. Default flush thresholds:

- `flush_rows = 20000` rows
- `flush_bytes = 1048576` (1MB)
- `flush_age_secs = 5`

Increase `flush_rows` and `flush_bytes` for higher throughput at the cost of latency.

### Allocator Feature

The `jemalloc` feature is declared in `Cargo.toml` and applies globally via
`#[global_allocator]` in `src/main.rs`:

```toml
[features]
jemalloc = ["dep:tikv-jemallocator", "dep:tikv-jemalloc-ctl"]
```

mimalloc was removed on 2026-04-17 per the DFE allocator policy.

## Runtime Tuning

### Batch Sizes

| Setting | Default | Tuning Guidance |
|---------|---------|-----------------|
| `flush_rows` | 20,000 | Increase for throughput, decrease for latency |
| `flush_bytes` | 1MB | Match to typical batch memory size |
| `flush_age_secs` | 5s | Lower for real-time, higher for batch |

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

**For production releases: let hyperi-ci do this.** Push to a `release`-channel
project and CI applies the full optimisation pipeline automatically. See
`TODO.md` → *Rust Release-Track Optimisation* for opt-in steps.

**For local development / one-off profiling:**

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
- hyperi-ci `docs/RUST-RELEASE-TRACK-OPTIMISATION.md` — channel × tier matrix
- hyperi-ci `docs/PGO-WORKLOAD-GUIDE.md` — workload design rules
