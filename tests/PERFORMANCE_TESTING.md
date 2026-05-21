# Performance Testing with Metrics Snapshots

This document describes how to use metrics snapshots to validate that code changes improve performance.

## Overview

The loader already exposes comprehensive Prometheus metrics. The metrics snapshot system captures these metrics before/after changes and generates comparison reports.

## Quick Start

### 1. Capture Baseline Metrics

Before making performance changes, capture a baseline:

```rust
use common::metrics::MetricsSnapshot;
use dfe_loader::metrics::Metrics;

// Run your workload
let metrics = Metrics::new();
// ... process messages ...

// Capture snapshot
let baseline = MetricsSnapshot::capture(&metrics.registry(), "baseline_v1");
baseline.save("benchmarks/baseline.json")?;
```

### 2. Make Your Changes

Implement your performance optimization.

### 3. Capture Current Metrics

Run the same workload with your changes:

```rust
let current = MetricsSnapshot::capture(&metrics.registry(), "optimized_v2");
current.save("benchmarks/current.json")?;
```

### 4. Compare & Validate

```rust
let comparison = current.compare(&baseline);
comparison.print_report();
comparison.save_markdown("benchmarks/comparison.md")?;

// Assert no regressions
assert!(comparison.regressions.is_empty(),
    "Performance regressions detected");
```

## Example Output

```
=== Metrics Comparison Report ===
Baseline: baseline_v1 (abc123)
Current:  optimized_v2 (def456)

✅ Improvements (3):
  loader_insert_latency_seconds_sum : 0.72 → 0.43 (-40.0%)
  loader_messages_processed_total : 10000.00 → 12000.00 (+20.0%)
  loader_messages_received_total : 10000.00 → 12000.00 (+20.0%)

❌ Regressions (0):

📊 Summary:
  Improvements: 3
  Regressions:  0
  Unchanged:    10

✅ PASS: Performance improved with no regressions
```

## Available Metrics

The loader tracks:

- **Throughput**: `loader_messages_processed_total`, `loader_rows_inserted_total`
- **Latency**: `loader_insert_latency_seconds_sum`, `loader_insert_latency_by_table_seconds`
- **Errors**: `loader_insert_errors_total`, `loader_messages_dlq_total`
- **Buffer**: `loader_buffer_rows`, `loader_buffer_bytes`, `loader_buffer_tables`
- **Kafka**: `loader_kafka_offsets_committed_total`, `loader_kafka_lag`

## Regression Detection

The system automatically detects regressions:

- **Latency/Errors**: Lower is better → increase is regression
- **Throughput**: Higher is better → decrease is regression
- **Memory/Buffer**: Lower is better → increase is regression

## Best Practices

### 1. Consistent Workloads

Ensure baseline and current tests use identical workloads:

- Same number of messages
- Same message sizes
- Same table distribution
- Same batch sizes

### 2. Multiple Runs

Run tests 3-5 times and average results to account for variance.

### 3. Save Baselines

Commit baseline snapshots to git for historical tracking:

```bash
git add benchmarks/baseline_*.json
git commit -m "perf: baseline before optimization X"
```

### 4. CI/CD Integration

Add performance regression tests to CI:

```bash
cargo test --test performance_example -- --nocapture
```

## Integration with Existing Benchmarks

The metrics snapshot system works with existing criterion benchmarks:

```rust
// benches/pipeline.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dfe_loader::metrics::Metrics;
use common::metrics::MetricsSnapshot;

fn bench_pipeline(c: &mut Criterion) {
    let metrics = Metrics::new();

    c.bench_function("pipeline_throughput", |b| {
        b.iter(|| {
            // Your benchmark code
            metrics.messages_processed.inc();
        });
    });

    // After benchmark, capture metrics
    let snapshot = MetricsSnapshot::capture(&metrics.registry(), "pipeline_bench");
    snapshot.save("target/bench_snapshot.json").ok();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
```

## Workflow Example

### Before Optimization

```bash
# Run baseline
cargo test --test performance_example baseline -- --nocapture

# Save snapshot
cp target/metrics_current.json benchmarks/baseline_pre_optimization.json
git add benchmarks/baseline_pre_optimization.json
git commit -m "perf: baseline before JSON parser optimization"
```

### After Optimization

```bash
# Make your changes
vim src/payload/parse.rs

# Run same test
cargo test --test performance_example optimized -- --nocapture

# Compare
cargo test --test performance_example compare -- --nocapture

# Review report
cat target/metrics_comparison.md
```

### If Regressions Detected

```bash
# Investigate which metrics regressed
cat target/metrics_comparison.md

# Adjust implementation
vim src/payload/parse.rs

# Re-test
cargo test --test performance_example -- --nocapture
```

## See Also

- [tests/performance_example.rs](performance_example.rs) - Full working example
- [tests/common/metrics.rs](common/metrics.rs) - Metrics snapshot implementation
- [src/metrics/prometheus.rs](../src/metrics/prometheus.rs) - Available metrics

## Troubleshooting

### Metrics Not Captured

Ensure the registry is properly registered:

```rust
let registry = Registry::new();
let metrics = Metrics::with_registry(registry.clone());

// Use metrics.registry() for snapshot
let snapshot = MetricsSnapshot::capture(&registry, "test");
```

### False Regressions

Histogram buckets may show "regressions" that aren't real - focus on `_sum` metrics:

```rust
// Good: actual latency sum
loader_insert_latency_seconds_sum

// Misleading: just bucket counts
loader_insert_latency_seconds_bucket
```

### Git Commit Not Captured

Ensure you're in a git repository:

```bash
cd /projects/dfe-loader
git status
```

---

**Last Updated:** 2026-01-12
**Author:** HyperI Platform Team
