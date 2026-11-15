# Comparing metrics snapshots

`tests/common/metrics.rs` turns Prometheus exposition text into a snapshot, compares two snapshots and reports what moved. `tests/performance_example.rs` shows the workflow on two fixed texts, so it checks the comparison, not the loader's speed.

```rust
use common::metrics::MetricsSnapshot;

// Text from a running loader's /metrics, or MetricsManager::render().
let baseline = MetricsSnapshot::from_text(&baseline_text, "baseline");
baseline.save(".tmp/metrics_baseline.json")?;

let current = MetricsSnapshot::from_text(&current_text, "candidate");
let comparison = current.compare(&baseline);
comparison.print_report();
comparison.save_markdown(".tmp/metrics_comparison.md")?;
assert!(comparison.regressions.is_empty());
```

A snapshot records the time, the git commit (`git rev-parse --short HEAD`) and each metric's value, summed across its label sets. `MetricsSnapshot::load` reads a saved one back.

## How a change is judged

`compare` puts each metric that moved under improvements or regressions by its name:

- `latency`, `error`, `dlq` or `lag` in the name: lower is better.
- `processed`, `inserted` or `flushed`: higher is better.
- `buffer` or `memory`: lower is better.
- Anything else: higher is better.

Histogram `_bucket` and `_count` series move with the request count, so judge latency on `_sum`. The example test leaves those two out of its regression check for that reason.

```bash
cargo nextest run --test performance_example --no-capture
```

`dfe-loader metrics-manifest` lists the metric names a snapshot of a running loader holds (see [../docs/operations.md](../docs/operations.md#observe)).
