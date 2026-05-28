// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Example of using metrics snapshots for performance testing
//!
//! This demonstrates how to capture metrics before/after changes
//! to validate performance improvements.
//!
//! Run with: cargo test --test `performance_example` -- --nocapture

mod common;
use common::metrics::MetricsSnapshot;

#[test]
fn example_metrics_snapshot_workflow() {
    // === Simulate baseline Prometheus text output ===
    let baseline_text = r"# HELP loader_messages_received_total Total messages received
# TYPE loader_messages_received_total counter
loader_messages_received_total 10000
# HELP loader_messages_processed_total Total messages processed
# TYPE loader_messages_processed_total counter
loader_messages_processed_total 10000
# HELP loader_batches_flushed_total Total batches flushed
# TYPE loader_batches_flushed_total counter
loader_batches_flushed_total 10
# HELP loader_rows_inserted_total Total rows inserted
# TYPE loader_rows_inserted_total counter
loader_rows_inserted_total 10000
# HELP loader_insert_latency_seconds Insert latency
# TYPE loader_insert_latency_seconds histogram
loader_insert_latency_seconds_sum 0.725
loader_insert_latency_seconds_count 10
";

    let baseline = MetricsSnapshot::from_text(baseline_text, "baseline_v1");
    baseline
        .save(".tmp/metrics_baseline.json")
        .expect("Failed to save baseline");

    eprintln!("Baseline snapshot saved to .tmp/metrics_baseline.json");

    // === Simulate optimised Prometheus text output ===
    let current_text = r"# HELP loader_messages_received_total Total messages received
# TYPE loader_messages_received_total counter
loader_messages_received_total 12000
# HELP loader_messages_processed_total Total messages processed
# TYPE loader_messages_processed_total counter
loader_messages_processed_total 12000
# HELP loader_batches_flushed_total Total batches flushed
# TYPE loader_batches_flushed_total counter
loader_batches_flushed_total 10
# HELP loader_rows_inserted_total Total rows inserted
# TYPE loader_rows_inserted_total counter
loader_rows_inserted_total 10000
# HELP loader_insert_latency_seconds Insert latency
# TYPE loader_insert_latency_seconds histogram
loader_insert_latency_seconds_sum 0.435
loader_insert_latency_seconds_count 10
";

    let current = MetricsSnapshot::from_text(current_text, "optimized_v2");
    current
        .save(".tmp/metrics_current.json")
        .expect("Failed to save current");

    eprintln!("Current snapshot saved to .tmp/metrics_current.json");

    // === Compare snapshots ===
    let comparison = current.compare(&baseline);
    comparison.print_report();

    comparison
        .save_markdown(".tmp/metrics_comparison.md")
        .expect("Failed to save report");

    eprintln!("\nComparison report saved to .tmp/metrics_comparison.md");

    // Assert no significant regressions
    let real_regressions: Vec<_> = comparison
        .regressions
        .iter()
        .filter(|r| !r.name.contains("_bucket") && !r.name.contains("_count"))
        .collect();

    assert!(
        real_regressions.is_empty(),
        "Performance regressions detected: {} metrics regressed\n{:?}",
        real_regressions.len(),
        real_regressions
    );
}

#[test]
#[ignore = "manual perf comparison — run with --ignored --nocapture"]
fn test_load_and_compare_snapshots() {
    let baseline = MetricsSnapshot::load(".tmp/metrics_baseline.json")
        .expect("Failed to load baseline - run example_metrics_snapshot_workflow first");

    let current = MetricsSnapshot::load(".tmp/metrics_current.json")
        .expect("Failed to load current - run example_metrics_snapshot_workflow first");

    let comparison = current.compare(&baseline);
    comparison.print_report();
}
