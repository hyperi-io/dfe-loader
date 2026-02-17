// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Example of using metrics snapshots for performance testing
//!
//! This demonstrates how to capture metrics before/after changes
//! to validate performance improvements.
//!
//! Run with: cargo test --test performance_example -- --nocapture

use prometheus::Registry;

mod common;
use common::metrics::MetricsSnapshot;
use dfe_loader::metrics::Metrics;

#[test]
fn example_metrics_snapshot_workflow() {
    // Create metrics registry
    let registry = Registry::new();
    let metrics = Metrics::with_registry(registry.clone());

    // === Simulate baseline workload ===
    eprintln!("Running baseline workload...");

    // Simulate processing 10,000 messages
    for _ in 0..10_000 {
        metrics.messages_received.inc();
        metrics.messages_processed.inc();
    }

    // Simulate 10 batch inserts with latency
    for i in 0..10 {
        metrics.batches_flushed.inc();
        metrics.rows_inserted.inc_by(1000.0);

        // Simulate varying latency (baseline: 50-100ms)
        let latency = 0.05 + (i as f64 * 0.005);
        metrics.insert_latency.observe(latency);
    }

    // Capture baseline snapshot
    let baseline = MetricsSnapshot::capture(&registry, "baseline_v1");
    baseline
        .save("target/metrics_baseline.json")
        .expect("Failed to save baseline");

    eprintln!("✓ Baseline snapshot saved to target/metrics_baseline.json");

    // === Simulate improved workload (after optimization) ===

    // Reset counters for fair comparison (in real test, use separate registry)
    let registry2 = Registry::new();
    let metrics2 = Metrics::with_registry(registry2.clone());

    eprintln!("Running optimized workload...");

    // Process same 10,000 messages (20% faster)
    for _ in 0..12_000 {
        metrics2.messages_received.inc();
        metrics2.messages_processed.inc();
    }

    // Same 10 batch inserts with improved latency
    for i in 0..10 {
        metrics2.batches_flushed.inc();
        metrics2.rows_inserted.inc_by(1000.0);

        // Improved latency (30-60ms, 40% improvement)
        let latency = 0.03 + (i as f64 * 0.003);
        metrics2.insert_latency.observe(latency);
    }

    // Capture current snapshot
    let current = MetricsSnapshot::capture(&registry2, "optimized_v2");
    current
        .save("target/metrics_current.json")
        .expect("Failed to save current");

    eprintln!("✓ Current snapshot saved to target/metrics_current.json");

    // === Compare snapshots ===
    let comparison = current.compare(&baseline);

    // Print to console
    comparison.print_report();

    // Save markdown report
    comparison
        .save_markdown("target/metrics_comparison.md")
        .expect("Failed to save report");

    eprintln!("\n✓ Comparison report saved to target/metrics_comparison.md");

    // Assert no significant regressions (ignore histogram bucket counts which increase with more data)
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
#[ignore] // Run manually with: cargo test test_load_and_compare_snapshots -- --ignored --nocapture
fn test_load_and_compare_snapshots() {
    // Load previously saved snapshots
    let baseline = MetricsSnapshot::load("target/metrics_baseline.json")
        .expect("Failed to load baseline - run example_metrics_snapshot_workflow first");

    let current = MetricsSnapshot::load("target/metrics_current.json")
        .expect("Failed to load current - run example_metrics_snapshot_workflow first");

    // Compare
    let comparison = current.compare(&baseline);
    comparison.print_report();
}
