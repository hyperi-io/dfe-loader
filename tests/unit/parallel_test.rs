// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Tests verifying parallel message processing via AdaptiveWorkerPool.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dfe_loader::pipeline::capture::CaptureOverrides;

use scalo::worker::{AdaptiveWorkerPool, WorkerPoolConfig};

/// Detected parallelism available to this process.
///
/// scalo's `WorkerPoolConfig::resolve_max_threads` caps the configured
/// `max_threads` at `available_parallelism()` and then validates
/// `min_threads <= max_threads` — so a hard-coded `min_threads` above the
/// core count makes `AdaptiveWorkerPool::new` panic. Sizing the pool from this
/// figure keeps the config valid on single-core CI shards and constrained test
/// runners while still exercising real parallelism wherever cores exist.
fn available() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

/// Verify that process_batch uses multiple threads.
///
/// Creates a worker pool with 4 threads, processes 40 messages, and checks
/// that more than 1 unique thread ID was observed. This proves rayon is
/// distributing work across threads, not running sequentially.
///
/// Under coverage tools (tarpaulin), thread scheduling is constrained by
/// instrumentation — parallelism may not manifest. We emit a warning
/// instead of failing in that case.
#[test]
fn test_process_batch_uses_multiple_threads() {
    // Size to the available cores (cap at 4). scalo clamps max_threads to
    // available_parallelism, so a fixed 4 would make min_threads > max_threads
    // on a single-core runner and panic in AdaptiveWorkerPool::new.
    let threads = available().min(4);
    let config = WorkerPoolConfig {
        min_threads: threads,
        max_threads: threads,
        ..Default::default()
    };
    let pool = AdaptiveWorkerPool::new(config);

    let thread_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
    let items: Vec<i32> = (0..40).collect();

    let tids = thread_ids.clone();
    let results: Vec<Result<i32, String>> = pool.process_batch(&items, |&item| {
        tids.lock().insert(std::thread::current().id());
        // Simulate CPU work so threads overlap
        std::thread::sleep(std::time::Duration::from_millis(2));
        Ok(item * 2)
    });

    assert_eq!(results.len(), 40);
    let unique_threads = thread_ids.lock().len();

    // Only assert real parallelism when more than one core is available.
    // Tarpaulin (coverage) also constrains thread scheduling, so skip there.
    #[cfg(not(tarpaulin))]
    if threads > 1 {
        assert!(
            unique_threads > 1,
            "Expected multiple threads, got {unique_threads} — parallelism not working"
        );
    }
    #[cfg(tarpaulin)]
    if unique_threads <= 1 {
        eprintln!(
            "WARNING: only {unique_threads} thread(s) observed under tarpaulin — \
             parallelism not verifiable under coverage instrumentation"
        );
    }
}

/// Verify that the semaphore throttle limits concurrent execution.
///
/// Pool has 4 threads but semaphore starts at 2 (min_threads). Only 2
/// should be active simultaneously.
#[test]
fn test_semaphore_throttle_limits_concurrency() {
    // The semaphore admits at most `min_threads` concurrent tasks. Size both
    // from available cores so the config stays valid on a single-core runner
    // (where max_threads clamps to 1): with 1 core, min == max == 1.
    let max_threads = available().min(4);
    let min_threads = (max_threads / 2).max(1);
    let config = WorkerPoolConfig {
        min_threads,
        max_threads,
        ..Default::default()
    };
    let pool = AdaptiveWorkerPool::new(config);

    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let items: Vec<i32> = (0..20).collect();

    let c = concurrent.clone();
    let mc = max_concurrent.clone();
    let _results: Vec<Result<i32, String>> = pool.process_batch(&items, |&item| {
        let current = c.fetch_add(1, Ordering::SeqCst) + 1;
        mc.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(10));
        c.fetch_sub(1, Ordering::SeqCst);
        Ok(item)
    });

    let observed = max_concurrent.load(Ordering::SeqCst);
    assert!(
        observed <= min_threads,
        "Expected at most {min_threads} concurrent (semaphore floor), got {observed}"
    );
}

/// Verify that CaptureOverrides.derive_config is safe for parallel access.
///
/// Creates a CaptureOverrides and calls derive_config from multiple threads
/// simultaneously. This should never panic or corrupt state.
#[test]
fn test_capture_derive_config_parallel_safety() {
    let metadata = dfe_loader::config::MetadataConfig::default();
    let overrides = CaptureOverrides::new(&metadata);

    // Size to available cores (cap at 4) so the config is valid on a
    // single-core runner; this test asserts correctness, not parallelism.
    let threads = available().min(4);
    let config = WorkerPoolConfig {
        min_threads: threads,
        max_threads: threads,
        ..Default::default()
    };
    let pool = AdaptiveWorkerPool::new(config);

    let tables: Vec<String> = (0..100).map(|i| format!("common.table_{i}")).collect();

    let results: Vec<Result<(), String>> = pool.process_batch(&tables, |table| {
        let config = overrides.derive_config(table);
        // All tables should have default config (Full mode)
        assert_eq!(config.mode, dfe_loader::config::CaptureMode::Full);
        Ok(())
    });

    assert_eq!(results.len(), 100);
    assert!(results.iter().all(Result::is_ok));
}
