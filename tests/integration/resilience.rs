// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Resilience tests — `MemoryGuard` backpressure.

use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

// === MemoryGuard tests ===

/// A guard reading usage from its own reservations, so a synthetic byte limit
/// means something in a test process whose real cgroup usage dwarfs it.
fn reservation_guard(config: MemoryGuardConfig) -> MemoryGuard {
    MemoryGuard::with_usage_source(config, UsageSource::Reservations)
}

#[test]
fn memory_guard_no_pressure_under_threshold() {
    let guard = reservation_guard(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(700); // 70% of 1000
    assert!(
        !guard.under_pressure(),
        "70% usage should not trigger pressure at 80% threshold"
    );
}

#[test]
fn memory_guard_pressure_above_threshold() {
    let guard = reservation_guard(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(850); // 85% of 1000
    assert!(
        guard.under_pressure(),
        "85% usage should trigger pressure at 80% threshold"
    );
}

#[test]
fn memory_guard_pressure_clears_after_release() {
    let guard = reservation_guard(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(900);
    assert!(guard.under_pressure(), "should be under pressure at 90%");

    guard.release(300);
    assert!(
        !guard.under_pressure(),
        "should not be under pressure after release to 60%"
    );
}

#[test]
fn memory_guard_tracks_bytes_accurately() {
    let guard = MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 10000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    // The lease balance is `reserved_bytes`; `current_bytes` is what the kernel
    // charges the process.
    guard.add_bytes(100);
    guard.add_bytes(200);
    guard.add_bytes(300);
    assert_eq!(guard.reserved_bytes(), 600);

    guard.release(150);
    assert_eq!(guard.reserved_bytes(), 450);
}

#[test]
fn memory_guard_exact_release_reaches_zero() {
    let guard = MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(100);
    guard.release(100);
    assert_eq!(guard.reserved_bytes(), 0, "exact release should reach 0");
}
