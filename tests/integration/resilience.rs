// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Resilience tests — MemoryGuard backpressure and CircuitBreaker state transitions.

use dfe_loader::clickhouse::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};

// === MemoryGuard tests ===

#[test]
fn memory_guard_no_pressure_under_threshold() {
    let guard = MemoryGuard::new(MemoryGuardConfig {
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
    let guard = MemoryGuard::new(MemoryGuardConfig {
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
    let guard = MemoryGuard::new(MemoryGuardConfig {
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

    guard.add_bytes(100);
    guard.add_bytes(200);
    guard.add_bytes(300);
    assert_eq!(guard.current_bytes(), 600);

    guard.release(150);
    assert_eq!(guard.current_bytes(), 450);
}

#[test]
fn memory_guard_exact_release_reaches_zero() {
    // NOTE: MemoryGuard::release() uses AtomicU64::fetch_sub which wraps on
    // underflow. Callers must ensure they don't release more than added.
    // This test documents that exact release works correctly.
    let guard = MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(100);
    guard.release(100); // Exact release is safe
    assert_eq!(guard.current_bytes(), 0, "exact release should reach 0");
}

// === CircuitBreaker tests ===

#[test]
fn circuit_breaker_starts_closed() {
    let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
    assert_eq!(
        cb.get_state("test.table"),
        CircuitState::Closed,
        "new table should be closed"
    );
}

#[test]
fn circuit_breaker_opens_after_failures() {
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);
    let table = "dfe.events";

    // Record failures up to threshold
    for _ in 0..3 {
        cb.record_failure(table);
    }

    assert_eq!(
        cb.get_state(table),
        CircuitState::Open,
        "should open after 3 failures"
    );
}

#[test]
fn circuit_breaker_allows_probe_after_open_duration() {
    let config = CircuitBreakerConfig {
        failure_threshold: 1,
        open_duration: std::time::Duration::ZERO, // Immediate transition for testing
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);
    let table = "dfe.events";

    cb.record_failure(table);
    assert_eq!(cb.get_state(table), CircuitState::Open);

    // With 0s open_duration, should transition to HalfOpen on next allow check
    // (implementation-dependent — checks time elapsed since open)
    let allowed = cb.allow_request(table);
    // After open_duration elapsed (0s), should be HalfOpen and allow one probe
    assert!(
        allowed,
        "should allow probe request after open_duration expires"
    );
    assert_eq!(cb.get_state(table), CircuitState::HalfOpen);
}

#[test]
fn circuit_breaker_closes_after_success_in_half_open() {
    let config = CircuitBreakerConfig {
        failure_threshold: 1,
        success_threshold: 1,
        open_duration: std::time::Duration::ZERO,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);
    let table = "dfe.events";

    // Open it
    cb.record_failure(table);
    assert_eq!(cb.get_state(table), CircuitState::Open);

    // Transition to HalfOpen
    cb.allow_request(table);
    assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

    // Success closes it
    cb.record_success(table);
    assert_eq!(
        cb.get_state(table),
        CircuitState::Closed,
        "should close after success in HalfOpen"
    );
}

#[test]
fn circuit_breaker_reopens_on_failure_in_half_open() {
    let config = CircuitBreakerConfig {
        failure_threshold: 1,
        open_duration: std::time::Duration::ZERO,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);
    let table = "dfe.events";

    // Open -> HalfOpen
    cb.record_failure(table);
    cb.allow_request(table);
    assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

    // Failure in HalfOpen reopens
    cb.record_failure(table);
    assert_eq!(
        cb.get_state(table),
        CircuitState::Open,
        "should reopen after failure in HalfOpen"
    );
}

#[test]
fn circuit_breaker_stats_track_states() {
    let config = CircuitBreakerConfig {
        failure_threshold: 1,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    // All closed initially
    let stats = cb.stats();
    assert_eq!(stats.open_count, 0);

    // Open one table
    cb.record_failure("table_a");
    let stats = cb.stats();
    assert_eq!(stats.open_count, 1);
    assert_eq!(stats.closed_count, 0);

    // Success on another leaves it closed
    cb.record_success("table_b");
    let stats = cb.stats();
    assert_eq!(stats.open_count, 1);
    assert_eq!(stats.closed_count, 1);
}

#[test]
fn circuit_breaker_reset_clears_all() {
    let config = CircuitBreakerConfig {
        failure_threshold: 1,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    cb.record_failure("table_a");
    cb.record_failure("table_b");
    assert_eq!(cb.get_state("table_a"), CircuitState::Open);
    assert_eq!(cb.get_state("table_b"), CircuitState::Open);

    cb.reset("table_a");
    assert_eq!(
        cb.get_state("table_a"),
        CircuitState::Closed,
        "reset table should be closed"
    );
    assert_eq!(
        cb.get_state("table_b"),
        CircuitState::Open,
        "non-reset table should remain open"
    );
}
