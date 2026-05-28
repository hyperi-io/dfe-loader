// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Circuit breaker for per-table failure detection
//!
//! Tracks failure rates per destination table and "opens" the circuit
//! when failures exceed threshold. This prevents cascading failures
//! and allows the system to recover gracefully.
//!
//! ## States
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Failures exceeded threshold, requests are rejected immediately
//! - **Half-Open**: Testing recovery, allowing limited requests
//!
//! ## Configuration
//!
//! - `failure_threshold`: Number of consecutive failures to open circuit
//! - `success_threshold`: Successes in half-open state to close circuit
//! - `open_duration`: How long circuit stays open before half-open
//! - `half_open_max_requests`: Max requests allowed in half-open state

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests pass through
    Closed,
    /// Failures exceeded threshold - requests rejected
    Open,
    /// Testing recovery - limited requests allowed
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Configuration for circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures to open the circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open state to close the circuit
    pub success_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open
    pub open_duration: Duration,
    /// Maximum requests allowed in half-open state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            open_duration: Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }
}

/// Per-table circuit breaker state
struct TableCircuit {
    /// Current state
    state: CircuitState,
    /// Consecutive failure count
    failure_count: u32,
    /// Consecutive success count (used in half-open)
    success_count: u32,
    /// When the circuit was opened
    opened_at: Option<Instant>,
    /// Requests made in half-open state
    half_open_requests: u32,
}

impl TableCircuit {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            opened_at: None,
            half_open_requests: 0,
        }
    }
}

/// Circuit breaker manager for all tables
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Per-table circuit state
    circuits: RwLock<HashMap<String, TableCircuit>>,
    /// Global metrics
    total_opens: AtomicU32,
    total_rejections: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with config
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            circuits: RwLock::new(HashMap::new()),
            total_opens: AtomicU32::new(0),
            total_rejections: AtomicU64::new(0),
        }
    }

    /// Check if request should be allowed for a table
    ///
    /// Returns `true` if request can proceed, `false` if circuit is open.
    pub fn allow_request(&self, table: &str) -> bool {
        let mut circuits = self.circuits.write();
        let circuit = circuits
            .entry(table.to_string())
            .or_insert_with(TableCircuit::new);

        match circuit.state {
            CircuitState::Closed => true,

            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(opened_at) = circuit.opened_at
                    && opened_at.elapsed() >= self.config.open_duration
                {
                    info!(table = %table, "Circuit transitioning to half-open");
                    circuit.state = CircuitState::HalfOpen;
                    circuit.half_open_requests = 0;
                    circuit.success_count = 0;
                    return self.allow_half_open_request(circuit, table);
                }
                debug!(table = %table, "Circuit open, rejecting request");
                self.total_rejections.fetch_add(1, Ordering::Relaxed);
                false
            }

            CircuitState::HalfOpen => self.allow_half_open_request(circuit, table),
        }
    }

    /// Check if a half-open request should be allowed
    fn allow_half_open_request(&self, circuit: &mut TableCircuit, table: &str) -> bool {
        if circuit.half_open_requests < self.config.half_open_max_requests {
            circuit.half_open_requests += 1;
            debug!(
                table = %table,
                requests = circuit.half_open_requests,
                max = self.config.half_open_max_requests,
                "Half-open request allowed"
            );
            true
        } else {
            debug!(table = %table, "Half-open request limit reached, rejecting");
            self.total_rejections.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Record a successful request
    pub fn record_success(&self, table: &str) {
        let mut circuits = self.circuits.write();
        let circuit = circuits
            .entry(table.to_string())
            .or_insert_with(TableCircuit::new);

        match circuit.state {
            CircuitState::Closed => {
                // Reset failure count on success
                circuit.failure_count = 0;
            }

            CircuitState::HalfOpen => {
                circuit.success_count += 1;
                debug!(
                    table = %table,
                    successes = circuit.success_count,
                    threshold = self.config.success_threshold,
                    "Half-open success recorded"
                );

                // Check if we should close the circuit
                if circuit.success_count >= self.config.success_threshold {
                    info!(table = %table, "Circuit closing after recovery");
                    circuit.state = CircuitState::Closed;
                    circuit.failure_count = 0;
                    circuit.success_count = 0;
                    circuit.opened_at = None;
                    circuit.half_open_requests = 0;
                }
            }

            CircuitState::Open => {
                // Shouldn't happen - requests should be rejected
                warn!(table = %table, "Success recorded while circuit open");
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&self, table: &str) {
        let mut circuits = self.circuits.write();
        let circuit = circuits
            .entry(table.to_string())
            .or_insert_with(TableCircuit::new);

        match circuit.state {
            CircuitState::Closed => {
                circuit.failure_count += 1;
                debug!(
                    table = %table,
                    failures = circuit.failure_count,
                    threshold = self.config.failure_threshold,
                    "Failure recorded"
                );

                // Check if we should open the circuit
                if circuit.failure_count >= self.config.failure_threshold {
                    warn!(
                        table = %table,
                        failures = circuit.failure_count,
                        "Circuit opening due to consecutive failures"
                    );
                    circuit.state = CircuitState::Open;
                    circuit.opened_at = Some(Instant::now());
                    self.total_opens.fetch_add(1, Ordering::Relaxed);
                }
            }

            CircuitState::HalfOpen => {
                // Any failure in half-open state reopens the circuit
                warn!(table = %table, "Failure in half-open state, reopening circuit");
                circuit.state = CircuitState::Open;
                circuit.opened_at = Some(Instant::now());
                circuit.success_count = 0;
                circuit.half_open_requests = 0;
                self.total_opens.fetch_add(1, Ordering::Relaxed);
            }

            CircuitState::Open => {
                // Already open, just update timestamp
                circuit.opened_at = Some(Instant::now());
            }
        }
    }

    /// Get the current state for a table
    pub fn get_state(&self, table: &str) -> CircuitState {
        let circuits = self.circuits.read();
        circuits
            .get(table)
            .map_or(CircuitState::Closed, |c| c.state)
    }

    /// Per-table circuit state for metrics emission.
    ///
    /// Returns `(table_name, state_u8)` where state is 0=closed, 1=open, 2=half-open.
    pub fn per_table_states(&self) -> Vec<(String, u8)> {
        let circuits = self.circuits.read();
        circuits
            .iter()
            .map(|(table, c)| {
                let state = match c.state {
                    CircuitState::Closed => 0,
                    CircuitState::Open => 1,
                    CircuitState::HalfOpen => 2,
                };
                (table.clone(), state)
            })
            .collect()
    }

    /// Get all tables with open circuits
    pub fn get_open_circuits(&self) -> Vec<String> {
        let circuits = self.circuits.read();
        circuits
            .iter()
            .filter(|(_, c)| c.state == CircuitState::Open)
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Get circuit breaker statistics
    pub fn stats(&self) -> CircuitBreakerStats {
        let circuits = self.circuits.read();
        let mut open_count = 0;
        let mut half_open_count = 0;
        let mut closed_count = 0;

        for circuit in circuits.values() {
            match circuit.state {
                CircuitState::Open => open_count += 1,
                CircuitState::HalfOpen => half_open_count += 1,
                CircuitState::Closed => closed_count += 1,
            }
        }

        CircuitBreakerStats {
            total_tables: circuits.len(),
            open_count,
            half_open_count,
            closed_count,
            total_opens: self.total_opens.load(Ordering::Relaxed),
            total_rejections: self.total_rejections.load(Ordering::Relaxed),
        }
    }

    /// Reset circuit for a table (manual override)
    pub fn reset(&self, table: &str) {
        let mut circuits = self.circuits.write();
        if let Some(circuit) = circuits.get_mut(table) {
            info!(table = %table, "Circuit manually reset");
            circuit.state = CircuitState::Closed;
            circuit.failure_count = 0;
            circuit.success_count = 0;
            circuit.opened_at = None;
            circuit.half_open_requests = 0;
        }
    }

    /// Reset all circuits (e.g., after config change)
    pub fn reset_all(&self) {
        let mut circuits = self.circuits.write();
        circuits.clear();
        info!("All circuits reset");
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub total_tables: usize,
    pub open_count: usize,
    pub half_open_count: usize,
    pub closed_count: usize,
    pub total_opens: u32,
    pub total_rejections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            open_duration: Duration::from_millis(100),
            half_open_max_requests: 2,
        }
    }

    #[test]
    fn test_circuit_starts_closed() {
        let cb = CircuitBreaker::new(test_config());
        assert_eq!(cb.get_state("test.table"), CircuitState::Closed);
        assert!(cb.allow_request("test.table"));
    }

    #[test]
    fn test_circuit_opens_after_failures() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // First two failures - still closed
        cb.record_failure(table);
        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Closed);
        assert!(cb.allow_request(table));

        // Third failure - opens circuit
        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Open);
        assert!(!cb.allow_request(table));
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Two failures
        cb.record_failure(table);
        cb.record_failure(table);

        // One success resets
        cb.record_success(table);

        // Need 3 more failures to open
        cb.record_failure(table);
        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Closed);

        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Open);
    }

    #[test]
    fn test_circuit_transitions_to_half_open() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        // Wait for open_duration
        std::thread::sleep(Duration::from_millis(150));

        // Next request should transition to half-open
        assert!(cb.allow_request(table));
        assert_eq!(cb.get_state(table), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_closes_on_success() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Open and wait
        for _ in 0..3 {
            cb.record_failure(table);
        }
        std::thread::sleep(Duration::from_millis(150));

        // Transition to half-open
        cb.allow_request(table);
        assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

        // Two successes should close (threshold is 2)
        cb.record_success(table);
        cb.record_success(table);
        assert_eq!(cb.get_state(table), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Open and wait
        for _ in 0..3 {
            cb.record_failure(table);
        }
        std::thread::sleep(Duration::from_millis(150));

        // Transition to half-open
        cb.allow_request(table);
        assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

        // One failure reopens
        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Open);
    }

    #[test]
    fn test_half_open_request_limit() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Open and wait
        for _ in 0..3 {
            cb.record_failure(table);
        }
        std::thread::sleep(Duration::from_millis(150));

        // First request transitions to half-open and is allowed
        assert!(cb.allow_request(table));

        // Second request allowed (max is 2)
        assert!(cb.allow_request(table));

        // Third request rejected (limit reached)
        assert!(!cb.allow_request(table));
    }

    #[test]
    fn test_stats() {
        let cb = CircuitBreaker::new(test_config());

        // Create some circuits in different states
        cb.allow_request("closed.table");

        for _ in 0..3 {
            cb.record_failure("open.table");
        }

        let stats = cb.stats();
        assert_eq!(stats.total_tables, 2);
        assert_eq!(stats.closed_count, 1);
        assert_eq!(stats.open_count, 1);
        assert_eq!(stats.total_opens, 1);
    }

    #[test]
    fn test_reset() {
        let cb = CircuitBreaker::new(test_config());
        let table = "test.table";

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        // Reset
        cb.reset(table);
        assert_eq!(cb.get_state(table), CircuitState::Closed);
        assert!(cb.allow_request(table));
    }

    #[test]
    fn test_independent_tables() {
        let cb = CircuitBreaker::new(test_config());

        // Open circuit for table_a
        for _ in 0..3 {
            cb.record_failure("table_a");
        }

        // table_a is open
        assert_eq!(cb.get_state("table_a"), CircuitState::Open);
        assert!(!cb.allow_request("table_a"));

        // table_b is still closed
        assert_eq!(cb.get_state("table_b"), CircuitState::Closed);
        assert!(cb.allow_request("table_b"));
    }

    // ========================================================================
    // reset_all()
    // ========================================================================

    #[test]
    fn test_reset_all_clears_all_circuits() {
        let cb = CircuitBreaker::new(test_config());

        // Open circuits for multiple tables
        for _ in 0..3 {
            cb.record_failure("table_a");
            cb.record_failure("table_b");
            cb.record_failure("table_c");
        }

        assert_eq!(cb.get_state("table_a"), CircuitState::Open);
        assert_eq!(cb.get_state("table_b"), CircuitState::Open);
        assert_eq!(cb.get_state("table_c"), CircuitState::Open);

        // stats reflects three registered tables
        let pre = cb.stats();
        assert_eq!(pre.total_tables, 3);
        assert_eq!(pre.open_count, 3);

        // reset_all clears the entire map
        cb.reset_all();

        // After reset_all, no tables are registered → get_state returns Closed default
        let post = cb.stats();
        assert_eq!(
            post.total_tables, 0,
            "reset_all should clear all circuit state"
        );
        assert_eq!(post.open_count, 0);
        assert_eq!(post.half_open_count, 0);
        assert_eq!(post.closed_count, 0);

        // But total_opens counter is NOT cleared (it's a cumulative metric)
        assert_eq!(
            post.total_opens, 3,
            "cumulative total_opens metric should survive reset_all"
        );

        // Requests on previously-open circuits should now proceed (fresh default)
        assert!(cb.allow_request("table_a"));
        assert!(cb.allow_request("table_b"));
        assert!(cb.allow_request("table_c"));
    }

    #[test]
    fn test_reset_all_on_empty() {
        let cb = CircuitBreaker::new(test_config());
        // reset_all on an empty breaker should not panic or misbehave
        cb.reset_all();
        let stats = cb.stats();
        assert_eq!(stats.total_tables, 0);
        assert_eq!(stats.total_opens, 0);
        assert_eq!(stats.total_rejections, 0);
    }

    // ========================================================================
    // get_open_circuits()
    // ========================================================================

    #[test]
    fn test_get_open_circuits_when_all_closed() {
        let cb = CircuitBreaker::new(test_config());
        // Register two tables in closed state via allow_request
        cb.allow_request("healthy.a");
        cb.allow_request("healthy.b");

        let open = cb.get_open_circuits();
        assert!(open.is_empty(), "No circuits should be open, got: {open:?}");
    }

    #[test]
    fn test_get_open_circuits_when_empty() {
        let cb = CircuitBreaker::new(test_config());
        // Nothing registered at all
        assert!(cb.get_open_circuits().is_empty());
    }

    #[test]
    fn test_get_open_circuits_filters_correctly() {
        let cb = CircuitBreaker::new(test_config());
        // Table A: closed (healthy)
        cb.allow_request("a.closed");
        // Table B: open
        for _ in 0..3 {
            cb.record_failure("b.open");
        }
        // Table C: open, then transition to half-open
        for _ in 0..3 {
            cb.record_failure("c.half_open");
        }
        std::thread::sleep(Duration::from_millis(150));
        cb.allow_request("c.half_open");

        let open = cb.get_open_circuits();
        assert_eq!(open.len(), 1, "Only 'b.open' should be Open, got: {open:?}");
        assert_eq!(open[0], "b.open");
    }

    // ========================================================================
    // Rapid state transitions
    // ========================================================================

    #[test]
    fn test_rapid_closed_open_half_open_closed_open_transitions() {
        let cb = CircuitBreaker::new(test_config());
        let table = "rapid.transitions";

        // Starts closed
        assert_eq!(cb.get_state(table), CircuitState::Closed);

        // Transition 1: closed -> open (3 failures)
        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        // Transition 2: open -> half-open (wait, probe)
        std::thread::sleep(Duration::from_millis(150));
        assert!(cb.allow_request(table));
        assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

        // Transition 3: half-open -> closed (2 successes)
        cb.record_success(table);
        cb.record_success(table);
        assert_eq!(cb.get_state(table), CircuitState::Closed);

        // Transition 4: closed -> open again
        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        // total_opens should record TWO open events
        let stats = cb.stats();
        assert_eq!(
            stats.total_opens, 2,
            "Two open transitions expected, got {}",
            stats.total_opens
        );
    }

    #[test]
    fn test_half_open_failure_increments_total_opens() {
        let cb = CircuitBreaker::new(test_config());
        let table = "reopen.table";

        // Open (1st open event)
        for _ in 0..3 {
            cb.record_failure(table);
        }
        std::thread::sleep(Duration::from_millis(150));

        // Half-open probe
        cb.allow_request(table);
        assert_eq!(cb.get_state(table), CircuitState::HalfOpen);

        // Failure reopens (2nd open event — this is the critical path)
        cb.record_failure(table);
        assert_eq!(cb.get_state(table), CircuitState::Open);

        let stats = cb.stats();
        assert_eq!(
            stats.total_opens, 2,
            "Half-open -> open should count as a new open"
        );
    }

    #[test]
    fn test_success_while_open_does_not_close() {
        // Edge case: if record_success is called while the circuit is Open
        // (e.g. a racey caller), it should NOT transition to closed.
        let cb = CircuitBreaker::new(test_config());
        let table = "racey.table";

        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        cb.record_success(table);
        // State should remain Open — success in Open state is a no-op (logged warn)
        assert_eq!(cb.get_state(table), CircuitState::Open);
    }

    #[test]
    fn test_failure_while_open_refreshes_opened_at() {
        // Additional failures while Open should refresh the opened_at timestamp,
        // effectively resetting the countdown to half-open.
        let cb = CircuitBreaker::new(test_config());
        let table = "persistent.failure";

        for _ in 0..3 {
            cb.record_failure(table);
        }
        assert_eq!(cb.get_state(table), CircuitState::Open);

        // Wait nearly the full open_duration, then record another failure
        std::thread::sleep(Duration::from_millis(80));
        cb.record_failure(table);

        // Since failure refreshed opened_at, we should still need to wait
        // the full 100ms from THIS failure. Check after only 50ms: still open.
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !cb.allow_request(table),
            "Circuit should stay open — failure refreshed opened_at"
        );
        assert_eq!(cb.get_state(table), CircuitState::Open);
    }

    // ========================================================================
    // CircuitBreakerStats
    // ========================================================================

    #[test]
    fn test_stats_all_fields() {
        let cb = CircuitBreaker::new(test_config());

        // Table A: closed
        cb.allow_request("a.closed");

        // Table B: open
        for _ in 0..3 {
            cb.record_failure("b.open");
        }

        // Table C: half-open
        for _ in 0..3 {
            cb.record_failure("c.half_open");
        }
        std::thread::sleep(Duration::from_millis(150));
        cb.allow_request("c.half_open");

        // Table D: open and rejected requests (counting rejections)
        for _ in 0..3 {
            cb.record_failure("d.rejected");
        }
        for _ in 0..5 {
            cb.allow_request("d.rejected"); // 5 rejected requests
        }

        let stats = cb.stats();
        assert_eq!(stats.total_tables, 4);
        assert_eq!(stats.closed_count, 1);
        assert_eq!(stats.open_count, 2); // b.open, d.rejected
        assert_eq!(stats.half_open_count, 1); // c.half_open
        assert_eq!(
            stats.total_opens, 3,
            "3 circuits opened, got {}",
            stats.total_opens
        );
        assert!(
            stats.total_rejections >= 5,
            "Expected >= 5 rejections, got {}",
            stats.total_rejections
        );
    }

    #[test]
    fn test_stats_after_reset() {
        let cb = CircuitBreaker::new(test_config());
        for _ in 0..3 {
            cb.record_failure("t");
        }

        // Cause rejections to increment counter
        cb.allow_request("t");
        cb.allow_request("t");

        let pre = cb.stats();
        assert_eq!(pre.open_count, 1);
        assert!(pre.total_rejections >= 2);

        // reset() only clears per-table state, not counters
        cb.reset("t");
        let post = cb.stats();
        assert_eq!(post.open_count, 0);
        assert_eq!(post.closed_count, 1);
        // Cumulative counter preserved
        assert_eq!(post.total_opens, pre.total_opens);
        assert_eq!(post.total_rejections, pre.total_rejections);
    }

    #[test]
    fn test_reset_nonexistent_table_noop() {
        let cb = CircuitBreaker::new(test_config());
        // Should not panic
        cb.reset("never.registered");
        let stats = cb.stats();
        assert_eq!(stats.total_tables, 0);
    }

    // ========================================================================
    // Concurrent access from multiple threads
    // ========================================================================

    #[test]
    fn test_concurrent_failure_recording() {
        use std::sync::Arc;

        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 50,
            success_threshold: 2,
            open_duration: Duration::from_secs(60),
            half_open_max_requests: 3,
        }));

        let thread_count = 8;
        let failures_per_thread = 20;

        let mut handles = Vec::with_capacity(thread_count);
        for tid in 0..thread_count {
            let cb = Arc::clone(&cb);
            handles.push(std::thread::spawn(move || {
                for _ in 0..failures_per_thread {
                    cb.record_failure("shared.table");
                    // Also record some across different tables to stress the map
                    cb.record_failure(&format!("thread.{tid}"));
                }
            }));
        }

        for h in handles {
            h.join().expect("thread join");
        }

        // shared.table saw 8*20 = 160 failures, well above threshold (50)
        assert_eq!(
            cb.get_state("shared.table"),
            CircuitState::Open,
            "High failure count should open the shared circuit"
        );

        // Each per-thread table saw 20 failures, below threshold (50) → closed
        for tid in 0..thread_count {
            assert_eq!(
                cb.get_state(&format!("thread.{tid}")),
                CircuitState::Closed,
                "Low-failure tables should stay closed"
            );
        }

        let stats = cb.stats();
        // total_tables = 1 shared + 8 per-thread
        assert_eq!(stats.total_tables, 1 + thread_count);
    }

    #[test]
    fn test_concurrent_mixed_success_failure() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 10,
            success_threshold: 2,
            open_duration: Duration::from_millis(50),
            half_open_max_requests: 5,
        }));

        let success_counter = Arc::new(AtomicU32::new(0));
        let rejected_counter = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        // Writers: record failures
        for _ in 0..4 {
            let cb = Arc::clone(&cb);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    cb.record_failure("t");
                }
            }));
        }
        // Readers: probe state, track outcomes
        for _ in 0..4 {
            let cb = Arc::clone(&cb);
            let succ = Arc::clone(&success_counter);
            let rej = Arc::clone(&rejected_counter);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    if cb.allow_request("t") {
                        succ.fetch_add(1, Ordering::Relaxed);
                    } else {
                        rej.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().expect("thread join");
        }

        // All operations were atomic — must not deadlock, must not panic.
        // Eventually the circuit should be Open (200 failures, threshold 10)
        // or HalfOpen (transition from probe). Must NOT be uninitialised.
        let state = cb.get_state("t");
        assert!(
            matches!(state, CircuitState::Open | CircuitState::HalfOpen),
            "Expected Open or HalfOpen after 200 failures, got {state:?}"
        );

        // Verify totals summed correctly with no lost updates
        let total_probes =
            success_counter.load(Ordering::Relaxed) + rejected_counter.load(Ordering::Relaxed);
        assert_eq!(
            total_probes, 200,
            "All 4*50 = 200 probes must be accounted for"
        );
    }

    #[test]
    fn test_concurrent_reset_with_failures() {
        use std::sync::Arc;

        let cb = Arc::new(CircuitBreaker::new(test_config()));

        // Open the circuit first
        for _ in 0..3 {
            cb.record_failure("reset.race");
        }
        assert_eq!(cb.get_state("reset.race"), CircuitState::Open);

        // Thread 1: rapidly records failures
        let cb1 = Arc::clone(&cb);
        let writer = std::thread::spawn(move || {
            for _ in 0..100 {
                cb1.record_failure("reset.race");
            }
        });

        // Thread 2: periodically resets
        let cb2 = Arc::clone(&cb);
        let resetter = std::thread::spawn(move || {
            for _ in 0..10 {
                cb2.reset("reset.race");
                std::thread::yield_now();
            }
        });

        writer.join().expect("writer thread");
        resetter.join().expect("resetter thread");

        // Both threads completed without deadlock/panic. Final state is
        // non-deterministic (depends on interleaving) but must be a valid enum.
        let final_state = cb.get_state("reset.race");
        assert!(
            matches!(
                final_state,
                CircuitState::Closed | CircuitState::Open | CircuitState::HalfOpen
            ),
            "Final state must be a valid variant: {final_state:?}"
        );
    }

    #[test]
    fn test_per_table_states_returns_all_tables() {
        let cb = CircuitBreaker::new(test_config());

        // Closed
        cb.allow_request("a");
        // Open
        for _ in 0..3 {
            cb.record_failure("b");
        }

        let states = cb.per_table_states();
        assert_eq!(states.len(), 2);

        let map: std::collections::HashMap<_, _> = states.into_iter().collect();
        assert_eq!(map.get("a"), Some(&0), "Closed = 0");
        assert_eq!(map.get("b"), Some(&1), "Open = 1");
    }

    #[test]
    fn test_circuit_state_display() {
        // Covers the Display impl
        assert_eq!(format!("{}", CircuitState::Closed), "closed");
        assert_eq!(format!("{}", CircuitState::Open), "open");
        assert_eq!(format!("{}", CircuitState::HalfOpen), "half-open");
    }

    #[test]
    fn test_default_circuit_breaker_config() {
        let cfg = CircuitBreakerConfig::default();
        assert_eq!(cfg.failure_threshold, 5);
        assert_eq!(cfg.success_threshold, 3);
        assert_eq!(cfg.open_duration, Duration::from_secs(30));
        assert_eq!(cfg.half_open_max_requests, 3);
    }

    #[test]
    fn test_default_circuit_breaker_impl() {
        let cb = CircuitBreaker::default();
        let stats = cb.stats();
        assert_eq!(stats.total_tables, 0);
        assert_eq!(stats.total_opens, 0);
    }
}
