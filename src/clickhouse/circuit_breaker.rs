// SPDX-License-Identifier: FSL-1.1-ALv2
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
}
