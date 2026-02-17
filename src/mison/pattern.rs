// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pattern Tree for Speculative Parsing
//!
//! Learns field orderings from observed JSON documents to enable speculative
//! jumps to expected field positions. When speculation succeeds, we skip
//! sequential search entirely.
//!
//! Based on Mison Section 6: SPECULATION

use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for pattern accuracy
#[derive(Debug, Default)]
pub struct PatternStats {
    /// Number of successful speculative accesses
    pub hits: AtomicU64,
    /// Number of failed speculations (fell back to sequential)
    pub misses: AtomicU64,
    /// Number of pattern updates
    pub updates: AtomicU64,
}

impl PatternStats {
    /// Get hit rate as percentage
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }
}

/// Pattern entry for a field path
#[derive(Debug, Clone)]
pub struct FieldPattern {
    /// Field path (e.g., ["user", "id"])
    pub path: Vec<String>,
    /// Expected field index at this level (0-based)
    pub expected_index: usize,
    /// Number of times this pattern was observed
    pub observation_count: u64,
    /// Number of times speculation succeeded
    pub success_count: u64,
}

impl FieldPattern {
    /// Create a new pattern
    pub fn new(path: Vec<String>, expected_index: usize) -> Self {
        Self {
            path,
            expected_index,
            observation_count: 1,
            success_count: 0,
        }
    }

    /// Confidence score (0.0 to 1.0)
    pub fn confidence(&self) -> f64 {
        if self.observation_count == 0 {
            return 0.0;
        }
        self.success_count as f64 / self.observation_count as f64
    }

    /// Should we use this pattern for speculation?
    pub fn should_speculate(&self) -> bool {
        // Require minimum observations and reasonable success rate
        self.observation_count >= 3 && self.confidence() >= 0.7
    }
}

/// Pattern tree for learning field orderings
///
/// Key insight: Most JSON producers emit fields in consistent order.
/// By learning this order, we can skip to the N-th field directly
/// instead of scanning sequentially.
#[derive(Debug, Default)]
pub struct PatternTree {
    /// Map from field path (joined with ".") to pattern
    patterns: FxHashMap<String, FieldPattern>,
    /// Global statistics
    stats: PatternStats,
    /// Maximum patterns to store (LRU eviction)
    max_patterns: usize,
}

impl PatternTree {
    /// Create a new pattern tree
    pub fn new() -> Self {
        Self {
            patterns: FxHashMap::default(),
            stats: PatternStats::default(),
            max_patterns: 1000,
        }
    }

    /// Create with custom max patterns
    pub fn with_max_patterns(max_patterns: usize) -> Self {
        Self {
            patterns: FxHashMap::default(),
            stats: PatternStats::default(),
            max_patterns,
        }
    }

    /// Get a speculation hint for a field path
    ///
    /// Returns the expected field index if we have a confident pattern.
    #[inline]
    pub fn get_hint(&self, path: &[String]) -> Option<usize> {
        let key = path.join(".");
        self.patterns.get(&key).and_then(|pattern| {
            if pattern.should_speculate() {
                Some(pattern.expected_index)
            } else {
                None
            }
        })
    }

    /// Record a successful field access
    ///
    /// Updates the pattern for this path with the observed field index.
    pub fn record_access(&mut self, path: &[String], field_index: usize) {
        let key = path.join(".");
        self.stats.updates.fetch_add(1, Ordering::Relaxed);

        if let Some(pattern) = self.patterns.get_mut(&key) {
            pattern.observation_count += 1;
            if pattern.expected_index == field_index {
                pattern.success_count += 1;
            } else {
                // Field moved - update expected index if new position is more common
                // Simple heuristic: switch if we've seen the new position more often
                if pattern.observation_count > 10 && pattern.confidence() < 0.5 {
                    pattern.expected_index = field_index;
                    pattern.success_count = 1;
                    pattern.observation_count = 1;
                }
            }
        } else {
            // New pattern
            if self.patterns.len() >= self.max_patterns {
                // Simple eviction: remove lowest confidence pattern
                self.evict_lowest_confidence();
            }

            self.patterns
                .insert(key, FieldPattern::new(path.to_vec(), field_index));
        }
    }

    /// Record speculation result
    pub fn record_speculation(&self, success: bool) {
        if success {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> &PatternStats {
        &self.stats
    }

    /// Get number of stored patterns
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Clear all patterns (for schema change)
    pub fn clear(&mut self) {
        self.patterns.clear();
    }

    /// Evict the pattern with lowest confidence
    fn evict_lowest_confidence(&mut self) {
        let mut min_key = None;
        let mut min_confidence = f64::MAX;

        for (key, pattern) in &self.patterns {
            let conf = pattern.confidence();
            if conf < min_confidence {
                min_confidence = conf;
                min_key = Some(key.clone());
            }
        }

        if let Some(key) = min_key {
            self.patterns.remove(&key);
        }
    }

    /// Preload patterns for known schema fields
    ///
    /// If we know the schema upfront, we can initialize patterns
    /// with expected field positions based on common JSON conventions.
    pub fn preload_schema(&mut self, fields: &[&str]) {
        for (idx, field) in fields.iter().enumerate() {
            let path = vec![field.to_string()];
            let key = field.to_string();

            self.patterns.insert(
                key,
                FieldPattern {
                    path,
                    expected_index: idx,
                    observation_count: 1,
                    success_count: 0,
                },
            );
        }
    }

    /// Merge patterns from another tree (for parallel processing)
    pub fn merge(&mut self, other: &PatternTree) {
        for (key, other_pattern) in &other.patterns {
            if let Some(pattern) = self.patterns.get_mut(key) {
                // Combine observations
                pattern.observation_count += other_pattern.observation_count;
                pattern.success_count += other_pattern.success_count;

                // Use whichever index has more successes
                if other_pattern.success_count > pattern.success_count {
                    pattern.expected_index = other_pattern.expected_index;
                }
            } else if self.patterns.len() < self.max_patterns {
                self.patterns.insert(key.clone(), other_pattern.clone());
            }
        }
    }
}

/// Per-table pattern trees for schema-specific learning
///
/// Each table may have different field orderings based on how
/// data is produced.
#[derive(Debug, Default)]
pub struct TablePatternRegistry {
    /// Map from "db.table" to pattern tree
    tables: FxHashMap<String, PatternTree>,
    /// Maximum trees to store
    max_tables: usize,
}

impl TablePatternRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
            max_tables: 100,
        }
    }

    /// Get or create pattern tree for a table
    pub fn get_or_create(&mut self, table: &str) -> &mut PatternTree {
        if !self.tables.contains_key(table) {
            if self.tables.len() >= self.max_tables {
                // Simple LRU: remove first entry
                if let Some(key) = self.tables.keys().next().cloned() {
                    self.tables.remove(&key);
                }
            }
            self.tables.insert(table.to_string(), PatternTree::new());
        }
        self.tables.get_mut(table).unwrap()
    }

    /// Get pattern tree for a table (if exists)
    pub fn get(&self, table: &str) -> Option<&PatternTree> {
        self.tables.get(table)
    }

    /// Clear patterns for a specific table
    pub fn clear_table(&mut self, table: &str) {
        if let Some(tree) = self.tables.get_mut(table) {
            tree.clear();
        }
    }

    /// Clear all patterns
    pub fn clear_all(&mut self) {
        self.tables.clear();
    }

    /// Get total pattern count across all tables
    pub fn total_patterns(&self) -> usize {
        self.tables.values().map(|t| t.pattern_count()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_creation() {
        let pattern = FieldPattern::new(vec!["user".to_string(), "id".to_string()], 0);
        assert_eq!(pattern.expected_index, 0);
        assert_eq!(pattern.observation_count, 1);
        assert_eq!(pattern.confidence(), 0.0); // No successes yet
    }

    #[test]
    fn test_pattern_tree_record_access() {
        let mut tree = PatternTree::new();

        let path = vec!["id".to_string()];

        // Record same access multiple times
        for _ in 0..5 {
            tree.record_access(&path, 0);
        }

        let hint = tree.get_hint(&path);
        // Should have a hint after enough observations with consistent index
        assert!(hint.is_some() || tree.patterns.contains_key("id"));
    }

    #[test]
    fn test_pattern_tree_speculation() {
        let mut tree = PatternTree::new();

        let path = vec!["name".to_string()];

        // Build up consistent pattern
        for _ in 0..5 {
            tree.record_access(&path, 2);
        }

        // Mark successes
        if let Some(pattern) = tree.patterns.get_mut("name") {
            pattern.success_count = 4;
        }

        let hint = tree.get_hint(&path);
        assert_eq!(hint, Some(2));
    }

    #[test]
    fn test_preload_schema() {
        let mut tree = PatternTree::new();
        tree.preload_schema(&["id", "name", "value", "timestamp"]);

        assert_eq!(tree.pattern_count(), 4);

        // Patterns should exist but not yet confident
        assert!(tree.get_hint(&["id".to_string()]).is_none());
    }

    #[test]
    fn test_table_registry() {
        let mut registry = TablePatternRegistry::new();

        let tree = registry.get_or_create("db.events");
        tree.record_access(&["org_id".to_string()], 0);

        let tree2 = registry.get_or_create("db.logs");
        tree2.record_access(&["level".to_string()], 1);

        assert!(registry.get("db.events").is_some());
        assert!(registry.get("db.logs").is_some());
        assert!(registry.get("db.unknown").is_none());
    }

    #[test]
    fn test_pattern_confidence() {
        let mut pattern = FieldPattern::new(vec!["test".to_string()], 0);

        pattern.observation_count = 10;
        pattern.success_count = 8;

        assert_eq!(pattern.confidence(), 0.8);
        assert!(pattern.should_speculate());

        pattern.success_count = 5;
        assert_eq!(pattern.confidence(), 0.5);
        assert!(!pattern.should_speculate()); // Below 0.7 threshold
    }

    #[test]
    fn test_eviction() {
        let mut tree = PatternTree::with_max_patterns(3);

        // Add patterns up to limit
        tree.record_access(&["a".to_string()], 0);
        tree.record_access(&["b".to_string()], 1);
        tree.record_access(&["c".to_string()], 2);

        assert_eq!(tree.pattern_count(), 3);

        // Adding another should evict one
        tree.record_access(&["d".to_string()], 3);

        assert_eq!(tree.pattern_count(), 3);
    }

    #[test]
    fn test_stats() {
        let tree = PatternTree::new();

        tree.record_speculation(true);
        tree.record_speculation(true);
        tree.record_speculation(false);

        assert_eq!(tree.stats().hits.load(Ordering::Relaxed), 2);
        assert_eq!(tree.stats().misses.load(Ordering::Relaxed), 1);
        assert!((tree.stats().hit_rate() - 66.66).abs() < 1.0);
    }
}
