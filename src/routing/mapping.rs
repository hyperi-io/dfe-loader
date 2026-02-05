// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Category to table mappings

use std::collections::HashMap;

/// Maps event categories to ClickHouse table names
pub struct CategoryMapping {
    mappings: HashMap<String, String>,
}

impl CategoryMapping {
    /// Create a new category mapping
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    /// Add a mapping
    pub fn insert(&mut self, category: String, table: String) {
        self.mappings.insert(category, table);
    }

    /// Get the table for a category
    pub fn get(&self, category: &str) -> Option<&String> {
        self.mappings.get(category)
    }
}

impl Default for CategoryMapping {
    fn default() -> Self {
        Self::new()
    }
}
