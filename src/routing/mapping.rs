// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Source value to table name mappings

use std::collections::HashMap;

/// Maps _source values to `ClickHouse` table names
pub struct SourceMapping {
    mappings: HashMap<String, String>,
}

impl SourceMapping {
    /// Create a new source mapping
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    /// Add a mapping
    pub fn insert(&mut self, source: String, table: String) {
        self.mappings.insert(source, table);
    }

    /// Get the table for a source value
    pub fn get(&self, source: &str) -> Option<&String> {
        self.mappings.get(source)
    }
}

impl Default for SourceMapping {
    fn default() -> Self {
        Self::new()
    }
}
