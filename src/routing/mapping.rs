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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_mapping_new_is_empty() {
        let m = SourceMapping::new();
        assert!(m.get("anything").is_none());
    }

    #[test]
    fn source_mapping_default_is_empty() {
        let m = SourceMapping::default();
        assert!(m.get("any").is_none());
    }

    #[test]
    fn source_mapping_insert_and_get() {
        let mut m = SourceMapping::new();
        m.insert("beats".to_string(), "beats_table".to_string());
        assert_eq!(m.get("beats"), Some(&"beats_table".to_string()));
    }

    #[test]
    fn source_mapping_overwrites_on_duplicate_key() {
        let mut m = SourceMapping::new();
        m.insert("k".to_string(), "v1".to_string());
        m.insert("k".to_string(), "v2".to_string());
        assert_eq!(m.get("k"), Some(&"v2".to_string()));
    }

    #[test]
    fn source_mapping_get_nonexistent_returns_none() {
        let mut m = SourceMapping::new();
        m.insert("one".to_string(), "table_one".to_string());
        assert!(m.get("two").is_none());
        assert!(m.get("").is_none());
    }

    #[test]
    fn source_mapping_get_unicode_keys() {
        let mut m = SourceMapping::new();
        m.insert("日本語".to_string(), "jp_table".to_string());
        assert_eq!(m.get("日本語"), Some(&"jp_table".to_string()));
    }

    #[test]
    fn source_mapping_many_entries_independent() {
        let mut m = SourceMapping::new();
        for i in 0..50 {
            m.insert(format!("src_{i}"), format!("table_{i}"));
        }
        for i in 0..50 {
            assert_eq!(m.get(&format!("src_{i}")), Some(&format!("table_{i}")));
        }
        assert!(m.get("src_999").is_none());
    }

    #[test]
    fn source_mapping_empty_string_key_allowed() {
        let mut m = SourceMapping::new();
        m.insert(String::new(), "empty_table".to_string());
        assert_eq!(m.get(""), Some(&"empty_table".to_string()));
    }
}
