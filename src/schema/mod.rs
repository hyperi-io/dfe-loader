// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Table metadata tags
//!
//! Tables support metadata tags stored in the `ClickHouse` COMMENT field,
//! using `@tag: key=value` syntax:
//!
//! ```sql
//! COMMENT '@schema_source: core | @schema_version: 2 | @created_by: dfe-engine'
//! ```
//!
//! dfe-loader reads these tags at runtime to detect capture configuration
//! overrides (e.g., whether to capture `_raw` for a given table).

use std::collections::BTreeMap;

/// Table-level tags stored in `ClickHouse` COMMENT field
///
/// Uses `@tag: key=value` syntax.
/// Tags are separated by ` | ` (pipe with spaces) for readability.
///
/// # Example
///
/// ```
/// use dfe_loader::schema::TableTags;
///
/// let tags = TableTags::new()
///     .with("schema_source", "core")
///     .with("custom_field", "custom_value");
///
/// assert!(tags.to_comment().contains("@schema_source: core"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct TableTags {
    /// Key-value pairs (ordered for deterministic output)
    tags: BTreeMap<String, String>,
}

impl TableTags {
    /// Create empty tags
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a tag (builder pattern)
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Add a tag in-place
    pub fn set(&mut self, key: &str, value: &str) {
        self.tags.insert(key.to_string(), value.to_string());
    }

    /// Get a tag value
    pub fn get(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(std::string::String::as_str)
    }

    /// Check if a tag exists
    pub fn has(&self, key: &str) -> bool {
        self.tags.contains_key(key)
    }

    /// Remove a tag
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.tags.remove(key)
    }

    /// Check if tags are empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Iterate over tags
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tags.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Format as `ClickHouse` COMMENT string
    ///
    /// Uses `@key: value` syntax, separated by ` | `
    pub fn to_comment(&self) -> String {
        self.tags
            .iter()
            .map(|(k, v)| format!("@{k}: {v}"))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    /// Parse tags from a `ClickHouse` COMMENT string
    ///
    /// Expects `@key: value` pairs separated by ` | `
    pub fn from_comment(comment: &str) -> Self {
        let mut tags = BTreeMap::new();

        for part in comment.split(" | ") {
            let part = part.trim();
            if let Some(stripped) = part.strip_prefix('@')
                && let Some((key, value)) = stripped.split_once(':')
            {
                tags.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        Self { tags }
    }
}

impl std::fmt::Display for TableTags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_comment())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_tags_custom() {
        let tags = TableTags::new()
            .with("schema_source", "core")
            .with("custom_tag", "custom_value");

        assert_eq!(tags.len(), 2);
        assert_eq!(tags.get("custom_tag"), Some("custom_value"));
    }

    #[test]
    fn test_table_tags_to_comment() {
        let tags = TableTags::new()
            .with("schema_source", "core")
            .with("schema_version", "2");
        let comment = tags.to_comment();

        assert!(comment.contains("@schema_source: core"));
        assert!(comment.contains("@schema_version: 2"));
        assert!(comment.contains(" | "));
    }

    #[test]
    fn test_table_tags_from_comment() {
        let comment = "@schema_source: core | @schema_version: 2 | @custom: value";
        let tags = TableTags::from_comment(comment);

        assert_eq!(tags.get("schema_source"), Some("core"));
        assert_eq!(tags.get("schema_version"), Some("2"));
        assert_eq!(tags.get("custom"), Some("value"));
        assert_eq!(tags.len(), 3);
    }

    #[test]
    fn test_table_tags_roundtrip() {
        let original = TableTags::new()
            .with("schema_source", "core")
            .with("extra", "data");
        let comment = original.to_comment();
        let parsed = TableTags::from_comment(&comment);

        assert_eq!(original.get("schema_source"), parsed.get("schema_source"));
        assert_eq!(original.get("extra"), parsed.get("extra"));
    }

    #[test]
    fn test_table_tags_empty() {
        let tags = TableTags::new();
        assert!(tags.is_empty());
        assert_eq!(tags.len(), 0);
        assert_eq!(tags.to_comment(), "");
    }

    #[test]
    fn test_table_tags_has() {
        let tags = TableTags::new().with("key", "value");
        assert!(tags.has("key"));
        assert!(!tags.has("nonexistent"));
    }

    #[test]
    fn test_table_tags_remove() {
        let mut tags = TableTags::new().with("key", "value");
        assert_eq!(tags.remove("key"), Some("value".to_string()));
        assert!(!tags.has("key"));
    }
}
