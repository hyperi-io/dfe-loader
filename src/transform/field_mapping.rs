// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Field mapping: rename or copy source fields to destination field names.
//!
//! Supports normalising raw field names to common schemas (ECS, CIM, custom).
//! Rules are pre-computed per table and filtered against the destination schema
//! for O(1) hot-path lookups with zero overhead when disabled.

use serde_json::{Map, Value};

/// Action to take when a mapping rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingAction {
    /// Source field removed, value moved to destination (zero-copy via `data.remove()`).
    Rename,
    /// Source field retained, value cloned to destination.
    Copy,
}

impl MappingAction {
    /// Parse from string ("rename" or "copy"), defaulting to Rename.
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "copy" => Self::Copy,
            _ => Self::Rename,
        }
    }
}

/// Where a mapping rule originated (for precedence and debugging).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleOrigin {
    /// From ClickHouse column comment (`@renamed` directive) — highest precedence.
    ColumnComment,
    /// From an external remap file (path stored for diagnostics).
    ExternalFile(String),
    /// From a built-in preset (ecs, cim, beats).
    Builtin(String),
    /// From inline config overrides.
    ConfigOverride,
}

/// A single field mapping rule.
///
/// Maps one or more source field names to a single destination field.
/// When multiple sources are specified, first match wins (`first()` semantics).
#[derive(Debug, Clone)]
pub struct FieldMappingRule {
    /// Source field names to try in order (first present wins).
    pub source_fields: Vec<String>,
    /// Destination field name (must exist in ClickHouse table schema).
    pub destination: String,
    /// Action: rename (remove source) or copy (keep source).
    pub action: MappingAction,
    /// Origin of this rule for debugging/logging.
    pub origin: RuleOrigin,
}

/// Pre-computed per-table mapping table for the hot path.
///
/// Only includes mappings where the destination column exists in the table schema.
/// Built once per table at startup/schema-refresh, then reused for every message.
#[derive(Debug, Clone)]
pub struct TableFieldMapping {
    /// Ordered rules for sequential application.
    rules: Vec<FieldMappingRule>,
}

impl TableFieldMapping {
    /// Create from a list of rules (already filtered to schema).
    pub fn new(rules: Vec<FieldMappingRule>) -> Self {
        Self { rules }
    }

    /// Create an empty mapping (no-op).
    pub fn empty() -> Self {
        Self { rules: vec![] }
    }

    /// Fast path check: true if no mappings exist (zero overhead when disabled).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Number of active rules.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Apply all field mappings to a data map.
    ///
    /// For each rule:
    /// - Skip if destination field already exists in data (no overwrite).
    /// - Try each source field in order (first match wins).
    /// - Rename: `data.remove()` for zero-copy ownership transfer.
    /// - Copy: `data.get().cloned()` since source is retained.
    ///
    /// Matches `@renamed` semantics from DDL-EXPRESSION.md.
    pub fn apply(&self, data: &mut Map<String, Value>) {
        if self.rules.is_empty() {
            return;
        }

        for rule in &self.rules {
            // Skip if destination already exists (don't overwrite upstream-populated fields)
            if data.contains_key(&rule.destination) {
                continue;
            }

            // Try each source field (first match wins)
            for source in &rule.source_fields {
                if data.contains_key(source) {
                    match rule.action {
                        MappingAction::Rename => {
                            if let Some(val) = data.remove(source) {
                                data.insert(rule.destination.clone(), val);
                            }
                        }
                        MappingAction::Copy => {
                            if let Some(val) = data.get(source) {
                                data.insert(rule.destination.clone(), val.clone());
                            }
                        }
                    }
                    break; // first match wins
                }
            }
        }
    }

    /// Get a reference to the rules (for testing/debugging).
    pub fn rules(&self) -> &[FieldMappingRule] {
        &self.rules
    }
}

/// Parse `@renamed` directives from a ClickHouse column comment string.
///
/// Supported formats:
/// - `@renamed: field_name`
/// - `@renamed: first(field1/field2/field3)`
///
/// Returns `None` if no `@renamed` directive is found.
///
/// # Examples
///
/// ```
/// use dfe_loader::transform::field_mapping::parse_renamed_directive;
///
/// assert_eq!(
///     parse_renamed_directive("@renamed: timestamp"),
///     Some(vec!["timestamp".to_string()])
/// );
///
/// assert_eq!(
///     parse_renamed_directive("@renamed: first(src_ip/srcip/source_ip)"),
///     Some(vec!["src_ip".to_string(), "srcip".to_string(), "source_ip".to_string()])
/// );
///
/// assert_eq!(parse_renamed_directive("some other comment"), None);
/// ```
pub fn parse_renamed_directive(comment: &str) -> Option<Vec<String>> {
    let prefix = "@renamed:";
    let idx = comment.find(prefix)?;
    let rest = comment[idx + prefix.len()..].trim();

    if rest.is_empty() {
        return None;
    }

    // Check for first() syntax: @renamed: first(a/b/c)
    if rest.starts_with("first(") {
        let end = rest.find(')')?;
        let fields_str = &rest[6..end]; // "first(" is 6 chars
        let fields: Vec<String> = fields_str
            .split('/')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if fields.is_empty() {
            return None;
        }
        return Some(fields);
    }

    // Simple field name: take until whitespace, pipe, or end
    let field = rest
        .split(|c: char| c.is_whitespace() || c == '|')
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;

    Some(vec![field])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ========================================================================
    // parse_renamed_directive tests
    // ========================================================================

    #[test]
    fn test_parse_renamed_simple() {
        assert_eq!(
            parse_renamed_directive("@renamed: timestamp"),
            Some(vec!["timestamp".to_string()])
        );
    }

    #[test]
    fn test_parse_renamed_first() {
        assert_eq!(
            parse_renamed_directive("@renamed: first(src_ip/srcip/source_ip)"),
            Some(vec![
                "src_ip".to_string(),
                "srcip".to_string(),
                "source_ip".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_renamed_with_surrounding_text() {
        assert_eq!(
            parse_renamed_directive("Some context | @renamed: field_name | more stuff"),
            Some(vec!["field_name".to_string()])
        );
    }

    #[test]
    fn test_parse_renamed_missing() {
        assert_eq!(parse_renamed_directive("some other comment"), None);
    }

    #[test]
    fn test_parse_renamed_empty() {
        assert_eq!(parse_renamed_directive(""), None);
    }

    #[test]
    fn test_parse_renamed_empty_directive() {
        assert_eq!(parse_renamed_directive("@renamed:"), None);
    }

    #[test]
    fn test_parse_renamed_empty_first() {
        assert_eq!(parse_renamed_directive("@renamed: first()"), None);
    }

    // ========================================================================
    // apply() tests
    // ========================================================================

    fn make_data(pairs: &[(&str, &str)]) -> Map<String, Value> {
        let mut map = Map::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), json!(v));
        }
        map
    }

    #[test]
    fn test_apply_rename() {
        let mapping = TableFieldMapping::new(vec![FieldMappingRule {
            source_fields: vec!["src_ip".to_string()],
            destination: "source.ip".to_string(),
            action: MappingAction::Rename,
            origin: RuleOrigin::Builtin("ecs".to_string()),
        }]);

        let mut data = make_data(&[("src_ip", "10.0.0.1"), ("other", "value")]);
        mapping.apply(&mut data);

        assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
        assert!(!data.contains_key("src_ip"), "source field should be removed");
        assert_eq!(data.get("other").unwrap(), "value");
    }

    #[test]
    fn test_apply_copy() {
        let mapping = TableFieldMapping::new(vec![FieldMappingRule {
            source_fields: vec!["src_ip".to_string()],
            destination: "related.ip".to_string(),
            action: MappingAction::Copy,
            origin: RuleOrigin::Builtin("ecs".to_string()),
        }]);

        let mut data = make_data(&[("src_ip", "10.0.0.1")]);
        mapping.apply(&mut data);

        assert_eq!(data.get("related.ip").unwrap(), "10.0.0.1");
        assert_eq!(data.get("src_ip").unwrap(), "10.0.0.1", "source should be retained");
    }

    #[test]
    fn test_apply_first_match_wins() {
        let mapping = TableFieldMapping::new(vec![FieldMappingRule {
            source_fields: vec![
                "src_ip".to_string(),
                "srcip".to_string(),
                "source_ip".to_string(),
            ],
            destination: "source.ip".to_string(),
            action: MappingAction::Rename,
            origin: RuleOrigin::Builtin("ecs".to_string()),
        }]);

        // Only srcip is present
        let mut data = make_data(&[("srcip", "192.168.1.1")]);
        mapping.apply(&mut data);

        assert_eq!(data.get("source.ip").unwrap(), "192.168.1.1");
        assert!(!data.contains_key("srcip"));
    }

    #[test]
    fn test_apply_skip_existing_destination() {
        let mapping = TableFieldMapping::new(vec![FieldMappingRule {
            source_fields: vec!["src_ip".to_string()],
            destination: "source.ip".to_string(),
            action: MappingAction::Rename,
            origin: RuleOrigin::Builtin("ecs".to_string()),
        }]);

        let mut data = make_data(&[("src_ip", "10.0.0.1"), ("source.ip", "existing")]);
        mapping.apply(&mut data);

        assert_eq!(
            data.get("source.ip").unwrap(),
            "existing",
            "should not overwrite"
        );
        assert_eq!(
            data.get("src_ip").unwrap(),
            "10.0.0.1",
            "source should not be removed when dest exists"
        );
    }

    #[test]
    fn test_apply_empty_is_noop() {
        let mapping = TableFieldMapping::empty();
        let mut data = make_data(&[("src_ip", "10.0.0.1")]);
        let original = data.clone();
        mapping.apply(&mut data);
        assert_eq!(data, original);
    }

    #[test]
    fn test_apply_missing_source() {
        let mapping = TableFieldMapping::new(vec![FieldMappingRule {
            source_fields: vec!["nonexistent".to_string()],
            destination: "source.ip".to_string(),
            action: MappingAction::Rename,
            origin: RuleOrigin::Builtin("ecs".to_string()),
        }]);

        let mut data = make_data(&[("src_ip", "10.0.0.1")]);
        let original = data.clone();
        mapping.apply(&mut data);
        assert_eq!(data, original, "no change when source missing");
    }

    #[test]
    fn test_apply_multiple_rules() {
        let mapping = TableFieldMapping::new(vec![
            FieldMappingRule {
                source_fields: vec!["src_ip".to_string()],
                destination: "source.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::Builtin("ecs".to_string()),
            },
            FieldMappingRule {
                source_fields: vec!["dst_ip".to_string()],
                destination: "destination.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::Builtin("ecs".to_string()),
            },
        ]);

        let mut data = make_data(&[("src_ip", "10.0.0.1"), ("dst_ip", "10.0.0.2")]);
        mapping.apply(&mut data);

        assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
        assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
        assert!(!data.contains_key("src_ip"));
        assert!(!data.contains_key("dst_ip"));
    }

    #[test]
    fn test_mapping_action_parse() {
        assert_eq!(MappingAction::parse("rename"), MappingAction::Rename);
        assert_eq!(MappingAction::parse("Rename"), MappingAction::Rename);
        assert_eq!(MappingAction::parse("copy"), MappingAction::Copy);
        assert_eq!(MappingAction::parse("Copy"), MappingAction::Copy);
        assert_eq!(MappingAction::parse("COPY"), MappingAction::Copy);
        assert_eq!(MappingAction::parse("anything_else"), MappingAction::Rename);
    }
}
