// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Computed columns — CEL expressions that produce column values at insert time.
//!
//! Expressions are sourced from:
//! 1. ClickHouse column comments (`@computed: <expr>` directive)
//! 2. Config cascade overrides (`computed_columns` section)
//!
//! Precedence (highest wins):
//! 1. Config per-table override (`overrides."db.table".column`)
//! 2. Config global (`columns.column`)
//! 3. ClickHouse column COMMENT `@computed:` directive

use std::collections::HashMap;

use rustc_hash::FxHashMap;
use tracing::{debug, warn};

use crate::config::ComputedColumnsConfig;

/// Origin of a computed column definition (for precedence).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputedOrigin {
    /// From ClickHouse column COMMENT `@computed:` directive (lowest)
    ColumnComment,
    /// From config global `computed_columns.columns` section
    ConfigGlobal,
    /// From config per-table `computed_columns.overrides."db.table"` (highest)
    ConfigOverride,
}

/// A single compiled computed column.
#[derive(Debug)]
struct CompiledColumn {
    /// Destination column name
    destination: String,
    /// Pre-compiled CEL program
    program: cel_interpreter::Program,
    /// Origin for precedence resolution
    _origin: ComputedOrigin,
}

/// Pre-compiled computed columns for a single table.
#[derive(Debug)]
pub struct TableComputedColumns {
    columns: Vec<CompiledColumn>,
}

impl TableComputedColumns {
    /// Evaluate all computed columns against the message data and insert results.
    ///
    /// - Skips columns that already exist in `data` (don't overwrite source data)
    /// - Evaluation failures silently skip the column (don't DLQ the message)
    pub fn evaluate(&self, data: &mut serde_json::Map<String, serde_json::Value>) {
        if self.columns.is_empty() {
            return;
        }

        // Build CEL context from current data
        let hash_data: HashMap<String, serde_json::Value> =
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let context = match hyperi_rustlib::expression::build_context(&hash_data) {
            Ok(ctx) => ctx,
            Err(_) => return,
        };

        for col in &self.columns {
            // Don't overwrite existing source data
            if data.contains_key(&col.destination) {
                continue;
            }

            match col.program.execute(&context) {
                Ok(value) => {
                    let json_val = cel_to_json(&value);
                    if !json_val.is_null() {
                        data.insert(col.destination.clone(), json_val);
                    }
                }
                Err(_) => {
                    // Evaluation failure → skip column silently
                }
            }
        }
    }

    /// Number of computed columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether there are no computed columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Cache of pre-compiled computed columns per table.
///
/// Follows the same pending/resolve pattern as `FieldMappingCache`:
/// - First encounter marks table as pending for async resolution
/// - Async resolution fetches column comments and builds compiled expressions
/// - Subsequent lookups use cached compiled programs
pub struct ComputedColumnCache {
    /// Pre-compiled per-table computed columns.
    tables: FxHashMap<String, TableComputedColumns>,
    /// Tables pending async column comment fetch.
    pending_tables: Vec<String>,
    /// Config-level expressions (for merging at build time).
    config: ComputedColumnsConfig,
}

impl ComputedColumnCache {
    /// Create a new cache with config overrides.
    pub fn new(config: ComputedColumnsConfig) -> Self {
        Self {
            tables: FxHashMap::default(),
            pending_tables: Vec::new(),
            config,
        }
    }

    /// Check whether any computed columns are configured (config or pending).
    #[must_use]
    pub fn has_config(&self) -> bool {
        !self.config.columns.is_empty() || !self.config.overrides.is_empty()
    }

    /// Get pre-compiled computed columns for a table.
    #[must_use]
    pub fn get(&self, table: &str) -> Option<&TableComputedColumns> {
        self.tables.get(table)
    }

    /// Mark a table for async column comment resolution (first time seen).
    pub fn mark_pending(&mut self, table: &str) {
        if !self.tables.contains_key(table) && !self.pending_tables.contains(&table.to_string()) {
            self.pending_tables.push(table.to_string());
        }
    }

    /// Take pending tables for async resolution.
    pub fn take_pending(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_tables)
    }

    /// Build and cache computed columns for a table after column comments are fetched.
    ///
    /// Merges expressions from column comments with config overrides:
    /// 1. Parse `@computed:` directives from column comments (lowest precedence)
    /// 2. Overlay config global expressions
    /// 3. Overlay config per-table overrides (highest precedence)
    pub fn build_and_cache(&mut self, table: &str, column_comments: &FxHashMap<String, String>) {
        // Collect expressions with precedence: column comment → config global → config override
        let mut expressions: FxHashMap<String, (String, ComputedOrigin)> = FxHashMap::default();

        // Layer 1: Column comments (lowest precedence)
        for (column_name, comment) in column_comments {
            if let Some(expr) = parse_computed_directive(comment) {
                expressions.insert(column_name.clone(), (expr, ComputedOrigin::ColumnComment));
            }
        }

        // Layer 2: Config global expressions
        for (column_name, expr) in &self.config.columns {
            expressions.insert(
                column_name.clone(),
                (expr.clone(), ComputedOrigin::ConfigGlobal),
            );
        }

        // Layer 3: Config per-table overrides (highest precedence)
        if let Some(table_overrides) = self.config.overrides.get(table) {
            for (column_name, expr) in table_overrides {
                expressions.insert(
                    column_name.clone(),
                    (expr.clone(), ComputedOrigin::ConfigOverride),
                );
            }
        }

        // Compile all expressions
        let mut columns = Vec::with_capacity(expressions.len());
        for (destination, (expr, origin)) in &expressions {
            match hyperi_rustlib::expression::compile(expr) {
                Ok(program) => {
                    columns.push(CompiledColumn {
                        destination: destination.clone(),
                        program,
                        _origin: *origin,
                    });
                }
                Err(e) => {
                    warn!(
                        table = %table,
                        column = %destination,
                        expr = %expr,
                        error = %e,
                        "Skipping invalid computed column expression"
                    );
                }
            }
        }

        if !columns.is_empty() {
            debug!(table = %table, count = columns.len(), "Compiled computed columns");
        }

        self.tables
            .insert(table.to_string(), TableComputedColumns { columns });
    }

    /// Build and cache computed columns for a table without column comments.
    /// Used when column comment fetch fails or returns empty.
    pub fn build_and_cache_no_comments(&mut self, table: &str) {
        let empty_comments = FxHashMap::default();
        self.build_and_cache(table, &empty_comments);
    }
}

// ============================================================================
// Directive Parser
// ============================================================================

/// Parse a `@computed:` directive from a ClickHouse column COMMENT.
///
/// Returns the CEL expression string if found.
///
/// Multiple directives can coexist in one comment, separated by `|`:
/// ```text
/// @renamed: first(src/source) | @computed: value + "_processed"
/// ```
///
/// # Examples
///
/// ```
/// use dfe_loader::transform::computed::parse_computed_directive;
///
/// assert_eq!(
///     parse_computed_directive(r#"@computed: risk_score > 80 ? "high" : "low""#),
///     Some(r#"risk_score > 80 ? "high" : "low""#.to_string())
/// );
///
/// assert_eq!(
///     parse_computed_directive("@renamed: src_ip | @computed: value + \"_enriched\""),
///     Some("value + \"_enriched\"".to_string())
/// );
///
/// assert_eq!(parse_computed_directive("some other comment"), None);
/// ```
pub fn parse_computed_directive(comment: &str) -> Option<String> {
    let prefix = "@computed:";
    let idx = comment.find(prefix)?;
    let rest = comment[idx + prefix.len()..].trim();

    if rest.is_empty() {
        return None;
    }

    // Take until pipe-preceded-by-space or end of string.
    // We need to be careful not to split on `||` (logical OR in CEL).
    // Pipe delimiter for directives is ` | ` (space-pipe-space).
    let expr = if let Some(pipe_idx) = rest.find(" | ") {
        rest[..pipe_idx].trim()
    } else {
        rest.trim()
    };

    if expr.is_empty() {
        None
    } else {
        Some(expr.to_string())
    }
}

// ============================================================================
// CEL Value → JSON Conversion
// ============================================================================

/// Convert a CEL evaluation result to a serde_json::Value.
fn cel_to_json(value: &cel_interpreter::Value) -> serde_json::Value {
    match value {
        cel_interpreter::Value::Bool(b) => serde_json::Value::Bool(*b),
        cel_interpreter::Value::Int(n) => serde_json::json!(*n),
        cel_interpreter::Value::UInt(n) => serde_json::json!(*n),
        cel_interpreter::Value::Float(f) => serde_json::json!(*f),
        cel_interpreter::Value::String(s) => serde_json::Value::String(s.to_string()),
        cel_interpreter::Value::Null => serde_json::Value::Null,
        cel_interpreter::Value::List(items) => {
            let arr: Vec<serde_json::Value> = items.iter().map(cel_to_json).collect();
            serde_json::Value::Array(arr)
        }
        cel_interpreter::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .map
                .iter()
                .filter_map(|(k, v)| {
                    if let cel_interpreter::objects::Key::String(s) = k {
                        Some((s.to_string(), cel_to_json(v)))
                    } else {
                        None
                    }
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        // Any other variant — return null (safe fallback)
        _ => serde_json::Value::Null,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // parse_computed_directive tests
    // ========================================================================

    #[test]
    fn test_parse_computed_simple() {
        assert_eq!(
            parse_computed_directive(r#"@computed: risk_score > 80 ? "high" : "low""#),
            Some(r#"risk_score > 80 ? "high" : "low""#.to_string())
        );
    }

    #[test]
    fn test_parse_computed_with_pipe_delimiter() {
        assert_eq!(
            parse_computed_directive(r#"@renamed: src_ip | @computed: value + "_enriched""#),
            Some(r#"value + "_enriched""#.to_string())
        );
    }

    #[test]
    fn test_parse_computed_preserves_logical_or() {
        // `||` in CEL should NOT be treated as a pipe delimiter
        assert_eq!(
            parse_computed_directive(
                r#"@computed: a.startsWith("10.") || a.startsWith("192.168.")"#
            ),
            Some(r#"a.startsWith("10.") || a.startsWith("192.168.")"#.to_string())
        );
    }

    #[test]
    fn test_parse_computed_missing() {
        assert_eq!(parse_computed_directive("some other comment"), None);
    }

    #[test]
    fn test_parse_computed_empty() {
        assert_eq!(parse_computed_directive(""), None);
    }

    #[test]
    fn test_parse_computed_empty_directive() {
        assert_eq!(parse_computed_directive("@computed:"), None);
    }

    #[test]
    fn test_parse_computed_whitespace_only() {
        assert_eq!(parse_computed_directive("@computed:   "), None);
    }

    // ========================================================================
    // cel_to_json tests
    // ========================================================================

    #[test]
    fn test_cel_to_json_bool() {
        let v = cel_interpreter::Value::Bool(true);
        assert_eq!(cel_to_json(&v), serde_json::json!(true));
    }

    #[test]
    fn test_cel_to_json_int() {
        let v = cel_interpreter::Value::Int(42);
        assert_eq!(cel_to_json(&v), serde_json::json!(42));
    }

    #[test]
    fn test_cel_to_json_float() {
        let v = cel_interpreter::Value::Float(1.234);
        assert_eq!(cel_to_json(&v), serde_json::json!(1.234));
    }

    #[test]
    fn test_cel_to_json_string() {
        let v = cel_interpreter::Value::String(std::sync::Arc::new("hello".to_string()));
        assert_eq!(cel_to_json(&v), serde_json::json!("hello"));
    }

    #[test]
    fn test_cel_to_json_null() {
        let v = cel_interpreter::Value::Null;
        assert_eq!(cel_to_json(&v), serde_json::Value::Null);
    }

    // ========================================================================
    // ComputedColumnCache tests
    // ========================================================================

    #[test]
    fn test_cache_empty_config() {
        let config = ComputedColumnsConfig::default();
        let cache = ComputedColumnCache::new(config);
        assert!(!cache.has_config());
    }

    #[test]
    fn test_cache_with_global_config() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "risk_label".to_string(),
            r#"risk_score > 80 ? "high" : "low""#.to_string(),
        );
        let cache = ComputedColumnCache::new(config);
        assert!(cache.has_config());
    }

    #[test]
    fn test_cache_pending_tables() {
        let config = ComputedColumnsConfig::default();
        let mut cache = ComputedColumnCache::new(config);

        cache.mark_pending("common.events");
        cache.mark_pending("common.metrics");

        // Duplicate should not add again
        cache.mark_pending("common.events");

        let pending = cache.take_pending();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"common.events".to_string()));
        assert!(pending.contains(&"common.metrics".to_string()));

        // Take again should be empty
        let pending2 = cache.take_pending();
        assert!(pending2.is_empty());
    }

    #[test]
    fn test_cache_build_from_comments() {
        let config = ComputedColumnsConfig::default();
        let mut cache = ComputedColumnCache::new(config);

        let mut comments = FxHashMap::default();
        comments.insert(
            "risk_label".to_string(),
            r#"@computed: risk_score > 80 ? "high" : "low""#.to_string(),
        );

        cache.build_and_cache("common.events", &comments);

        let tc = cache.get("common.events").unwrap();
        assert_eq!(tc.len(), 1);
    }

    #[test]
    fn test_cache_config_overrides_comment() {
        // Config global should override column comment for same column
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "risk_label".to_string(),
            r#"risk_score > 90 ? "critical" : "ok""#.to_string(),
        );

        let mut cache = ComputedColumnCache::new(config);

        let mut comments = FxHashMap::default();
        comments.insert(
            "risk_label".to_string(),
            r#"@computed: risk_score > 80 ? "high" : "low""#.to_string(),
        );

        cache.build_and_cache("common.events", &comments);

        // Should have exactly 1 column (config overwrites comment for same destination)
        let tc = cache.get("common.events").unwrap();
        assert_eq!(tc.len(), 1);
    }

    #[test]
    fn test_cache_per_table_override() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "risk_label".to_string(),
            r#"risk_score > 80 ? "high" : "low""#.to_string(),
        );

        let mut table_overrides = indexmap::IndexMap::new();
        table_overrides.insert(
            "risk_label".to_string(),
            r#"risk_score > 90 ? "critical" : "ok""#.to_string(),
        );
        config
            .overrides
            .insert("common.alerts".to_string(), table_overrides);

        let mut cache = ComputedColumnCache::new(config);
        let comments = FxHashMap::default();

        // Table with override
        cache.build_and_cache("common.alerts", &comments);
        let tc = cache.get("common.alerts").unwrap();
        assert_eq!(tc.len(), 1);

        // Table without override gets global
        cache.build_and_cache("common.events", &comments);
        let tc2 = cache.get("common.events").unwrap();
        assert_eq!(tc2.len(), 1);
    }

    #[test]
    fn test_cache_invalid_expression_skipped() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "broken".to_string(),
            "this is not valid CEL <<<>>>".to_string(),
        );
        config
            .columns
            .insert("working".to_string(), r#"status == "active""#.to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache_no_comments("common.events");

        let tc = cache.get("common.events").unwrap();
        // Only the valid expression should compile
        assert_eq!(tc.len(), 1);
    }

    #[test]
    fn test_evaluate_computed_columns() {
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("is_high".to_string(), "score > 80".to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache_no_comments("common.events");

        let tc = cache.get("common.events").unwrap();

        let mut data = serde_json::Map::new();
        data.insert("score".to_string(), serde_json::json!(95));

        tc.evaluate(&mut data);

        assert_eq!(data.get("is_high"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn test_evaluate_does_not_overwrite_existing() {
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("status".to_string(), r#""computed_value""#.to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache_no_comments("common.events");

        let tc = cache.get("common.events").unwrap();

        let mut data = serde_json::Map::new();
        data.insert("status".to_string(), serde_json::json!("original"));

        tc.evaluate(&mut data);

        // Should NOT be overwritten
        assert_eq!(data.get("status"), Some(&serde_json::json!("original")));
    }

    #[test]
    fn test_evaluate_missing_field_skips() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "result".to_string(),
            r#"missing_field == "value""#.to_string(),
        );

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache_no_comments("common.events");

        let tc = cache.get("common.events").unwrap();

        let mut data = serde_json::Map::new();
        data.insert("other".to_string(), serde_json::json!("hello"));

        tc.evaluate(&mut data);

        // result should not be added (expression failed on missing field)
        assert!(!data.contains_key("result"));
    }

    #[test]
    fn test_evaluate_string_expression() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert(
            "risk_label".to_string(),
            r#"score > 80 ? "high" : score > 40 ? "medium" : "low""#.to_string(),
        );

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache_no_comments("common.events");

        let tc = cache.get("common.events").unwrap();

        // High score
        let mut data1 = serde_json::Map::new();
        data1.insert("score".to_string(), serde_json::json!(95));
        tc.evaluate(&mut data1);
        assert_eq!(data1.get("risk_label"), Some(&serde_json::json!("high")));

        // Medium score
        let mut data2 = serde_json::Map::new();
        data2.insert("score".to_string(), serde_json::json!(60));
        tc.evaluate(&mut data2);
        assert_eq!(data2.get("risk_label"), Some(&serde_json::json!("medium")));

        // Low score
        let mut data3 = serde_json::Map::new();
        data3.insert("score".to_string(), serde_json::json!(20));
        tc.evaluate(&mut data3);
        assert_eq!(data3.get("risk_label"), Some(&serde_json::json!("low")));
    }
}
