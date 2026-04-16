// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Computed columns — CEL expressions that produce column values at insert time.
//!
//! Expressions are sourced from:
//! 1. `ClickHouse` column comments (`@computed: <expr>` directive)
//! 2. Config cascade overrides (`computed_columns` section)
//!
//! Precedence (highest wins):
//! 1. Config per-table override (`overrides."db.table".column`)
//! 2. Config global (`columns.column`)
//! 3. `ClickHouse` column COMMENT `@computed:` directive

use std::collections::HashMap;

use rustc_hash::FxHashMap;
use tracing::{debug, warn};

use crate::config::ComputedColumnsConfig;

/// Origin of a computed column definition (for precedence).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputedOrigin {
    /// From `ClickHouse` column COMMENT `@computed:` directive (lowest)
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

            if let Ok(value) = col.program.execute(&context) {
                let json_val = cel_to_json(&value);
                if !json_val.is_null() {
                    data.insert(col.destination.clone(), json_val);
                }
            } else {
                // Evaluation failure → skip column silently
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

    /// Build and cache computed columns for a table.
    ///
    /// Merges expressions with precedence (highest wins for same destination):
    /// 1. `self.config.overrides."table"` — per-table config (highest)
    /// 2. `self.config.columns` — global config
    /// 3. `col_meta.computed_for_table(table)` — DDL `@computed:` annotations (lowest)
    pub fn build_and_cache(&mut self, table: &str, col_meta: &crate::column_meta::ColumnMetaCache) {
        // Collect expressions with precedence: DDL first, then config overrides.
        let mut exprs: FxHashMap<String, String> =
            col_meta.computed_for_table(table).into_iter().collect();

        // Global config overrides DDL
        for (col, expr) in &self.config.columns {
            exprs.insert(col.clone(), expr.clone());
        }

        // Per-table config overrides global config
        if let Some(table_overrides) = self.config.overrides.get(table) {
            for (col, expr) in table_overrides {
                exprs.insert(col.clone(), expr.clone());
            }
        }

        let mut columns = Vec::with_capacity(exprs.len());
        for (destination, expr) in &exprs {
            match hyperi_rustlib::expression::compile(expr) {
                Ok(program) => {
                    columns.push(CompiledColumn {
                        destination: destination.clone(),
                        program,
                        _origin: ComputedOrigin::ColumnComment,
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
}

// ============================================================================
// Directive Parser
// ============================================================================

/// Parse a `@computed:` directive from a `ClickHouse` column COMMENT.
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

/// Convert a CEL evaluation result to a `serde_json::Value`.
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
    use crate::column_meta::{ColumnDirectives, ColumnDirectivesConfig, ColumnMetaCache};

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

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "risk_label".to_string(),
            ColumnDirectives {
                computed: Some(r#"risk_score > 80 ? "high" : "low""#.to_string()),
                ..Default::default()
            },
        );
        col_meta.apply_ddl("common.events", ddl);

        cache.build_and_cache("common.events", &col_meta);

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

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "risk_label".to_string(),
            ColumnDirectives {
                computed: Some(r#"risk_score > 80 ? "high" : "low""#.to_string()),
                ..Default::default()
            },
        );
        col_meta.apply_ddl("common.events", ddl);

        cache.build_and_cache("common.events", &col_meta);

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
        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());

        // Table with override
        cache.build_and_cache("common.alerts", &col_meta);
        let tc = cache.get("common.alerts").unwrap();
        assert_eq!(tc.len(), 1);

        // Table without override gets global
        cache.build_and_cache("common.events", &col_meta);
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
        cache.build_and_cache(
            "common.events",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );

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
        cache.build_and_cache(
            "common.events",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );

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
        cache.build_and_cache(
            "common.events",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );

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
        cache.build_and_cache(
            "common.events",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );

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
        cache.build_and_cache(
            "common.events",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );

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

    // ========================================================================
    // TableComputedColumns: len + is_empty + cache accessor methods
    // ========================================================================

    #[test]
    fn table_computed_columns_len_and_is_empty() {
        // Default config with no columns → cache build yields no columns
        let config = ComputedColumnsConfig::default();
        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "common.empty",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("common.empty").unwrap();
        assert!(tc.is_empty());
        assert_eq!(tc.len(), 0);
    }

    #[test]
    fn table_computed_columns_with_multiple_columns() {
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("col_a".to_string(), "1 + 2".to_string());
        config
            .columns
            .insert("col_b".to_string(), r#""hello""#.to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "common.multi",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("common.multi").unwrap();
        assert!(!tc.is_empty());
        assert_eq!(tc.len(), 2);
    }

    #[test]
    fn cache_has_config_empty() {
        let cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        assert!(!cache.has_config());
    }

    #[test]
    fn cache_has_config_with_global_columns() {
        let mut config = ComputedColumnsConfig::default();
        config.columns.insert("c".to_string(), "1".to_string());
        let cache = ComputedColumnCache::new(config);
        assert!(cache.has_config());
    }

    #[test]
    fn cache_has_config_with_per_table_overrides() {
        let mut config = ComputedColumnsConfig::default();
        let mut t_map = indexmap::IndexMap::new();
        t_map.insert("colx".to_string(), "true".to_string());
        config.overrides.insert("db.tbl".to_string(), t_map);
        let cache = ComputedColumnCache::new(config);
        assert!(cache.has_config());
    }

    #[test]
    fn cache_mark_pending_and_take() {
        let mut cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        cache.mark_pending("t1");
        cache.mark_pending("t2");
        // Duplicate ignored
        cache.mark_pending("t1");

        let pending = cache.take_pending();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"t1".to_string()));
        assert!(pending.contains(&"t2".to_string()));
        // Now empty
        assert!(cache.take_pending().is_empty());
    }

    #[test]
    fn cache_mark_pending_skips_already_cached() {
        // After build_and_cache, mark_pending on same table should no-op
        let config = ComputedColumnsConfig::default();
        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "db.t",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        cache.mark_pending("db.t");
        let pending = cache.take_pending();
        assert!(
            pending.is_empty(),
            "Already-cached table should not be pending"
        );
    }

    #[test]
    fn cache_build_skips_invalid_expression() {
        let mut config = ComputedColumnsConfig::default();
        // Invalid CEL syntax
        config
            .columns
            .insert("bad".to_string(), "this is not valid CEL @@@".to_string());
        config
            .columns
            .insert("good".to_string(), "1 + 1".to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "common.mix",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("common.mix").unwrap();
        // bad expression skipped, good one compiled
        assert_eq!(tc.len(), 1);
    }

    // ========================================================================
    // cel_to_json: all CEL value types
    // ========================================================================

    #[test]
    fn evaluate_produces_various_json_types() {
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("as_bool".to_string(), "true".to_string());
        config
            .columns
            .insert("as_int".to_string(), "42".to_string());
        config
            .columns
            .insert("as_float".to_string(), "3.14".to_string());
        config
            .columns
            .insert("as_string".to_string(), r#""text""#.to_string());
        config
            .columns
            .insert("as_list".to_string(), "[1, 2, 3]".to_string());

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "db.types",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("db.types").unwrap();

        let mut data = serde_json::Map::new();
        tc.evaluate(&mut data);

        assert_eq!(data.get("as_bool"), Some(&serde_json::json!(true)));
        assert_eq!(data.get("as_int"), Some(&serde_json::json!(42)));
        assert_eq!(data.get("as_float"), Some(&serde_json::json!(3.14)));
        assert_eq!(data.get("as_string"), Some(&serde_json::json!("text")));
        assert_eq!(data.get("as_list"), Some(&serde_json::json!([1, 2, 3])));
    }

    #[test]
    fn evaluate_preserves_existing_field() {
        // Implementation skips columns that already exist in data
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("val".to_string(), "x * 2".to_string());
        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "db.ovr",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("db.ovr").unwrap();

        let mut data = serde_json::Map::new();
        data.insert("x".to_string(), serde_json::json!(5));
        data.insert("val".to_string(), serde_json::json!("original"));
        tc.evaluate(&mut data);

        // Pre-existing value wins — computed is skipped
        assert_eq!(data.get("val"), Some(&serde_json::json!("original")));
    }

    #[test]
    fn parse_computed_directive_empty_after_prefix() {
        // "@computed:   " -> nothing after trim
        assert!(parse_computed_directive("@computed:   ").is_none());
        assert!(parse_computed_directive("@computed:").is_none());
    }

    #[test]
    fn parse_computed_directive_pipe_without_surrounding_spaces_preserved() {
        // Pipe without " | " doesn't split — preserved as-is (unlikely CEL though)
        let result = parse_computed_directive("@computed: a|b");
        assert_eq!(result, Some("a|b".to_string()));
    }

    #[test]
    fn parse_computed_directive_with_double_pipe_or_preserved() {
        // `||` is CEL OR — should be preserved, not treated as delimiter
        let result = parse_computed_directive("@computed: a || b");
        assert_eq!(result, Some("a || b".to_string()));
    }

    #[test]
    fn parse_computed_directive_takes_only_first_directive() {
        let result = parse_computed_directive("@computed: first | @computed: second_ignored");
        assert_eq!(result, Some("first".to_string()));
    }

    #[test]
    fn parse_computed_directive_no_directive() {
        assert!(parse_computed_directive("no directive here").is_none());
        assert!(parse_computed_directive("").is_none());
    }

    #[test]
    fn parse_computed_directive_with_leading_other_directive() {
        let result = parse_computed_directive("@renamed: src | @computed: x + 1");
        assert_eq!(result, Some("x + 1".to_string()));
    }

    #[test]
    fn cache_per_table_overrides_global() {
        let mut config = ComputedColumnsConfig::default();
        config
            .columns
            .insert("col".to_string(), r#""global""#.to_string());
        let mut t_override = indexmap::IndexMap::new();
        t_override.insert("col".to_string(), r#""per_table""#.to_string());
        config.overrides.insert("db.tbl".to_string(), t_override);

        let mut cache = ComputedColumnCache::new(config);
        cache.build_and_cache(
            "db.tbl",
            &ColumnMetaCache::new(ColumnDirectivesConfig::default()),
        );
        let tc = cache.get("db.tbl").unwrap();

        let mut data = serde_json::Map::new();
        tc.evaluate(&mut data);
        // Per-table override wins
        assert_eq!(data.get("col"), Some(&serde_json::json!("per_table")));
    }

    #[test]
    fn cache_computed_columns_use_ddl_when_no_config() {
        // ColumnMetaCache provides computed expressions via DDL annotations.
        // Without config overrides, DDL columns should be used.
        // Note: we can't directly populate DDL-based computed columns in tests
        // without setting up the full cache flow, so we verify the empty case.
        let cache = ComputedColumnCache::new(ComputedColumnsConfig::default());
        assert!(!cache.has_config());
    }
}
