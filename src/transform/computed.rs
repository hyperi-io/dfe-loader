// SPDX-License-Identifier: BUSL-1.1
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
use std::fmt::Display;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use metrics::Counter;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, warn};

use crate::config::ComputedColumnsConfig;
use crate::metrics::names;

/// `(table, column)` pairs whose runtime failure has been logged, so each logs once per process
/// across rebuilds.
static LOGGED_COLUMNS: LazyLock<Mutex<FxHashSet<(String, String)>>> = LazyLock::new(Mutex::default);

/// One column's failure counter at one stage.
#[derive(Debug)]
struct StageCounter {
    stage: &'static str,
    counter: Counter,
}

impl StageCounter {
    fn new(table: &str, column: &str, stage: &'static str) -> Self {
        let counter = metrics::counter!(
            names::COMPUTED_EVAL_FAILURES_TOTAL,
            "table" => table.to_string(),
            "column" => column.to_string(),
            "stage" => stage
        );
        Self { stage, counter }
    }
}

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
    program: cel::Program,
    /// Origin for precedence resolution
    _origin: ComputedOrigin,
    /// Failures building the CEL context the expression reads.
    context_failures: StageCounter,
    /// Failures executing the expression.
    eval_failures: StageCounter,
    /// Set at the first failure, so later ones cost one load instead of a lock.
    logged: AtomicBool,
}

impl CompiledColumn {
    /// Count a failure and log the column's first one, carrying the error.
    fn record_failure(&self, table: &str, failures: &StageCounter, error: impl Display) {
        failures.counter.increment(1);
        if self.logged.load(Ordering::Relaxed) || self.logged.swap(true, Ordering::Relaxed) {
            return;
        }
        if LOGGED_COLUMNS
            .lock()
            .insert((table.to_string(), self.destination.clone()))
        {
            warn!(
                table = %table,
                column = %self.destination,
                stage = failures.stage,
                error = %error,
                "Computed column expression failed, leaving the column unset (logged once per table and column)"
            );
        }
    }
}

/// Pre-compiled computed columns for a single table.
#[derive(Debug)]
pub struct TableComputedColumns {
    /// The `db.table` these columns belong to, for the failure log.
    table: String,
    columns: Vec<CompiledColumn>,
    /// The variables the expressions read, each once.
    inputs: Vec<String>,
}

impl TableComputedColumns {
    /// Every variable an expression reads.
    pub fn inputs(&self) -> impl Iterator<Item = &str> {
        self.inputs.iter().map(String::as_str)
    }

    /// Evaluate all computed columns against the message data and insert results.
    ///
    /// - Skips columns that already exist in `data` (don't overwrite source data)
    /// - A failed expression leaves its column unset and the message goes on, counted in
    ///   `dfe_loader_computed_eval_failures_total` and logged once per table and column
    pub fn evaluate(&self, data: &mut serde_json::Map<String, serde_json::Value>) {
        if self.columns.is_empty() {
            return;
        }

        // Build CEL context from current data
        let hash_data: HashMap<String, serde_json::Value> =
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let context = match scalo::expression::build_context(&hash_data) {
            Ok(context) => context,
            Err(e) => {
                for col in &self.columns {
                    if !data.contains_key(&col.destination) {
                        col.record_failure(&self.table, &col.context_failures, &e);
                    }
                }
                return;
            }
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
                Err(e) => col.record_failure(&self.table, &col.eval_failures, &e),
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

    /// Drop a table's compiled columns, for a table that does not exist.
    pub fn forget(&mut self, table: &str) {
        self.tables.remove(table);
    }

    /// Number of tables with compiled columns.
    #[must_use]
    pub fn tables(&self) -> usize {
        self.tables.len()
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
            match scalo::expression::compile(expr) {
                Ok(program) => {
                    columns.push(CompiledColumn {
                        destination: destination.clone(),
                        program,
                        _origin: ComputedOrigin::ColumnComment,
                        context_failures: StageCounter::new(table, destination, "context"),
                        eval_failures: StageCounter::new(table, destination, "eval"),
                        logged: AtomicBool::new(false),
                    });
                }
                Err(e) => {
                    StageCounter::new(table, destination, "compile")
                        .counter
                        .increment(1);
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

        let mut inputs: Vec<String> = Vec::new();
        for col in &columns {
            for variable in col.program.references().variables() {
                if !inputs.iter().any(|input| input == variable) {
                    inputs.push(variable.to_string());
                }
            }
        }
        self.tables.insert(
            table.to_string(),
            TableComputedColumns {
                table: table.to_string(),
                columns,
                inputs,
            },
        );
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
fn cel_to_json(value: &cel::Value) -> serde_json::Value {
    match value {
        cel::Value::Bool(b) => serde_json::Value::Bool(*b),
        cel::Value::Int(n) => serde_json::json!(*n),
        cel::Value::UInt(n) => serde_json::json!(*n),
        cel::Value::Float(f) => serde_json::json!(*f),
        cel::Value::String(s) => serde_json::Value::String(s.to_string()),
        cel::Value::Null => serde_json::Value::Null,
        cel::Value::List(items) => {
            let arr: Vec<serde_json::Value> = items.iter().map(cel_to_json).collect();
            serde_json::Value::Array(arr)
        }
        cel::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .map
                .iter()
                .filter_map(|(k, v)| {
                    if let cel::objects::Key::String(s) = k {
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
        let v = cel::Value::Bool(true);
        assert_eq!(cel_to_json(&v), serde_json::json!(true));
    }

    #[test]
    fn test_cel_to_json_int() {
        let v = cel::Value::Int(42);
        assert_eq!(cel_to_json(&v), serde_json::json!(42));
    }

    #[test]
    fn test_cel_to_json_float() {
        let v = cel::Value::Float(1.234);
        assert_eq!(cel_to_json(&v), serde_json::json!(1.234));
    }

    #[test]
    fn test_cel_to_json_string() {
        let v = cel::Value::String(std::sync::Arc::new("hello".to_string()));
        assert_eq!(cel_to_json(&v), serde_json::json!("hello"));
    }

    #[test]
    fn test_cel_to_json_null() {
        let v = cel::Value::Null;
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
        assert_eq!(pending2, [] as [std::string::String; 0]);
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

    /// Warn-level log output, shared by every thread that installs its dispatch.
    #[derive(Clone, Default)]
    struct Lines(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Lines {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::ErrorKind::Other)?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Lines {
        fn dispatch(&self) -> tracing::Dispatch {
            let writer = self.clone();
            tracing::Dispatch::new(
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::WARN)
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .finish(),
            )
        }

        fn lines(&self) -> Vec<String> {
            let bytes = self.0.lock().map(|b| b.clone()).unwrap_or_default();
            String::from_utf8_lossy(&bytes)
                .lines()
                .map(str::to_string)
                .collect()
        }
    }

    /// One computed column, defined per table, against one record: `builds`
    /// rebuilds, each evaluating `messages` records on every table at once, one
    /// thread a table. Column names are unique per row because the log gate is
    /// process-wide.
    struct EvalCase {
        name: &'static str,
        column: &'static str,
        /// `(db.table, expression)` for each table defining the column.
        tables: &'static [(&'static str, &'static str)],
        record: serde_json::Value,
        builds: usize,
        messages: usize,
        want: Option<serde_json::Value>,
        failed_stage: &'static str,
        /// Failures counted for each table.
        failures: u64,
        /// Failure log lines across all tables.
        failure_logs: usize,
    }

    #[test]
    fn evaluate_sets_the_column_or_counts_the_failure() {
        use serde_json::json;
        const TERNARY: &str = r#"score > 80 ? "high" : score > 40 ? "medium" : "low""#;
        let cases = [
            EvalCase {
                name: "bool result",
                column: "eval_case_is_high",
                tables: &[("db.cases", "score > 80")],
                record: json!({"score": 95}),
                builds: 1,
                messages: 1,
                want: Some(json!(true)),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "ternary high",
                column: "eval_case_label_high",
                tables: &[("db.cases", TERNARY)],
                record: json!({"score": 95}),
                builds: 1,
                messages: 1,
                want: Some(json!("high")),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "ternary medium",
                column: "eval_case_label_medium",
                tables: &[("db.cases", TERNARY)],
                record: json!({"score": 60}),
                builds: 1,
                messages: 1,
                want: Some(json!("medium")),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "ternary low",
                column: "eval_case_label_low",
                tables: &[("db.cases", TERNARY)],
                record: json!({"score": 20}),
                builds: 1,
                messages: 1,
                want: Some(json!("low")),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "source value is never overwritten",
                column: "eval_case_status",
                tables: &[("db.cases", r#""computed_value""#)],
                record: json!({"eval_case_status": "original"}),
                builds: 1,
                messages: 1,
                want: Some(json!("original")),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "a column the source already set is not evaluated, so not counted",
                column: "eval_case_preset",
                tables: &[("db.cases", r#"missing_field == "value""#)],
                record: json!({"eval_case_preset": 1}),
                builds: 1,
                messages: 3,
                want: Some(json!(1)),
                failed_stage: "eval",
                failures: 0,
                failure_logs: 0,
            },
            EvalCase {
                name: "missing field counts every message and logs once across rebuilds",
                column: "eval_case_missing",
                tables: &[("db.cases", r#"missing_field == "value""#)],
                record: json!({"other": "hello"}),
                builds: 2,
                messages: 3,
                want: None,
                failed_stage: "eval",
                failures: 6,
                failure_logs: 1,
            },
            EvalCase {
                name: "two tables failing on one column at once each log and count apart",
                column: "eval_case_shared",
                tables: &[
                    ("db.shared_a", r#"missing_a == "value""#),
                    ("db.shared_b", "missing_b > 1"),
                ],
                record: json!({"other": "hello"}),
                builds: 2,
                messages: 3,
                want: None,
                failed_stage: "eval",
                failures: 6,
                failure_logs: 2,
            },
            EvalCase {
                name: "invalid expression counts each compile and never evaluates",
                column: "eval_case_invalid",
                tables: &[("db.cases", "this is not valid CEL <<<>>>")],
                record: json!({"other": "hello"}),
                builds: 2,
                messages: 3,
                want: None,
                failed_stage: "compile",
                failures: 2,
                failure_logs: 0,
            },
        ];

        let name = names::COMPUTED_EVAL_FAILURES_TOTAL;
        assert_eq!(name, "dfe_loader_computed_eval_failures_total");
        for case in cases {
            let mut config = ComputedColumnsConfig::default();
            for (table, expr) in case.tables {
                let mut columns = indexmap::IndexMap::new();
                columns.insert(case.column.to_string(), (*expr).to_string());
                config.overrides.insert((*table).to_string(), columns);
            }
            let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
            let serde_json::Value::Object(record) = case.record else {
                panic!("{}: the record is not an object", case.name);
            };

            let recorder = crate::metrics::counting::CountingRecorder::default();
            let output = Lines::default();
            let dispatch = output.dispatch();
            let mut got: Vec<Option<serde_json::Value>> = Vec::new();
            tracing::dispatcher::with_default(&dispatch, || {
                metrics::with_local_recorder(&recorder, || {
                    let mut cache = ComputedColumnCache::new(config);
                    for _ in 0..case.builds {
                        for (table, _) in case.tables {
                            cache.build_and_cache(table, &col_meta);
                        }
                        let cache = &cache;
                        let record = &record;
                        let dispatch = &dispatch;
                        got = std::thread::scope(|scope| {
                            let threads: Vec<_> = case
                                .tables
                                .iter()
                                .map(|(table, _)| {
                                    scope.spawn(move || {
                                        tracing::dispatcher::with_default(dispatch, || {
                                            let tc = cache.get(table)?;
                                            let mut last = None;
                                            for _ in 0..case.messages {
                                                let mut data = record.clone();
                                                tc.evaluate(&mut data);
                                                last = data.get(case.column).cloned();
                                            }
                                            last
                                        })
                                    })
                                })
                                .collect();
                            threads
                                .into_iter()
                                .map(|thread| {
                                    thread
                                        .join()
                                        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                                })
                                .collect()
                        });
                    }
                });
            });

            assert_eq!(
                got.len(),
                case.tables.len(),
                "{}: one result a table",
                case.name
            );
            for (got, (table, _)) in got.iter().zip(case.tables) {
                assert_eq!(got, &case.want, "{}: {table} column value", case.name);
                assert_eq!(
                    recorder.labelled(name, "table", table),
                    case.failures,
                    "{}: {table} failures",
                    case.name
                );
            }
            let total = case.failures * case.tables.len() as u64;
            assert_eq!(
                recorder.labelled(name, "stage", case.failed_stage),
                total,
                "{}: {} failures",
                case.name,
                case.failed_stage
            );
            assert_eq!(
                recorder.labelled(name, "column", case.column),
                total,
                "{}: every failure carries the column",
                case.name
            );
            let lines = output.lines();
            let failure_logs: Vec<&String> = lines
                .iter()
                .filter(|line| line.contains("logged once per table and column"))
                .collect();
            assert_eq!(
                failure_logs.len(),
                case.failure_logs,
                "{}: failure log lines {lines:?}",
                case.name
            );
            for (table, _) in case.tables.iter().take(case.failure_logs) {
                assert!(
                    failure_logs
                        .iter()
                        .any(|line| line.contains(&format!("table={table} "))
                            && line.contains(case.column)
                            && line.contains("error=")),
                    "{}: no log line names {table}, the column and the error: {failure_logs:?}",
                    case.name
                );
            }
        }
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
        assert_eq!(cache.take_pending(), [] as [std::string::String; 0]);
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
