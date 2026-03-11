// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Unified column directive framework.
//!
//! Every directive available as a ClickHouse column COMMENT annotation has an
//! exact equivalent in the config cascade. Config always wins over DDL comments.
//!
//! **Supported directives:**
//!
//! | Annotation         | Config field  | Meaning                                        |
//! |--------------------|---------------|------------------------------------------------|
//! | `@skip`            | `skip: true`  | Omit column from insert entirely               |
//! | `@default:value`   | `default:`    | Substitute when column is null or absent       |
//! | `@renamed:path`    | `renamed:`    | Source field path(s) for this column           |
//! | `@computed:expr`   | `computed:`   | CEL expression producing this column's value   |
//! | `@coerce:category` | `coerce:`     | Override type category for coercion            |
//!
//! **Resolution order (highest → lowest priority):**
//! 1. Config `column_directives.tables."db.table"."col"` — per-table per-column
//! 2. Config `column_directives.global."col"` — global per-column name
//! 3. ClickHouse column COMMENT `@directive` annotations — DDL layer
//!
//! **Usage example:**
//!
//! ```toml
//! [column_directives.global.severity]
//! default = "unknown"
//!
//! [column_directives.tables."dfe.events".debug_info]
//! skip = true
//!
//! [column_directives.tables."dfe.events"._json]
//! skip = true
//! ```
//!
//! Equivalent DDL annotations:
//! ```sql
//! ALTER TABLE dfe.events MODIFY COLUMN severity String COMMENT '@default:unknown';
//! ALTER TABLE dfe.events MODIFY COLUMN debug_info String COMMENT '@skip';
//! ```

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Public types
// ============================================================================

/// Resolved directives for a single column (merged config + DDL, config wins).
#[derive(Debug, Clone, Default)]
pub struct ColumnDirectives {
    /// Omit this column from inserts.
    pub skip: bool,
    /// Substitute this value when the column is null or absent in source data.
    pub default: Option<Value>,
    /// Source field path(s) to read from (first present wins). Empty = use column name.
    pub renamed: Vec<String>,
    /// CEL expression that produces this column's value.
    pub computed: Option<String>,
    /// Override type category for coercion (e.g. "DateTime64", "IPv4").
    pub coerce: Option<String>,
}

/// Config entry for one column — all fields optional, only set fields take effect.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnDirectivesEntry {
    /// Omit this column from inserts.
    #[serde(default)]
    pub skip: bool,
    /// Default value when column is null or absent.
    pub default: Option<Value>,
    /// Source field: `"field"` or `"first(a/b/c)"`.
    pub renamed: Option<String>,
    /// CEL expression producing this column's value.
    pub computed: Option<String>,
    /// Type category override for coercion.
    pub coerce: Option<String>,
}

/// Config section for all column-level directives.
///
/// The config equivalent of ClickHouse column COMMENT annotations.
/// Config always wins — DDL annotations only fill gaps not covered by config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnDirectivesConfig {
    /// Global: applies to matching column names across all tables.
    #[serde(default)]
    pub global: FxHashMap<String, ColumnDirectivesEntry>,
    /// Per-table: key is `"db.table"`.
    #[serde(default)]
    pub tables: FxHashMap<String, FxHashMap<String, ColumnDirectivesEntry>>,
}

// ============================================================================
// Cache
// ============================================================================

/// Thread-safe per-column directive cache merging config and DDL layers.
///
/// Config layer is immutable after construction.
/// DDL layer is updated from background schema resolution via `apply_ddl`.
pub struct ColumnMetaCache {
    config: ColumnDirectivesConfig,
    ddl: RwLock<FxHashMap<String, FxHashMap<String, ColumnDirectives>>>,
}

impl ColumnMetaCache {
    /// Create from config. Config layer is fixed at construction.
    #[must_use]
    pub fn new(config: ColumnDirectivesConfig) -> Self {
        Self {
            config,
            ddl: RwLock::new(FxHashMap::default()),
        }
    }

    /// Get merged directives for a specific column in a table.
    ///
    /// Resolution: per-table config > global config > DDL comment.
    #[must_use]
    pub fn get(&self, table: &str, col: &str) -> ColumnDirectives {
        // Per-table config wins unconditionally
        if let Some(table_cols) = self.config.tables.get(table)
            && let Some(entry) = table_cols.get(col)
        {
            return entry_to_directives(entry);
        }
        // Global config
        if let Some(entry) = self.config.global.get(col) {
            return entry_to_directives(entry);
        }
        // DDL layer (lowest)
        let ddl = self.ddl.read();
        if let Some(table_ddl) = ddl.get(table)
            && let Some(d) = table_ddl.get(col)
        {
            return d.clone();
        }
        ColumnDirectives::default()
    }

    /// Apply DDL-layer directives for a table (called from the background resolver).
    ///
    /// Config entries always take precedence — DDL is stored but only consulted
    /// when no config entry exists for a column.
    pub fn apply_ddl(&self, table: &str, col_directives: FxHashMap<String, ColumnDirectives>) {
        let mut ddl = self.ddl.write();
        ddl.insert(table.to_string(), col_directives);
    }

    /// Returns the set of column names with `skip = true` for a table.
    ///
    /// Iterates all known column names across config and DDL layers.
    #[must_use]
    pub fn skip_columns(&self, table: &str) -> Vec<String> {
        // Collect all columns we know about and whether they're skipped.
        // Config always wins for any column it mentions.
        let mut verdict: FxHashMap<String, bool> = FxHashMap::default();

        let ddl = self.ddl.read();
        if let Some(ddl_cols) = ddl.get(table) {
            for (col, d) in ddl_cols {
                verdict.insert(col.clone(), d.skip);
            }
        }
        // Global config overrides DDL
        for (col, entry) in &self.config.global {
            verdict.insert(col.clone(), entry.skip);
        }
        // Per-table config overrides all
        if let Some(table_cols) = self.config.tables.get(table) {
            for (col, entry) in table_cols {
                verdict.insert(col.clone(), entry.skip);
            }
        }
        verdict
            .into_iter()
            .filter_map(|(col, skip)| if skip { Some(col) } else { None })
            .collect()
    }

    /// Returns all computed expressions for a table: `(column_name, cel_expr)`.
    ///
    /// Config entries win over DDL for the same column.
    #[must_use]
    pub fn computed_for_table(&self, table: &str) -> Vec<(String, String)> {
        let mut result: FxHashMap<String, String> = FxHashMap::default();

        // DDL first (lowest)
        {
            let ddl = self.ddl.read();
            if let Some(ddl_cols) = ddl.get(table) {
                for (col, d) in ddl_cols {
                    if let Some(ref expr) = d.computed {
                        result.insert(col.clone(), expr.clone());
                    }
                }
            }
        }
        // Global config overrides DDL
        for (col, entry) in &self.config.global {
            if let Some(ref expr) = entry.computed {
                result.insert(col.clone(), expr.clone());
            }
        }
        // Per-table config wins
        if let Some(table_cols) = self.config.tables.get(table) {
            for (col, entry) in table_cols {
                if let Some(ref expr) = entry.computed {
                    result.insert(col.clone(), expr.clone());
                }
            }
        }
        result.into_iter().collect()
    }

    /// Returns all rename rules for a table: `(column_name, source_fields)`.
    ///
    /// Config entries win over DDL for the same column.
    #[must_use]
    pub fn renamed_for_table(&self, table: &str) -> Vec<(String, Vec<String>)> {
        let mut result: FxHashMap<String, Vec<String>> = FxHashMap::default();

        // DDL first (lowest)
        {
            let ddl = self.ddl.read();
            if let Some(ddl_cols) = ddl.get(table) {
                for (col, d) in ddl_cols {
                    if !d.renamed.is_empty() {
                        result.insert(col.clone(), d.renamed.clone());
                    }
                }
            }
        }
        // Global config overrides DDL
        for (col, entry) in &self.config.global {
            if let Some(ref renamed) = entry.renamed {
                let sources = parse_renamed_value(renamed);
                if !sources.is_empty() {
                    result.insert(col.clone(), sources);
                }
            }
        }
        // Per-table config wins
        if let Some(table_cols) = self.config.tables.get(table) {
            for (col, entry) in table_cols {
                if let Some(ref renamed) = entry.renamed {
                    let sources = parse_renamed_value(renamed);
                    if !sources.is_empty() {
                        result.insert(col.clone(), sources);
                    }
                }
            }
        }
        result.into_iter().collect()
    }
}

// ============================================================================
// Directive parser
// ============================================================================

/// Parse all column COMMENT directives into a `ColumnDirectives`.
///
/// Handles `@skip`, `@default:value`, `@renamed:path`, `@computed:expr`, `@coerce:type`.
/// Multiple directives in one comment are space-separated.
///
/// # Examples
///
/// ```
/// use dfe_loader::column_meta::parse_directives;
///
/// let d = parse_directives("@skip");
/// assert!(d.skip);
///
/// let d = parse_directives("@default:unknown @renamed:event_severity");
/// assert_eq!(d.default, Some(serde_json::Value::String("unknown".into())));
/// assert_eq!(d.renamed, vec!["event_severity"]);
///
/// let d = parse_directives("@renamed:first(src_ip/srcip/source_ip)");
/// assert_eq!(d.renamed, vec!["src_ip", "srcip", "source_ip"]);
/// ```
pub fn parse_directives(comment: &str) -> ColumnDirectives {
    let mut d = ColumnDirectives::default();
    if comment.is_empty() {
        return d;
    }

    let bytes = comment.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        // Find the next '@'
        let Some(at_off) = memchr::memchr(b'@', &bytes[pos..]) else {
            break;
        };
        pos += at_off + 1;

        // Read the directive name up to ':' or whitespace
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b':' && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let name = &comment[name_start..pos];
        let has_colon = pos < bytes.len() && bytes[pos] == b':';

        match name {
            "skip" => {
                d.skip = true;
            }
            "default" if has_colon => {
                pos += 1; // skip ':'
                let val = read_until_next_directive(comment, &mut pos);
                if !val.is_empty() {
                    d.default = parse_scalar_value(val);
                }
            }
            "renamed" if has_colon => {
                pos += 1;
                let val = read_until_next_directive(comment, &mut pos);
                d.renamed = parse_renamed_value(val);
            }
            "computed" if has_colon => {
                pos += 1;
                let val = read_until_next_directive(comment, &mut pos);
                if !val.is_empty() {
                    d.computed = Some(val.to_string());
                }
            }
            "coerce" if has_colon => {
                pos += 1;
                let val = read_until_next_directive(comment, &mut pos);
                if !val.is_empty() {
                    d.coerce = Some(val.to_string());
                }
            }
            _ => {} // unknown directive — skip
        }
    }

    d
}

/// Read from `pos` until whitespace+`@` or end of string. Returns trimmed slice.
/// Advances `pos` to the position of the `@` of the next directive (or end).
///
/// Trailing visual separators (`|`, `,`, `;`) are stripped so that column
/// comments like `"Human description | @default:unknown | @renamed:sev"` work.
fn read_until_next_directive<'a>(s: &'a str, pos: &mut usize) -> &'a str {
    let start = *pos;
    let bytes = s.as_bytes();

    /// Walk backwards from `end`, skipping whitespace and visual separators.
    fn strip_trailing_separators(bytes: &[u8], start: usize, end: usize) -> usize {
        let mut e = end;
        while e > start {
            match bytes[e - 1] {
                b' ' | b'\t' | b'\r' | b'\n' | b'|' | b',' | b';' => e -= 1,
                _ => break,
            }
        }
        e
    }

    let mut i = start;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            // Peek ahead past whitespace to find next non-whitespace char
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'@' {
                let end = strip_trailing_separators(bytes, start, i);
                *pos = j;
                return s[start..end].trim();
            }
        }
        i += 1;
    }
    *pos = bytes.len();
    let end = strip_trailing_separators(bytes, start, bytes.len());
    s[start..end].trim()
}

/// Parse `renamed` value: `"field"` or `"first(a/b/c)"` → `Vec<String>`.
#[must_use]
pub fn parse_renamed_value(s: &str) -> Vec<String> {
    let s = s.trim();
    if let Some(inner) = s.strip_prefix("first(").and_then(|t| t.strip_suffix(')')) {
        inner
            .split('/')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    } else if !s.is_empty() {
        vec![s.to_string()]
    } else {
        vec![]
    }
}

/// Parse a scalar value from a string for `@default:value`.
///
/// Tries JSON first (handles numbers, booleans, quoted strings), then falls back
/// to treating the raw string as a bare string value.
fn parse_scalar_value(s: &str) -> Option<Value> {
    if s.is_empty() {
        return None;
    }
    // Try JSON parse (numbers, booleans, null, quoted strings)
    if let Ok(v) = serde_json::from_str(s) {
        return Some(v);
    }
    // Bare string (e.g. @default:unknown instead of @default:"unknown")
    Some(Value::String(s.to_string()))
}

// ============================================================================
// Internal helpers
// ============================================================================

fn entry_to_directives(entry: &ColumnDirectivesEntry) -> ColumnDirectives {
    ColumnDirectives {
        skip: entry.skip,
        default: entry.default.clone(),
        renamed: entry
            .renamed
            .as_deref()
            .map(parse_renamed_value)
            .unwrap_or_default(),
        computed: entry.computed.clone(),
        coerce: entry.coerce.clone(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_skip() {
        let d = parse_directives("@skip");
        assert!(d.skip);
        assert!(d.renamed.is_empty());
        assert!(d.default.is_none());
    }

    #[test]
    fn test_parse_default_bare_string() {
        let d = parse_directives("@default:unknown");
        assert_eq!(d.default, Some(Value::String("unknown".into())));
    }

    #[test]
    fn test_parse_default_number() {
        let d = parse_directives("@default:0");
        assert_eq!(d.default, Some(Value::Number(0.into())));
    }

    #[test]
    fn test_parse_renamed_simple() {
        let d = parse_directives("@renamed:event_severity");
        assert_eq!(d.renamed, vec!["event_severity"]);
    }

    #[test]
    fn test_parse_renamed_first() {
        let d = parse_directives("@renamed:first(src_ip/srcip/source_ip)");
        assert_eq!(d.renamed, vec!["src_ip", "srcip", "source_ip"]);
    }

    #[test]
    fn test_parse_computed() {
        let d = parse_directives("@computed:_timestamp + 1000");
        assert_eq!(d.computed.as_deref(), Some("_timestamp + 1000"));
    }

    #[test]
    fn test_parse_coerce() {
        let d = parse_directives("@coerce:IPv4");
        assert_eq!(d.coerce.as_deref(), Some("IPv4"));
    }

    #[test]
    fn test_parse_multiple_directives() {
        let d = parse_directives("@default:unknown @renamed:event_severity @skip");
        assert!(d.skip);
        assert_eq!(d.default, Some(Value::String("unknown".into())));
        assert_eq!(d.renamed, vec!["event_severity"]);
    }

    #[test]
    fn test_parse_with_surrounding_text() {
        let d = parse_directives("Event severity level | @default:unknown | @renamed:sev");
        assert_eq!(d.default, Some(Value::String("unknown".into())));
        assert_eq!(d.renamed, vec!["sev"]);
    }

    #[test]
    fn test_parse_empty() {
        let d = parse_directives("");
        assert!(!d.skip);
        assert!(d.default.is_none());
        assert!(d.renamed.is_empty());
        assert!(d.computed.is_none());
    }

    #[test]
    fn test_cache_config_wins_over_ddl() {
        use serde_json::json;
        let mut config = ColumnDirectivesConfig::default();
        config.global.insert(
            "severity".into(),
            ColumnDirectivesEntry {
                default: Some(json!("unknown")),
                ..Default::default()
            },
        );
        let cache = ColumnMetaCache::new(config);

        // Apply DDL with different default
        let mut ddl_map = FxHashMap::default();
        ddl_map.insert(
            "severity".into(),
            ColumnDirectives {
                default: Some(json!("ddl_default")),
                ..Default::default()
            },
        );
        cache.apply_ddl("dfe.events", ddl_map);

        // Config global wins over DDL
        let d = cache.get("dfe.events", "severity");
        assert_eq!(d.default, Some(json!("unknown")));
    }

    #[test]
    fn test_cache_per_table_wins_over_global() {
        use serde_json::json;
        let mut config = ColumnDirectivesConfig::default();
        config.global.insert(
            "severity".into(),
            ColumnDirectivesEntry {
                default: Some(json!("unknown")),
                ..Default::default()
            },
        );
        let mut table_cols = FxHashMap::default();
        table_cols.insert(
            "severity".into(),
            ColumnDirectivesEntry {
                default: Some(json!("info")),
                ..Default::default()
            },
        );
        config.tables.insert("dfe.events".into(), table_cols);

        let cache = ColumnMetaCache::new(config);
        let d = cache.get("dfe.events", "severity");
        assert_eq!(d.default, Some(json!("info")));

        // Different table falls back to global
        let d2 = cache.get("dfe.other", "severity");
        assert_eq!(d2.default, Some(json!("unknown")));
    }

    #[test]
    fn test_skip_columns() {
        let mut config = ColumnDirectivesConfig::default();
        config.global.insert(
            "debug_info".into(),
            ColumnDirectivesEntry {
                skip: true,
                ..Default::default()
            },
        );
        let cache = ColumnMetaCache::new(config);
        let skipped = cache.skip_columns("any.table");
        assert!(skipped.contains(&"debug_info".to_string()));
    }

    #[test]
    fn test_parse_renamed_value_first_syntax() {
        assert_eq!(parse_renamed_value("first(a/b/c)"), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_renamed_value_simple() {
        assert_eq!(parse_renamed_value("field_name"), vec!["field_name"]);
    }
}
