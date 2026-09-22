// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Unified column directive framework.
//!
//! Every directive available as a `ClickHouse` column COMMENT annotation has an
//! exact equivalent in the config cascade. Config always wins over DDL comments.
//!
//! **Supported directives:**
//!
//! | Annotation         | Config field  | Meaning                                        |
//! |--------------------|---------------|------------------------------------------------|
//! | `@skip`            | `skip: true`  | Omit column from insert entirely               |
//! | `@default:value`   | `default:`    | Substitute when column is null or absent       |
//! | `@source:path`     | `renamed:`    | Source field path(s), with optional `\| fallback` |
//! | `@renamed:path`    | `renamed:`    | Source field path(s) for this column           |
//! | `@computed:expr`   | `computed:`   | CEL expression producing this column's value   |
//! | `@coerce:category` | `coerce:`     | Override type category for coercion            |
//!
//! `@source` is the vocabulary dfe-schemas emits; `@renamed` is the equivalent
//! hand-written form. Both resolve to the same `renamed` source-path list.
//!
//! **Resolution order (highest → lowest priority):**
//! 1. Config `column_directives.tables."db.table"."col"` — per-table per-column
//! 2. Config `column_directives.global."col"` — global per-column name
//! 3. `ClickHouse` column COMMENT `@directive` annotations — DDL layer
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

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Public types
// ============================================================================

/// Resolved directives for a single column (merged config + DDL, config wins).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnDirectives {
    /// Omit this column from inserts.
    pub skip: bool,
    /// Substitute this value when the column is null or absent in source data.
    pub default: Option<Value>,
    /// Source field path(s) to read from (first present wins). Empty = use column name.
    pub renamed: Vec<String>,
    /// CEL expression that produces this column's value.
    pub computed: Option<String>,
    /// Override type category for coercion (e.g. "`DateTime64`", "IPv4").
    pub coerce: Option<String>,
}

/// Config entry for one column — all fields optional, only set fields take effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
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
/// The config equivalent of `ClickHouse` column COMMENT annotations.
/// Config always wins — DDL annotations only fill gaps not covered by config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
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
    ddl_version: AtomicU64,
}

impl ColumnMetaCache {
    /// Create from config. Config layer is fixed at construction.
    #[must_use]
    pub fn new(config: ColumnDirectivesConfig) -> Self {
        Self {
            config,
            ddl: RwLock::new(FxHashMap::default()),
            ddl_version: AtomicU64::new(0),
        }
    }

    /// Generation of the DDL layer, bumped by every `apply_ddl`.
    ///
    /// A caller caching work derived from directives compares this to know
    /// whether its derived state still matches the directives in force.
    #[must_use]
    pub fn ddl_version(&self) -> u64 {
        self.ddl_version.load(Ordering::Acquire)
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
        // The generation is cache-wide, so bumping it on a periodic refresh that
        // changed nothing would rebuild every table's derived state.
        if ddl.get(table) == Some(&col_directives) {
            return;
        }
        ddl.insert(table.to_string(), col_directives);
        // Bumped under the write lock so a reader that sees the new directives
        // never reads the old version and keeps stale derived state.
        self.ddl_version.fetch_add(1, Ordering::Release);
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
/// Handles `@skip`, `@default:value`, `@source:path`, `@renamed:path`,
/// `@computed:expr`, `@coerce:type`. Multiple directives in one comment are
/// space-separated.
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
///
/// // @source is the dfe-schemas form, and the DDL generator appends the
/// // human description after " - ".
/// let d = parse_directives("@source: host.name - Host the event came from");
/// assert_eq!(d.renamed, vec!["host.name"]);
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
                d.renamed = parse_renamed_value(strip_path_description(val));
            }
            "source" if has_colon => {
                pos += 1;
                let val = strip_path_description(read_until_next_directive(comment, &mut pos));
                // `@source: path | fallback` fills the column when every source
                // path is absent -- exactly what `default` already does, so the
                // fallback lands there rather than growing a second mechanism.
                let (path, fallback) = match val.split_once('|') {
                    Some((p, f)) => (p.trim(), Some(f.trim())),
                    None => (val, None),
                };
                d.renamed = parse_renamed_value(path);
                if d.default.is_none()
                    && let Some(fallback) = fallback
                {
                    d.default = parse_fallback_value(fallback);
                }
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

/// Directive names the parser recognises.
///
/// A `@` only starts a directive when the word after it is one of these. ECS
/// puts its most important field at `@timestamp`, which collides with the
/// directive sigil -- `@source: @timestamp` must read the field, not stop dead.
const DIRECTIVE_NAMES: [&[u8]; 6] = [
    b"skip",
    b"default",
    b"source",
    b"renamed",
    b"computed",
    b"coerce",
];

/// Whether the `@` at byte index `at` begins a known directive.
fn is_directive_start(bytes: &[u8], at: usize) -> bool {
    let start = at + 1;
    let mut end = start;
    while end < bytes.len() && bytes[end] != b':' && !bytes[end].is_ascii_whitespace() {
        end += 1;
    }
    DIRECTIVE_NAMES.contains(&&bytes[start..end])
}

/// Trim the human description the DDL generator appends to a directive.
///
/// Column COMMENTs are emitted as `<directive> - <description>`, and a source
/// field path never contains a space-hyphen-space, so the first one ends the path.
fn strip_path_description(s: &str) -> &str {
    match s.find(" - ") {
        Some(i) => s[..i].trim_end(),
        None => s,
    }
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
            if j < bytes.len() && bytes[j] == b'@' && is_directive_start(bytes, j) {
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

/// Parse the `| fallback` half of an `@source` directive.
///
/// The vocabulary is closed (`docs/clickhouse/DDL-DIRECTIVES.md`): `now()`,
/// `uuid()`, `null`, a quoted string literal, a number, a bool. Anything else
/// is a FIELD REFERENCE the extractor cannot resolve -- `@source: first(_source)
/// | topic_name` is the live example, and treating it as a literal writes the
/// string "topic_name" into every `_source` that had no value. An unresolvable
/// fallback leaves the column absent instead.
///
/// `null` also leaves the column absent: for a Nullable column that IS NULL,
/// and for a non-Nullable one it lets the column DEFAULT apply rather than
/// failing the row.
fn parse_fallback_value(s: &str) -> Option<Value> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Resolved per row in the extractor, so they travel as markers.
    if s == "now()" || s == "uuid()" {
        return Some(Value::String(s.to_string()));
    }
    match serde_json::from_str::<Value>(s) {
        Ok(Value::Null) => None,
        Ok(v) => Some(v),
        Err(_) => {
            static UNKNOWN_FALLBACK_TS: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            if scalo::logger::log_debounced(&UNKNOWN_FALLBACK_TS, 300_000) {
                tracing::warn!(
                    fallback = %s,
                    "Unsupported @source fallback ignored, column left absent (max 1 per 5m)"
                );
            }
            None
        }
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

    // ========================================================================
    // @source — the vocabulary dfe-schemas emits
    // ========================================================================

    #[test]
    fn test_parse_source_simple() {
        let d = parse_directives("@source: message");
        assert_eq!(d.renamed, vec!["message"]);
    }

    #[test]
    fn test_parse_source_dotted_path() {
        let d = parse_directives("@source: log.file.path");
        assert_eq!(d.renamed, vec!["log.file.path"]);
    }

    #[test]
    fn test_parse_source_first_list() {
        let d = parse_directives("@source: first(timestamp/timeUnixNano)");
        assert_eq!(d.renamed, vec!["timestamp", "timeUnixNano"]);
    }

    #[test]
    fn test_parse_source_fallback_becomes_default() {
        let d = parse_directives("@source: timestamp | now()");
        assert_eq!(d.renamed, vec!["timestamp"]);
        assert_eq!(d.default, Some(Value::String("now()".into())));
    }

    #[test]
    fn test_parse_source_quoted_literal_fallback() {
        let d = parse_directives("@source: status | \"unknown\"");
        assert_eq!(d.renamed, vec!["status"]);
        assert_eq!(d.default, Some(Value::String("unknown".into())));
    }

    #[test]
    fn test_parse_source_bare_word_fallback_is_ignored() {
        // Verbatim from a deployed dfe.filebeat. `topic_name` is a FIELD
        // REFERENCE the extractor cannot resolve, so inserting it as a literal
        // would stamp the string "topic_name" into every _source with no value.
        let d = parse_directives(
            "@source: first(_source) | topic_name - Data source label (falls back to the topic)",
        );
        assert_eq!(d.renamed, vec!["_source"]);
        assert_eq!(
            d.default, None,
            "an unresolvable fallback must leave the column absent"
        );
    }

    #[test]
    fn test_parse_source_fallback_vocabulary() {
        // docs/clickhouse/DDL-DIRECTIVES.md defines exactly these forms.
        let cases: [(&str, Option<Value>); 7] = [
            ("@source: t | now()", Some(Value::String("now()".into()))),
            ("@source: t | uuid()", Some(Value::String("uuid()".into()))),
            ("@source: t | null", None),
            ("@source: t | \"lit\"", Some(Value::String("lit".into()))),
            ("@source: t | 0", Some(Value::Number(0.into()))),
            ("@source: t | 42", Some(Value::Number(42.into()))),
            ("@source: t | false", Some(Value::Bool(false))),
        ];
        for (comment, expected) in cases {
            assert_eq!(parse_directives(comment).default, expected, "{comment}");
        }
    }

    #[test]
    fn test_parse_default_directive_still_takes_a_bare_string() {
        // The closed vocabulary applies to the `|` fallback only -- @default is
        // its own directive and has always taken a bare value.
        let d = parse_directives("@default:unknown @source: status");
        assert_eq!(d.default, Some(Value::String("unknown".into())));
        assert_eq!(d.renamed, vec!["status"]);
    }

    #[test]
    fn test_parse_source_at_timestamp_field() {
        // ECS puts its most important field at @timestamp, which collides with
        // the directive sigil.
        let d = parse_directives("@source: @timestamp");
        assert_eq!(d.renamed, vec!["@timestamp"]);
    }

    #[test]
    fn test_parse_source_at_timestamp_with_ddl_description() {
        // The exact COMMENT the engine emits for dfe-schemas filebeat.timestamp.
        let d = parse_directives("@source: @timestamp - Event timestamp (ECS @timestamp)");
        assert_eq!(d.renamed, vec!["@timestamp"]);
    }

    #[test]
    fn test_parse_source_at_timestamp_inside_first_list() {
        let d = parse_directives("@source: first(timestamp/@timestamp/time)");
        assert_eq!(d.renamed, vec!["timestamp", "@timestamp", "time"]);
    }

    #[test]
    fn test_parse_source_strips_ddl_description() {
        let d = parse_directives("@source: host.name - Host the event was collected from");
        assert_eq!(d.renamed, vec!["host.name"]);
    }

    #[test]
    fn test_parse_source_path_may_contain_spaces() {
        let d = parse_directives("@source: Report Refresh Date - Reporting date");
        assert_eq!(d.renamed, vec!["Report Refresh Date"]);
    }

    #[test]
    fn test_parse_source_alongside_another_directive() {
        let d = parse_directives("@source: agent.type @coerce:LowCardinality");
        assert_eq!(d.renamed, vec!["agent.type"]);
        assert_eq!(d.coerce.as_deref(), Some("LowCardinality"));
    }

    #[test]
    fn test_parse_source_explicit_default_wins_over_fallback() {
        let d = parse_directives("@default:zero @source: value | \"fallback\"");
        assert_eq!(d.default, Some(Value::String("zero".into())));
        assert_eq!(d.renamed, vec!["value"]);
    }

    #[test]
    fn test_parse_source_against_the_deployed_filebeat_comments() {
        // Verbatim from system.columns on a deployed dfe.filebeat, so this
        // pins the parser to what ClickHouse actually stores rather than to
        // an idealised directive.
        let deployed = [
            (
                "@source: @timestamp - Event timestamp (ECS @timestamp)",
                "@timestamp",
            ),
            (
                "@source: host.name - Host the event was collected from",
                "host.name",
            ),
            ("@source: agent.type - Shipping agent type", "agent.type"),
            (
                "@source: agent.version - Shipping agent version",
                "agent.version",
            ),
            (
                "@source: event.module - Filebeat module that produced the event",
                "event.module",
            ),
            (
                "@source: event.dataset - Module dataset (ECS event.dataset)",
                "event.dataset",
            ),
            (
                "@source: log.file.path - Source file the line was read from",
                "log.file.path",
            ),
            (
                "@source: source.ip - Source address (ECS source.ip)",
                "source.ip",
            ),
            (
                "@source: user.name - User associated with the event (ECS user.name)",
                "user.name",
            ),
            (
                "@source: process.name - Process that emitted the line (ECS process.name)",
                "process.name",
            ),
            (
                "@source: process.pid - Process ID (ECS process.pid)",
                "process.pid",
            ),
            ("@source: message - Log line content", "message"),
        ];

        for (comment, expected) in deployed {
            let d = parse_directives(comment);
            assert_eq!(d.renamed, vec![expected], "parsing {comment:?}");
        }
    }

    #[test]
    fn test_parse_renamed_strips_ddl_description() {
        let d = parse_directives("@renamed: src_ip - Source address");
        assert_eq!(d.renamed, vec!["src_ip"]);
    }

    #[test]
    fn test_unknown_at_word_is_not_a_directive_boundary() {
        assert!(!is_directive_start(b"@timestamp", 0));
        assert!(is_directive_start(b"@source: x", 0));
        assert!(is_directive_start(b"@skip", 0));
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

    // ========================================================================
    // parse_renamed_value edge cases
    // ========================================================================

    #[test]
    fn test_parse_renamed_value_empty() {
        assert!(parse_renamed_value("").is_empty());
    }

    #[test]
    fn test_parse_renamed_value_first_empty_inner() {
        assert!(parse_renamed_value("first()").is_empty());
    }

    #[test]
    fn test_parse_renamed_value_first_with_whitespace() {
        let result = parse_renamed_value("first( a / b /c )");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_renamed_value_single_field_with_whitespace() {
        let result = parse_renamed_value("  field_x  ");
        assert_eq!(result, vec!["field_x"]);
    }

    // ========================================================================
    // ColumnMetaCache: apply_ddl + get
    // ========================================================================

    #[test]
    fn test_cache_apply_ddl_and_get() {
        let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl_cols = FxHashMap::default();
        ddl_cols.insert(
            "col1".to_string(),
            ColumnDirectives {
                skip: true,
                ..Default::default()
            },
        );
        cache.apply_ddl("db.t", ddl_cols);

        let result = cache.get("db.t", "col1");
        assert!(result.skip);
    }

    #[test]
    fn test_cache_get_nonexistent_returns_default() {
        let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let result = cache.get("unknown.t", "col");
        assert!(!result.skip);
        assert!(result.default.is_none());
    }

    #[test]
    fn test_cache_config_global_merges_with_ddl() {
        let mut config = ColumnDirectivesConfig::default();
        config.global.insert(
            "common_col".to_string(),
            ColumnDirectivesEntry {
                skip: true,
                ..Default::default()
            },
        );
        let cache = ColumnMetaCache::new(config);
        // No DDL applied — config still applies via get()
        let result = cache.get("any.table", "common_col");
        assert!(result.skip);
    }

    #[test]
    fn test_skip_columns_multiple_sources() {
        let mut config = ColumnDirectivesConfig::default();
        // Global config skips col_g
        config.global.insert(
            "col_g".to_string(),
            ColumnDirectivesEntry {
                skip: true,
                ..Default::default()
            },
        );
        // Per-table config skips col_t for db.t
        let mut tbl_cfg = FxHashMap::default();
        tbl_cfg.insert(
            "col_t".to_string(),
            ColumnDirectivesEntry {
                skip: true,
                ..Default::default()
            },
        );
        config.tables.insert("db.t".to_string(), tbl_cfg);

        let cache = ColumnMetaCache::new(config);

        // DDL for db.t skips col_d AND col_t (but table config wins)
        let mut ddl_cols = FxHashMap::default();
        ddl_cols.insert(
            "col_d".to_string(),
            ColumnDirectives {
                skip: true,
                ..Default::default()
            },
        );
        cache.apply_ddl("db.t", ddl_cols);

        let skips: std::collections::HashSet<String> =
            cache.skip_columns("db.t").into_iter().collect();
        // All three skip-flagged cols present
        assert!(skips.contains("col_g"));
        assert!(skips.contains("col_t"));
        assert!(skips.contains("col_d"));
    }

    #[test]
    fn test_computed_for_table_merges_sources() {
        let mut config = ColumnDirectivesConfig::default();
        config.global.insert(
            "col_global".to_string(),
            ColumnDirectivesEntry {
                computed: Some("1 + 1".to_string()),
                ..Default::default()
            },
        );
        let cache = ColumnMetaCache::new(config);

        // Add DDL-level computed
        let mut ddl_cols = FxHashMap::default();
        ddl_cols.insert(
            "col_ddl".to_string(),
            ColumnDirectives {
                computed: Some("x * 2".to_string()),
                ..Default::default()
            },
        );
        cache.apply_ddl("db.t", ddl_cols);

        let computed: std::collections::HashMap<String, String> =
            cache.computed_for_table("db.t").into_iter().collect();
        assert_eq!(computed.get("col_global"), Some(&"1 + 1".to_string()));
        assert_eq!(computed.get("col_ddl"), Some(&"x * 2".to_string()));
    }

    #[test]
    fn test_computed_for_table_config_wins() {
        let mut config = ColumnDirectivesConfig::default();
        // Global has one value
        config.global.insert(
            "col".to_string(),
            ColumnDirectivesEntry {
                computed: Some("global_val".to_string()),
                ..Default::default()
            },
        );
        let cache = ColumnMetaCache::new(config);

        // DDL has another value for same col
        let mut ddl_cols = FxHashMap::default();
        ddl_cols.insert(
            "col".to_string(),
            ColumnDirectives {
                computed: Some("ddl_val".to_string()),
                ..Default::default()
            },
        );
        cache.apply_ddl("db.t", ddl_cols);

        let computed: std::collections::HashMap<String, String> =
            cache.computed_for_table("db.t").into_iter().collect();
        // Config wins over DDL
        assert_eq!(computed.get("col"), Some(&"global_val".to_string()));
    }

    #[test]
    fn test_renamed_for_table_from_config() {
        let mut config = ColumnDirectivesConfig::default();
        let mut tbl = FxHashMap::default();
        tbl.insert(
            "dest".to_string(),
            ColumnDirectivesEntry {
                renamed: Some("first(src1/src2)".to_string()),
                ..Default::default()
            },
        );
        config.tables.insert("db.t".to_string(), tbl);

        let cache = ColumnMetaCache::new(config);
        let renamed: std::collections::HashMap<String, Vec<String>> =
            cache.renamed_for_table("db.t").into_iter().collect();
        let sources = renamed.get("dest").expect("dest should have rename");
        assert_eq!(sources, &vec!["src1".to_string(), "src2".to_string()]);
    }

    #[test]
    fn test_renamed_for_table_from_ddl() {
        let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl_cols = FxHashMap::default();
        ddl_cols.insert(
            "dest".to_string(),
            ColumnDirectives {
                renamed: vec!["src_a".to_string(), "src_b".to_string()],
                ..Default::default()
            },
        );
        cache.apply_ddl("db.t", ddl_cols);

        let renamed: std::collections::HashMap<String, Vec<String>> =
            cache.renamed_for_table("db.t").into_iter().collect();
        assert_eq!(
            renamed.get("dest"),
            Some(&vec!["src_a".to_string(), "src_b".to_string()])
        );
    }

    #[test]
    fn test_parse_scalar_value_returns_number() {
        let result = parse_scalar_value("42");
        assert_eq!(result, Some(Value::Number(42.into())));
    }

    #[test]
    fn test_parse_scalar_value_returns_string_for_bare() {
        let result = parse_scalar_value("unquoted");
        assert_eq!(result, Some(Value::String("unquoted".to_string())));
    }

    #[test]
    fn test_parse_scalar_value_empty_is_none() {
        assert!(parse_scalar_value("").is_none());
    }

    #[test]
    fn test_parse_scalar_value_returns_bool() {
        let result = parse_scalar_value("true");
        assert_eq!(result, Some(Value::Bool(true)));
    }
}
