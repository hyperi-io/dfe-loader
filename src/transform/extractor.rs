// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Schema-guided SIMD field extractor for the `json_primary` pipeline mode.
//!
//! Parses the raw payload **once** with `sonic_rs::from_slice`, then performs
//! O(1) hash lookups for each schema column.
//!
//! Benchmarks (`benches/simdjson_spike.rs`, batch=10K) confirm the full-parse
//! approach outperforms `get_from_slice × N` in all realistic cases:
//! - 2.7× faster at N=30 flat payload (196 ms → 72 ms)
//! - 4.2× faster at N=15 nested payload (each miss still scans the full doc)
//! - Tied at N=15 flat payload with high field hit-rate
//!   Misses are free with the DOM approach (O(1) hash lookup returns None)
//!   but cost a full document scan with `get_from_slice`.
//!
//! ## What this does NOT do
//!
//! - No flattening — a dotted source path is walked in place, never materialised
//! - No `_json` injection — that is spliced zero-copy at serialisation time
//! - No coercion — `DeltaCoercer` runs on the returned map after extraction
//!
//! ## Column handling
//!
//! | Column | Behaviour |
//! |---|---|
//! | `_uuid`, `_timestamp_load` | Skipped — ClickHouse DEFAULT generates these |
//! | `_json` | Skipped — zero-copy splice at serialisation time |
//! | `_timestamp_received` | Always set to current UTC time |
//! | `DateTime`/`DateTime64` | RFC3339 normalised to ClickHouse's text form |
//! | `@skip` directive | Excluded from insert |
//! | `@source:host.name` | Dotted path descends the parsed payload |
//! | `@renamed:first(a/b/c)` | First-match source field lookup |
//! | `@default:value` | Applied when all source fields are absent |
//! | `@source:field \| now()` | Fallback resolved to the ingest timestamp |
//! | `@source:field \| topic_name` | Fallback resolved to the source the message topic names |
//!
//! ## Source fields more than one column reads
//!
//! A column takes its source field by moving it out of the parsed payload, so
//! a field two columns read would reach only the first of them. The per-table
//! plan counts every column's source paths once and copies the shared ones, so
//! `_timestamp` and a meta `timestamp` column both fill from one input field.

use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{Map, Value};
use tracing::debug;

use crate::transform::transformer::fmt_ts;

use crate::clickhouse::TableSchema;
use crate::column_meta::{ColumnMetaCache, TOPIC_NAME_MARKER};
use crate::config::{MetadataConfig, RoutingConfig};

/// The outcome of one header pass.
///
/// `empty_reason` is `Some` exactly when `fields` is empty, so the caller that
/// rejects the message logs why without re-deriving it.
pub struct HeaderExtraction {
    /// Promoted columns, keyed by destination column name.
    pub fields: Map<String, Value>,
    /// Why nothing was promoted.
    pub empty_reason: Option<&'static str>,
}

/// The source paths one column reads, in the order the extractor tries them.
///
/// Both the extraction loop and the table plan resolve a column through
/// `sources_for`, so a column cannot be counted as claiming one path and then
/// read from another.
enum Sources<'a> {
    /// The column takes no value from the payload.
    None,
    /// Try each path in order; the first one present wins.
    List(&'a [String]),
    /// Try each compiled-in path in order; the first one present wins.
    Fixed(&'static [&'static str]),
    /// Try the first path, then the second when there is one.
    Pair(&'a str, Option<&'a str>),
}

impl<'a> Sources<'a> {
    /// Visit each source path in order, stopping at the first `f` accepts.
    #[inline]
    fn try_each(&self, mut f: impl FnMut(&'a str) -> bool) -> bool {
        match *self {
            Sources::None => false,
            Sources::List(paths) => paths.iter().any(|path| f(path.as_str())),
            Sources::Fixed(paths) => paths.iter().any(|path| f(path)),
            Sources::Pair(first, second) => f(first) || second.is_some_and(f),
        }
    }
}

/// The payload keys `_timestamp` reads, in order.
///
/// `@timestamp` is ECS's spelling, so without it a beats event lands with its
/// load time as its event time (dfe-engine#498). `timestamp` stays first, which
/// leaves a source already sending that key unaffected.
static TIMESTAMP_SOURCES: &[&str] = &["timestamp", "@timestamp", "_timestamp"];

/// Per-table extraction state derived from the schema and its directives.
///
/// Built on the first row of a table and reused by every row after it, so
/// resolving the shared source fields never reaches the per-row path.
struct TablePlan {
    /// Column names the plan was built from.
    columns: Box<[String]>,
    /// DDL generation the plan was built from.
    ddl_version: u64,
    /// Source paths more than one column of this table reads.
    contended: FxHashSet<Box<str>>,
}

impl TablePlan {
    /// Whether the plan still describes this schema and directive generation.
    ///
    /// An ALTER that adds, drops or renames a column changes the name list, and
    /// a changed COMMENT bumps the DDL generation; either rebuilds the plan
    /// rather than leaving a new collision undetected.
    fn is_current(&self, schema: &TableSchema, ddl_version: u64) -> bool {
        self.ddl_version == ddl_version
            && self.columns.len() == schema.columns.len()
            && self
                .columns
                .iter()
                .zip(&schema.columns)
                .all(|(name, col)| *name == col.name)
    }
}

/// Schema-guided SIMD field extractor.
///
/// Created once from config at startup. The only mutable state is the per-table
/// plan cache, behind an `RwLock` -- safe to share across tasks.
pub struct HeaderExtractor {
    /// Field name in source data to extract for `_org_id` (RLS column).
    org_id_field: String,
    /// Field names to check for `_source` value (first match wins).
    source_fields: Vec<String>,
    /// Whether to extract `_source`.
    capture_source: bool,
    /// Whether the common header (all `_`-prefixed columns) is enabled.
    metadata_enabled: bool,
    /// Per-table plans, built on first use and rebuilt when the table changes.
    plans: RwLock<FxHashMap<String, Arc<TablePlan>>>,
}

impl HeaderExtractor {
    /// Create from config.
    pub fn new(metadata: &MetadataConfig, routing: &RoutingConfig) -> Self {
        Self {
            org_id_field: routing
                .org_id_field
                .clone()
                .unwrap_or_else(|| "org_id".to_string()),
            source_fields: metadata.source_fields.clone(),
            capture_source: metadata.capture_source,
            metadata_enabled: metadata.enabled,
            plans: RwLock::new(FxHashMap::default()),
        }
    }

    /// Resolve the source paths a column reads.
    ///
    /// Mirrors the branch order of `extract()`: the columns `ClickHouse`
    /// generates, then `_timestamp`, then `@skip`, then `@source`/`@renamed`,
    /// then the column-name rules.
    fn sources_for<'a>(&'a self, name: &'a str, renamed: &'a [String], skip: bool) -> Sources<'a> {
        if matches!(
            name,
            "_uuid" | "_timestamp_load" | "_json" | "_timestamp_received"
        ) {
            return Sources::None;
        }
        // _timestamp reads the payload ahead of any directive.
        if name == "_timestamp" {
            return Sources::Fixed(TIMESTAMP_SOURCES);
        }
        if skip {
            return Sources::None;
        }
        if !renamed.is_empty() {
            return Sources::List(renamed);
        }
        match name {
            "_org_id" => Sources::Pair(&self.org_id_field, None),
            "_source" if self.capture_source && self.metadata_enabled => {
                Sources::List(&self.source_fields)
            }
            // _foo reads "foo" before falling back to the literal "_foo".
            _ if name.starts_with('_') => Sources::Pair(&name[1..], Some(name)),
            _ => Sources::Pair(name, None),
        }
    }

    /// Get this table's plan, rebuilding it when the table changed under it.
    fn plan_for(
        &self,
        table: &str,
        schema: &TableSchema,
        col_meta: &ColumnMetaCache,
    ) -> Arc<TablePlan> {
        let ddl_version = col_meta.ddl_version();
        {
            let plans = self.plans.read();
            if let Some(plan) = plans.get(table)
                && plan.is_current(schema, ddl_version)
            {
                return Arc::clone(plan);
            }
        }
        let plan = Arc::new(self.build_plan(table, schema, col_meta, ddl_version));
        self.plans
            .write()
            .insert(table.to_string(), Arc::clone(&plan));
        plan
    }

    /// Find the source paths more than one column of this table reads.
    ///
    /// A `first(a/b/c)` column claims every path in its list because which one
    /// it takes is a property of the row, so a path any two columns could reach
    /// is counted as shared.
    fn build_plan(
        &self,
        table: &str,
        schema: &TableSchema,
        col_meta: &ColumnMetaCache,
        ddl_version: u64,
    ) -> TablePlan {
        let mut claims: FxHashMap<Box<str>, u32> = FxHashMap::default();
        for col in &schema.columns {
            let directives = col_meta.get(table, &col.name);
            let sources = self.sources_for(&col.name, &directives.renamed, directives.skip);
            sources.try_each(|path| {
                if let Some(count) = claims.get_mut(path) {
                    *count += 1;
                } else {
                    claims.insert(Box::from(path), 1);
                }
                false
            });
        }

        let contended: FxHashSet<Box<str>> = claims
            .into_iter()
            .filter_map(|(path, count)| (count > 1).then_some(path))
            .collect();
        if !contended.is_empty() {
            debug!(
                table = %table,
                paths = contended.len(),
                "Source fields read by more than one column are copied, not moved"
            );
        }

        TablePlan {
            columns: schema.columns.iter().map(|col| col.name.clone()).collect(),
            ddl_version,
            contended,
        }
    }

    /// Extract promoted fields from raw JSON bytes using schema column guidance.
    ///
    /// Parses the payload once with `sonic_rs::from_slice`, then performs O(1)
    /// hash lookups per schema column. Returns only promoted fields — `_json` is
    /// NOT included (spliced zero-copy at serialisation time).
    ///
    /// Column directives are read from `col_meta` with full config cascade:
    /// per-table config > global config > DDL `@directive` annotations.
    ///
    /// A pass that promotes nothing carries the reason back to the caller; the
    /// caller owns the rejection, so it owns the log and the counter (#145).
    ///
    /// `topic_source` is the source this message's topic names, which the
    /// `topic_name` fallback resolves to; `None` or an empty label leaves that
    /// fallback unresolved and the column absent.
    pub fn extract(
        &self,
        raw: &[u8],
        table: &str,
        schema: &TableSchema,
        col_meta: &ColumnMetaCache,
        topic_source: Option<&str>,
    ) -> HeaderExtraction {
        let now = Utc::now();
        let mut map = Map::with_capacity(schema.columns.len());
        let plan = self.plan_for(table, schema, col_meta);
        let contended = &plan.contended;

        // Parse once — all schema column lookups are O(1) hash operations on this map.
        // Misses are free here; with get_from_slice each miss still scans the full document.
        // Take ownership of the parsed map so we can move Values out (zero-clone).
        // Each take_path call removes the value from `parsed` and moves it into
        // the output map -- no Value::clone() on the hot path, except for the
        // source fields `plan.contended` names, which more than one column reads.
        // A payload the header pass cannot read used to return an empty map at
        // debug level, and the row landed with every column at its type default.
        // Name the reason and let the caller reject it (#144).
        let mut parsed = match sonic_rs::from_slice::<Value>(raw) {
            Ok(Value::Object(map)) => map,
            Ok(_) => {
                return HeaderExtraction {
                    fields: map,
                    empty_reason: Some("payload is not a JSON object"),
                };
            }
            Err(e) => {
                debug!(table = %table, error = %e, "Payload parse failed, skipping extraction");
                return HeaderExtraction {
                    fields: map,
                    empty_reason: Some("payload did not parse"),
                };
            }
        };

        for col in &schema.columns {
            let name = &col.name;

            // ClickHouse generates these — never inject from source data.
            if matches!(name.as_str(), "_uuid" | "_timestamp_load" | "_json") {
                continue;
            }

            // _timestamp_received: always the current ingest time, not from source.
            if name == "_timestamp_received" {
                if self.metadata_enabled {
                    map.insert(name.clone(), Value::String(fmt_ts(&now)));
                }
                continue;
            }

            // _timestamp: extract from source, fall back to now() if missing.
            // The column is NOT Nullable and has no DEFAULT — omitting it would
            // produce 1970-01-01 00:00:00.000 (epoch zero).
            if name == "_timestamp" {
                let found = self
                    .sources_for(name, &[], false)
                    .try_each(|path| take_path(&mut parsed, path, name, &mut map, contended));
                if found {
                    normalise_datetime(&mut map, name);
                } else {
                    map.insert(name.clone(), Value::String(fmt_ts(&now)));
                }
                continue;
            }

            let directives = col_meta.get(table, name);
            if directives.skip {
                continue;
            }

            // Priority: @renamed directive > per-column defaults > column name.
            let found = self
                .sources_for(name, &directives.renamed, directives.skip)
                .try_each(|path| take_path(&mut parsed, path, name, &mut map, contended));

            // Apply column default when all source fields were absent.
            if !found
                && let Some(ref default) = directives.default
                && let Some(value) = resolve_default(default, &now, topic_source)
            {
                map.insert(name.clone(), value);
            } else if found && is_datetime(col) {
                normalise_datetime(&mut map, name);
            }
        }

        debug!(table = %table, fields = map.len(), "Extracted promoted fields");
        // A valid object whose fields match no column promotes nothing too, and
        // used to reach the caller indistinguishable from a full pass.
        let empty_reason = map
            .is_empty()
            .then_some("no schema column matched the payload");
        HeaderExtraction {
            fields: map,
            empty_reason,
        }
    }
}

/// Whether a column takes a `ClickHouse` date-time text value.
///
/// `TypeTag` is resolved once when the schema is parsed, so this is a
/// discriminant compare rather than the two `&str` compares a category costs.
#[inline]
fn is_datetime(col: &crate::clickhouse::types::ColumnInfo) -> bool {
    use crate::clickhouse_ext::parsed_type::TypeTag;
    matches!(col.parsed_type.tag, TypeTag::DateTime | TypeTag::DateTime64)
}

/// Rewrite an RFC3339 date-time into the text form `ClickHouse` parses.
///
/// Both transforms and the receiver emit `2026-09-07T05:53:53.385Z`, and
/// `JSONEachRow`'s `DateTime64` reader stops at the zone suffix and rejects the
/// whole batch (code 27), so every row in it is lost. Anything that is not
/// RFC3339 is left alone: the space-separated form and epoch numbers already
/// parse, and a value this cannot read is the coercer's to judge, not ours.
#[inline]
fn normalise_datetime(map: &mut Map<String, Value>, name: &str) {
    let Some(value) = map.get_mut(name) else {
        return;
    };
    let Some(text) = value.as_str() else {
        return;
    };
    if !looks_rfc3339(text) {
        return;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
        *value = Value::String(fmt_ts(&dt.with_timezone(&Utc)));
    }
}

/// Whether a value can be RFC3339, cheaply enough to run on every row.
///
/// The common case is already `ClickHouse`'s text form -- a space at byte 10 and
/// no zone -- so it costs one byte compare and a look at the last six, and never
/// the parse or the `String` the rewrite allocates. RFC3339 needs a `T`/`t`
/// separator there, or, for the space-separated form chrono also accepts, a
/// trailing `Z` or a `+HH:MM` / `-HH:MM` offset.
#[inline]
fn looks_rfc3339(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() < 11 {
        return false;
    }
    if bytes[10] == b'T' || bytes[10] == b't' {
        return true;
    }
    if matches!(bytes[bytes.len() - 1], b'Z' | b'z') {
        return true;
    }
    bytes[bytes.len() - 6..]
        .iter()
        .any(|b| matches!(b, b'+' | b'-'))
}

/// Resolve a parsed `@default` / `@source` fallback against this message's context.
///
/// `now()`, `uuid()` and `topic_name` are the fallbacks that cannot be
/// parse-time constants, so they travel as literal markers and are resolved
/// here. `uuid()` is v7 to match the time-ordered form the schemas use.
///
/// The parser has already rejected any fallback outside the documented
/// vocabulary, so nothing reaching here is a stray field name.
///
/// `None` leaves the column absent: a `topic_name` with no topic to resolve
/// against must stay NULL rather than stamp the marker text into a dimension.
#[inline]
fn resolve_default(
    default: &Value,
    now: &chrono::DateTime<Utc>,
    topic_source: Option<&str>,
) -> Option<Value> {
    match default.as_str() {
        Some("now()") => Some(Value::String(fmt_ts(now))),
        Some("uuid()") => Some(Value::String(uuid::Uuid::now_v7().to_string())),
        Some(TOPIC_NAME_MARKER) => topic_source
            .filter(|source| !source.is_empty())
            .map(|source| Value::String(source.to_string())),
        _ => Some(default.clone()),
    }
}

/// Remove a source field from the parsed object, descending a dotted path.
///
/// A flat key (and a payload already flattened to literal dotted keys) takes a
/// single hash lookup; only a genuine miss on a dotted path walks the tree, so
/// nested ECS payloads map without a flatten pass. Nothing is allocated either way.
#[inline]
fn remove_path(parsed: &mut Map<String, Value>, path: &str) -> Option<Value> {
    if let Some(v) = parsed.remove(path) {
        return Some(v);
    }
    if !path.contains('.') {
        return None;
    }

    let mut parts = path.split('.');
    let mut current = parsed.get_mut(parts.next()?)?;
    let mut parts = parts.peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            return current.as_object_mut()?.remove(part);
        }
        current = current.as_object_mut()?.get_mut(part)?;
    }
    None
}

/// Read a source field without removing it, descending a dotted path.
///
/// The borrowing twin of `remove_path`, for a field a later column still reads.
#[inline]
fn get_path<'a>(parsed: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    if let Some(v) = parsed.get(path) {
        return Some(v);
    }
    if !path.contains('.') {
        return None;
    }

    let mut parts = path.split('.');
    let mut current = parsed.get(parts.next()?)?;
    let mut parts = parts.peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            return current.as_object()?.get(part);
        }
        current = current.as_object()?.get(part)?;
    }
    None
}

/// Take a source field into the output map, moving it unless it is shared.
///
/// A common-header column used to empty the field it read, so a meta column
/// reading the same field found nothing and landed NULL. A path in `contended`
/// is copied instead, which leaves it there for the columns that follow.
///
/// `contended` is empty for every table with no shared source field, so the
/// ordinary column costs one `is_empty` check and keeps the move.
#[inline]
fn take_path(
    parsed: &mut Map<String, Value>,
    source: &str,
    dest: &str,
    map: &mut Map<String, Value>,
    contended: &FxHashSet<Box<str>>,
) -> bool {
    if !contended.is_empty() && contended.contains(source) {
        let Some(value) = get_path(parsed, source) else {
            return false;
        };
        map.insert(dest.to_string(), value.clone());
        return true;
    }
    if let Some(value) = remove_path(parsed, source) {
        map.insert(dest.to_string(), value);
        true
    } else {
        false
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::{ColumnInfo, ParsedType};
    use crate::column_meta::{ColumnDirectivesConfig, ColumnMetaCache};
    use crate::config::{MetadataConfig, RoutingConfig};

    fn make_schema(columns: &[&str]) -> TableSchema {
        TableSchema {
            database: "dfe".to_string(),
            table: "events".to_string(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnInfo {
                    name: name.to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: (i as u64) + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                })
                .collect(),
            comment: String::new(),
        }
    }

    fn empty_col_meta() -> ColumnMetaCache {
        ColumnMetaCache::new(ColumnDirectivesConfig::default())
    }

    fn default_extractor() -> HeaderExtractor {
        let metadata = MetadataConfig::default();
        let routing = RoutingConfig::default();
        HeaderExtractor::new(&metadata, &routing)
    }

    #[test]
    fn test_extracts_matching_schema_column() {
        let extractor = default_extractor();
        let raw = br#"{"severity": "high", "src_ip": "1.2.3.4", "extra": "ignored"}"#;
        let schema = make_schema(&["severity", "src_ip"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;

        assert_eq!(map.get("severity"), Some(&Value::String("high".into())));
        assert_eq!(map.get("src_ip"), Some(&Value::String("1.2.3.4".into())));
        assert!(
            !map.contains_key("extra"),
            "non-schema field must not be promoted"
        );
    }

    #[test]
    fn test_skips_uuid_and_timestamp_load() {
        let extractor = default_extractor();
        let raw = br#"{"_uuid": "abc", "_timestamp_load": "2026-01-01"}"#;
        let schema = make_schema(&["_uuid", "_timestamp_load"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert!(
            map.is_empty(),
            "_uuid and _timestamp_load must never be injected"
        );
    }

    #[test]
    fn test_skips_json_column() {
        let extractor = default_extractor();
        let raw = br#"{"_json": "anything"}"#;
        let schema = make_schema(&["_json"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert!(
            map.is_empty(),
            "_json must not be injected (zero-copy splice path)"
        );
    }

    #[test]
    fn test_injects_timestamp_received() {
        let extractor = default_extractor();
        let raw = br"{}";
        let schema = make_schema(&["_timestamp_received"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert!(
            map.contains_key("_timestamp_received"),
            "_timestamp_received must always be injected"
        );
        // Verify format: "YYYY-MM-DD HH:MM:SS.mmm"
        if let Some(Value::String(ts)) = map.get("_timestamp_received") {
            assert!(ts.len() >= 19, "timestamp must be at least 19 chars: {ts}");
        }
    }

    #[test]
    fn test_extracts_org_id_from_routing_field() {
        let extractor = default_extractor(); // org_id_field = "org_id"
        let raw = br#"{"org_id": "acme", "data": "x"}"#;
        let schema = make_schema(&["_org_id"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("_org_id"), Some(&Value::String("acme".into())));
    }

    #[test]
    fn test_skip_directive_excludes_column() {
        use crate::column_meta::{ColumnDirectives, ColumnDirectivesConfig, ColumnMetaCache};
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        let raw = br#"{"debug_info": "verbose"}"#;
        let schema = make_schema(&["debug_info"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "debug_info".to_string(),
            ColumnDirectives {
                skip: true,
                ..Default::default()
            },
        );
        col_meta.apply_ddl("dfe.events", ddl);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert!(
            !map.contains_key("debug_info"),
            "skipped column must not be promoted"
        );
    }

    #[test]
    fn test_renamed_directive_first_match() {
        use crate::column_meta::{ColumnDirectivesConfig, ColumnDirectivesEntry, ColumnMetaCache};
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        // Source data has "src_ip" but not "source_ip"
        let raw = br#"{"src_ip": "10.0.0.1"}"#;
        let schema = make_schema(&["source.ip"]);

        // Config says source.ip comes from first(source_ip/src_ip)
        let mut config = ColumnDirectivesConfig::default();
        let mut table_cols = FxHashMap::default();
        table_cols.insert(
            "source.ip".to_string(),
            ColumnDirectivesEntry {
                renamed: Some("first(source_ip/src_ip)".to_string()),
                ..Default::default()
            },
        );
        config.tables.insert("dfe.events".to_string(), table_cols);
        let col_meta = ColumnMetaCache::new(config);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("source.ip"),
            Some(&Value::String("10.0.0.1".into()))
        );
    }

    #[test]
    fn test_default_applied_when_field_absent() {
        use crate::column_meta::{ColumnDirectives, ColumnDirectivesConfig, ColumnMetaCache};
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        let raw = br#"{"other": "data"}"#;
        let schema = make_schema(&["severity"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "severity".to_string(),
            ColumnDirectives {
                default: Some(Value::String("unknown".into())),
                ..Default::default()
            },
        );
        col_meta.apply_ddl("dfe.events", ddl);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("severity"), Some(&Value::String("unknown".into())));
    }

    #[test]
    fn test_numeric_and_bool_values() {
        let extractor = default_extractor();
        let raw = br#"{"count": 42, "is_active": true, "ratio": 0.75}"#;
        let schema = make_schema(&["count", "is_active", "ratio"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("count"), Some(&serde_json::json!(42)));
        assert_eq!(map.get("is_active"), Some(&serde_json::json!(true)));
        assert_eq!(map.get("ratio"), Some(&serde_json::json!(0.75)));
    }

    #[test]
    fn test_underscore_column_tries_stripped_name_first() {
        let _extractor = default_extractor();
        // _source column: try "source" (stripped) before "_source"
        let raw = br#"{"source": "auth"}"#;
        let schema = make_schema(&["_source"]);

        // Disable capture_source so it falls through to the general _ handling
        let metadata = MetadataConfig {
            capture_source: false,
            ..Default::default()
        };
        let extractor2 = HeaderExtractor::new(&metadata, &RoutingConfig::default());
        let col_meta = empty_col_meta();

        let map = extractor2
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("_source"), Some(&Value::String("auth".into())));
    }

    #[test]
    fn test_timestamp_fallback_to_now_when_missing() {
        // _timestamp is NOT Nullable and has no DEFAULT — omitting it produces
        // epoch zero (1970-01-01 00:00:00.000). The extractor must inject now().
        let extractor = default_extractor();
        let raw = br#"{"severity": "high"}"#; // No "timestamp" or "_timestamp" field
        let schema = make_schema(&["_timestamp", "severity"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;

        // _timestamp must be present (not omitted)
        assert!(
            map.contains_key("_timestamp"),
            "_timestamp must be injected when source field is missing"
        );
        // Must be a valid timestamp string, not epoch zero
        if let Some(Value::String(ts)) = map.get("_timestamp") {
            assert!(
                !ts.starts_with("1970"),
                "_timestamp must not be epoch zero, got: {ts}"
            );
            assert!(ts.len() >= 19, "timestamp must be at least 19 chars: {ts}");
            // Verify it starts with a recent year (2026+)
            assert!(
                ts.starts_with("202"),
                "_timestamp should be current time, got: {ts}"
            );
        } else {
            panic!("_timestamp must be a string value");
        }
    }

    // ========================================================================
    // Nested ECS payloads via @source dotted paths
    // ========================================================================

    /// The dfe-schemas filebeat meta schema, as the loader sees it after the
    /// engine has joined each column's expr and comment into the DDL COMMENT.
    fn filebeat_col_meta() -> ColumnMetaCache {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let comments = [
            (
                "timestamp",
                "@source: @timestamp - Event timestamp (ECS @timestamp)",
            ),
            (
                "host_name",
                "@source: host.name - Host the event was collected from",
            ),
            ("agent_type", "@source: agent.type - Shipping agent type"),
            (
                "log_file_path",
                "@source: log.file.path - Source file the line was read from",
            ),
            (
                "source_ip",
                "@source: source.ip - Source address (ECS source.ip)",
            ),
            (
                "process_pid",
                "@source: process.pid - Process ID (ECS process.pid)",
            ),
            ("message", "@source: message - Log line content"),
        ];
        let mut ddl = FxHashMap::default();
        for (col, comment) in comments {
            ddl.insert(col.to_string(), parse_directives(comment));
        }
        let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        cache.apply_ddl("dfe.filebeat", ddl);
        cache
    }

    const FILEBEAT_PAYLOAD: &[u8] = br#"{
        "@timestamp": "2026-08-18T04:11:00.123Z",
        "host": {"name": "web-01", "os": {"family": "debian"}},
        "agent": {"type": "filebeat", "version": "8.17.0"},
        "log": {"file": {"path": "/var/log/syslog"}},
        "source": {"ip": "10.0.0.9"},
        "process": {"name": "sshd", "pid": 4242},
        "message": "Accepted password for svc-ingest",
        "ecs": {"version": "8.11.0"}
    }"#;

    #[test]
    fn test_nested_ecs_paths_populate_typed_columns() {
        let extractor = default_extractor();
        let schema = make_schema(&[
            "timestamp",
            "host_name",
            "agent_type",
            "log_file_path",
            "message",
        ]);
        let col_meta = filebeat_col_meta();

        let map = extractor
            .extract(FILEBEAT_PAYLOAD, "dfe.filebeat", &schema, &col_meta, None)
            .fields;

        assert_eq!(
            map.get("host_name"),
            Some(&Value::String("web-01".into())),
            "host.name must descend the nested payload"
        );
        assert_eq!(
            map.get("agent_type"),
            Some(&Value::String("filebeat".into())),
            "agent.type must descend the nested payload"
        );
        assert_eq!(
            map.get("log_file_path"),
            Some(&Value::String("/var/log/syslog".into())),
            "log.file.path must descend two levels"
        );
        assert_eq!(
            map.get("timestamp"),
            Some(&Value::String("2026-08-18T04:11:00.123Z".into())),
            "@timestamp must survive the directive sigil"
        );
        assert_eq!(
            map.get("message"),
            Some(&Value::String("Accepted password for svc-ingest".into())),
            "the flat column must keep working"
        );
    }

    #[test]
    fn test_nested_path_moves_non_string_leaf() {
        let extractor = default_extractor();
        let schema = make_schema(&["process_pid", "source_ip"]);
        let col_meta = filebeat_col_meta();

        let map = extractor
            .extract(FILEBEAT_PAYLOAD, "dfe.filebeat", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("process_pid"), Some(&serde_json::json!(4242)));
        assert_eq!(
            map.get("source_ip"),
            Some(&Value::String("10.0.0.9".into()))
        );
    }

    #[test]
    fn test_dotted_path_misses_leave_column_absent() {
        let extractor = default_extractor();
        // "host" is a scalar here, so host.name cannot resolve.
        let raw = br#"{"host": "web-01", "agent": {}}"#;
        let schema = make_schema(&["host_name", "agent_type"]);
        let col_meta = filebeat_col_meta();

        let map = extractor
            .extract(raw, "dfe.filebeat", &schema, &col_meta, None)
            .fields;
        assert!(!map.contains_key("host_name"));
        assert!(!map.contains_key("agent_type"));
    }

    #[test]
    fn test_literal_dotted_key_still_matches() {
        // A payload already flattened upstream keeps working — the literal key
        // is the fast path, tried before any descent.
        let extractor = default_extractor();
        let raw = br#"{"host.name": "web-02"}"#;
        let schema = make_schema(&["host_name"]);
        let col_meta = filebeat_col_meta();

        let map = extractor
            .extract(raw, "dfe.filebeat", &schema, &col_meta, None)
            .fields;
        assert_eq!(map.get("host_name"), Some(&Value::String("web-02".into())));
    }

    #[test]
    fn test_source_now_fallback_resolves_to_ingest_time() {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        let raw = br#"{"other": "data"}"#;
        let schema = make_schema(&["event_time"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "event_time".to_string(),
            parse_directives("@source: timestamp | now()"),
        );
        col_meta.apply_ddl("dfe.events", ddl);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        let Some(Value::String(ts)) = map.get("event_time") else {
            panic!("event_time must be a string timestamp");
        };
        assert_ne!(ts, "now()", "the now() marker must be resolved, not stored");
        assert!(ts.starts_with("202"), "expected a current timestamp: {ts}");
    }

    #[test]
    fn test_source_field_reference_fallback_leaves_the_column_absent() {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        // `host_name` names a field the extractor cannot see, so _source must
        // stay absent rather than be stamped with the string "host_name" -- it
        // is a LowCardinality dimension operators group by.
        let extractor = default_extractor();
        let raw = br#"{"message": "no source key here"}"#;
        let schema = make_schema(&["_source"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "_source".to_string(),
            parse_directives(
                "@source: first(_source) | host_name - Data source label (falls back to the host)",
            ),
        );
        col_meta.apply_ddl("dfe.filebeat", ddl);

        let map = extractor
            .extract(raw, "dfe.filebeat", &schema, &col_meta, Some("filebeat"))
            .fields;
        assert!(
            !map.contains_key("_source"),
            "an unresolvable fallback must not become a literal: {:?}",
            map.get("_source")
        );
    }

    // ---- the topic_name fallback (#187) ----

    /// The directive dfe-schemas ships in `common-header/timeseries.yaml`, so
    /// every table on that profile is covered by these tests.
    const TIMESERIES_SOURCE_DIRECTIVE: &str = "@source: first(_source) | topic_name - Data source label (e.g. beats, syslog, crowdstrike-edr)";

    fn timeseries_source_col_meta(table: &str) -> ColumnMetaCache {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "_source".to_string(),
            parse_directives(TIMESERIES_SOURCE_DIRECTIVE),
        );
        col_meta.apply_ddl(table, ddl);
        col_meta
    }

    #[test]
    fn the_topic_name_fallback_labels_a_record_that_carries_no_source() {
        // The measured case on dfe-accept: 10 rows in dfe."cisco-ios", _source
        // NULL on all of them, because a transform's ECS output carries no
        // _source field of its own.
        let extractor = default_extractor();
        let raw = br#"{"message": "ecs output, no source key"}"#;
        let schema = make_schema(&["_source"]);
        let col_meta = timeseries_source_col_meta("dfe.cisco-ios");

        let map = extractor
            .extract(raw, "dfe.cisco-ios", &schema, &col_meta, Some("cisco-ios"))
            .fields;
        assert_eq!(
            map.get("_source"),
            Some(&Value::String("cisco-ios".into())),
            "the topic's source must label the row rather than leaving it NULL"
        );
    }

    #[test]
    fn a_record_that_carries_its_own_source_keeps_it_over_the_topic() {
        // `first(_source)` resolves first, which is why this defect hid on the
        // landing table: most of its traffic already carries the field.
        let extractor = default_extractor();
        let raw = br#"{"_source": "crowdstrike-edr", "message": "named itself"}"#;
        let schema = make_schema(&["_source"]);
        let col_meta = timeseries_source_col_meta("dfe.main");

        let map = extractor
            .extract(raw, "dfe.main", &schema, &col_meta, Some("main"))
            .fields;
        assert_eq!(
            map.get("_source"),
            Some(&Value::String("crowdstrike-edr".into())),
            "the record's own _source outranks the topic's"
        );
    }

    #[test]
    fn the_topic_name_marker_never_reaches_the_column_as_text() {
        // With no topic to resolve against the column stays absent -- a NULL is
        // visibly missing, the marker's own text is a dimension nobody queries.
        let extractor = default_extractor();
        let raw = br#"{"message": "ecs output, no source key"}"#;
        let schema = make_schema(&["_source"]);
        let col_meta = timeseries_source_col_meta("dfe.cisco-ios");

        for topic_source in [None, Some("")] {
            let map = extractor
                .extract(raw, "dfe.cisco-ios", &schema, &col_meta, topic_source)
                .fields;
            assert!(
                !map.contains_key("_source"),
                "the marker must not become a literal: {:?}",
                map.get("_source")
            );
        }
    }

    #[test]
    fn test_source_uuid_fallback_resolves_to_a_uuid() {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        let raw = br#"{"other": "data"}"#;
        let schema = make_schema(&["trace_id"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "trace_id".to_string(),
            parse_directives("@source: trace.id | uuid()"),
        );
        col_meta.apply_ddl("dfe.events", ddl);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        let Some(Value::String(id)) = map.get("trace_id") else {
            panic!("trace_id must be a string uuid");
        };
        assert_ne!(
            id, "uuid()",
            "the uuid() marker must be resolved, not stored"
        );
        assert!(
            uuid::Uuid::parse_str(id).is_ok(),
            "expected a uuid, got: {id}"
        );
    }

    #[test]
    fn test_timestamp_extracted_from_source_when_present() {
        // When "timestamp" exists in source data, it should be used instead of now().
        let extractor = default_extractor();
        let raw = br#"{"timestamp": "2026-03-15 10:30:00.123", "severity": "high"}"#;
        let schema = make_schema(&["_timestamp", "severity"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-03-15 10:30:00.123".into()))
        );
    }

    #[test]
    fn test_timestamp_read_from_ecs_at_timestamp_498() {
        // Every Elastic shipper sends the event time as `@timestamp`, never
        // `timestamp`. Reading only `timestamp` left `| now()` to fire, so each
        // beats event landed bucketed on its load time (dfe-engine#498).
        let extractor = default_extractor();
        let raw = br#"{"@timestamp": "2024-03-05 01:02:03.456", "message": "x"}"#;
        let schema = make_schema(&["_timestamp", "message"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2024-03-05 01:02:03.456".into())),
            "_timestamp must carry the event's own time, not the load time"
        );
    }

    #[test]
    fn test_plain_timestamp_wins_over_at_timestamp_498() {
        // A payload carrying both keeps the undecorated spelling, so a source
        // already sending `timestamp` is unaffected by the ECS addition.
        let extractor = default_extractor();
        let raw =
            br#"{"timestamp": "2026-03-15 10:30:00.123", "@timestamp": "2024-03-05 01:02:03.456"}"#;
        let schema = make_schema(&["_timestamp"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-03-15 10:30:00.123".into()))
        );
    }

    // ========================================================================
    // RFC3339 timestamps are normalised for the wire (#144)
    // ========================================================================

    /// A schema whose named columns carry a `DateTime64(3)` type.
    fn make_datetime_schema(columns: &[&str]) -> TableSchema {
        let mut schema = make_schema(columns);
        for col in &mut schema.columns {
            col.type_name = "DateTime64(3)".to_string();
            col.parsed_type = ParsedType::parse("DateTime64(3)");
        }
        schema
    }

    #[test]
    fn test_timestamp_rfc3339_z_is_normalised() {
        // The form both transforms and the receiver emit. Left verbatim it
        // fails the whole JSONEachRow batch with code 27.
        let extractor = default_extractor();
        let raw = br#"{"timestamp": "2026-09-07T05:53:53.385Z"}"#;
        let schema = make_datetime_schema(&["_timestamp"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-09-07 05:53:53.385".into())),
            "a Z suffix must be normalised away, not carried to ClickHouse"
        );
    }

    #[test]
    fn test_timestamp_rfc3339_offset_is_converted_to_utc() {
        let extractor = default_extractor();
        let raw = br#"{"timestamp": "2026-09-07T15:53:53.385+10:00"}"#;
        let schema = make_datetime_schema(&["_timestamp"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-09-07 05:53:53.385".into())),
            "an offset must be applied, not truncated"
        );
    }

    #[test]
    fn test_datetime_column_rfc3339_is_normalised() {
        // Not only the common header: any DateTime column fed by @source hits
        // the same reader.
        let extractor = default_extractor();
        let raw = br#"{"seen_at": "2026-09-07T05:53:53.385Z"}"#;
        let schema = make_datetime_schema(&["seen_at"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("seen_at"),
            Some(&Value::String("2026-09-07 05:53:53.385".into()))
        );
    }

    #[test]
    fn test_non_rfc3339_timestamp_is_left_alone() {
        let extractor = default_extractor();
        let raw = br#"{"timestamp": 1788760433385}"#;
        let schema = make_datetime_schema(&["_timestamp"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::Number(1_788_760_433_385_i64.into())),
            "epoch millis already parse — normalisation must not touch them"
        );
    }

    #[test]
    fn test_string_column_holding_a_timestamp_is_untouched() {
        let extractor = default_extractor();
        let raw = br#"{"observed": "2026-09-07T05:53:53.385Z"}"#;
        let schema = make_schema(&["observed"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("observed"),
            Some(&Value::String("2026-09-07T05:53:53.385Z".into())),
            "a String column keeps the text it was given"
        );
    }

    // ========================================================================
    // A header pass that promotes nothing (#144)
    // ========================================================================

    #[test]
    fn test_non_object_payload_promotes_nothing() {
        let extractor = default_extractor();
        let schema = make_schema(&["message"]);
        let col_meta = empty_col_meta();

        let out = extractor.extract(b"[1,2,3]", "dfe.events", &schema, &col_meta, None);
        assert!(
            out.fields.is_empty(),
            "a non-object payload has no columns to promote"
        );
        assert_eq!(out.empty_reason, Some("payload is not a JSON object"));
    }

    #[test]
    fn test_unparseable_payload_promotes_nothing() {
        let extractor = default_extractor();
        let schema = make_schema(&["message"]);
        let col_meta = empty_col_meta();

        let out = extractor.extract(b"{not json", "dfe.events", &schema, &col_meta, None);
        assert!(
            out.fields.is_empty(),
            "an unparseable payload promotes nothing"
        );
        assert_eq!(out.empty_reason, Some("payload did not parse"));
    }

    #[test]
    fn test_valid_object_matching_no_column_reports_a_reason() {
        // The silent case: this parses, it is an object, and not one field is a
        // column. It used to reach the caller looking like any other empty pass.
        let extractor = default_extractor();
        let schema = make_schema(&["message"]);
        let col_meta = empty_col_meta();

        let out = extractor.extract(br#"{"other":1}"#, "dfe.events", &schema, &col_meta, None);
        assert!(out.fields.is_empty());
        assert_eq!(
            out.empty_reason,
            Some("no schema column matched the payload"),
            "a valid object promoting nothing must still say why"
        );
    }

    #[test]
    fn test_a_full_pass_reports_no_reason() {
        let extractor = default_extractor();
        let schema = make_schema(&["message"]);
        let col_meta = empty_col_meta();

        let out = extractor.extract(
            br#"{"message":"hi"}"#,
            "dfe.events",
            &schema,
            &col_meta,
            None,
        );
        assert_eq!(out.empty_reason, None);
    }

    // ========================================================================
    // The already-normal date-time value costs no parse and no alloc
    // ========================================================================

    #[test]
    fn test_clickhouse_space_form_is_not_reallocated() {
        let mut map = Map::new();
        map.insert(
            "_timestamp".to_string(),
            Value::String("2026-09-07 05:53:53.385".to_string()),
        );
        let before = map["_timestamp"].as_str().expect("string").as_ptr();

        normalise_datetime(&mut map, "_timestamp");

        let after = map["_timestamp"].as_str().expect("string");
        assert_eq!(after, "2026-09-07 05:53:53.385");
        assert_eq!(
            after.as_ptr(),
            before,
            "the guard path must leave the value where it was, not rebuild it"
        );
    }

    #[test]
    fn test_rfc3339_guard_admits_only_what_can_parse() {
        assert!(looks_rfc3339("2026-09-07T05:53:53.385Z"));
        assert!(looks_rfc3339("2026-09-07t05:53:53Z"));
        assert!(looks_rfc3339("2026-09-07 05:53:53.385+10:00"));
        assert!(!looks_rfc3339("2026-09-07 05:53:53.385"));
        assert!(!looks_rfc3339("2026-09-07 05:53:53"));
        assert!(!looks_rfc3339("2026-09-07"));
        assert!(!looks_rfc3339(""));
    }

    #[test]
    fn test_space_form_survives_a_datetime_column() {
        let extractor = default_extractor();
        let raw = br#"{"seen_at": "2026-09-07 05:53:53.385"}"#;
        let schema = make_datetime_schema(&["seen_at"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("seen_at"),
            Some(&Value::String("2026-09-07 05:53:53.385".into())),
            "ClickHouse's own text form must pass through untouched"
        );
    }

    // ========================================================================
    // Source fields more than one column reads (dfe-engine#456)
    // ========================================================================

    /// The `dfe.proofsyslog` directives, as the loader reads them from the DDL.
    fn syslog_col_meta() -> ColumnMetaCache {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let mut ddl = FxHashMap::default();
        ddl.insert(
            "timestamp".to_string(),
            parse_directives("@source: first(timestamp/@timestamp/time) - Event timestamp"),
        );
        ddl.insert(
            "_timestamp".to_string(),
            parse_directives("@source: timestamp | now()"),
        );
        let cache = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        cache.apply_ddl("dfe.proofsyslog", ddl);
        cache
    }

    #[test]
    fn test_header_and_meta_column_both_fill_from_one_timestamp_field() {
        // One syslog record on dfe.proofsyslog landed _timestamp filled and
        // timestamp NULL: the header column moved "timestamp" out of the
        // payload before the meta column's first(timestamp/@timestamp/time)
        // reached it.
        let extractor = default_extractor();
        let raw = br#"{"timestamp": "2026-09-21T04:45:00Z", "hostname": "proof-host-01"}"#;
        let schema = make_datetime_schema(&["_timestamp", "timestamp"]);
        let col_meta = syslog_col_meta();

        let map = extractor
            .extract(raw, "dfe.proofsyslog", &schema, &col_meta, None)
            .fields;

        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-09-21 04:45:00.000".into())),
            "the header column still fills"
        );
        assert_eq!(
            map.get("timestamp"),
            Some(&Value::String("2026-09-21 04:45:00.000".into())),
            "the meta column reading the same field must fill too, not land NULL"
        );
    }

    #[test]
    fn test_underscore_header_column_leaves_the_plain_field_behind() {
        // The collision is not only `timestamp`: any header column `_foo` reads
        // "foo" before falling back to "_foo", so it can empty the field a
        // plain `foo` column reads.
        let extractor = default_extractor();
        let raw = br#"{"tenant": "acme"}"#;
        let schema = make_schema(&["_tenant", "tenant"]);
        let col_meta = empty_col_meta();

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;

        assert_eq!(map.get("_tenant"), Some(&Value::String("acme".into())));
        assert_eq!(
            map.get("tenant"),
            Some(&Value::String("acme".into())),
            "the stripped-name rule must not empty the field its own column reads"
        );
    }

    #[test]
    fn test_uncontended_path_moves_the_value_rather_than_copying_it() {
        // The move is what keeps the hot path free of a per-row allocation, so
        // the promoted value must be the same buffer the payload held.
        let mut parsed = Map::new();
        parsed.insert("message".to_string(), Value::String("m".repeat(64)));
        let before = parsed["message"].as_str().expect("string").as_ptr();

        let mut map = Map::new();
        let contended = FxHashSet::default();
        assert!(take_path(
            &mut parsed,
            "message",
            "message",
            &mut map,
            &contended
        ));

        assert_eq!(
            map["message"].as_str().expect("string").as_ptr(),
            before,
            "an uncontended field must move, not allocate a copy"
        );
        assert!(
            !parsed.contains_key("message"),
            "a moved field leaves the payload"
        );
    }

    #[test]
    fn test_contended_path_copies_and_leaves_the_field_for_the_next_column() {
        let mut parsed = Map::new();
        parsed.insert("timestamp".to_string(), Value::String("t".repeat(64)));
        let before = parsed["timestamp"].as_str().expect("string").as_ptr();

        let mut map = Map::new();
        let mut contended: FxHashSet<Box<str>> = FxHashSet::default();
        contended.insert(Box::from("timestamp"));
        assert!(take_path(
            &mut parsed,
            "timestamp",
            "_timestamp",
            &mut map,
            &contended
        ));

        assert_ne!(
            map["_timestamp"].as_str().expect("string").as_ptr(),
            before,
            "a shared field is copied"
        );
        assert!(
            parsed.contains_key("timestamp"),
            "the copy must leave the field for the columns that follow"
        );
    }

    #[test]
    fn test_ordinary_schema_shares_no_source_field() {
        // The copy is confined to the collision: with nothing shared the set is
        // empty, so every column takes the move branch.
        let extractor = default_extractor();
        let schema = make_schema(&[
            "_timestamp",
            "_timestamp_received",
            "_source",
            "_org_id",
            "hostname",
            "app_name",
            "message",
        ]);
        let col_meta = empty_col_meta();

        let plan = extractor.plan_for("dfe.events", &schema, &col_meta);
        assert!(
            plan.contended.is_empty(),
            "no column shares a source field here: {:?}",
            plan.contended
        );
    }

    #[test]
    fn test_plan_finds_only_the_shared_path() {
        let extractor = default_extractor();
        let schema = make_datetime_schema(&["_timestamp", "timestamp"]);
        let col_meta = syslog_col_meta();

        let plan = extractor.plan_for("dfe.proofsyslog", &schema, &col_meta);
        let mut shared: Vec<&str> = plan.contended.iter().map(AsRef::as_ref).collect();
        shared.sort_unstable();
        assert_eq!(
            shared,
            vec!["@timestamp", "timestamp"],
            "both spellings are now read by two columns; `time` is read by one and keeps the move"
        );
    }

    #[test]
    fn test_header_and_meta_column_both_fill_from_at_timestamp_498() {
        // The beats case of the shared-field rule: with `@timestamp` the only
        // event time in the payload, the header column must not empty it before
        // the meta column's first(timestamp/@timestamp/time) reaches it.
        let extractor = default_extractor();
        let raw = br#"{"@timestamp": "2024-03-05T01:02:03.456Z", "hostname": "probe498"}"#;
        let schema = make_datetime_schema(&["_timestamp", "timestamp"]);
        let col_meta = syslog_col_meta();

        let map = extractor
            .extract(raw, "dfe.proofsyslog", &schema, &col_meta, None)
            .fields;

        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2024-03-05 01:02:03.456".into())),
            "the header column carries the event's own time"
        );
        assert_eq!(
            map.get("timestamp"),
            Some(&Value::String("2024-03-05 01:02:03.456".into())),
            "the meta column reading the same field must fill too, not land NULL"
        );
    }

    #[test]
    fn test_plan_rebuilds_when_a_directive_starts_a_collision() {
        use crate::column_meta::parse_directives;
        use rustc_hash::FxHashMap;

        let extractor = default_extractor();
        let schema = make_schema(&["_timestamp", "event_time"]);
        let col_meta = empty_col_meta();
        let raw = br#"{"timestamp": "2026-09-21 04:45:00.000"}"#;

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert!(
            !map.contains_key("event_time"),
            "event_time reads its own name, which this payload has no field for"
        );

        let mut ddl = FxHashMap::default();
        ddl.insert(
            "event_time".to_string(),
            parse_directives("@source: timestamp"),
        );
        col_meta.apply_ddl("dfe.events", ddl);

        let map = extractor
            .extract(raw, "dfe.events", &schema, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("event_time"),
            Some(&Value::String("2026-09-21 04:45:00.000".into())),
            "the plan must rebuild on a directive change, or _timestamp takes the field alone"
        );
        assert_eq!(
            map.get("_timestamp"),
            Some(&Value::String("2026-09-21 04:45:00.000".into()))
        );
    }

    #[test]
    fn test_plan_rebuilds_when_a_column_is_added() {
        let extractor = default_extractor();
        let col_meta = empty_col_meta();
        let raw = br#"{"tenant": "acme"}"#;

        let before = make_schema(&["_tenant"]);
        let map = extractor
            .extract(raw, "dfe.events", &before, &col_meta, None)
            .fields;
        assert_eq!(map.get("_tenant"), Some(&Value::String("acme".into())));

        // An ALTER adds the plain column the header column was already reading.
        let after = make_schema(&["_tenant", "tenant"]);
        let map = extractor
            .extract(raw, "dfe.events", &after, &col_meta, None)
            .fields;
        assert_eq!(
            map.get("tenant"),
            Some(&Value::String("acme".into())),
            "a plan keyed on the old column list would miss the new collision"
        );
        assert_eq!(map.get("_tenant"), Some(&Value::String("acme".into())));
    }
}
