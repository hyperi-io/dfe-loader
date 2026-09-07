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

use chrono::Utc;
use serde_json::{Map, Value};
use tracing::{debug, error};

use crate::transform::transformer::fmt_ts;

use crate::clickhouse::TableSchema;
use crate::column_meta::ColumnMetaCache;
use crate::config::{MetadataConfig, RoutingConfig};

/// Schema-guided SIMD field extractor.
///
/// Created once from config at startup. Stateless — safe to share across tasks.
pub struct HeaderExtractor {
    /// Field name in source data to extract for `_org_id` (RLS column).
    org_id_field: String,
    /// Field names to check for `_source` value (first match wins).
    source_fields: Vec<String>,
    /// Whether to extract `_source`.
    capture_source: bool,
    /// Whether the common header (all `_`-prefixed columns) is enabled.
    metadata_enabled: bool,
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
    pub fn extract(
        &self,
        raw: &[u8],
        table: &str,
        schema: &TableSchema,
        col_meta: &ColumnMetaCache,
    ) -> Map<String, Value> {
        let now = Utc::now();
        let mut map = Map::with_capacity(schema.columns.len());

        // Parse once — all schema column lookups are O(1) hash operations on this map.
        // Misses are free here; with get_from_slice each miss still scans the full document.
        // Take ownership of the parsed map so we can move Values out (zero-clone).
        // Each lookup_move/lookup_first_move call removes the value from `parsed`
        // and moves it into the output map — no Value::clone() on the hot path.
        // A payload the header pass cannot read used to return an empty map at
        // debug level, and the row landed with every column at its type default.
        // Say so at ERROR and let the caller reject it (#144).
        let mut parsed = match sonic_rs::from_slice::<Value>(raw) {
            Ok(Value::Object(map)) => map,
            Ok(_) => {
                log_header_pass_skipped(table, "payload is not a JSON object");
                return map;
            }
            Err(e) => {
                log_header_pass_skipped(table, "payload did not parse");
                debug!(table = %table, error = %e, "Payload parse failed, skipping extraction");
                return map;
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
                let found = lookup_move(&mut parsed, "timestamp", name, &mut map)
                    || lookup_move(&mut parsed, name, name, &mut map);
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

            // Determine source field path(s) for extraction.
            // Priority: @renamed directive > per-column defaults > column name.
            let found = if directives.renamed.is_empty() {
                match name.as_str() {
                    "_org_id" => lookup_move(&mut parsed, &self.org_id_field, name, &mut map),
                    "_source" if self.capture_source && self.metadata_enabled => {
                        lookup_first_move(&mut parsed, &self.source_fields, name, &mut map)
                    }
                    _ if name.starts_with('_') => {
                        // _foo → try "foo" (stripped) first, then "_foo" as literal fallback.
                        lookup_move(&mut parsed, &name[1..], name, &mut map)
                            || lookup_move(&mut parsed, name, name, &mut map)
                    }
                    _ => lookup_move(&mut parsed, name, name, &mut map),
                }
            } else {
                lookup_first_move(&mut parsed, &directives.renamed, name, &mut map)
            };

            // Apply column default when all source fields were absent.
            if !found && let Some(ref default) = directives.default {
                map.insert(name.clone(), resolve_default(default, &now));
            } else if found && is_datetime(col) {
                normalise_datetime(&mut map, name);
            }
        }

        debug!(table = %table, fields = map.len(), "Extracted promoted fields");
        map
    }
}

/// Report a header pass that promoted nothing, at most once a minute.
///
/// The table name goes in the log, never in a metric label: it comes from a
/// payload field with no allowlist, so labelling it would let untrusted input
/// grow the label set without bound.
pub(crate) fn log_header_pass_skipped(table: &str, reason: &str) {
    metrics::counter!("dfe_loader_header_pass_skipped_total").increment(1);
    static SKIPPED_TS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if scalo::logger::log_debounced(&SKIPPED_TS, 60_000) {
        error!(
            table = %table,
            reason = reason,
            "Header pass promoted no columns, rejecting the message (max 1 per 60s)"
        );
    }
}

/// Whether a column takes a `ClickHouse` date-time text value.
#[inline]
fn is_datetime(col: &crate::clickhouse::types::ColumnInfo) -> bool {
    use crate::clickhouse_ext::parsed_type::ParsedTypeExt;
    matches!(
        col.parsed_type.coercer_category(),
        "DateTime" | "DateTime64"
    )
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
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
        *value = Value::String(fmt_ts(&dt.with_timezone(&Utc)));
    }
}

/// Resolve a parsed `@default` / `@source` fallback against this extraction's clock.
///
/// `now()` and `uuid()` are the two fallbacks that cannot be parse-time
/// constants, so they travel as literal markers and are resolved here, once per
/// row. `uuid()` is v7 to match the time-ordered form the schemas use.
///
/// The parser has already rejected any fallback outside the documented
/// vocabulary, so nothing reaching here is a stray field name.
#[inline]
fn resolve_default(default: &Value, now: &chrono::DateTime<Utc>) -> Value {
    match default.as_str() {
        Some("now()") => Value::String(fmt_ts(now)),
        Some("uuid()") => Value::String(uuid::Uuid::now_v7().to_string()),
        _ => default.clone(),
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

/// Move a source field from the parsed object into the output map (zero-clone).
///
/// Uses `remove_path()` to take ownership of the Value — no allocation for the
/// value itself. The key allocation (`dest.to_string()`) is unavoidable since
/// `Map<String, Value>` requires owned keys.
#[inline]
fn lookup_move(
    parsed: &mut serde_json::Map<String, Value>,
    source: &str,
    dest: &str,
    map: &mut Map<String, Value>,
) -> bool {
    if let Some(v) = remove_path(parsed, source) {
        map.insert(dest.to_string(), v);
        true
    } else {
        false
    }
}

/// Try source field names in order, moving the first match (zero-clone).
#[inline]
fn lookup_first_move(
    parsed: &mut serde_json::Map<String, Value>,
    sources: &[String],
    dest: &str,
    map: &mut Map<String, Value>,
) -> bool {
    for source in sources {
        if let Some(v) = remove_path(parsed, source.as_str()) {
            map.insert(dest.to_string(), v);
            return true;
        }
    }
    false
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);

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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
        assert_eq!(map.get("severity"), Some(&Value::String("unknown".into())));
    }

    #[test]
    fn test_numeric_and_bool_values() {
        let extractor = default_extractor();
        let raw = br#"{"count": 42, "is_active": true, "ratio": 0.75}"#;
        let schema = make_schema(&["count", "is_active", "ratio"]);
        let col_meta = empty_col_meta();

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor2.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);

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

        let map = extractor.extract(FILEBEAT_PAYLOAD, "dfe.filebeat", &schema, &col_meta);

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

        let map = extractor.extract(FILEBEAT_PAYLOAD, "dfe.filebeat", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.filebeat", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.filebeat", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        // Verbatim from a deployed dfe.filebeat. `topic_name` names a field the
        // extractor cannot see, so _source must stay absent rather than be
        // stamped with the string "topic_name" -- it is a LowCardinality
        // dimension operators group by.
        let extractor = default_extractor();
        let raw = br#"{"message": "no source key here"}"#;
        let schema = make_schema(&["_source"]);

        let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
        let mut ddl = FxHashMap::default();
        ddl.insert(
            "_source".to_string(),
            parse_directives(
                "@source: first(_source) | topic_name - Data source label (falls back to the topic)",
            ),
        );
        col_meta.apply_ddl("dfe.filebeat", ddl);

        let map = extractor.extract(raw, "dfe.filebeat", &schema, &col_meta);
        assert!(
            !map.contains_key("_source"),
            "an unresolvable fallback must not become a literal: {:?}",
            map.get("_source")
        );
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(raw, "dfe.events", &schema, &col_meta);
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

        let map = extractor.extract(b"[1,2,3]", "dfe.events", &schema, &col_meta);
        assert!(
            map.is_empty(),
            "a non-object payload has no columns to promote"
        );
    }

    #[test]
    fn test_unparseable_payload_promotes_nothing() {
        let extractor = default_extractor();
        let schema = make_schema(&["message"]);
        let col_meta = empty_col_meta();

        let map = extractor.extract(b"{not json", "dfe.events", &schema, &col_meta);
        assert!(map.is_empty(), "an unparseable payload promotes nothing");
    }
}
