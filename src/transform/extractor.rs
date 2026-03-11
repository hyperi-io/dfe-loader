// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Schema-guided SIMD field extractor for the `json_primary` pipeline mode.
//!
//! Extracts only the columns present in the destination table schema from raw
//! JSON bytes, using `sonic_rs::get_from_slice` for one SIMD scan per column
//! rather than a full DOM parse.
//!
//! ## What this does NOT do
//!
//! - No flattening — only top-level field extraction per schema column
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
//! | `@skip` directive | Excluded from insert |
//! | `@renamed:first(a/b/c)` | First-match source field lookup |
//! | `@default:value` | Applied when all source fields are absent |

use chrono::Utc;
use serde_json::{Map, Value};
use sonic_rs::{from_str as sonic_from_str, get_from_slice};
use tracing::debug;

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
    /// Returns a `Map<String, Value>` of promoted fields only.
    /// `_json` is NOT included — it is spliced at serialisation time.
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

            let directives = col_meta.get(table, name);
            if directives.skip {
                continue;
            }

            // Determine source field path(s) for extraction.
            // Priority: @renamed directive > per-column defaults > column name.
            let found = if !directives.renamed.is_empty() {
                extract_first(raw, &directives.renamed, name, &mut map)
            } else {
                match name.as_str() {
                    "_org_id" => extract_first(
                        raw,
                        std::slice::from_ref(&self.org_id_field),
                        name,
                        &mut map,
                    ),
                    "_source" if self.capture_source && self.metadata_enabled => {
                        extract_first(raw, &self.source_fields, name, &mut map)
                    }
                    _ if name.starts_with('_') => {
                        // _foo → try source field "foo" first (common convention),
                        // then "_foo" as a literal fallback.
                        let stripped = name[1..].to_string();
                        extract_first(raw, &[stripped, name.clone()], name, &mut map)
                    }
                    _ => extract_first(raw, &[name.clone()], name, &mut map),
                }
            };

            // Apply column default when all source fields were absent.
            if !found {
                if let Some(ref default) = directives.default {
                    map.insert(name.clone(), default.clone());
                }
            }
        }

        debug!(table = %table, fields = map.len(), "Extracted promoted fields");
        map
    }
}

/// Try source fields in order — insert the first found value into `map`.
///
/// Returns `true` if any source field was found and inserted.
#[inline]
fn extract_first(raw: &[u8], sources: &[String], dest: &str, map: &mut Map<String, Value>) -> bool {
    for source in sources {
        if let Ok(lazy) = get_from_slice(raw, &[source.as_str()]) {
            // as_raw_str() returns the raw JSON text of the value (e.g. `"hello"`, `42`, `true`).
            // sonic_rs::from_str re-parses this into a typed Value (4-8x faster than serde_json).
            if let Ok(v) = sonic_from_str::<Value>(lazy.as_raw_str()) {
                map.insert(dest.to_string(), v);
                return true;
            }
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
        let raw = br#"{}"#;
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
        let mut metadata = MetadataConfig::default();
        metadata.capture_source = false;
        let extractor2 = HeaderExtractor::new(&metadata, &RoutingConfig::default());
        let col_meta = empty_col_meta();

        let map = extractor2.extract(raw, "dfe.events", &schema, &col_meta);
        assert_eq!(map.get("_source"), Some(&Value::String("auth".into())));
    }
}
