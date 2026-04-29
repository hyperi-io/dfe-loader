// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Routes messages to destination db.table based on event data
//!
//! Routing happens PRE-flattening using dot notation for nested field access.
//! db is extracted from first matching field in priority list (default: `org_id`)
//! table is extracted from first matching field in priority list (default: _source)
//!
//! ## Performance
//!
//! Prefer `route_value()` when you already have a parsed `serde_json::Value` to avoid
//! double-parsing. Use `route()` only when you only have raw bytes.
//!
//! Uses `Cow<str>` internally to avoid String allocations when returning references.

use std::borrow::Cow;

use rustc_hash::FxHashMap;
use serde_json::Value;
use tracing::warn;

use crate::config::{MetadataConfig, RoutingConfig};
use crate::payload::parse::{
    extract_field_json, extract_field_json_cow, extract_nested_field_json,
    extract_nested_field_json_cow,
};

/// A pre-compiled CEL routing rule.
struct CompiledRoutingRule {
    program: cel::Program,
    target: String,
    db: Option<String>,
}

/// Route result with destination info
#[derive(Debug, Clone, PartialEq)]
pub enum RouteResult {
    /// Route to specific db.table
    Table(String),
    /// Send to DLQ (routing failed and DLQ enabled)
    Dlq(String),
}

/// Routes messages to db.table based on event data fields
///
/// Extracts database and table names from configured field priority lists.
/// Falls back to configurable defaults if no fields match.
///
/// ## Shared Schema (Default)
///
/// By default, all organisations share the common database:
/// - `db_fields` is empty → always use `default_db`
/// - All events go to `common.{table}`
/// - `org_id` extracted to `_org_id` field for row-level security
///
/// ## Per-Org Routing (Optional)
///
/// Explicitly list orgs in `org_routes` config. Only listed orgs receive
/// a dedicated database — all others always go to `default_db`.
pub struct Router {
    /// Pre-compiled CEL routing rules (top-to-bottom, first match wins)
    compiled_rules: Vec<CompiledRoutingRule>,
    /// Fields to check for database name (first match wins)
    /// Empty = always use `default_db` (shared schema)
    db_fields: Vec<String>,
    /// Fields to check for table name (first match wins)
    table_fields: Vec<String>,
    /// Default database if no `db_field` matches (or `db_fields` empty), or if
    /// the org is not listed in `org_routes`.
    default_db: String,
    /// Default table if no `table_field` matches
    default_table: String,
    /// Field to extract for _`org_id` column (for RLS)
    org_id_field: Option<String>,
    /// Per-org database routing: `org_id` → effective database name.
    /// Only orgs listed here get their own database.
    org_routes: FxHashMap<String, String>,
    /// Source value to table name mapping
    source_to_table: FxHashMap<String, String>,
    /// Whether DLQ is enabled
    dlq_enabled: bool,
    /// Fields to check for _source value (first match wins)
    source_fields: Vec<String>,
    /// Topic suffixes to strip when deriving _source from Kafka topic
    topic_suffixes: Vec<String>,
}

impl Router {
    /// Create a new router from routing and metadata config
    pub fn new(config: &RoutingConfig) -> Self {
        Self::with_metadata(config, &MetadataConfig::default())
    }

    /// Create a new router from routing and metadata config
    ///
    /// Metadata config provides `source_fields` for _source extraction.
    /// When `compat_v2_source` is enabled, `event_category/tags.event_category`
    /// are prepended to both `source_fields` and `table_fields`.
    pub fn with_metadata(config: &RoutingConfig, metadata: &MetadataConfig) -> Self {
        // Convert HashMap to FxHashMap for faster lookups
        let source_to_table: FxHashMap<String, String> = config
            .source_to_table
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Build org_routes: org_id → effective database name
        let org_routes: FxHashMap<String, String> = config
            .org_routes
            .iter()
            .map(|r| (r.org_id.clone(), r.effective_database().to_string()))
            .collect();

        // Build source_fields from metadata config
        let mut source_fields = metadata.source_fields.clone();
        let mut table_fields = config.table_fields.clone();

        // Pre-DFE 2.2 compat: prepend legacy fields
        if config.compat_v2_source {
            let legacy = vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ];
            let mut combined_source = legacy.clone();
            combined_source.extend(source_fields);
            source_fields = combined_source;

            let mut combined_table = legacy;
            combined_table.extend(table_fields);
            table_fields = combined_table;
        }

        // Compile CEL routing rules (invalid rules are skipped with warning)
        let compiled_rules: Vec<CompiledRoutingRule> = config
            .rules
            .iter()
            .filter_map(
                |rule| match hyperi_rustlib::expression::compile(&rule.when) {
                    Ok(program) => Some(CompiledRoutingRule {
                        program,
                        target: rule.target.clone(),
                        db: rule.db.clone(),
                    }),
                    Err(e) => {
                        warn!(
                            expr = %rule.when,
                            target = %rule.target,
                            error = %e,
                            "Skipping invalid routing rule"
                        );
                        None
                    }
                },
            )
            .collect();

        Self {
            compiled_rules,
            db_fields: config.db_fields.clone(),
            table_fields,
            default_db: config.default_db.clone(),
            default_table: config.default_table.clone(),
            org_id_field: config.org_id_field.clone(),
            org_routes,
            source_to_table,
            dlq_enabled: config.dlq.enabled,
            source_fields,
            topic_suffixes: config.topic_suffixes.clone(),
        }
    }

    /// Extract the database name from the payload.
    ///
    /// Logic:
    /// 1. If `db_fields` is empty → always use `default_db` (shared schema).
    /// 2. Extract the first matching value from `db_fields`.
    /// 3. If that value is listed in `org_routes` → use its mapped database.
    /// 4. Otherwise → use `default_db`.
    #[inline]
    pub fn extract_db(&self, payload: &[u8]) -> String {
        // Delegate to the Cow path — borrows from `self` or the payload
        // when possible and only allocates on `into_owned()` for the
        // legacy `String`-returning callers.
        self.extract_db_cow(payload).into_owned()
    }

    /// Extract a single field from payload (helper for `org_id_field`)
    #[inline]
    fn extract_single_field(&self, payload: &[u8], field: &str) -> Option<String> {
        if field.contains('.') {
            extract_nested_field_json(payload, field)
        } else {
            extract_field_json(payload, field)
        }
    }

    /// Extract the table name from the payload
    #[inline]
    pub fn extract_table(&self, payload: &[u8]) -> String {
        // Delegate to the Cow path — see `extract_db` for rationale.
        self.extract_table_cow(payload).into_owned()
    }

    /// Route a message to db.table based on its payload (raw bytes)
    ///
    /// **Performance note**: This parses the JSON. If you already have a parsed
    /// `serde_json::Value`, use `route_value()` instead to avoid double-parsing.
    ///
    /// Returns "db.table" string for buffer routing.
    pub fn route(&self, payload: &[u8]) -> RouteResult {
        let db = self.extract_db(payload);
        let table = self.extract_table(payload);

        self.build_route_result(&db, &table)
    }

    /// Zero-copy route from raw bytes.
    ///
    /// Uses zero-copy field extraction to avoid String allocations when extracting
    /// routing fields from non-escaped JSON strings (the common case).
    /// The only guaranteed allocation is the final "db.table" string.
    ///
    /// **This is the fastest routing method** for raw bytes - use when you haven't
    /// parsed the JSON yet and may not need to (e.g., DLQ routing).
    #[inline]
    pub fn route_cow(&self, payload: &[u8]) -> RouteResult {
        let db = self.extract_db_cow(payload);
        let table = self.extract_table_cow(payload);

        self.build_route_result(&db, &table)
    }

    /// Zero-copy extract db from raw payload.
    ///
    /// Returns `Cow::Borrowed` for non-escaped strings (zero-copy from payload),
    /// `Cow::Owned` for escaped strings or when using default.
    /// Uses same logic as `extract_db()` for consistency.
    #[inline]
    fn extract_db_cow<'a>(&'a self, payload: &'a [u8]) -> Cow<'a, str> {
        // db_fields empty = always use default (shared schema)
        if self.db_fields.is_empty() {
            return Cow::Borrowed(&self.default_db);
        }

        // Extract from db_fields and look up in org_routes
        if let Some(org_id_cow) = self.extract_first_match_cow(payload, &self.db_fields)
            && let Some(db) = self.org_routes.get(org_id_cow.as_ref())
        {
            return Cow::Owned(db.clone());
        }

        // Fall back to default database
        Cow::Borrowed(&self.default_db)
    }

    /// Zero-copy extract table from raw payload.
    ///
    /// Returns `Cow::Borrowed` for non-escaped strings (zero-copy from payload),
    /// `Cow::Owned` for escaped strings or when using default/mapping.
    #[inline]
    fn extract_table_cow<'a>(&'a self, payload: &'a [u8]) -> Cow<'a, str> {
        let table_cow = self.extract_first_match_cow(payload, &self.table_fields);

        match table_cow {
            Some(cow) => {
                // Check legacy source_to_table mapping
                if let Some(mapped) = self.source_to_table.get(cow.as_ref()) {
                    Cow::Borrowed(mapped.as_str())
                } else {
                    cow
                }
            }
            None => Cow::Borrowed(&self.default_table),
        }
    }

    /// Zero-copy field extraction from raw bytes.
    ///
    /// Returns `Cow::Borrowed` for non-escaped strings, `Cow::Owned` for escaped.
    #[inline]
    fn extract_first_match_cow<'a>(
        &self,
        payload: &'a [u8],
        fields: &[String],
    ) -> Option<Cow<'a, str>> {
        for field in fields {
            let value = if field.contains('.') {
                extract_nested_field_json_cow(payload, field)
            } else {
                extract_field_json_cow(payload, field)
            };
            if value.is_some() {
                return value;
            }
        }
        None
    }

    /// Route a message to db.table based on already-parsed JSON Value
    ///
    /// **This is the preferred method** when you've already parsed the JSON,
    /// as it avoids a second parse. The orchestrator should use this.
    ///
    /// CEL routing rules are evaluated first (top-to-bottom, first match wins).
    /// If no rule matches, falls through to field-extraction routing.
    ///
    /// Returns "db.table" string for buffer routing.
    #[inline]
    pub fn route_value(&self, value: &Value) -> RouteResult {
        // Try CEL rules first (top-to-bottom, first match wins)
        if let Some(result) = self.try_cel_rules(value) {
            return result;
        }

        // Fall through to field-extraction routing
        let db = self.extract_db_from_value(value);
        let table = self.extract_table_from_value(value);

        self.build_route_result(&db, &table)
    }

    /// Evaluate CEL routing rules against the message.
    ///
    /// Builds a CEL context from the JSON value's top-level keys, then
    /// evaluates each compiled rule. Returns the first match.
    /// Returns None if no rules exist or none match.
    fn try_cel_rules(&self, value: &Value) -> Option<RouteResult> {
        if self.compiled_rules.is_empty() {
            return None;
        }

        // Pass serde_json::Map directly — build_context accepts any iterator
        // of (&String, &Value), no clone needed.
        let obj = value.as_object()?;
        let context = match hyperi_rustlib::expression::build_context(obj) {
            Ok(ctx) => ctx,
            Err(_) => return None,
        };

        for rule in &self.compiled_rules {
            if let Ok(cel::Value::Bool(true)) = rule.program.execute(&context) {
                let db = rule.db.as_deref().unwrap_or(&self.default_db);
                return Some(RouteResult::Table(build_db_table_string(db, &rule.target)));
            }
        }

        None
    }

    /// Build `RouteResult` from db and table strings
    ///
    /// Uses pre-allocated String with exact capacity to avoid format!() overhead.
    #[inline]
    fn build_route_result(&self, db: &str, table: &str) -> RouteResult {
        // Validate we have non-empty values
        if db.is_empty() || table.is_empty() {
            if self.dlq_enabled {
                return RouteResult::Dlq("empty_db_or_table".into());
            }
            // Use defaults for empty values
            let db = if db.is_empty() { &self.default_db } else { db };
            let table = if table.is_empty() {
                &self.default_table
            } else {
                table
            };
            return RouteResult::Table(build_db_table_string(db, table));
        }

        RouteResult::Table(build_db_table_string(db, table))
    }

    /// Extract db from an already-parsed Value (avoids re-parsing)
    ///
    /// Returns borrowed reference when using default, owned when from payload.
    /// Uses same logic as `extract_db()` for consistency.
    #[inline]
    fn extract_db_from_value<'a>(&'a self, value: &'a Value) -> Cow<'a, str> {
        // db_fields empty = always use default (shared schema)
        if self.db_fields.is_empty() {
            return Cow::Borrowed(&self.default_db);
        }

        // Extract from db_fields and look up in org_routes
        if let Some(org_id) = self.extract_first_match_from_value(value, &self.db_fields)
            && let Some(db) = self.org_routes.get(org_id)
        {
            return Cow::Borrowed(db.as_str());
        }

        // Fall back to default database
        Cow::Borrowed(&self.default_db)
    }

    /// Extract table from an already-parsed Value (avoids re-parsing)
    ///
    /// Returns borrowed reference when possible, owned when mapped.
    #[inline]
    fn extract_table_from_value<'a>(&'a self, value: &'a Value) -> Cow<'a, str> {
        let table = self
            .extract_first_match_from_value(value, &self.table_fields)
            .unwrap_or(&self.default_table);

        // Check legacy source_to_table mapping
        if let Some(mapped) = self.source_to_table.get(table) {
            Cow::Borrowed(mapped.as_str())
        } else {
            Cow::Borrowed(table)
        }
    }

    /// Extract a field value from parsed JSON Value using dot notation
    ///
    /// Returns borrowed `&str` from the Value to avoid allocation.
    #[inline]
    fn extract_first_match_from_value<'a>(
        &self,
        value: &'a Value,
        fields: &[String],
    ) -> Option<&'a str> {
        for field in fields {
            if let Some(val) = self.get_nested_field(value, field)
                && let Some(s) = val.as_str()
            {
                return Some(s);
            }
        }
        None
    }

    /// Get a nested field from a Value using dot notation (e.g., "`tags.event_category`")
    ///
    /// Fast path for simple (non-nested) field names to avoid iterator allocation.
    #[inline]
    fn get_nested_field<'a>(&self, value: &'a Value, path: &str) -> Option<&'a Value> {
        // Fast path: no dot means simple field access (most common case)
        if !path.contains('.') {
            return value.get(path);
        }

        // Slow path: nested field access
        let mut current = value;
        for part in path.split('.') {
            current = current.get(part)?;
        }
        Some(current)
    }

    /// Route with a pre-extracted source value
    ///
    /// Looks up the source value in `source_to_table` for a name mapping,
    /// falling back to using the source value directly as the table name.
    pub fn route_source(&self, source: &str) -> RouteResult {
        let table = if let Some(mapped) = self.source_to_table.get(source) {
            mapped.as_str()
        } else {
            source
        };

        RouteResult::Table(build_db_table_string(&self.default_db, table))
    }

    /// Extract _source value from parsed JSON Value
    ///
    /// Checks `source_fields` in order (first match wins).
    /// Returns None if no field matches — caller should fall back to topic derivation.
    #[inline]
    pub fn extract_source_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        self.extract_first_match_from_value(value, &self.source_fields)
    }

    /// Derive _source from Kafka topic name by stripping configured suffixes
    ///
    /// Strips the first matching suffix from `topic_suffixes`.
    /// Example: topic "`auth_land`" with suffix "_land" → "auth"
    /// If no suffix matches, returns the full topic name.
    #[inline]
    pub fn derive_source_from_topic(&self, topic: &str) -> String {
        for suffix in &self.topic_suffixes {
            if let Some(stripped) = topic.strip_suffix(suffix.as_str()) {
                return stripped.to_string();
            }
        }
        topic.to_string()
    }

    /// Extract `org_id` for _`org_id` field (from parsed Value)
    ///
    /// Used by transformer to populate _`org_id` column for row-level security.
    /// Returns None if `org_id_field` not configured or field not found.
    #[inline]
    pub fn extract_org_id_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        let field = self.org_id_field.as_ref()?;
        self.get_nested_field(value, field)?.as_str()
    }

    /// Extract `org_id` for _`org_id` field (from raw bytes)
    ///
    /// Used by transformer to populate _`org_id` column for row-level security.
    /// Returns None if `org_id_field` not configured or field not found.
    #[inline]
    pub fn extract_org_id(&self, payload: &[u8]) -> Option<String> {
        let field = self.org_id_field.as_ref()?;
        self.extract_single_field(payload, field)
    }

    /// Get the configured `org_id` field name (if any)
    pub fn org_id_field(&self) -> Option<&str> {
        self.org_id_field.as_deref()
    }
}

/// Build "db.table" string efficiently with pre-allocated capacity.
///
/// Avoids format!() macro overhead by using `push_str` directly.
#[inline]
fn build_db_table_string(db: &str, table: &str) -> String {
    let mut result = String::with_capacity(db.len() + 1 + table.len());
    result.push_str(db);
    result.push('.');
    result.push_str(table);
    result
}

impl Default for Router {
    fn default() -> Self {
        Self {
            compiled_rules: vec![],
            // Default: db_fields empty = shared schema (all to dfe.*)
            db_fields: vec![],
            table_fields: vec!["_source".to_string()],
            default_db: "dfe".to_string(),
            default_table: "default".to_string(),
            // Extract org_id for _org_id column (RLS)
            org_id_field: Some("org_id".to_string()),
            // No per-org routing by default (shared schema)
            org_routes: FxHashMap::default(),
            source_to_table: FxHashMap::default(),
            dlq_enabled: true,
            source_fields: vec!["_source".to_string()],
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_config() -> RoutingConfig {
        let mut source_to_table = HashMap::new();
        source_to_table.insert("auth".to_string(), "events_auth".to_string());
        source_to_table.insert("api".to_string(), "events_api".to_string());

        RoutingConfig {
            db_fields: vec!["org_id".to_string()],
            table_fields: vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ],
            default_db: "dfe".to_string(),
            default_table: "default".to_string(),
            org_id_field: Some("org_id".to_string()),
            // Explicitly list orgs used in tests for per-org routing.
            org_routes: vec![
                crate::config::OrgRoute {
                    org_id: "acme".to_string(),
                    database: None,
                },
                crate::config::OrgRoute {
                    org_id: "tenant1".to_string(),
                    database: None,
                },
                crate::config::OrgRoute {
                    org_id: "bigcorp".to_string(),
                    database: None,
                },
                crate::config::OrgRoute {
                    org_id: "customer123".to_string(),
                    database: None,
                },
                crate::config::OrgRoute {
                    org_id: "nested_org".to_string(),
                    database: None,
                },
            ],
            source_to_table,
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: crate::config::DlqConfig::default(),
            rules: vec![],
        }
    }

    #[test]
    fn test_router_db_table_routing() {
        let router = Router::new(&test_config());
        let payload = br#"{"org_id": "acme", "event_category": "login", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.login".to_string())
        );
    }

    #[test]
    fn test_router_db_table_with_mapping() {
        let router = Router::new(&test_config());
        // "auth" maps to "events_auth" in source_to_table
        let payload = br#"{"org_id": "acme", "event_category": "auth", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.events_auth".to_string())
        );
    }

    #[test]
    fn test_router_nested_table_field() {
        let router = Router::new(&test_config());
        // No event_category at top level, but tags.event_category exists
        let payload =
            br#"{"org_id": "tenant1", "tags": {"event_category": "api"}, "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("tenant1.events_api".to_string())
        );
    }

    #[test]
    fn test_router_default_db() {
        let router = Router::new(&test_config());
        // No org_id field, should use default_db
        let payload = br#"{"event_category": "network", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    #[test]
    fn test_router_default_table() {
        let router = Router::new(&test_config());
        // No event_category or tags.event_category, should use default_table
        let payload = br#"{"org_id": "acme", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.default".to_string())
        );
    }

    #[test]
    fn test_router_all_defaults() {
        let router = Router::new(&test_config());
        // No matching fields at all
        let payload = br#"{"user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_extract_db() {
        // Default: shared schema (db_fields empty)
        let router = Router::default();

        let p1 = br#"{"org_id": "acme", "_source": "auth"}"#;
        assert_eq!(router.extract_db(p1), "dfe".to_string()); // Shared schema: goes to dfe

        let p2 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_db(p2), "dfe".to_string());

        // Per-org routing: acme listed in org_routes → gets own database
        let router_routed = Router::new(&test_config());
        let p3 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(router_routed.extract_db(p3), "acme".to_string());
        assert_eq!(router_routed.extract_db(p2), "dfe".to_string()); // No org_id → default
    }

    #[test]
    fn test_extract_table() {
        let router = Router::default();

        // _source field (default table_fields)
        let p1 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_table(p1), "auth".to_string());

        // No match → default table
        let p2 = br#"{"user_id": 123}"#;
        assert_eq!(router.extract_table(p2), "default".to_string());

        // event_category not matched by default (need compat mode)
        let p3 = br#"{"event_category": "api"}"#;
        assert_eq!(router.extract_table(p3), "default".to_string());
    }

    #[test]
    fn test_custom_db_fields() {
        let mut config = test_config();
        config.db_fields = vec!["tenant_id".to_string(), "org_id".to_string()];
        // tenant1 and acme are already in test_config's org_routes

        let router = Router::new(&config);

        // tenant_id takes priority
        let p1 = br#"{"tenant_id": "tenant1", "org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(
            router.route(p1),
            RouteResult::Table("tenant1.events_auth".to_string())
        );

        // Fallback to org_id
        let p2 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(
            router.route(p2),
            RouteResult::Table("acme.events_auth".to_string())
        );

        // Not in org_routes → dfe (default)
        let p3 = br#"{"org_id": "other", "event_category": "auth"}"#;
        assert_eq!(
            router.route(p3),
            RouteResult::Table("dfe.events_auth".to_string())
        );
    }

    #[test]
    fn test_nested_db_field() {
        let mut config = test_config();
        config.db_fields = vec!["metadata.org_id".to_string(), "org_id".to_string()];
        // nested_org is in test_config's org_routes

        let router = Router::new(&config);

        let payload = br#"{"metadata": {"org_id": "nested_org"}, "event_category": "auth"}"#;
        assert_eq!(
            router.route(payload),
            RouteResult::Table("nested_org.events_auth".to_string())
        );
    }

    #[test]
    fn test_route_source() {
        let router = Router::new(&test_config());

        // "auth" is mapped to "events_auth" in source_to_table
        assert_eq!(
            router.route_source("auth"),
            RouteResult::Table("dfe.events_auth".to_string())
        );

        // Unmapped source value is used as the table name directly
        assert_eq!(
            router.route_source("network"),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    #[test]
    fn test_route_value_basic() {
        let router = Router::new(&test_config());
        let value = serde_json::json!({
            "org_id": "acme",
            "event_category": "login",
            "user_id": 123
        });

        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("acme.login".to_string())
        );
    }

    #[test]
    fn test_route_value_with_mapping() {
        let router = Router::new(&test_config());
        let value = serde_json::json!({
            "org_id": "acme",
            "event_category": "auth",
            "user_id": 123
        });

        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("acme.events_auth".to_string())
        );
    }

    #[test]
    fn test_route_value_nested() {
        let router = Router::new(&test_config());
        let value = serde_json::json!({
            "org_id": "tenant1",
            "tags": {"event_category": "api"},
            "user_id": 123
        });

        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("tenant1.events_api".to_string())
        );
    }

    #[test]
    fn test_route_value_defaults() {
        let router = Router::new(&test_config());
        let value = serde_json::json!({"user_id": 123});

        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_route_value_matches_route() {
        // Ensure route() and route_value() give the same result
        let router = Router::new(&test_config());
        let payload = br#"{"org_id": "acme", "event_category": "auth", "user_id": 123}"#;
        let value: Value = serde_json::from_slice(payload).unwrap();

        assert_eq!(router.route(payload), router.route_value(&value));
    }

    #[test]
    fn test_route_cow_matches_route() {
        // Ensure route_cow() (zero-copy) and route() give the same result
        let router = Router::new(&test_config());

        let payloads = [
            br#"{"org_id": "acme", "event_category": "auth", "user_id": 123}"#.as_slice(),
            br#"{"org_id": "tenant1", "event_category": "login"}"#.as_slice(),
            br#"{"org_id": "acme", "tags": {"event_category": "api"}}"#.as_slice(),
            br#"{"event_category": "network"}"#.as_slice(), // no org_id
            br#"{"org_id": "test"}"#.as_slice(),            // no event_category
            br#"{"user_id": 123}"#.as_slice(),              // no routing fields
        ];

        for payload in payloads {
            assert_eq!(
                router.route_cow(payload),
                router.route(payload),
                "Mismatch for payload: {:?}",
                std::str::from_utf8(payload).unwrap()
            );
        }
    }

    #[test]
    fn test_route_cow_with_escaped_strings() {
        // Ensure escaped strings are handled correctly
        let router = Router::default();

        // org_id with escape sequence
        let payload = br#"{"org_id": "acme\ncorp", "_source": "auth"}"#;
        let result = router.route_cow(payload);

        // Should still route correctly (value is "acme\ncorp" with actual newline)
        assert!(matches!(result, RouteResult::Table(ref s) if s.contains("auth")));
    }

    #[test]
    fn test_shared_schema_default() {
        // Default behaviour: all to dfe database
        let router = Router::default();

        let p1 = br#"{"org_id": "acme", "_source": "auth"}"#;
        let p2 = br#"{"org_id": "bigcorp", "_source": "api"}"#;
        let p3 = br#"{"_source": "network"}"#;

        // All go to dfe.* (shared schema)
        assert_eq!(router.route(p1), RouteResult::Table("dfe.auth".to_string()));
        assert_eq!(router.route(p2), RouteResult::Table("dfe.api".to_string()));
        assert_eq!(
            router.route(p3),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    #[test]
    fn test_org_routes_explicit() {
        // Only listed orgs get their own database; others fall back to default_db.
        let config = RoutingConfig {
            table_fields: vec!["event_category".to_string()],
            db_fields: vec!["org_id".to_string()],
            org_routes: vec![
                crate::config::OrgRoute {
                    org_id: "acme".to_string(),
                    database: None,
                },
                crate::config::OrgRoute {
                    org_id: "bigcorp".to_string(),
                    database: None,
                },
            ],
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        let p2 = br#"{"org_id": "bigcorp", "event_category": "api"}"#;
        let p3 = br#"{"org_id": "other", "event_category": "network"}"#;

        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme.auth".to_string())
        );
        assert_eq!(
            router.route(p2),
            RouteResult::Table("bigcorp.api".to_string())
        );
        // not listed → dfe (default)
        assert_eq!(
            router.route(p3),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    #[test]
    fn test_org_routes_custom_database() {
        // org_route with explicit database override
        let config = RoutingConfig {
            table_fields: vec!["event_category".to_string()],
            db_fields: vec!["org_id".to_string()],
            org_routes: vec![crate::config::OrgRoute {
                org_id: "acme".to_string(),
                database: Some("acme_dfe".to_string()),
            }],
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        // "acme" maps to "acme_dfe" (custom database name)
        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme_dfe.auth".to_string())
        );
    }

    #[test]
    fn test_extract_org_id_from_value() {
        let router = Router::default();

        let value1 = serde_json::json!({"org_id": "acme", "event_category": "auth"});
        assert_eq!(router.extract_org_id_from_value(&value1), Some("acme"));

        let value2 = serde_json::json!({"event_category": "auth"});
        assert_eq!(router.extract_org_id_from_value(&value2), None);

        // Nested org_id
        let config = RoutingConfig {
            org_id_field: Some("tenant.id".to_string()),
            ..Default::default()
        };
        let router2 = Router::new(&config);

        let value3 = serde_json::json!({"tenant": {"id": "acme"}, "event_category": "auth"});
        assert_eq!(router2.extract_org_id_from_value(&value3), Some("acme"));
    }

    #[test]
    fn test_extract_org_id_bytes() {
        let router = Router::default();

        let p1 = br#"{"org_id": "acme", "_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p1), Some("acme".to_string()));

        let p2 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p2), None);
    }

    #[test]
    fn test_derive_source_from_topic() {
        let router = Router::default();

        // Strip _land suffix
        assert_eq!(router.derive_source_from_topic("auth_land"), "auth");

        // Strip _load suffix
        assert_eq!(router.derive_source_from_topic("network_load"), "network");

        // No suffix to strip
        assert_eq!(router.derive_source_from_topic("events"), "events");

        // First matching suffix wins
        assert_eq!(router.derive_source_from_topic("dfe_land"), "dfe");

        // Suffix only at end, not middle
        assert_eq!(
            router.derive_source_from_topic("land_events"),
            "land_events"
        );
    }

    #[test]
    fn test_extract_source_from_value() {
        let router = Router::default();

        // _source field present
        let v1 = serde_json::json!({"_source": "auth", "data": "test"});
        assert_eq!(router.extract_source_from_value(&v1), Some("auth"));

        // _source field absent
        let v2 = serde_json::json!({"event_category": "auth", "data": "test"});
        assert_eq!(router.extract_source_from_value(&v2), None);
    }

    #[test]
    fn test_compat_v2_source() {
        // Pre-DFE 2.2 compat: event_category/tags.event_category prepended
        let routing_config = RoutingConfig {
            compat_v2_source: true,
            ..Default::default()
        };

        let metadata_config = MetadataConfig {
            source_fields: vec!["_source".to_string()],
            ..Default::default()
        };

        let router = Router::with_metadata(&routing_config, &metadata_config);

        // event_category matched (prepended by compat mode)
        let v1 = serde_json::json!({"event_category": "auth"});
        assert_eq!(router.extract_source_from_value(&v1), Some("auth"));

        // tags.event_category matched (prepended by compat mode)
        let v2 = serde_json::json!({"tags": {"event_category": "api"}});
        assert_eq!(router.extract_source_from_value(&v2), Some("api"));

        // _source still works (it's in the list after compat fields)
        let v3 = serde_json::json!({"_source": "network"});
        assert_eq!(router.extract_source_from_value(&v3), Some("network"));

        // event_category takes priority over _source (prepended first)
        let v4 = serde_json::json!({"event_category": "auth", "_source": "network"});
        assert_eq!(router.extract_source_from_value(&v4), Some("auth"));
    }

    #[test]
    fn test_compat_v2_source_routing() {
        // Compat mode also prepends to table_fields for routing
        let routing_config = RoutingConfig {
            compat_v2_source: true,
            ..Default::default()
        };

        let router = Router::with_metadata(&routing_config, &MetadataConfig::default());

        // event_category routes to table (via compat prepend)
        let p1 = br#"{"event_category": "auth"}"#;
        assert_eq!(router.route(p1), RouteResult::Table("dfe.auth".to_string()));

        // _source also works for routing
        let p2 = br#"{"_source": "network"}"#;
        assert_eq!(
            router.route(p2),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    // ========================================================================
    // CEL routing rule tests
    // ========================================================================

    #[test]
    fn test_cel_rule_basic_match() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"severity == "critical""#.to_string(),
                target: "alerts".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"severity": "critical", "message": "disk full"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.alerts".to_string())
        );
    }

    #[test]
    fn test_cel_rule_no_match_falls_through() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"severity == "critical""#.to_string(),
                target: "alerts".to_string(),
                db: None,
            }],
            table_fields: vec!["event_category".to_string()],
            ..Default::default()
        };
        let router = Router::new(&config);

        // Rule doesn't match — falls through to field extraction
        let value = serde_json::json!({"severity": "info", "event_category": "auth"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.auth".to_string())
        );
    }

    #[test]
    fn test_cel_rule_first_match_wins() {
        let config = RoutingConfig {
            rules: vec![
                crate::config::RoutingRule {
                    when: r#"severity == "critical""#.to_string(),
                    target: "critical_alerts".to_string(),
                    db: None,
                },
                crate::config::RoutingRule {
                    when: r#"severity == "critical""#.to_string(),
                    target: "other_alerts".to_string(),
                    db: None,
                },
            ],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"severity": "critical"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.critical_alerts".to_string())
        );
    }

    #[test]
    fn test_cel_rule_with_custom_db() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"severity == "critical""#.to_string(),
                target: "alerts".to_string(),
                db: Some("critical_db".to_string()),
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"severity": "critical"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("critical_db.alerts".to_string())
        );
    }

    #[test]
    fn test_cel_rule_string_functions() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"message.contains("audit")"#.to_string(),
                target: "audit_table".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"message": "user audit trail logged"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.audit_table".to_string())
        );

        // No match
        let value2 = serde_json::json!({"message": "normal log"});
        assert_eq!(
            router.route_value(&value2),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_cel_rule_in_operator() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"status in ["active", "pending"]"#.to_string(),
                target: "active_events".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"status": "active"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.active_events".to_string())
        );

        let value2 = serde_json::json!({"status": "closed"});
        assert_eq!(
            router.route_value(&value2),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_cel_rule_logical_operators() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"severity == "critical" && action == "login""#.to_string(),
                target: "critical_auth".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        // Both conditions met
        let value = serde_json::json!({"severity": "critical", "action": "login"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.critical_auth".to_string())
        );

        // Only one condition met
        let value2 = serde_json::json!({"severity": "critical", "action": "logout"});
        assert_eq!(
            router.route_value(&value2),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_cel_rule_invalid_expression_skipped() {
        let config = RoutingConfig {
            rules: vec![
                crate::config::RoutingRule {
                    when: "this is not valid CEL <<<>>>".to_string(),
                    target: "broken".to_string(),
                    db: None,
                },
                crate::config::RoutingRule {
                    when: r#"severity == "critical""#.to_string(),
                    target: "alerts".to_string(),
                    db: None,
                },
            ],
            ..Default::default()
        };
        let router = Router::new(&config);

        // Invalid rule skipped, valid rule still works
        let value = serde_json::json!({"severity": "critical"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.alerts".to_string())
        );
    }

    #[test]
    fn test_cel_rule_missing_field_no_match() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: r#"severity == "critical""#.to_string(),
                target: "alerts".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        // severity field not present — rule should not match
        let value = serde_json::json!({"message": "hello"});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.default".to_string())
        );
    }

    #[test]
    fn test_cel_rule_arithmetic() {
        let config = RoutingConfig {
            rules: vec![crate::config::RoutingRule {
                when: "amount > 10000".to_string(),
                target: "high_value".to_string(),
                db: None,
            }],
            ..Default::default()
        };
        let router = Router::new(&config);

        let value = serde_json::json!({"amount": 15000});
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.high_value".to_string())
        );

        let value2 = serde_json::json!({"amount": 500});
        assert_eq!(
            router.route_value(&value2),
            RouteResult::Table("dfe.default".to_string())
        );
    }
}
