// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Routes messages to destination db.table based on event data
//!
//! Routing happens PRE-flattening using dot notation for nested field access.
//! The table is the first `table_fields` entry carrying a value (default:
//! `_source`), else the source the topic names, else `default_table`. The
//! database is `default_db` unless `org_routes` lists the record's tenant.
//! A null or empty routing value counts as absent.
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

use crate::config::{MetadataConfig, RoutingConfig};
use crate::payload::parse::{
    extract_field_json, extract_field_json_cow, extract_nested_field_json,
    extract_nested_field_json_cow,
};

/// Route result with destination info
#[derive(Debug, Clone, PartialEq)]
pub enum RouteResult {
    /// Route to specific db.table
    Table(String),
    /// Send to DLQ (routing failed and DLQ enabled)
    Dlq(String),
}

/// How the destination table was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableChoice {
    /// A routing field named the table.
    Named,
    /// No routing field carried a value, so a source's own topic named the
    /// table instead.
    Topic,
    /// Neither a routing field nor the topic named a table, so `default_table`
    /// was used.
    DefaultFallback,
}

/// Routes messages to db.table based on event data fields
///
/// ## Shared Schema (Default)
///
/// By default, all organisations share the common database:
/// - `org_routes` is empty -> always use `default_db`
/// - All events go to `dfe.{table}`
/// - the tenant lands in the `_org_id` column for row-level security
///
/// ## Per-Org Routing (Optional)
///
/// Explicitly list orgs in `org_routes` config. Only listed orgs receive
/// a dedicated database -- all others always go to `default_db`.
pub struct Router {
    /// Fields to check for table name (first match wins)
    table_fields: Vec<String>,
    /// Database for every org not listed in `org_routes`.
    default_db: String,
    /// Default table if no `table_field` matches
    default_table: String,
    /// Field the tenant is read from.
    org_id_field: Option<String>,
    /// Tenant for a record that carries none.
    default_org_id: String,
    /// Per-org database routing: `org_id` -> effective database name.
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

/// Whether a routing value is absent in all but name: null or empty text.
#[inline]
fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => false,
    }
}

impl Router {
    /// A router reading `_source` from the default metadata `source_fields`.
    pub fn new(config: &RoutingConfig) -> Self {
        Self::with_metadata(config, &MetadataConfig::default())
    }

    /// A router reading `_source` from the configured metadata `source_fields`.
    pub fn with_metadata(config: &RoutingConfig, metadata: &MetadataConfig) -> Self {
        let source_to_table: FxHashMap<String, String> = config
            .source_to_table
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // org_id -> effective database name
        let org_routes: FxHashMap<String, String> = config
            .org_routes
            .iter()
            .map(|r| (r.org_id.clone(), r.effective_database().to_string()))
            .collect();

        Self {
            table_fields: config.table_fields.clone(),
            default_db: config.default_db.clone(),
            default_table: config.default_table.clone(),
            org_id_field: config.org_id_field.clone(),
            default_org_id: config.default_org_id.clone(),
            org_routes,
            source_to_table,
            dlq_enabled: config.dlq.enabled,
            source_fields: metadata.source_fields.clone(),
            topic_suffixes: config.topic_suffixes.clone(),
        }
    }

    /// Extract the database name from the payload.
    ///
    /// `default_db` unless `org_routes` lists the record's tenant.
    #[inline]
    pub fn extract_db(&self, payload: &[u8]) -> String {
        // Delegate to the Cow path -- borrows from `self` or the payload
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
        // Delegate to the Cow path -- see `extract_db` for rationale.
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
    /// Uses same logic as `extract_db_from_value()` for consistency.
    #[inline]
    fn extract_db_cow<'a>(&'a self, payload: &'a [u8]) -> Cow<'a, str> {
        // No org listed = shared schema, and nothing to read.
        if self.org_routes.is_empty() {
            return Cow::Borrowed(&self.default_db);
        }
        let org = self
            .org_id_field
            .as_deref()
            .and_then(|field| self.field_cow(payload, field))
            .unwrap_or(Cow::Borrowed(self.default_org_id.as_str()));
        match self.org_routes.get(org.as_ref()) {
            Some(db) => Cow::Owned(db.clone()),
            None => Cow::Borrowed(&self.default_db),
        }
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

    /// Zero-copy read of one text field, `None` when it is absent, not text or empty.
    #[inline]
    fn field_cow<'a>(&self, payload: &'a [u8], field: &str) -> Option<Cow<'a, str>> {
        let value = if field.contains('.') {
            extract_nested_field_json_cow(payload, field)
        } else {
            extract_field_json_cow(payload, field)
        };
        value.filter(|v| !v.is_empty())
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
        fields
            .iter()
            .find_map(|field| self.field_cow(payload, field))
    }

    /// Route a message to db.table based on already-parsed JSON Value
    ///
    /// **This is the preferred method** when you've already parsed the JSON,
    /// as it avoids a second parse. The orchestrator should use this.
    ///
    /// Returns "db.table" string for buffer routing.
    #[inline]
    pub fn route_value(&self, value: &Value) -> RouteResult {
        self.route_value_traced(value).0
    }

    /// Route an already-parsed JSON Value and report how the table was chosen.
    ///
    /// The default-table fallback is otherwise silent, so a source whose records
    /// stopped carrying the routing field reads as landing traffic (#159).
    #[inline]
    pub fn route_value_traced(&self, value: &Value) -> (RouteResult, TableChoice) {
        // An empty topic strips no suffix, so it never names a source.
        self.route_value_on_topic(value, "")
    }

    /// Route an already-parsed JSON Value, letting a source's own topic name the
    /// table when the record does not.
    ///
    /// A transform emits ECS, which carries no DFE routing field, so on
    /// `{source}_load` the topic name is the only thing that names the source
    /// and the record would otherwise land in the landing table (#184).
    #[inline]
    pub fn route_value_on_topic(&self, value: &Value, topic: &str) -> (RouteResult, TableChoice) {
        let db = self.extract_db_from_value(value);
        let (table, choice) = match self.extract_first_match_from_value(value, &self.table_fields) {
            Some(named) => (named, TableChoice::Named),
            None => match self.source_topic_name(topic) {
                Some(source) => (source, TableChoice::Topic),
                None => (self.default_table.as_str(), TableChoice::DefaultFallback),
            },
        };

        // The legacy source_to_table mapping applies to a topic's source and to
        // the default too.
        let table = self
            .source_to_table
            .get(table)
            .map_or(table, String::as_str);

        (self.build_route_result(db, table), choice)
    }

    /// The source a topic belongs to, or `None` when the topic names no source.
    ///
    /// A source's topic is its name plus a configured suffix, so a topic with no
    /// suffix to strip names no source; the landing topic strips to the default
    /// table, and its records carry no source of their own.
    #[inline]
    fn source_topic_name<'t>(&self, topic: &'t str) -> Option<&'t str> {
        let source = self
            .topic_suffixes
            .iter()
            .find_map(|suffix| topic.strip_suffix(suffix.as_str()))?;
        let table = self
            .source_to_table
            .get(source)
            .map_or(source, String::as_str);
        (table != self.default_table).then_some(source)
    }

    /// Whether the topic is a source's own topic rather than the landing topic.
    ///
    /// A source topic names a table of its own, so a record from one arriving
    /// without a routing field is routed by topic rather than to the default.
    #[inline]
    pub fn is_source_topic(&self, topic: &str) -> bool {
        self.source_topic_name(topic).is_some()
    }

    /// Build `RouteResult` from db and table strings
    ///
    /// Uses pre-allocated String with exact capacity to avoid format!() overhead.
    #[inline]
    fn build_route_result(&self, db: &str, table: &str) -> RouteResult {
        // Only an empty configured default reaches here empty.
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
    /// `default_db` unless `org_routes` lists the record's tenant.
    #[inline]
    fn extract_db_from_value(&self, value: &Value) -> &str {
        // No org listed = shared schema, and nothing to read.
        if self.org_routes.is_empty() {
            return &self.default_db;
        }
        self.org_routes
            .get(self.org_id_or_default(value))
            .map_or(self.default_db.as_str(), String::as_str)
    }

    /// Extract a field value from parsed JSON Value using dot notation
    ///
    /// Returns borrowed `&str` from the Value to avoid allocation. A null or
    /// empty value is skipped, so the next field gets its turn.
    #[inline]
    fn extract_first_match_from_value<'a>(
        &self,
        value: &'a Value,
        fields: &[String],
    ) -> Option<&'a str> {
        fields.iter().find_map(|field| {
            self.get_nested_field(value, field)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        })
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
    /// Checks `source_fields` in order (first match wins), skipping a null or
    /// empty value. Returns None if no field matches -- caller should fall back
    /// to topic derivation.
    #[inline]
    pub fn extract_source_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        self.extract_first_match_from_value(value, &self.source_fields)
    }

    /// Derive _source from Kafka topic name by stripping configured suffixes
    ///
    /// Strips the first matching suffix from `topic_suffixes`.
    /// Example: topic "`auth_land`" with suffix "_land" -> "auth"
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

    /// The record's own tenant, from parsed JSON.
    ///
    /// `None` when `org_id_field` is not configured, or the field is absent,
    /// not text, or empty.
    #[inline]
    pub fn extract_org_id_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        let field = self.org_id_field.as_ref()?;
        let org = self.get_nested_field(value, field)?;
        if is_blank(org) {
            return None;
        }
        org.as_str()
    }

    /// The tenant the record lands in: its own, else `default_org_id`.
    #[inline]
    pub fn org_id_or_default<'a>(&'a self, value: &'a Value) -> &'a str {
        self.extract_org_id_from_value(value)
            .unwrap_or(&self.default_org_id)
    }

    /// The record's own tenant, from raw bytes.
    ///
    /// `None` when `org_id_field` is not configured, or the field is absent,
    /// not text, or empty.
    #[inline]
    pub fn extract_org_id(&self, payload: &[u8]) -> Option<String> {
        let field = self.org_id_field.as_ref()?;
        self.extract_single_field(payload, field)
            .filter(|org| !org.is_empty())
    }

    /// Get the configured `org_id` field name (if any)
    pub fn org_id_field(&self) -> Option<&str> {
        self.org_id_field.as_deref()
    }

    /// The tenant a record that carries none lands in.
    pub fn default_org_id(&self) -> &str {
        &self.default_org_id
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
    /// Built from the default configs, so the landing table is authored once.
    fn default() -> Self {
        Self::new(&RoutingConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn org(org_id: &str, database: Option<&str>) -> crate::config::OrgRoute {
        crate::config::OrgRoute {
            org_id: org_id.to_string(),
            database: database.map(str::to_string),
        }
    }

    fn test_config() -> RoutingConfig {
        let mut source_to_table = HashMap::new();
        source_to_table.insert("auth".to_string(), "events_auth".to_string());
        source_to_table.insert("api".to_string(), "events_api".to_string());

        RoutingConfig {
            table_fields: vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ],
            default_db: "dfe".to_string(),
            default_table: "main".to_string(),
            // Explicitly list orgs used in tests for per-org routing.
            org_routes: vec![
                org("acme", None),
                org("tenant1", None),
                org("bigcorp", None),
                org("customer123", None),
            ],
            source_to_table,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            ..RoutingConfig::default()
        }
    }

    #[test]
    fn test_router_db_table_routing() {
        let router = Router::new(&test_config());
        let payload = br#"{"_org_id": "acme", "event_category": "login", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.login".to_string())
        );
    }

    #[test]
    fn test_router_db_table_with_mapping() {
        let router = Router::new(&test_config());
        // "auth" maps to "events_auth" in source_to_table
        let payload = br#"{"_org_id": "acme", "event_category": "auth", "user_id": 123}"#;

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
            br#"{"_org_id": "tenant1", "tags": {"event_category": "api"}, "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("tenant1.events_api".to_string())
        );
    }

    #[test]
    fn test_router_default_db() {
        let router = Router::new(&test_config());
        // No tenant, so the default org, which org_routes does not list
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
        let payload = br#"{"_org_id": "acme", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.main".to_string())
        );
    }

    #[test]
    fn test_router_all_defaults() {
        let router = Router::new(&test_config());
        // No matching fields at all
        let payload = br#"{"user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("dfe.main".to_string())
        );
    }

    #[test]
    fn test_extract_db() {
        // Default: shared schema (no org_routes)
        let router = Router::default();

        let p1 = br#"{"_org_id": "acme", "_source": "auth"}"#;
        assert_eq!(router.extract_db(p1), "dfe".to_string()); // Shared schema: goes to dfe

        let p2 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_db(p2), "dfe".to_string());

        // Per-org routing: acme listed in org_routes -> gets own database
        let router_routed = Router::new(&test_config());
        let p3 = br#"{"_org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(router_routed.extract_db(p3), "acme".to_string());
        assert_eq!(router_routed.extract_db(p2), "dfe".to_string()); // No org -> default
    }

    #[test]
    fn test_extract_table() {
        let router = Router::default();

        // _source field (default table_fields)
        let p1 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_table(p1), "auth".to_string());

        // No match -> default table
        let p2 = br#"{"user_id": 123}"#;
        assert_eq!(router.extract_table(p2), "main".to_string());

        // event_category is not a routing field by default
        let p3 = br#"{"event_category": "api"}"#;
        assert_eq!(router.extract_table(p3), "main".to_string());
    }

    #[test]
    fn org_routes_key_on_the_tenant_and_never_on_a_payload_org_id() {
        let router = Router::new(&test_config());

        // A sender's own org_id is data, so it picks no database.
        let sender = br#"{"org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(
            router.route(sender),
            RouteResult::Table("dfe.events_auth".to_string())
        );
        let value: Value = serde_json::from_slice(sender).unwrap();
        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.events_auth".to_string())
        );

        // Not in org_routes -> dfe (default)
        let other = br#"{"_org_id": "other", "event_category": "auth"}"#;
        assert_eq!(
            router.route(other),
            RouteResult::Table("dfe.events_auth".to_string())
        );
    }

    #[test]
    fn org_routes_read_a_nested_org_id_field() {
        let config = RoutingConfig {
            org_id_field: Some("metadata.org_id".to_string()),
            org_routes: vec![org("nested_org", None)],
            table_fields: vec!["event_category".to_string()],
            ..RoutingConfig::default()
        };
        let router = Router::new(&config);

        let payload = br#"{"metadata": {"org_id": "nested_org"}, "event_category": "auth"}"#;
        assert_eq!(
            router.route(payload),
            RouteResult::Table("nested_org.auth".to_string())
        );
        let value: Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(router.route(payload), router.route_value(&value));
    }

    #[test]
    fn a_record_with_no_tenant_routes_by_the_default_org() {
        let config = RoutingConfig {
            org_routes: vec![org("default", Some("shared_default"))],
            ..RoutingConfig::default()
        };
        let router = Router::new(&config);

        for payload in [
            br#"{"_source": "auth"}"#.as_slice(),
            br#"{"_org_id": null, "_source": "auth"}"#.as_slice(),
            br#"{"_org_id": "", "_source": "auth"}"#.as_slice(),
        ] {
            let value: Value = serde_json::from_slice(payload).unwrap();
            let expected = RouteResult::Table("shared_default.auth".to_string());
            assert_eq!(router.route_value(&value), expected, "{value}");
            assert_eq!(router.route_cow(payload), expected, "{value}");
        }
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
            "_org_id": "acme",
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
            "_org_id": "acme",
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
            "_org_id": "tenant1",
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
            RouteResult::Table("dfe.main".to_string())
        );
    }

    #[test]
    fn test_route_value_matches_route() {
        // Ensure route() and route_value() give the same result
        let router = Router::new(&test_config());
        let payload = br#"{"_org_id": "acme", "event_category": "auth", "user_id": 123}"#;
        let value: Value = serde_json::from_slice(payload).unwrap();

        assert_eq!(router.route(payload), router.route_value(&value));
    }

    #[test]
    fn test_route_cow_matches_route() {
        // Ensure route_cow() (zero-copy) and route() give the same result
        let router = Router::new(&test_config());

        let payloads = [
            br#"{"_org_id": "acme", "event_category": "auth", "user_id": 123}"#.as_slice(),
            br#"{"_org_id": "tenant1", "event_category": "login"}"#.as_slice(),
            br#"{"_org_id": "acme", "tags": {"event_category": "api"}}"#.as_slice(),
            br#"{"event_category": "network"}"#.as_slice(), // no org
            br#"{"_org_id": "test"}"#.as_slice(),           // no event_category
            br#"{"user_id": 123}"#.as_slice(),              // no routing fields
            br#"{"_org_id": "", "event_category": ""}"#.as_slice(), // blank both
        ];

        for payload in payloads {
            let value: Value = serde_json::from_slice(payload).unwrap();
            assert_eq!(
                router.route_cow(payload),
                router.route(payload),
                "Mismatch for payload: {:?}",
                std::str::from_utf8(payload).unwrap()
            );
            assert_eq!(router.route(payload), router.route_value(&value));
        }
    }

    #[test]
    fn test_route_cow_with_escaped_strings() {
        // Ensure escaped strings are handled correctly
        let router = Router::default();

        // org_id with escape sequence
        let payload = br#"{"_org_id": "acme\ncorp", "_source": "auth"}"#;
        let result = router.route_cow(payload);

        // Should still route correctly (value is "acme\ncorp" with actual newline)
        assert!(matches!(result, RouteResult::Table(ref s) if s.contains("auth")));
    }

    #[test]
    fn test_shared_schema_default() {
        // Default behaviour: all to dfe database
        let router = Router::default();

        let p1 = br#"{"_org_id": "acme", "_source": "auth"}"#;
        let p2 = br#"{"_org_id": "bigcorp", "_source": "api"}"#;
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
            org_routes: vec![org("acme", None), org("bigcorp", None)],
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"_org_id": "acme", "event_category": "auth"}"#;
        let p2 = br#"{"_org_id": "bigcorp", "event_category": "api"}"#;
        let p3 = br#"{"_org_id": "other", "event_category": "network"}"#;

        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme.auth".to_string())
        );
        assert_eq!(
            router.route(p2),
            RouteResult::Table("bigcorp.api".to_string())
        );
        // not listed -> dfe (default)
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
            org_routes: vec![org("acme", Some("acme_dfe"))],
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"_org_id": "acme", "event_category": "auth"}"#;
        // "acme" maps to "acme_dfe" (custom database name)
        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme_dfe.auth".to_string())
        );
    }

    #[test]
    fn test_extract_org_id_from_value() {
        let router = Router::default();

        let value1 = serde_json::json!({"_org_id": "acme", "event_category": "auth"});
        assert_eq!(router.extract_org_id_from_value(&value1), Some("acme"));

        let value2 = serde_json::json!({"event_category": "auth"});
        assert_eq!(router.extract_org_id_from_value(&value2), None);

        // A payload's own org_id field is data, not the tenant.
        let bare = serde_json::json!({"org_id": "acme"});
        assert_eq!(router.extract_org_id_from_value(&bare), None);

        // Null and empty text are no tenant at all.
        for blank in [
            serde_json::json!({"_org_id": null}),
            serde_json::json!({"_org_id": ""}),
        ] {
            assert_eq!(router.extract_org_id_from_value(&blank), None, "{blank}");
            assert_eq!(router.org_id_or_default(&blank), "default", "{blank}");
        }
        assert_eq!(router.org_id_or_default(&value1), "acme");

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

        let p1 = br#"{"_org_id": "acme", "_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p1), Some("acme".to_string()));

        let p2 = br#"{"_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p2), None);

        let p3 = br#"{"org_id": "acme", "_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p3), None);

        let p4 = br#"{"_org_id": "", "_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p4), None);

        let p5 = br#"{"_org_id": null, "_source": "auth"}"#;
        assert_eq!(router.extract_org_id(p5), None);
    }

    #[test]
    fn the_default_org_follows_the_config() {
        let config = RoutingConfig {
            default_org_id: "tenant-zero".to_string(),
            ..Default::default()
        };
        let router = Router::new(&config);
        assert_eq!(router.default_org_id(), "tenant-zero");
        assert_eq!(
            router.org_id_or_default(&serde_json::json!({"_org_id": ""})),
            "tenant-zero"
        );
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

        // Null and empty text are no source at all.
        for blank in [
            serde_json::json!({"_source": null}),
            serde_json::json!({"_source": ""}),
        ] {
            assert_eq!(router.extract_source_from_value(&blank), None, "{blank}");
        }
    }

    #[test]
    fn the_source_fields_come_from_the_metadata_config() {
        let metadata = MetadataConfig {
            source_fields: vec!["source_name".to_string(), "_source".to_string()],
            ..Default::default()
        };
        let router = Router::with_metadata(&RoutingConfig::default(), &metadata);

        let value = serde_json::json!({"source_name": "auth", "_source": "api"});
        assert_eq!(router.extract_source_from_value(&value), Some("auth"));
        let only_second = serde_json::json!({"source_name": "", "_source": "api"});
        assert_eq!(router.extract_source_from_value(&only_second), Some("api"));
    }

    // ---- a blank _source lands in the default table, never the DLQ ----

    #[test]
    fn a_blank_source_routes_to_the_default_table_not_the_dlq() {
        // The DLQ is on by default, which is what an empty table name used to reach.
        let router = Router::default();

        for payload in [
            br#"{"_source": ""}"#.as_slice(),
            br#"{"_source": null}"#.as_slice(),
            br#"{"message": "no source"}"#.as_slice(),
        ] {
            let value: Value = serde_json::from_slice(payload).unwrap();
            let expected = RouteResult::Table("dfe.main".to_string());
            assert_eq!(
                router.route_value_on_topic(&value, "main_land"),
                (expected.clone(), TableChoice::DefaultFallback),
                "{value}"
            );
            assert_eq!(router.route_cow(payload), expected, "{value}");
            assert_eq!(router.route(payload), expected, "{value}");
        }
    }

    #[test]
    fn a_blank_source_on_a_source_topic_routes_by_the_topic() {
        let router = Router::default();
        let value = serde_json::json!({"_source": "", "message": "x"});

        assert_eq!(
            router.route_value_on_topic(&value, "cisco-ios_load"),
            (
                RouteResult::Table("dfe.cisco-ios".to_string()),
                TableChoice::Topic
            )
        );
    }

    #[test]
    fn a_blank_first_table_field_hands_over_to_the_next() {
        let router = Router::new(&test_config());
        let value = serde_json::json!({"event_category": "", "tags": {"event_category": "api"}});

        assert_eq!(
            router.route_value(&value),
            RouteResult::Table("dfe.events_api".to_string())
        );
        let payload = serde_json::to_vec(&value).unwrap();
        assert_eq!(router.route_cow(&payload), router.route_value(&value));
    }

    // ---- default-table fallback signal (#159) ----

    #[test]
    fn traced_route_reports_the_default_fallback() {
        let router = Router::new(&test_config());

        let named = serde_json::json!({"_org_id": "acme", "event_category": "login"});
        assert_eq!(
            router.route_value_traced(&named),
            (
                RouteResult::Table("acme.login".to_string()),
                TableChoice::Named
            )
        );

        // The elastic case: the transform emitted no routing field at all.
        let unnamed = serde_json::json!({"_org_id": "acme", "message": "no source here"});
        assert_eq!(
            router.route_value_traced(&unnamed),
            (
                RouteResult::Table("acme.main".to_string()),
                TableChoice::DefaultFallback
            )
        );
    }

    #[test]
    fn traced_route_matches_route_value() {
        let router = Router::new(&test_config());
        for payload in [
            serde_json::json!({"_org_id": "acme", "event_category": "auth"}),
            serde_json::json!({"tags": {"event_category": "login"}}),
            serde_json::json!({"message": "nothing routable"}),
        ] {
            assert_eq!(
                router.route_value_traced(&payload).0,
                router.route_value(&payload)
            );
        }
    }

    #[test]
    fn only_a_source_topic_counts_as_one() {
        let router = Router::new(&test_config());

        // Suffix stripped, name is not the default table.
        assert!(router.is_source_topic("elastic_load"));
        assert!(router.is_source_topic("auth_land"));
        // The landing topic derives the default table itself.
        assert!(!router.is_source_topic("main_land"));
        // No suffix to strip, and the whole name is the default table.
        assert!(!router.is_source_topic("main"));
    }

    #[test]
    fn a_topic_mapped_onto_the_default_table_is_not_a_source_topic() {
        let mut config = test_config();
        config
            .source_to_table
            .insert("legacy".to_string(), "main".to_string());
        let router = Router::new(&config);

        assert!(!router.is_source_topic("legacy_load"));
    }

    // ---- routing a record by its source topic (#184) ----

    #[test]
    fn a_source_topic_names_the_table_the_record_did_not() {
        let router = Router::new(&test_config());
        let unnamed = serde_json::json!({"_org_id": "acme", "message": "no source here"});

        assert_eq!(
            router.route_value_on_topic(&unnamed, "cisco-ios_load"),
            (
                RouteResult::Table("acme.cisco-ios".to_string()),
                TableChoice::Topic
            ),
            "a transform's ECS output carries no routing field, so the topic names the table"
        );
        assert_eq!(
            router.route_value_on_topic(&unnamed, "cisco-ios_land"),
            (
                RouteResult::Table("acme.cisco-ios".to_string()),
                TableChoice::Topic
            ),
            "a source's landing topic names the same source"
        );
    }

    #[test]
    fn the_landing_topic_and_a_suffixless_topic_keep_the_default_fallback() {
        let router = Router::new(&test_config());
        let unnamed = serde_json::json!({"_org_id": "acme", "message": "no source here"});

        for topic in ["main_land", "dfe.events", ""] {
            assert_eq!(
                router.route_value_on_topic(&unnamed, topic),
                (
                    RouteResult::Table("acme.main".to_string()),
                    TableChoice::DefaultFallback
                ),
                "{topic} names no source of its own"
            );
        }
    }

    #[test]
    fn a_record_that_names_its_table_ignores_the_topic() {
        let router = Router::new(&test_config());
        let named = serde_json::json!({"_org_id": "acme", "event_category": "login"});

        assert_eq!(
            router.route_value_on_topic(&named, "cisco-ios_load"),
            (
                RouteResult::Table("acme.login".to_string()),
                TableChoice::Named
            )
        );
    }

    #[test]
    fn a_topic_mapped_by_source_to_table_routes_to_the_mapped_table() {
        let mut config = test_config();
        config
            .source_to_table
            .insert("cisco-ios".to_string(), "cisco_ios".to_string());
        let router = Router::new(&config);
        let unnamed = serde_json::json!({"_org_id": "acme", "message": "no source here"});

        assert_eq!(
            router.route_value_on_topic(&unnamed, "cisco-ios_load"),
            (
                RouteResult::Table("acme.cisco_ios".to_string()),
                TableChoice::Topic
            )
        );
    }
}
