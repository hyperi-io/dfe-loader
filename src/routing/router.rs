// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Routes messages to destination db.table based on event data
//!
//! Routing happens PRE-flattening using dot notation for nested field access.
//! db is extracted from first matching field in priority list (default: org_id)
//! table is extracted from first matching field in priority list (default: event_category)
//!
//! ## Performance
//!
//! Prefer `route_value()` when you already have a parsed `serde_json::Value` to avoid
//! double-parsing. Use `route()` only when you only have raw bytes.
//!
//! Uses `Cow<str>` internally to avoid String allocations when returning references.

use std::borrow::Cow;

use rustc_hash::{FxHashMap, FxHashSet};
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
/// Enable per-org databases via:
/// - `routed_orgs`: allowlist of org_ids that get own database
/// - `route_all_by_org`: route ALL orgs to own database
pub struct Router {
    /// Fields to check for database name (first match wins)
    /// Empty = always use default_db (shared schema)
    db_fields: Vec<String>,
    /// Fields to check for table name (first match wins)
    table_fields: Vec<String>,
    /// Default database if no db_field matches (or db_fields empty)
    default_db: String,
    /// Default table if no table_field matches
    default_table: String,
    /// Field to extract for _org_id column (for RLS)
    org_id_field: Option<String>,
    /// Orgs that get their own database (allowlist)
    routed_orgs: FxHashSet<String>,
    /// Route all orgs to own databases (overrides routed_orgs)
    route_all_by_org: bool,
    /// Legacy: category to table mapping (for backwards compatibility)
    category_to_table: FxHashMap<String, String>,
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
    /// Metadata config provides source_fields for _source extraction.
    /// When compat_v2_source is enabled, event_category/tags.event_category
    /// are prepended to both source_fields and table_fields.
    pub fn with_metadata(config: &RoutingConfig, metadata: &MetadataConfig) -> Self {
        // Convert HashMap to FxHashMap for faster lookups
        let category_to_table: FxHashMap<String, String> = config
            .category_to_table
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Convert routed_orgs Vec to FxHashSet for O(1) lookups
        let routed_orgs: FxHashSet<String> = config.routed_orgs.iter().cloned().collect();

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

        Self {
            db_fields: config.db_fields.clone(),
            table_fields,
            default_db: config.default_db.clone(),
            default_table: config.default_table.clone(),
            org_id_field: config.org_id_field.clone(),
            routed_orgs,
            route_all_by_org: config.route_all_by_org,
            category_to_table,
            dlq_enabled: config.dlq.enabled,
            source_fields,
            topic_suffixes: config.topic_suffixes.clone(),
        }
    }

    /// Extract a field value from JSON payload using dot notation for nested access
    ///
    /// Tries each field in the list in order, returning the first match.
    #[inline]
    fn extract_first_match(&self, payload: &[u8], fields: &[String]) -> Option<String> {
        for field in fields {
            let value = if field.contains('.') {
                extract_nested_field_json(payload, field)
            } else {
                extract_field_json(payload, field)
            };
            if value.is_some() {
                return value;
            }
        }
        None
    }

    /// Extract the database name from the payload
    ///
    /// Logic:
    /// 1. If route_all_by_org=true: extract from org_id_field → use as database
    /// 2. If db_fields empty: use default_db (shared schema)
    /// 3. Extract from db_fields, check if in routed_orgs allowlist
    /// 4. Fall back to default_db
    #[inline]
    pub fn extract_db(&self, payload: &[u8]) -> String {
        // Route all by org: extract org_id and use as database
        if self.route_all_by_org {
            if let Some(ref field) = self.org_id_field {
                if let Some(org_id) = self.extract_single_field(payload, field) {
                    return org_id;
                }
            }
            // No org_id found, fall back to default
            return self.default_db.clone();
        }

        // db_fields empty = always use default (shared schema)
        if self.db_fields.is_empty() {
            return self.default_db.clone();
        }

        // Extract from db_fields
        if let Some(org_id) = self.extract_first_match(payload, &self.db_fields) {
            // Check if org_id is in routed_orgs allowlist
            if self.routed_orgs.contains(&org_id) {
                return org_id;
            }
        }

        // Fall back to default database
        self.default_db.clone()
    }

    /// Extract a single field from payload (helper for org_id_field)
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
        let table = self
            .extract_first_match(payload, &self.table_fields)
            .unwrap_or_else(|| self.default_table.clone());

        // Check legacy category_to_table mapping
        if let Some(mapped) = self.category_to_table.get(&table) {
            mapped.clone()
        } else {
            table
        }
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
    /// Returns Cow::Borrowed for non-escaped strings (zero-copy from payload),
    /// Cow::Owned for escaped strings or when using default.
    /// Uses same logic as extract_db() for consistency.
    #[inline]
    fn extract_db_cow<'a>(&'a self, payload: &'a [u8]) -> Cow<'a, str> {
        // Route all by org: extract org_id and use as database
        if self.route_all_by_org {
            if let Some(ref field) = self.org_id_field {
                if let Some(org_id_cow) = self.extract_single_field_cow(payload, field) {
                    return org_id_cow;
                }
            }
            // No org_id found, fall back to default
            return Cow::Borrowed(&self.default_db);
        }

        // db_fields empty = always use default (shared schema)
        if self.db_fields.is_empty() {
            return Cow::Borrowed(&self.default_db);
        }

        // Extract from db_fields
        if let Some(org_id_cow) = self.extract_first_match_cow(payload, &self.db_fields) {
            let org_id_str = org_id_cow.as_ref();
            // Check if org_id is in routed_orgs allowlist
            if self.routed_orgs.contains(org_id_str) {
                return org_id_cow;
            }
        }

        // Fall back to default database
        Cow::Borrowed(&self.default_db)
    }

    /// Extract a single field with zero-copy (helper for org_id_field)
    #[inline]
    fn extract_single_field_cow<'a>(&self, payload: &'a [u8], field: &str) -> Option<Cow<'a, str>> {
        if field.contains('.') {
            extract_nested_field_json_cow(payload, field)
        } else {
            extract_field_json_cow(payload, field)
        }
    }

    /// Zero-copy extract table from raw payload.
    ///
    /// Returns Cow::Borrowed for non-escaped strings (zero-copy from payload),
    /// Cow::Owned for escaped strings or when using default/mapping.
    #[inline]
    fn extract_table_cow<'a>(&'a self, payload: &'a [u8]) -> Cow<'a, str> {
        let table_cow = self.extract_first_match_cow(payload, &self.table_fields);

        match table_cow {
            Some(cow) => {
                // Check legacy category_to_table mapping
                if let Some(mapped) = self.category_to_table.get(cow.as_ref()) {
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
    /// Returns Cow::Borrowed for non-escaped strings, Cow::Owned for escaped.
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
    /// Returns "db.table" string for buffer routing.
    #[inline]
    pub fn route_value(&self, value: &Value) -> RouteResult {
        let db = self.extract_db_from_value(value);
        let table = self.extract_table_from_value(value);

        self.build_route_result(&db, &table)
    }

    /// Build RouteResult from db and table strings
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
    /// Uses same logic as extract_db() for consistency.
    #[inline]
    fn extract_db_from_value<'a>(&'a self, value: &'a Value) -> Cow<'a, str> {
        // Route all by org: extract org_id and use as database
        if self.route_all_by_org {
            if let Some(ref field) = self.org_id_field {
                if let Some(org_id) = self.get_nested_field(value, field) {
                    if let Some(s) = org_id.as_str() {
                        return Cow::Borrowed(s);
                    }
                }
            }
            // No org_id found, fall back to default
            return Cow::Borrowed(&self.default_db);
        }

        // db_fields empty = always use default (shared schema)
        if self.db_fields.is_empty() {
            return Cow::Borrowed(&self.default_db);
        }

        // Extract from db_fields
        if let Some(org_id) = self.extract_first_match_from_value(value, &self.db_fields) {
            // Check if org_id is in routed_orgs allowlist
            if self.routed_orgs.contains(org_id) {
                return Cow::Borrowed(org_id);
            }
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

        // Check legacy category_to_table mapping
        if let Some(mapped) = self.category_to_table.get(table) {
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
            if let Some(val) = self.get_nested_field(value, field) {
                if let Some(s) = val.as_str() {
                    return Some(s);
                }
            }
        }
        None
    }

    /// Get a nested field from a Value using dot notation (e.g., "tags.event_category")
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

    /// Route with pre-extracted category (legacy compatibility)
    pub fn route_category(&self, category: &str) -> RouteResult {
        let table = if let Some(mapped) = self.category_to_table.get(category) {
            mapped.as_str()
        } else {
            category
        };

        RouteResult::Table(build_db_table_string(&self.default_db, table))
    }

    /// Extract _source value from parsed JSON Value
    ///
    /// Checks source_fields in order (first match wins).
    /// Returns None if no field matches — caller should fall back to topic derivation.
    #[inline]
    pub fn extract_source_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        self.extract_first_match_from_value(value, &self.source_fields)
    }

    /// Derive _source from Kafka topic name by stripping configured suffixes
    ///
    /// Strips the first matching suffix from topic_suffixes.
    /// Example: topic "auth_land" with suffix "_land" → "auth"
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

    /// Extract org_id for _org_id field (from parsed Value)
    ///
    /// Used by transformer to populate _org_id column for row-level security.
    /// Returns None if org_id_field not configured or field not found.
    #[inline]
    pub fn extract_org_id_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        let field = self.org_id_field.as_ref()?;
        self.get_nested_field(value, field)?.as_str()
    }

    /// Extract org_id for _org_id field (from raw bytes)
    ///
    /// Used by transformer to populate _org_id column for row-level security.
    /// Returns None if org_id_field not configured or field not found.
    #[inline]
    pub fn extract_org_id(&self, payload: &[u8]) -> Option<String> {
        let field = self.org_id_field.as_ref()?;
        self.extract_single_field(payload, field)
    }

    /// Get the configured org_id field name (if any)
    pub fn org_id_field(&self) -> Option<&str> {
        self.org_id_field.as_deref()
    }
}

/// Build "db.table" string efficiently with pre-allocated capacity.
///
/// Avoids format!() macro overhead by using push_str directly.
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
            // Default: db_fields empty = shared schema (all to dfe.*)
            db_fields: vec![],
            table_fields: vec!["_source".to_string()],
            default_db: "dfe".to_string(),
            default_table: "default".to_string(),
            // Extract org_id for _org_id column (RLS)
            org_id_field: Some("org_id".to_string()),
            // No per-org routing by default (shared schema)
            routed_orgs: FxHashSet::default(),
            route_all_by_org: false,
            category_to_table: FxHashMap::default(),
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
        let mut category_to_table = HashMap::new();
        category_to_table.insert("auth".to_string(), "events_auth".to_string());
        category_to_table.insert("api".to_string(), "events_api".to_string());

        RoutingConfig {
            db_fields: vec!["org_id".to_string()],
            table_fields: vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ],
            default_db: "dfe".to_string(),
            default_table: "default".to_string(),
            org_id_field: Some("org_id".to_string()),
            routed_orgs: vec![], // Old behaviour: all orgs get own DB (use route_all_by_org)
            route_all_by_org: true, // Simulate old default behaviour for these tests
            category_to_table,
            mapping_file: None,
            topic_suffixes: vec!["_land".to_string(), "_load".to_string()],
            compat_v2_source: false,
            dlq: crate::config::DlqConfig {
                enabled: true,
                topic_suffix: ".dlq".to_string(),
            },
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
        // "auth" maps to "events_auth" in category_to_table
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

        // OLD behaviour: route_all_by_org = true
        let mut config = test_config();
        config.route_all_by_org = true;
        let router_old = Router::new(&config);

        let p3 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(router_old.extract_db(p3), "acme".to_string());
        assert_eq!(router_old.extract_db(p2), "dfe".to_string()); // No org_id
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
        config.route_all_by_org = false; // Disable route_all_by_org for this test
        config.db_fields = vec!["tenant_id".to_string(), "org_id".to_string()];
        config.routed_orgs = vec!["tenant1".to_string(), "acme".to_string()]; // Allowlist

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

        // Not in allowlist → dfe (default)
        let p3 = br#"{"org_id": "other", "event_category": "auth"}"#;
        assert_eq!(
            router.route(p3),
            RouteResult::Table("dfe.events_auth".to_string())
        );
    }

    #[test]
    fn test_nested_db_field() {
        let mut config = test_config();
        config.route_all_by_org = false; // Disable route_all_by_org
        config.db_fields = vec!["metadata.org_id".to_string(), "org_id".to_string()];
        config.routed_orgs = vec!["nested_org".to_string()]; // Allowlist

        let router = Router::new(&config);

        let payload = br#"{"metadata": {"org_id": "nested_org"}, "event_category": "auth"}"#;
        assert_eq!(
            router.route(payload),
            RouteResult::Table("nested_org.events_auth".to_string())
        );
    }

    #[test]
    fn test_route_category_legacy() {
        let router = Router::new(&test_config());

        // Mapped category
        assert_eq!(
            router.route_category("auth"),
            RouteResult::Table("dfe.events_auth".to_string())
        );

        // Unmapped category uses category as table
        assert_eq!(
            router.route_category("network"),
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
    fn test_routed_orgs_allowlist() {
        // Allowlist: only specific orgs get own database
        let config = RoutingConfig {
            table_fields: vec!["event_category".to_string()],
            db_fields: vec!["org_id".to_string()],
            routed_orgs: vec!["acme".to_string(), "bigcorp".to_string()],
            route_all_by_org: false,
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        let p2 = br#"{"org_id": "bigcorp", "event_category": "api"}"#;
        let p3 = br#"{"org_id": "other", "event_category": "network"}"#;

        // acme and bigcorp get own DB
        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme.auth".to_string())
        );
        assert_eq!(
            router.route(p2),
            RouteResult::Table("bigcorp.api".to_string())
        );

        // other goes to dfe (default)
        assert_eq!(
            router.route(p3),
            RouteResult::Table("dfe.network".to_string())
        );
    }

    #[test]
    fn test_route_all_by_org() {
        // Route ALL orgs to own databases
        let config = RoutingConfig {
            table_fields: vec!["event_category".to_string()],
            org_id_field: Some("org_id".to_string()),
            route_all_by_org: true,
            ..Default::default()
        };

        let router = Router::new(&config);

        let p1 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        let p2 = br#"{"org_id": "customer123", "event_category": "api"}"#;

        // Each org gets own database
        assert_eq!(
            router.route(p1),
            RouteResult::Table("acme.auth".to_string())
        );
        assert_eq!(
            router.route(p2),
            RouteResult::Table("customer123.api".to_string())
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
        let mut routing_config = RoutingConfig::default();
        routing_config.compat_v2_source = true;

        let mut metadata_config = MetadataConfig::default();
        metadata_config.source_fields = vec!["_source".to_string()];

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
        let mut routing_config = RoutingConfig::default();
        routing_config.compat_v2_source = true;

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
}
