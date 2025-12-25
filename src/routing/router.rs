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

use std::collections::HashMap;

use serde_json::Value;

use crate::config::RoutingConfig;
use crate::payload::parse::{extract_field_json, extract_nested_field_json};

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
pub struct Router {
    /// Fields to check for database name (first match wins)
    db_fields: Vec<String>,
    /// Fields to check for table name (first match wins)
    table_fields: Vec<String>,
    /// Default database if no db_field matches
    default_db: String,
    /// Default table if no table_field matches
    default_table: String,
    /// Legacy: category to table mapping (for backwards compatibility)
    category_to_table: HashMap<String, String>,
    /// Whether DLQ is enabled
    dlq_enabled: bool,
}

impl Router {
    /// Create a new router from config
    pub fn new(config: &RoutingConfig) -> Self {
        Self {
            db_fields: config.db_fields.clone(),
            table_fields: config.table_fields.clone(),
            default_db: config.default_db.clone(),
            default_table: config.default_table.clone(),
            category_to_table: config.category_to_table.clone(),
            dlq_enabled: config.dlq.enabled,
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
    #[inline]
    pub fn extract_db(&self, payload: &[u8]) -> String {
        self.extract_first_match(payload, &self.db_fields)
            .unwrap_or_else(|| self.default_db.clone())
    }

    /// Extract the table name from the payload
    #[inline]
    pub fn extract_table(&self, payload: &[u8]) -> String {
        let table = self.extract_first_match(payload, &self.table_fields)
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
    #[inline]
    fn build_route_result(&self, db: &str, table: &str) -> RouteResult {
        // Validate we have non-empty values
        if db.is_empty() || table.is_empty() {
            if self.dlq_enabled {
                return RouteResult::Dlq("empty_db_or_table".to_string());
            }
            // Use defaults for empty values
            let db = if db.is_empty() { &self.default_db } else { db };
            let table = if table.is_empty() { &self.default_table } else { table };
            return RouteResult::Table(format!("{}.{}", db, table));
        }

        RouteResult::Table(format!("{}.{}", db, table))
    }

    /// Extract db from an already-parsed Value (avoids re-parsing)
    #[inline]
    fn extract_db_from_value(&self, value: &Value) -> String {
        self.extract_first_match_from_value(value, &self.db_fields)
            .unwrap_or_else(|| self.default_db.clone())
    }

    /// Extract table from an already-parsed Value (avoids re-parsing)
    #[inline]
    fn extract_table_from_value(&self, value: &Value) -> String {
        let table = self.extract_first_match_from_value(value, &self.table_fields)
            .unwrap_or_else(|| self.default_table.clone());

        // Check legacy category_to_table mapping
        if let Some(mapped) = self.category_to_table.get(&table) {
            mapped.clone()
        } else {
            table
        }
    }

    /// Extract a field value from parsed JSON Value using dot notation
    #[inline]
    fn extract_first_match_from_value(&self, value: &Value, fields: &[String]) -> Option<String> {
        for field in fields {
            if let Some(val) = self.get_nested_field(value, field) {
                if let Some(s) = val.as_str() {
                    return Some(s.to_string());
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
            mapped.clone()
        } else {
            category.to_string()
        };

        RouteResult::Table(format!("{}.{}", self.default_db, table))
    }
}

impl Default for Router {
    fn default() -> Self {
        Self {
            db_fields: vec!["org_id".to_string()],
            table_fields: vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ],
            default_db: "common".to_string(),
            default_table: "common".to_string(),
            category_to_table: HashMap::new(),
            dlq_enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            default_db: "common".to_string(),
            default_table: "common".to_string(),
            category_to_table,
            mapping_file: None,
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
        let payload = br#"{"org_id": "tenant1", "tags": {"event_category": "api"}, "user_id": 123}"#;

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
            RouteResult::Table("common.network".to_string())
        );
    }

    #[test]
    fn test_router_default_table() {
        let router = Router::new(&test_config());
        // No event_category or tags.event_category, should use default_table
        let payload = br#"{"org_id": "acme", "user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("acme.common".to_string())
        );
    }

    #[test]
    fn test_router_all_defaults() {
        let router = Router::new(&test_config());
        // No matching fields at all
        let payload = br#"{"user_id": 123}"#;

        assert_eq!(
            router.route(payload),
            RouteResult::Table("common.common".to_string())
        );
    }

    #[test]
    fn test_extract_db() {
        let router = Router::default();

        let p1 = br#"{"org_id": "acme", "event_category": "auth"}"#;
        assert_eq!(router.extract_db(p1), "acme".to_string());

        let p2 = br#"{"event_category": "auth"}"#;
        assert_eq!(router.extract_db(p2), "common".to_string());
    }

    #[test]
    fn test_extract_table() {
        let router = Router::default();

        // Direct field
        let p1 = br#"{"event_category": "auth"}"#;
        assert_eq!(router.extract_table(p1), "auth".to_string());

        // Nested field (fallback)
        let p2 = br#"{"tags": {"event_category": "api"}}"#;
        assert_eq!(router.extract_table(p2), "api".to_string());

        // No match
        let p3 = br#"{"user_id": 123}"#;
        assert_eq!(router.extract_table(p3), "common".to_string());
    }

    #[test]
    fn test_custom_db_fields() {
        let mut config = test_config();
        config.db_fields = vec!["tenant_id".to_string(), "org_id".to_string()];

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
    }

    #[test]
    fn test_nested_db_field() {
        let mut config = test_config();
        config.db_fields = vec!["metadata.org_id".to_string(), "org_id".to_string()];

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
            RouteResult::Table("common.events_auth".to_string())
        );

        // Unmapped category uses category as table
        assert_eq!(
            router.route_category("network"),
            RouteResult::Table("common.network".to_string())
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
            RouteResult::Table("common.common".to_string())
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
}
