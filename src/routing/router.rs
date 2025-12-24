//! Routes messages to destination tables based on event_category

use std::collections::HashMap;

use crate::config::RoutingConfig;
use crate::payload::parse::{extract_field_json, extract_nested_field_json};

/// Route result with destination info
#[derive(Debug, Clone, PartialEq)]
pub enum RouteResult {
    /// Route to specific table
    Table(String),
    /// Send to DLQ (no category found and no default)
    Dlq(String),
}

/// Routes messages based on event_category field
pub struct Router {
    routing_field: String,
    fallback_field: Option<String>,
    category_to_table: HashMap<String, String>,
    default_table: Option<String>,
    dlq_enabled: bool,
}

impl Router {
    /// Create a new router from config
    pub fn new(config: &RoutingConfig) -> Self {
        Self {
            routing_field: config.routing_field.clone(),
            fallback_field: config.fallback_field.clone(),
            category_to_table: config.category_to_table.clone(),
            default_table: config.default_table.clone(),
            dlq_enabled: config.dlq.enabled,
        }
    }

    /// Extract the category field from JSON payload
    #[inline]
    pub fn extract_category(&self, payload: &[u8]) -> Option<String> {
        // Try primary field first
        if let Some(value) = if self.routing_field.contains('.') {
            extract_nested_field_json(payload, &self.routing_field)
        } else {
            extract_field_json(payload, &self.routing_field)
        } {
            return Some(value);
        }

        // Try fallback field
        if let Some(ref fallback) = self.fallback_field {
            if fallback.contains('.') {
                extract_nested_field_json(payload, fallback)
            } else {
                extract_field_json(payload, fallback)
            }
        } else {
            None
        }
    }

    /// Route a message based on its category
    pub fn route(&self, payload: &[u8]) -> RouteResult {
        let category = self.extract_category(payload);

        match category {
            Some(cat) => {
                // Check explicit mapping
                if let Some(table) = self.category_to_table.get(&cat) {
                    return RouteResult::Table(table.clone());
                }

                // No explicit mapping - use category as table name or default
                if let Some(ref default) = self.default_table {
                    RouteResult::Table(default.clone())
                } else {
                    // Use category as table name
                    RouteResult::Table(cat)
                }
            }
            None => {
                // No category found
                if let Some(ref default) = self.default_table {
                    RouteResult::Table(default.clone())
                } else if self.dlq_enabled {
                    RouteResult::Dlq("no_category".to_string())
                } else {
                    // Fallback to "unknown" table if DLQ is disabled
                    RouteResult::Table("unknown".to_string())
                }
            }
        }
    }

    /// Route with pre-extracted category
    pub fn route_category(&self, category: &str) -> RouteResult {
        // Check explicit mapping
        if let Some(table) = self.category_to_table.get(category) {
            return RouteResult::Table(table.clone());
        }

        // No explicit mapping - use default or category name
        if let Some(ref default) = self.default_table {
            RouteResult::Table(default.clone())
        } else {
            RouteResult::Table(category.to_string())
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self {
            routing_field: "event_category".to_string(),
            fallback_field: Some("tags.event_category".to_string()),
            category_to_table: HashMap::new(),
            default_table: None,
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
            routing_field: "event_category".to_string(),
            fallback_field: Some("tags.event_category".to_string()),
            category_to_table,
            mapping_file: None,
            default_table: None,
            dlq: crate::config::DlqConfig {
                enabled: true,
                topic_suffix: ".dlq".to_string(),
            },
        }
    }

    #[test]
    fn test_router_direct_match() {
        let router = Router::new(&test_config());
        let payload = br#"{"event_category": "auth", "user_id": 123}"#;

        assert_eq!(router.route(payload), RouteResult::Table("events_auth".to_string()));
    }

    #[test]
    fn test_router_fallback_field() {
        let router = Router::new(&test_config());
        let payload = br#"{"tags": {"event_category": "api"}, "user_id": 123}"#;

        assert_eq!(router.route(payload), RouteResult::Table("events_api".to_string()));
    }

    #[test]
    fn test_router_unmapped_category() {
        let router = Router::new(&test_config());
        let payload = br#"{"event_category": "network"}"#;

        // Unmapped category uses category name as table
        assert_eq!(router.route(payload), RouteResult::Table("network".to_string()));
    }

    #[test]
    fn test_router_no_category_dlq() {
        let router = Router::new(&test_config());
        let payload = br#"{"user_id": 123}"#;

        assert_eq!(router.route(payload), RouteResult::Dlq("no_category".to_string()));
    }

    #[test]
    fn test_router_with_default_table() {
        let mut config = test_config();
        config.default_table = Some("events_default".to_string());
        let router = Router::new(&config);

        // Unknown category goes to default
        let payload = br#"{"event_category": "unknown_type"}"#;
        assert_eq!(router.route(payload), RouteResult::Table("events_default".to_string()));

        // No category also goes to default
        let payload2 = br#"{"user_id": 123}"#;
        assert_eq!(router.route(payload2), RouteResult::Table("events_default".to_string()));
    }

    #[test]
    fn test_extract_category() {
        let router = Router::default();

        // Direct field
        let p1 = br#"{"event_category": "auth"}"#;
        assert_eq!(router.extract_category(p1), Some("auth".to_string()));

        // Nested field
        let p2 = br#"{"tags": {"event_category": "api"}}"#;
        assert_eq!(router.extract_category(p2), Some("api".to_string()));

        // No category
        let p3 = br#"{"user_id": 123}"#;
        assert_eq!(router.extract_category(p3), None);
    }
}
