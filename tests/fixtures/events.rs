// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Event fixture builders

use serde_json::{json, Map, Value};

/// Builder for creating test events
#[derive(Debug, Clone)]
pub struct EventBuilder {
    org_id: Option<String>,
    category: Option<String>,
    action: Option<String>,
    timestamp: Option<i64>,
    fields: Map<String, Value>,
}

impl EventBuilder {
    /// Create a new event builder
    pub fn new() -> Self {
        Self {
            org_id: None,
            category: None,
            action: None,
            timestamp: None,
            fields: Map::new(),
        }
    }

    /// Set org_id
    pub fn org_id<S: Into<String>>(mut self, org_id: S) -> Self {
        self.org_id = Some(org_id.into());
        self
    }

    /// Set event_category
    pub fn category<S: Into<String>>(mut self, category: S) -> Self {
        self.category = Some(category.into());
        self
    }

    /// Set action
    pub fn action<S: Into<String>>(mut self, action: S) -> Self {
        self.action = Some(action.into());
        self
    }

    /// Set timestamp (Unix milliseconds)
    pub fn timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Set timestamp to now
    pub fn with_timestamp_now(mut self) -> Self {
        self.timestamp = Some(chrono::Utc::now().timestamp_millis());
        self
    }

    /// Add a custom field
    pub fn with_field<S: Into<String>>(mut self, key: S, value: Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Add a string field
    pub fn with_string<S1: Into<String>, S2: Into<String>>(self, key: S1, value: S2) -> Self {
        self.with_field(key, json!(value.into()))
    }

    /// Add an integer field
    pub fn with_int<S: Into<String>>(self, key: S, value: i64) -> Self {
        self.with_field(key, json!(value))
    }

    /// Add a float field
    pub fn with_float<S: Into<String>>(self, key: S, value: f64) -> Self {
        self.with_field(key, json!(value))
    }

    /// Add a boolean field
    pub fn with_bool<S: Into<String>>(self, key: S, value: bool) -> Self {
        self.with_field(key, json!(value))
    }

    /// Build the event as serde_json::Value
    pub fn build(self) -> Value {
        let mut event = self.fields;

        if let Some(org_id) = self.org_id {
            event.insert("org_id".to_string(), json!(org_id));
        }

        if let Some(category) = self.category {
            event.insert("event_category".to_string(), json!(category));
        }

        if let Some(action) = self.action {
            event.insert("action".to_string(), json!(action));
        }

        if let Some(timestamp) = self.timestamp {
            event.insert("timestamp".to_string(), json!(timestamp));
        }

        Value::Object(event)
    }

    /// Build as Map<String, Value> (for buffer.push)
    pub fn build_map(self) -> Map<String, Value> {
        match self.build() {
            Value::Object(map) => map,
            _ => unreachable!(),
        }
    }

    /// Build as JSON bytes
    pub fn build_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self.build()).unwrap()
    }
}

impl Default for EventBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch event builder
pub struct BatchEventBuilder {
    count: usize,
    org_ids: Vec<String>,
    categories: Vec<String>,
    base_timestamp: i64,
}

impl BatchEventBuilder {
    /// Create a new batch builder
    pub fn new() -> Self {
        Self {
            count: 100,
            org_ids: vec!["test_org".to_string()],
            categories: vec!["test".to_string()],
            base_timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Set number of events to generate
    pub fn count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    /// Set org_ids to cycle through
    pub fn with_orgs(mut self, orgs: Vec<&str>) -> Self {
        self.org_ids = orgs.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set categories to cycle through
    pub fn with_categories(mut self, categories: Vec<&str>) -> Self {
        self.categories = categories.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set base timestamp
    pub fn base_timestamp(mut self, ts: i64) -> Self {
        self.base_timestamp = ts;
        self
    }

    /// Build batch of events
    pub fn build(self) -> Vec<Value> {
        (0..self.count)
            .map(|i| {
                let org_id = &self.org_ids[i % self.org_ids.len()];
                let category = &self.categories[i % self.categories.len()];

                EventBuilder::new()
                    .org_id(org_id)
                    .category(category)
                    .action(format!("action_{}", i % 10))
                    .timestamp(self.base_timestamp + (i as i64 * 1000))
                    .with_int("id", i as i64)
                    .with_float("value", i as f64 * 1.5)
                    .build()
            })
            .collect()
    }

    /// Build batch as Map<String, Value>
    pub fn build_maps(self) -> Vec<Map<String, Value>> {
        self.build()
            .into_iter()
            .map(|v| match v {
                Value::Object(map) => map,
                _ => unreachable!(),
            })
            .collect()
    }

    /// Build batch as JSON bytes
    pub fn build_bytes(self) -> Vec<Vec<u8>> {
        self.build()
            .into_iter()
            .map(|v| serde_json::to_vec(&v).unwrap())
            .collect()
    }
}

impl Default for BatchEventBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_builder() {
        let event = EventBuilder::new()
            .org_id("acme")
            .category("auth")
            .action("login")
            .with_int("user_id", 1001)
            .with_timestamp_now()
            .build();

        assert_eq!(event["org_id"], "acme");
        assert_eq!(event["event_category"], "auth");
        assert_eq!(event["action"], "login");
        assert_eq!(event["user_id"], 1001);
        assert!(event["timestamp"].is_i64());
    }

    #[test]
    fn test_batch_event_builder() {
        let events = BatchEventBuilder::new()
            .count(10)
            .with_orgs(vec!["acme", "bigcorp"])
            .with_categories(vec!["auth", "api"])
            .build();

        assert_eq!(events.len(), 10);

        // Check cycling through orgs
        assert_eq!(events[0]["org_id"], "acme");
        assert_eq!(events[1]["org_id"], "bigcorp");
        assert_eq!(events[2]["org_id"], "acme");

        // Check cycling through categories
        assert_eq!(events[0]["event_category"], "auth");
        assert_eq!(events[1]["event_category"], "api");
        assert_eq!(events[2]["event_category"], "auth");
    }
}
