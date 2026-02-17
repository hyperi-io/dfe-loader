// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Main transformer that applies all transformations
//!
//! Pipeline: Parse → Extract tags → Flatten → Timestamp → _raw rename → Metadata → Sanitize → Remove routing fields
//! Note: _json is handled by ArrowBatchBuilder sidecar (zero-copy from raw bytes), not by the transformer.

use chrono::Utc;
use serde_json::{Map, Value};

use crate::config::{FieldSanitizationConfig, MetadataConfig, RoutingConfig, TimestampDqConfig};
use crate::transform::flatten::flatten_value_owned;
use crate::transform::timestamp::{TimestampResult, TimestampValidator};
use crate::Result;

/// Static field names (avoids allocation per message)
/// Input field names - what we read from source data
static TIMESTAMP_INPUT_FIELD: &str = "timestamp";
static TIMESTAMP_RECEIVED_INPUT_FIELD: &str = "timestamp_received";
/// Output field names - all common header fields use underscore prefix
static TIMESTAMP_OUTPUT_FIELD: &str = "_timestamp";
static TIMESTAMP_RECEIVED_OUTPUT_FIELD: &str = "_timestamp_received";
static TIMESTAMP_COLLECTOR_FIELD: &str = "_timestamp_collector";

/// Transform result with metadata
pub struct TransformResult {
    pub data: Map<String, Value>,
    pub warnings: Vec<String>,
}

/// Applies transformations to JSON events
///
/// Common Header v2 features:
/// - Extract `_tags` from config-driven source fields before flattening
/// - Capture `_json` (raw payload) before transformation
/// - Inject `_raw` from source field (zero-copy rename)
/// - Remove routing fields after extraction
///
/// Uses static strings for common field names to avoid allocation per message.
pub struct Transformer {
    timestamp_validator: TimestampValidator,
    extract_collector_timestamp: bool,
    collector_timestamp_path: String,
    strip_at_prefix: bool,
    collapse_underscores: bool,
    trim_underscores: bool,
    flatten_nested: bool,

    // Common Header v2: Tags handling
    tags_fields: Vec<String>,
    tags_output: String,
    drop_tags: bool,

    // Common Header v2: _json field name (used for sanitization preservation)
    // Note: _json capture is controlled at the orchestrator/buffer level, not here.
    // The ArrowBatchBuilder builds _json from raw bytes via sidecar (zero-copy).
    json_output: String,

    // Common Header v2: _raw injection (zero-copy rename from source field)
    capture_raw: bool,
    raw_source_fields: Vec<String>,
    raw_output: String,

    // Common Header v2: Routing field removal
    remove_routing_fields: bool,
    routing_db_fields: Vec<String>,
    routing_table_fields: Vec<String>,

    // Common Header v2: _org_id field injection
    org_id_output: String,
}

impl Transformer {
    /// Create a new transformer with config
    pub fn new(
        timestamp_config: &TimestampDqConfig,
        metadata_config: &MetadataConfig,
        sanitization_config: &FieldSanitizationConfig,
    ) -> Self {
        Self {
            timestamp_validator: TimestampValidator::new(timestamp_config),
            extract_collector_timestamp: metadata_config.extract_timestamp_collector,
            collector_timestamp_path: metadata_config.collector_timestamp_path.clone(),
            strip_at_prefix: sanitization_config.strip_at_prefix,
            collapse_underscores: sanitization_config.collapse_underscores,
            trim_underscores: sanitization_config.trim_underscores,
            flatten_nested: true,

            // Common Header v2: Tags handling
            tags_fields: metadata_config.tags_fields.clone(),
            tags_output: metadata_config.tags_output.clone(),
            drop_tags: metadata_config.drop_tags,

            // Common Header v2: _json field name (capture controlled at orchestrator level)
            json_output: metadata_config.json_output.clone(),

            // Common Header v2: _raw injection
            capture_raw: metadata_config.capture_raw,
            raw_source_fields: metadata_config.raw_source_fields.clone(),
            raw_output: metadata_config.raw_output.clone(),

            // Common Header v2: Routing field removal (empty until with_routing called)
            remove_routing_fields: metadata_config.remove_routing_fields,
            routing_db_fields: Vec::new(),
            routing_table_fields: Vec::new(),

            // Common Header v2: _org_id field injection
            org_id_output: "_org_id".to_string(),
        }
    }

    /// Create transformer with routing config for field removal
    pub fn with_routing(
        timestamp_config: &TimestampDqConfig,
        metadata_config: &MetadataConfig,
        sanitization_config: &FieldSanitizationConfig,
        routing_config: &RoutingConfig,
    ) -> Self {
        let mut transformer = Self::new(timestamp_config, metadata_config, sanitization_config);
        transformer.routing_db_fields = routing_config.db_fields.clone();
        transformer.routing_table_fields = routing_config.table_fields.clone();
        transformer
    }

    /// Get the _json output field name
    #[must_use]
    pub fn json_output(&self) -> &str {
        &self.json_output
    }

    /// Get the _raw output field name
    #[must_use]
    pub fn raw_output(&self) -> &str {
        &self.raw_output
    }

    /// Transform a parsed JSON value for Common Header v2 processing.
    ///
    /// Applies: flatten → timestamp validation → `_raw` rename → `_tags` extraction →
    /// `_org_id` injection → routing field removal → field sanitization.
    ///
    /// Note: `_json` is NOT injected here. It's built from raw Kafka bytes directly
    /// in `ArrowBatchBuilder` sidecar (zero-copy via `Buffer::from_vec`).
    ///
    /// ## Parameters
    ///
    /// - `value`: Parsed JSON object to transform
    /// - `org_id`: Optional org_id value for _org_id field (RLS)
    pub fn transform_with_raw(
        &self,
        value: Value,
        org_id: Option<&str>,
    ) -> Result<TransformResult> {
        // Cache current time once per message to avoid multiple syscalls
        let now = Utc::now();
        // OPTIMIZATION: Lazy timestamp formatting - only format if we need to insert
        let mut now_str: Option<String> = None;

        // Lazy warnings allocation - only allocate if we actually have warnings
        let mut warnings: Option<Vec<String>> = None;

        // Ensure we have an object to work with
        let obj = match value {
            Value::Object(map) => map,
            _ => return Err(crate::Error::Transform("Expected JSON object".into())),
        };

        // Step 1: Extract _tags BEFORE flattening (preserves nested structure)
        // OPTIMIZATION: Skip entirely if tags_fields is empty
        let extracted_tags = if !self.drop_tags && !self.tags_fields.is_empty() {
            self.extract_tags(&obj)
        } else {
            None
        };

        // Step 2: Flatten nested objects (owned version avoids cloning leaf values)
        let mut data = if self.flatten_nested {
            flatten_value_owned(Value::Object(obj))
        } else {
            obj
        };

        // Step 3: Validate/correct timestamp
        // Read from input field (timestamp), remove it, write to output field (_timestamp)
        if let Some(ts_value) = data.remove(TIMESTAMP_INPUT_FIELD) {
            let ts_result = match &ts_value {
                Value::String(s) => self.timestamp_validator.validate_with_now(s, now),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        self.timestamp_validator.validate_unix_with_now(i, now)
                    } else {
                        TimestampResult::Invalid("Invalid number format".into())
                    }
                }
                _ => TimestampResult::Invalid("Timestamp must be string or number".into()),
            };

            match ts_result {
                TimestampResult::Valid(dt) => {
                    data.insert(
                        TIMESTAMP_OUTPUT_FIELD.into(),
                        Value::String(dt.to_rfc3339()),
                    );
                }
                TimestampResult::Corrected(dt, reason) => {
                    warnings.get_or_insert_with(Vec::new).push(reason);
                    data.insert(
                        TIMESTAMP_OUTPUT_FIELD.into(),
                        Value::String(dt.to_rfc3339()),
                    );
                }
                TimestampResult::Invalid(reason) => {
                    warnings.get_or_insert_with(Vec::new).push(reason);
                    // OPTIMIZATION: Lazy format - only format now() if we actually need it
                    let ts = now_str.get_or_insert_with(|| now.to_rfc3339()).clone();
                    data.insert(TIMESTAMP_OUTPUT_FIELD.into(), Value::String(ts));
                }
            }
        } else {
            // No timestamp field - inject current time (lazy format)
            let ts = now_str.get_or_insert_with(|| now.to_rfc3339()).clone();
            data.insert(TIMESTAMP_OUTPUT_FIELD.into(), Value::String(ts));
        }

        // Step 4: Extract collector timestamp if present
        // Use remove() to take ownership instead of get().cloned() to avoid allocation
        if self.extract_collector_timestamp {
            if let Some(ts) = data.remove(&self.collector_timestamp_path) {
                data.insert(TIMESTAMP_COLLECTOR_FIELD.into(), ts);
            }
        }

        // Step 4b: Extract timestamp_received if present (nullable)
        // This is when the receiver/loader received the event
        if let Some(ts) = data.remove(TIMESTAMP_RECEIVED_INPUT_FIELD) {
            data.insert(TIMESTAMP_RECEIVED_OUTPUT_FIELD.into(), ts);
        }

        // Step 5: _json is now built from raw bytes directly in ArrowBatchBuilder sidecar
        // (zero-copy via Buffer::from_vec). No longer injected into the Map here.
        // The orchestrator passes raw_payload to BufferManager.push() instead.

        // Step 5b: @renamed: first(logoriginal/_raw/raw/raw_log/message) → _raw
        // Zero-copy rename via data.remove() — ownership transfer, no clone.
        // Silent no-op if destination already present (upstream may populate _raw directly).
        if self.capture_raw && !data.contains_key(&self.raw_output) {
            for field in &self.raw_source_fields {
                if let Some(val) = data.remove(field.as_str()) {
                    data.insert(self.raw_output.clone(), val);
                    break;
                }
            }
        }

        // Step 6: Add extracted _tags
        if let Some(tags) = extracted_tags {
            data.insert(self.tags_output.clone(), tags);
        }

        // Step 7: Inject _org_id for row-level security (RLS)
        if let Some(org) = org_id {
            data.insert(self.org_id_output.clone(), Value::String(org.to_string()));
        }

        // Step 8: Remove routing fields (they're only used for db.table routing)
        if self.remove_routing_fields {
            self.remove_routing_fields_from(&mut data);
        }

        // Step 9: Sanitize field names
        let sanitized = self.sanitize_fields(data);

        Ok(TransformResult {
            data: sanitized,
            warnings: warnings.unwrap_or_default(),
        })
    }

    /// Transform a parsed JSON value (takes ownership to avoid cloning)
    ///
    /// Legacy method - use transform_with_raw for Common Header v2 features.
    pub fn transform(&self, value: Value) -> Result<TransformResult> {
        self.transform_with_raw(value, None)
    }

    /// Extract tags from the first matching field in tags_fields
    ///
    /// Returns the extracted value (preserves nested structure).
    /// Uses dot notation for nested field access.
    fn extract_tags(&self, obj: &Map<String, Value>) -> Option<Value> {
        for field in &self.tags_fields {
            if let Some(value) = self.get_nested_field_from_map(obj, field) {
                return Some(value.clone());
            }
        }
        None
    }

    /// Get a nested field from a Map using dot notation
    fn get_nested_field_from_map<'a>(
        &self,
        obj: &'a Map<String, Value>,
        path: &str,
    ) -> Option<&'a Value> {
        // Fast path: no dot means simple field access
        if !path.contains('.') {
            return obj.get(path);
        }

        // Slow path: nested field access
        let mut parts = path.split('.');
        let first = parts.next()?;
        let mut current = obj.get(first)?;

        for part in parts {
            current = current.get(part)?;
        }

        Some(current)
    }

    /// Remove routing fields from the data map
    ///
    /// Removes both db_fields and table_fields used for routing.
    /// Only removes top-level fields (flattened keys like "tags.event_category").
    fn remove_routing_fields_from(&self, data: &mut Map<String, Value>) {
        // Remove db fields
        for field in &self.routing_db_fields {
            // For nested fields like "tags.org_id", after flattening it becomes "tags.org_id"
            data.remove(field);
        }

        // Remove table fields
        for field in &self.routing_table_fields {
            data.remove(field);
        }
    }

    /// Check if any sanitization is enabled
    #[inline]
    fn needs_sanitization(&self) -> bool {
        self.strip_at_prefix || self.collapse_underscores || self.trim_underscores
    }

    /// Sanitize field names for ClickHouse compatibility
    ///
    /// Fast path: returns input unchanged if no sanitization is enabled.
    /// Uses owned key sanitization to avoid cloning keys that don't need changes.
    fn sanitize_fields(&self, data: Map<String, Value>) -> Map<String, Value> {
        // Fast path: no sanitization needed
        if !self.needs_sanitization() {
            return data;
        }

        let mut result = Map::with_capacity(data.len());

        for (key, value) in data {
            // Move key ownership - avoids clone when no sanitization needed
            let sanitized_key = self.sanitize_key_owned(key);
            result.insert(sanitized_key, value);
        }

        result
    }

    /// Sanitize a single field key, returning owned key with minimal allocation
    ///
    /// When no sanitization needed, returns the original key moved (zero allocation).
    /// When sanitization needed, applies transformations.
    ///
    /// Note: Keys starting with underscore (like _tags, _uuid) are preserved
    /// when they are system fields (output field names from config or common header).
    #[inline]
    fn sanitize_key_owned(&self, key: String) -> String {
        // Preserve system fields that start with underscore
        // These are common header fields that must not be sanitized
        if key == self.tags_output
            || key == self.json_output
            || key == self.raw_output
            || key == self.org_id_output
            || key == TIMESTAMP_OUTPUT_FIELD         // _timestamp
            || key == TIMESTAMP_RECEIVED_OUTPUT_FIELD // _timestamp_received
            || key == TIMESTAMP_COLLECTOR_FIELD      // _timestamp_collector
            || key == "_timestamp_load"              // ClickHouse DEFAULT
            || key == "_uuid"
        // ClickHouse DEFAULT
        {
            return key;
        }

        // Check if we need to modify
        let needs_change = (self.strip_at_prefix && key.starts_with('@'))
            || (self.collapse_underscores && key.contains("__"))
            || (self.trim_underscores && (key.starts_with('_') || key.ends_with('_')));

        // Fast path: no changes needed - return as-is (zero allocation)
        if !needs_change {
            return key;
        }

        // Slow path: apply transformations
        let mut sanitized = if self.strip_at_prefix && key.starts_with('@') {
            key[1..].to_string()
        } else {
            key
        };

        // Collapse multiple underscores - OPTIMIZATION: single-pass algorithm
        // Avoids O(n²) while contains() + replace() loop
        if self.collapse_underscores && sanitized.contains("__") {
            let mut result = String::with_capacity(sanitized.len());
            let mut prev_underscore = false;
            for c in sanitized.chars() {
                if c == '_' {
                    if !prev_underscore {
                        result.push(c);
                    }
                    prev_underscore = true;
                } else {
                    result.push(c);
                    prev_underscore = false;
                }
            }
            sanitized = result;
        }

        // Trim leading/trailing underscores
        if self.trim_underscores {
            sanitized = sanitized.trim_matches('_').to_string();
        }

        // Handle empty key after sanitization
        if sanitized.is_empty() {
            sanitized = "unnamed".to_string();
        }

        sanitized
    }

    /// Transform raw JSON bytes
    pub fn transform_bytes(&self, json: &[u8]) -> Result<Vec<u8>> {
        let value: Value = sonic_rs::from_slice(json)
            .map_err(|e| crate::Error::Json(format!("Parse error: {}", e)))?;

        let result = self.transform_with_raw(value, None)?;

        serde_json::to_vec(&Value::Object(result.data))
            .map_err(|e| crate::Error::Json(format!("Serialize error: {}", e)))
    }
}

impl Default for Transformer {
    fn default() -> Self {
        Self {
            timestamp_validator: TimestampValidator::default(),
            extract_collector_timestamp: true,
            collector_timestamp_path: "tags.collector.timestamp".to_string(),
            strip_at_prefix: true,
            collapse_underscores: true,
            trim_underscores: true,
            flatten_nested: true,

            // Common Header v2 defaults
            tags_fields: vec![
                "tags".to_string(),
                "_tags".to_string(),
                "meta".to_string(),
                "metadata.tags".to_string(),
            ],
            tags_output: "_tags".to_string(),
            drop_tags: false,

            json_output: "_json".to_string(),

            capture_raw: true,
            raw_source_fields: vec!["logoriginal".to_string()],
            raw_output: "_raw".to_string(),

            remove_routing_fields: true,
            routing_db_fields: vec!["org_id".to_string()],
            routing_table_fields: vec![
                "event_category".to_string(),
                "tags.event_category".to_string(),
            ],

            org_id_output: "_org_id".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transformer_basic() {
        let transformer = Transformer::default();
        let input = serde_json::json!({
            "event": "login",
            "user_id": 123
        });

        let result = transformer.transform(input).unwrap();
        assert_eq!(result.data["event"], "login");
        assert_eq!(result.data["user_id"], 123);
    }

    #[test]
    fn test_transformer_flatten() {
        let transformer = Transformer::default();
        let input = serde_json::json!({
            "event": "login",
            "user": {
                "id": 123,
                "name": "test"
            }
        });

        let result = transformer.transform(input).unwrap();
        assert_eq!(result.data["user.id"], 123);
        assert_eq!(result.data["user.name"], "test");
    }

    #[test]
    fn test_transformer_sanitize_at_prefix() {
        let transformer = Transformer::default();
        let input = serde_json::json!({
            "@timestamp": "2024-12-24T10:00:00Z",
            "@version": "1"
        });

        let result = transformer.transform(input).unwrap();

        // @timestamp gets sanitized to "timestamp" AFTER timestamp processing,
        // so _timestamp is injected with current time and @timestamp becomes "timestamp"
        assert!(result.data.contains_key("_timestamp")); // injected current time
        assert!(result.data.contains_key("timestamp")); // sanitized from @timestamp
        assert!(result.data.contains_key("version")); // sanitized from @version
        assert!(!result.data.contains_key("@timestamp"));
        assert!(!result.data.contains_key("@version"));
    }

    #[test]
    fn test_transformer_collapse_underscores() {
        let transformer = Transformer::default();
        let input = serde_json::json!({
            "field__with___underscores": "value"
        });

        let result = transformer.transform(input).unwrap();
        assert!(result.data.contains_key("field_with_underscores"));
    }

    #[test]
    fn test_transformer_bytes() {
        let transformer = Transformer::default();
        let input = br#"{"event": "test"}"#;

        let output = transformer.transform_bytes(input).unwrap();
        let parsed: Value = serde_json::from_slice(&output).unwrap();

        assert_eq!(parsed["event"], "test");
    }

    #[test]
    fn test_transformer_extracts_tags() {
        let transformer = Transformer::default();
        let raw = br#"{"event": "login", "tags": {"source": "api", "level": "info"}}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _tags should contain the extracted tags object
        assert!(result.data.contains_key("_tags"));
        let tags = &result.data["_tags"];
        assert!(tags.is_object() || tags.is_string()); // Could be object or serialized
    }

    #[test]
    fn test_transformer_no_json_in_map() {
        // _json is built from raw bytes in ArrowBatchBuilder sidecar,
        // NOT injected into the Map by the transformer.
        let transformer = Transformer::default();
        let raw = br#"{"event": "login", "user_id": 123}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _json should NOT be in the Map (sidecar handles it)
        assert!(!result.data.contains_key("_json"));
    }

    #[test]
    fn test_transformer_removes_routing_fields() {
        let transformer = Transformer::default();
        let raw = br#"{"org_id": "acme", "event_category": "auth", "data": "test"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // Routing fields should be removed
        assert!(!result.data.contains_key("org_id"));
        assert!(!result.data.contains_key("event_category"));
        // Other fields should remain
        assert!(result.data.contains_key("data"));
    }

    #[test]
    fn test_transformer_preserves_underscore_system_fields() {
        let transformer = Transformer::default();
        let raw = br#"{"event": "test", "tags": {"level": "info"}}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _tags should not be trimmed to "tags" by underscore sanitization
        assert!(result.data.contains_key("_tags"));
        assert!(!result.data.contains_key("tags")); // Should not exist as "tags" after extraction
    }

    #[test]
    fn test_transformer_drop_tags() {
        let transformer = Transformer {
            drop_tags: true,
            ..Default::default()
        };

        let raw = br#"{"event": "login", "tags": {"source": "api"}}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _tags should not be present when drop_tags is true
        assert!(!result.data.contains_key("_tags"));
    }

    #[test]
    fn test_transformer_captures_raw_from_logoriginal() {
        let transformer = Transformer::default();
        let raw = br#"{"event": "login", "logoriginal": "raw syslog line here"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _raw should contain the value from logoriginal (zero-copy rename)
        assert_eq!(result.data["_raw"], "raw syslog line here");
        // Source field should be removed (rename, not copy)
        assert!(!result.data.contains_key("logoriginal"));
    }

    #[test]
    fn test_transformer_raw_first_match_wins() {
        // Override with multiple source fields to test first-match
        let transformer = Transformer {
            raw_source_fields: vec!["logoriginal".to_string(), "message".to_string()],
            ..Default::default()
        };
        // Both logoriginal and message exist — logoriginal is first in raw_source_fields
        let raw = br#"{"event": "test", "logoriginal": "first", "message": "second"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        assert_eq!(result.data["_raw"], "first");
        // logoriginal removed, message remains (only first match consumed)
        assert!(!result.data.contains_key("logoriginal"));
        assert!(result.data.contains_key("message"));
    }

    #[test]
    fn test_transformer_raw_is_rename_not_copy() {
        let transformer = Transformer::default();
        // logoriginal is the default source field
        let raw = br#"{"event": "test", "logoriginal": "the log line"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // logoriginal is in raw_source_fields — should be renamed to _raw
        assert_eq!(result.data["_raw"], "the log line");
        // Source field gone (ownership transferred)
        assert!(!result.data.contains_key("logoriginal"));
    }

    #[test]
    fn test_transformer_raw_silent_noop_if_present() {
        let transformer = Transformer::default();
        // Data already has _raw populated by upstream — rename should be skipped
        let raw = br#"{"event": "test", "logoriginal": "source value", "_raw": "upstream value"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _raw should keep the upstream value (silent no-op)
        assert_eq!(result.data["_raw"], "upstream value");
        // logoriginal should NOT be removed (rename was skipped)
        assert!(result.data.contains_key("logoriginal"));
    }

    #[test]
    fn test_transformer_raw_disabled() {
        let transformer = Transformer {
            capture_raw: false,
            ..Default::default()
        };

        let raw = br#"{"event": "test", "logoriginal": "raw line"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _raw should NOT be present when capture_raw is false
        assert!(!result.data.contains_key("_raw"));
        // Source field should remain untouched
        assert!(result.data.contains_key("logoriginal"));
    }

    #[test]
    fn test_transformer_raw_no_match() {
        let transformer = Transformer::default();
        // No raw_source_fields present in data
        let raw = br#"{"event": "test", "custom_field": "value"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _raw should not be present (no matching source field)
        assert!(!result.data.contains_key("_raw"));
    }

    #[test]
    fn test_transformer_does_not_inject_json() {
        // _json is now built from raw bytes in ArrowBatchBuilder sidecar,
        // not injected by the transformer. Verify it's absent from the Map.
        let transformer = Transformer::default();
        let raw = br#"{"event": "test"}"#;
        let value: Value = serde_json::from_slice(raw).unwrap();

        let result = transformer.transform_with_raw(value, None).unwrap();

        // _json should NEVER be in the Map (handled by ArrowBatchBuilder sidecar)
        assert!(!result.data.contains_key("_json"));
    }

    #[test]
    fn test_transformer_output_accessors() {
        let transformer = Transformer::default();
        assert_eq!(transformer.json_output(), "_json");
        assert_eq!(transformer.raw_output(), "_raw");
    }
}
