//! Main transformer that applies all transformations
//!
//! Pipeline: Parse → Flatten → Timestamp → Metadata → Sanitize

use chrono::Utc;
use serde_json::{Map, Value};

use crate::config::{FieldSanitizationConfig, MetadataConfig, TimestampDqConfig};
use crate::transform::flatten::flatten_value;
use crate::transform::timestamp::{TimestampResult, TimestampValidator};
use crate::Result;

/// Transform result with metadata
pub struct TransformResult {
    pub data: Map<String, Value>,
    pub warnings: Vec<String>,
}

/// Applies transformations to JSON events
pub struct Transformer {
    timestamp_validator: TimestampValidator,
    timestamp_field: String,
    inject_load_timestamp: bool,
    load_timestamp_field: String,
    extract_collector_timestamp: bool,
    collector_timestamp_path: String,
    strip_at_prefix: bool,
    collapse_underscores: bool,
    trim_underscores: bool,
    flatten_nested: bool,
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
            timestamp_field: "timestamp".to_string(),
            inject_load_timestamp: metadata_config.inject_timestamp_load,
            load_timestamp_field: "timestamp_load".to_string(),
            extract_collector_timestamp: metadata_config.extract_timestamp_collector,
            collector_timestamp_path: metadata_config.collector_timestamp_path.clone(),
            strip_at_prefix: sanitization_config.strip_at_prefix,
            collapse_underscores: sanitization_config.collapse_underscores,
            trim_underscores: sanitization_config.trim_underscores,
            flatten_nested: true,
        }
    }

    /// Transform a parsed JSON value
    pub fn transform(&self, value: Value) -> Result<TransformResult> {
        let mut warnings = Vec::new();

        // Step 1: Flatten nested objects
        let mut data = if self.flatten_nested {
            flatten_value(&value)
        } else if let Value::Object(map) = value {
            map
        } else {
            return Err(crate::Error::Transform("Expected JSON object".into()));
        };

        // Step 2: Validate/correct timestamp
        if let Some(ts_value) = data.get(&self.timestamp_field) {
            let ts_result = match ts_value {
                Value::String(s) => self.timestamp_validator.validate(s),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        self.timestamp_validator.validate_unix(i)
                    } else {
                        TimestampResult::Invalid("Invalid number format".into())
                    }
                }
                _ => TimestampResult::Invalid("Timestamp must be string or number".into()),
            };

            match ts_result {
                TimestampResult::Valid(dt) => {
                    data.insert(
                        self.timestamp_field.clone(),
                        Value::String(dt.to_rfc3339()),
                    );
                }
                TimestampResult::Corrected(dt, reason) => {
                    warnings.push(reason);
                    data.insert(
                        self.timestamp_field.clone(),
                        Value::String(dt.to_rfc3339()),
                    );
                }
                TimestampResult::Invalid(reason) => {
                    warnings.push(reason);
                    // Replace with now
                    data.insert(
                        self.timestamp_field.clone(),
                        Value::String(Utc::now().to_rfc3339()),
                    );
                }
            }
        }

        // Step 3: Inject load timestamp
        if self.inject_load_timestamp {
            data.insert(
                self.load_timestamp_field.clone(),
                Value::String(Utc::now().to_rfc3339()),
            );
        }

        // Step 4: Extract collector timestamp if present
        if self.extract_collector_timestamp {
            if let Some(ts) = data.get(&self.collector_timestamp_path).cloned() {
                data.insert("timestamp_collector".to_string(), ts);
            }
        }

        // Step 5: Sanitize field names
        let sanitized = self.sanitize_fields(data);

        Ok(TransformResult {
            data: sanitized,
            warnings,
        })
    }

    /// Sanitize field names for ClickHouse compatibility
    fn sanitize_fields(&self, data: Map<String, Value>) -> Map<String, Value> {
        let mut result = Map::new();

        for (key, value) in data {
            let mut sanitized_key = key;

            // Strip @ prefix (common in Elasticsearch)
            if self.strip_at_prefix && sanitized_key.starts_with('@') {
                sanitized_key = sanitized_key[1..].to_string();
            }

            // Collapse multiple underscores
            if self.collapse_underscores {
                while sanitized_key.contains("__") {
                    sanitized_key = sanitized_key.replace("__", "_");
                }
            }

            // Trim leading/trailing underscores
            if self.trim_underscores {
                sanitized_key = sanitized_key.trim_matches('_').to_string();
            }

            // Handle empty key after sanitization
            if sanitized_key.is_empty() {
                sanitized_key = "unnamed".to_string();
            }

            result.insert(sanitized_key, value);
        }

        result
    }

    /// Transform raw JSON bytes
    pub fn transform_bytes(&self, json: &[u8]) -> Result<Vec<u8>> {
        let value: Value = sonic_rs::from_slice(json)
            .map_err(|e| crate::Error::Json(format!("Parse error: {}", e)))?;

        let result = self.transform(value)?;

        serde_json::to_vec(&Value::Object(result.data))
            .map_err(|e| crate::Error::Json(format!("Serialize error: {}", e)))
    }
}

impl Default for Transformer {
    fn default() -> Self {
        Self {
            timestamp_validator: TimestampValidator::default(),
            timestamp_field: "timestamp".to_string(),
            inject_load_timestamp: true,
            load_timestamp_field: "timestamp_load".to_string(),
            extract_collector_timestamp: true,
            collector_timestamp_path: "tags.collector.timestamp".to_string(),
            strip_at_prefix: true,
            collapse_underscores: true,
            trim_underscores: true,
            flatten_nested: true,
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
        assert!(result.data.contains_key("timestamp_load"));
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
        assert!(result.data.contains_key("timestamp"));
        assert!(result.data.contains_key("version"));
        assert!(!result.data.contains_key("@timestamp"));
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
}
