//! JSON flattening (nested objects → dot notation)
//!
//! Converts nested JSON objects like:
//!   {"tags": {"category": "auth"}}
//! To flattened form:
//!   {"tags.category": "auth"}
//!
//! ## Performance
//!
//! - `flatten_value_owned()` - Single message, takes ownership
//! - `BatchFlattener` - Multiple messages with same schema, amortizes key computation
//!
//! ## Batch Processing
//!
//! For messages with uniform structure, use `BatchFlattener` to amortize the
//! cost of computing flattened key paths:
//!
//! ```ignore
//! let flattener = BatchFlattener::from_sample(&sample_value);
//! for value in messages {
//!     let flattened = flattener.flatten_fast(value);
//! }
//! ```

use compact_str::CompactString;
use serde_json::{Map, Value};

/// Flatten a nested JSON value into a flat map with dot notation keys.
///
/// Arrays are converted to JSON string representation.
///
/// **Note**: This clones values. Use `flatten_value_owned()` when you own the Value.
pub fn flatten_value(value: &Value) -> Map<String, Value> {
    let mut result = Map::new();
    flatten_recursive(value, String::new(), &mut result);
    result
}

/// Flatten a nested JSON value into a flat map, taking ownership to avoid clones.
///
/// This is the preferred method in the hot path when you already own the Value.
/// Arrays are converted to JSON string representation.
#[inline]
pub fn flatten_value_owned(value: Value) -> Map<String, Value> {
    let mut result = Map::new();
    // Estimate depth of 4 with average key length of 16 chars
    let mut prefix_buf = String::with_capacity(64);
    flatten_recursive_owned_buffered(value, &mut prefix_buf, &mut result);
    result
}

fn flatten_recursive(value: &Value, prefix: String, result: &mut Map<String, Value>) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                let new_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_recursive(val, new_key, result);
            }
        }
        Value::Array(arr) => {
            // Convert arrays to JSON string representation
            result.insert(prefix, Value::String(serde_json::to_string(arr).unwrap_or_default()));
        }
        _ => {
            // Leaf value (string, number, bool, null)
            result.insert(prefix, value.clone());
        }
    }
}

/// Optimized owned version using a reusable prefix buffer
///
/// Reduces allocations by reusing a single String buffer for building prefixes.
/// The buffer is extended and truncated instead of creating new Strings.
/// For top-level fields (depth 0), takes ownership of key directly without cloning.
fn flatten_recursive_owned_buffered(value: Value, prefix_buf: &mut String, result: &mut Map<String, Value>) {
    match value {
        Value::Object(map) => {
            let base_len = prefix_buf.len();
            let is_top_level = base_len == 0;

            for (key, val) in map {
                // Check if this is a leaf value we can optimize
                let is_leaf = !matches!(val, Value::Object(_));

                if is_top_level && is_leaf {
                    // Top-level leaf: use key directly without building prefix (zero copy)
                    match val {
                        Value::Array(arr) => {
                            result.insert(key, Value::String(serde_json::to_string(&arr).unwrap_or_default()));
                        }
                        _ => {
                            result.insert(key, val);
                        }
                    }
                } else {
                    // Build key in buffer for recursion
                    if base_len > 0 {
                        prefix_buf.push('.');
                    }
                    prefix_buf.push_str(&key);

                    flatten_recursive_owned_buffered(val, prefix_buf, result);

                    // Restore buffer to original length for next iteration
                    prefix_buf.truncate(base_len);
                }
            }
        }
        Value::Array(arr) => {
            // Convert arrays to JSON string representation
            // Must clone prefix since we may need it for sibling keys
            let key = prefix_buf.clone();
            result.insert(key, Value::String(serde_json::to_string(&arr).unwrap_or_default()));
        }
        _ => {
            // Leaf value - must clone prefix since we may need it for sibling keys
            let key = prefix_buf.clone();
            result.insert(key, value);
        }
    }
}

/// Flatten JSON bytes, returning flattened JSON bytes.
pub fn flatten(json: &[u8]) -> crate::Result<Vec<u8>> {
    let value: Value = sonic_rs::from_slice(json)
        .map_err(|e| crate::Error::Json(format!("Failed to parse JSON for flattening: {}", e)))?;

    let flattened = flatten_value(&value);
    let result = serde_json::to_vec(&Value::Object(flattened))
        .map_err(|e| crate::Error::Json(format!("Failed to serialize flattened JSON: {}", e)))?;

    Ok(result)
}

// ============================================================================
// Batch Flattening - Schema-aware optimization for uniform messages
// ============================================================================

use std::sync::Arc;

/// Pre-computed flattened key for a field path.
///
/// Uses Arc<str> for zero-copy reuse across multiple messages.
#[derive(Debug, Clone)]
struct KeyMapping {
    /// Original field name at this level
    field_name: CompactString,
    /// Pre-computed flattened key (shared across messages)
    flat_key: Arc<str>,
    /// Whether this is a leaf field (vs nested object)
    is_leaf: bool,
    /// Whether this leaf is an array (needs JSON serialization)
    is_array: bool,
    /// Child mappings for nested objects
    children: Vec<KeyMapping>,
}

/// Batch flattener with pre-computed key mappings.
///
/// Analyzes a sample message to detect structure, then uses cached key strings
/// to efficiently flatten subsequent messages with the same structure.
///
/// ## Performance
///
/// The main optimization is key string reuse via `Arc<str>`:
/// - Keys are allocated once during analysis
/// - Each flattened message reuses the same key references
/// - Reduces allocation overhead significantly for large batches
///
/// ## Usage
///
/// ```ignore
/// let flattener = BatchFlattener::from_sample(&sample);
/// for value in messages {
///     let flattened = flattener.flatten_fast(value);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct BatchFlattener {
    /// Root-level key mappings
    mappings: Vec<KeyMapping>,
    /// Expected field count for result map capacity
    field_count: usize,
    /// Whether the schema is uniform (all fields are predictable)
    uniform: bool,
}

impl Default for BatchFlattener {
    fn default() -> Self {
        Self {
            mappings: Vec::new(),
            field_count: 0,
            uniform: false,
        }
    }
}

impl BatchFlattener {
    /// Create a new batch flattener from a sample value.
    ///
    /// Analyzes the sample to detect structure and pre-compute key mappings.
    pub fn from_sample(sample: &Value) -> Self {
        let mut mappings = Vec::new();
        let mut field_count = 0;
        let mut prefix_buf = String::with_capacity(64);

        Self::build_mappings(sample, &mut prefix_buf, &mut mappings, &mut field_count);

        let uniform = !mappings.is_empty();

        Self {
            mappings,
            field_count,
            uniform,
        }
    }

    /// Build key mappings recursively.
    fn build_mappings(
        value: &Value,
        prefix: &mut String,
        mappings: &mut Vec<KeyMapping>,
        field_count: &mut usize,
    ) {
        let Value::Object(map) = value else {
            return;
        };

        let base_len = prefix.len();

        for (key, val) in map {
            // Build flattened key
            if base_len > 0 {
                prefix.push('.');
            }
            prefix.push_str(key);

            let flat_key: Arc<str> = Arc::from(prefix.as_str());

            match val {
                Value::Object(_) => {
                    // Nested object - recurse
                    let mut children = Vec::new();
                    Self::build_mappings(val, prefix, &mut children, field_count);

                    mappings.push(KeyMapping {
                        field_name: CompactString::from(key.as_str()),
                        flat_key,
                        is_leaf: false,
                        is_array: false,
                        children,
                    });
                }
                Value::Array(_) => {
                    // Leaf: array
                    mappings.push(KeyMapping {
                        field_name: CompactString::from(key.as_str()),
                        flat_key,
                        is_leaf: true,
                        is_array: true,
                        children: Vec::new(),
                    });
                    *field_count += 1;
                }
                _ => {
                    // Leaf: scalar
                    mappings.push(KeyMapping {
                        field_name: CompactString::from(key.as_str()),
                        flat_key,
                        is_leaf: true,
                        is_array: false,
                        children: Vec::new(),
                    });
                    *field_count += 1;
                }
            }

            // Restore prefix
            prefix.truncate(base_len);
        }
    }

    /// Flatten a value using pre-computed key mappings.
    ///
    /// Uses a single-pass traversal with cached keys.
    #[inline]
    pub fn flatten_fast(&self, value: Value) -> Map<String, Value> {
        if !self.uniform {
            return flatten_value_owned(value);
        }

        let mut result = Map::with_capacity(self.field_count);

        let Value::Object(root) = value else {
            return result;
        };

        Self::extract_with_mappings(root, &self.mappings, &mut result);
        result
    }

    /// Extract values using key mappings (single-pass traversal).
    fn extract_with_mappings(
        mut obj: Map<String, Value>,
        mappings: &[KeyMapping],
        result: &mut Map<String, Value>,
    ) {
        for mapping in mappings {
            // Try to remove the value from the object
            if let Some(val) = obj.remove(mapping.field_name.as_str()) {
                if mapping.is_leaf {
                    // Leaf value - insert with cached key
                    let key = mapping.flat_key.to_string();
                    if mapping.is_array {
                        result.insert(
                            key,
                            Value::String(serde_json::to_string(&val).unwrap_or_default()),
                        );
                    } else {
                        result.insert(key, val);
                    }
                } else {
                    // Nested object - recurse
                    if let Value::Object(nested) = val {
                        Self::extract_with_mappings(nested, &mapping.children, result);
                    }
                }
            }
        }
    }

    /// Flatten a batch of values efficiently.
    pub fn flatten_batch(&self, values: Vec<Value>) -> Vec<Map<String, Value>> {
        let mut results = Vec::with_capacity(values.len());
        for value in values {
            results.push(self.flatten_fast(value));
        }
        results
    }

    /// Flatten a batch of values in parallel (for large batches).
    #[cfg(feature = "parallel")]
    pub fn flatten_batch_parallel(&self, values: Vec<Value>) -> Vec<Map<String, Value>> {
        use rayon::prelude::*;
        values
            .into_par_iter()
            .map(|v| self.flatten_fast(v))
            .collect()
    }

    /// Check if this flattener has a valid schema.
    pub fn is_uniform(&self) -> bool {
        self.uniform
    }

    /// Get the number of fields in the schema.
    pub fn field_count(&self) -> usize {
        self.field_count
    }
}

/// Statistics for batch flattening operations
#[derive(Debug, Clone, Default)]
pub struct BatchFlattenStats {
    /// Messages processed via fast path
    pub fast_path_count: usize,
    /// Messages that fell back to regular flattening
    pub fallback_count: usize,
    /// Total fields extracted
    pub total_fields: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten_simple() {
        let input = br#"{"key": "value"}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn test_flatten_nested() {
        let input = br#"{"tags": {"category": "auth", "level": "info"}}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["tags.category"], "auth");
        assert_eq!(parsed["tags.level"], "info");
    }

    #[test]
    fn test_flatten_deeply_nested() {
        let input = br#"{"a": {"b": {"c": "deep"}}}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["a.b.c"], "deep");
    }

    #[test]
    fn test_flatten_array() {
        let input = br#"{"items": [1, 2, 3]}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        // Arrays become JSON strings
        assert_eq!(parsed["items"], "[1,2,3]");
    }

    #[test]
    fn test_flatten_mixed() {
        let input = br#"{"event": "login", "user": {"id": 123, "name": "test"}}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["event"], "login");
        assert_eq!(parsed["user.id"], 123);
        assert_eq!(parsed["user.name"], "test");
    }

    #[test]
    fn test_flatten_preserves_types() {
        let input = br#"{"str": "text", "num": 42, "bool": true, "null": null}"#;
        let result = flatten(input).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["str"], "text");
        assert_eq!(parsed["num"], 42);
        assert_eq!(parsed["bool"], true);
        assert_eq!(parsed["null"], Value::Null);
    }

    // ========================================================================
    // BatchFlattener tests
    // ========================================================================

    #[test]
    fn test_batch_flattener_creation() {
        use serde_json::json;

        let sample = json!({
            "event": "login",
            "user": {"id": 123, "name": "test"}
        });

        let flattener = BatchFlattener::from_sample(&sample);

        assert!(flattener.is_uniform());
        assert_eq!(flattener.field_count(), 3); // event, user.id, user.name
    }

    #[test]
    fn test_batch_flattener_simple() {
        use serde_json::json;

        let sample = json!({"key": "value1"});
        let flattener = BatchFlattener::from_sample(&sample);

        let result = flattener.flatten_fast(json!({"key": "value2"}));

        assert_eq!(result.len(), 1);
        assert_eq!(result.get("key").unwrap(), "value2");
    }

    #[test]
    fn test_batch_flattener_nested() {
        use serde_json::json;

        let sample = json!({
            "event": "login",
            "user": {"id": 123, "name": "alice"}
        });
        let flattener = BatchFlattener::from_sample(&sample);

        let result = flattener.flatten_fast(json!({
            "event": "logout",
            "user": {"id": 456, "name": "bob"}
        }));

        assert_eq!(result.len(), 3);
        assert_eq!(result.get("event").unwrap(), "logout");
        assert_eq!(result.get("user.id").unwrap(), 456);
        assert_eq!(result.get("user.name").unwrap(), "bob");
    }

    #[test]
    fn test_batch_flattener_with_arrays() {
        use serde_json::json;

        let sample = json!({
            "name": "test",
            "items": [1, 2, 3]
        });
        let flattener = BatchFlattener::from_sample(&sample);

        let result = flattener.flatten_fast(json!({
            "name": "prod",
            "items": [4, 5, 6]
        }));

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("name").unwrap(), "prod");
        assert_eq!(result.get("items").unwrap(), "[4,5,6]");
    }

    #[test]
    fn test_batch_flattener_batch() {
        use serde_json::json;

        let sample = json!({"id": 0, "value": "a"});
        let flattener = BatchFlattener::from_sample(&sample);

        let batch: Vec<Value> = (0..5)
            .map(|i| json!({"id": i, "value": format!("v{}", i)}))
            .collect();

        let results = flattener.flatten_batch(batch);

        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.get("id").unwrap(), i);
            assert_eq!(result.get("value").unwrap(), &format!("v{}", i));
        }
    }

    #[test]
    fn test_batch_flattener_deep_nesting() {
        use serde_json::json;

        let sample = json!({
            "a": {"b": {"c": {"d": "deep"}}}
        });
        let flattener = BatchFlattener::from_sample(&sample);

        let result = flattener.flatten_fast(json!({
            "a": {"b": {"c": {"d": "value"}}}
        }));

        assert_eq!(result.len(), 1);
        assert_eq!(result.get("a.b.c.d").unwrap(), "value");
    }

    #[test]
    fn test_batch_flattener_default_fallback() {
        use serde_json::json;

        // Default flattener has no schema, should fall back
        let flattener = BatchFlattener::default();
        assert!(!flattener.is_uniform());

        let result = flattener.flatten_fast(json!({"key": "value"}));
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("key").unwrap(), "value");
    }
}
