//! JSON flattening (nested objects → dot notation)
//!
//! Converts nested JSON objects like:
//!   {"tags": {"category": "auth"}}
//! To flattened form:
//!   {"tags.category": "auth"}
//!
//! ## Performance
//!
//! Prefer `flatten_value_owned()` when you own the Value to avoid cloning.

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
}
