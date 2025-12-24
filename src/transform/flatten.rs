//! JSON flattening (nested objects → dot notation)
//!
//! Converts nested JSON objects like:
//!   {"tags": {"category": "auth"}}
//! To flattened form:
//!   {"tags.category": "auth"}

use serde_json::{Map, Value};

/// Flatten a nested JSON value into a flat map with dot notation keys.
///
/// Arrays are converted to JSON string representation.
pub fn flatten_value(value: &Value) -> Map<String, Value> {
    let mut result = Map::new();
    flatten_recursive(value, String::new(), &mut result);
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
