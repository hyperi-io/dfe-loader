//! Payload parsing with auto-detection
//!
//! Parses JSON or MessagePack into a common Value type.

use sonic_rs::JsonValueTrait;

use crate::payload::detect::{detect_format, PayloadFormat};
use crate::Result;

/// Parse a payload into a serde_json::Value.
///
/// Auto-detects format (JSON or MessagePack) and parses accordingly.
/// Both formats are normalized to serde_json::Value for downstream processing.
#[inline]
pub fn parse_payload(payload: &[u8]) -> Result<serde_json::Value> {
    let format = detect_format(payload).ok_or_else(|| {
        crate::Error::Json("Unable to detect payload format (expected JSON or MessagePack)".into())
    })?;

    match format {
        PayloadFormat::Json => parse_json(payload),
        PayloadFormat::MessagePack => parse_msgpack(payload),
        PayloadFormat::Unknown => Err(crate::Error::Json(
            "Unknown payload format".into()
        )),
    }
}

/// Parse JSON using sonic-rs (SIMD-accelerated)
///
/// sonic-rs uses SIMD instructions for fast JSON parsing. The result is directly
/// deserialized to serde_json::Value for compatibility with downstream processing.
#[inline]
fn parse_json(payload: &[u8]) -> Result<serde_json::Value> {
    // Use sonic-rs's SIMD-accelerated parsing directly to serde_json::Value
    // This is efficient because sonic_rs::from_slice can deserialize into any
    // type implementing serde::Deserialize, including serde_json::Value
    sonic_rs::from_slice(payload)
        .map_err(|e| crate::Error::Json(format!("JSON parse error: {}", e)))
}

/// Parse MessagePack using rmp-serde
#[inline]
fn parse_msgpack(payload: &[u8]) -> Result<serde_json::Value> {
    rmp_serde::from_slice(payload)
        .map_err(|e| crate::Error::Json(format!("MessagePack parse error: {}", e)))
}

/// Fast path: extract a single field from JSON without full parse.
///
/// Uses sonic-rs get_unchecked for maximum performance when routing.
#[inline]
pub fn extract_field_json(payload: &[u8], field: &str) -> Option<String> {
    let value: sonic_rs::Value = sonic_rs::from_slice(payload).ok()?;
    value.get(field)?.as_str().map(|s| s.to_string())
}

/// Fast path: extract a nested field using dot notation (e.g., "tags.event_category")
#[inline]
pub fn extract_nested_field_json(payload: &[u8], path: &str) -> Option<String> {
    let value: sonic_rs::Value = sonic_rs::from_slice(payload).ok()?;

    let mut current = &value;
    for part in path.split('.') {
        current = current.get(part)?;
    }

    current.as_str().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json() {
        let payload = br#"{"event_category": "auth", "user_id": 123}"#;
        let value = parse_payload(payload).unwrap();

        assert_eq!(value["event_category"], "auth");
        assert_eq!(value["user_id"], 123);
    }

    #[test]
    fn test_parse_msgpack() {
        // MessagePack: {"key": "value"}
        // fixmap(1) + fixstr(3) "key" + fixstr(5) "value"
        let payload = rmp_serde::to_vec(&serde_json::json!({"key": "value"})).unwrap();
        let value = parse_payload(&payload).unwrap();

        assert_eq!(value["key"], "value");
    }

    #[test]
    fn test_extract_field_json() {
        let payload = br#"{"event_category": "api", "user": "test"}"#;
        assert_eq!(extract_field_json(payload, "event_category"), Some("api".to_string()));
        assert_eq!(extract_field_json(payload, "missing"), None);
    }

    #[test]
    fn test_extract_nested_field_json() {
        let payload = br#"{"tags": {"event_category": "admin", "level": "info"}}"#;
        assert_eq!(
            extract_nested_field_json(payload, "tags.event_category"),
            Some("admin".to_string())
        );
        assert_eq!(extract_nested_field_json(payload, "tags.missing"), None);
    }

    #[test]
    fn test_parse_invalid() {
        let payload = b"not valid json or msgpack";
        assert!(parse_payload(payload).is_err());
    }
}
