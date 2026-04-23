// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Payload parsing with auto-detection
//!
//! Parses JSON or `MessagePack` into a common Value type.
//!
//! ## On-Demand Field Access
//!
//! For routing (extracting 1-2 fields), use the on-demand `get_*` functions
//! which are 4-8x faster than full DOM parsing. These use sonic-rs's SIMD-
//! accelerated path navigation without building a full value tree.
//!
//! For full document processing (flattening, transformation), use `parse_payload`
//! which does a full DOM parse.

use sonic_rs::{JsonValueTrait, LazyValue, get_from_slice};

use crate::Result;
use crate::payload::{PayloadFormat, detect_format};

/// Parse a payload into a `serde_json::Value`.
///
/// Auto-detects format (JSON or `MessagePack`) and parses accordingly.
/// Both formats are normalized to `serde_json::Value` for downstream processing.
#[inline]
pub fn parse_payload(payload: &[u8]) -> Result<serde_json::Value> {
    let format = detect_format(payload).ok_or_else(|| {
        crate::Error::Json("Unable to detect payload format (expected JSON or MessagePack)".into())
    })?;

    match format {
        PayloadFormat::Json => parse_json(payload),
        PayloadFormat::MessagePack => parse_msgpack(payload),
        PayloadFormat::Unknown => Err(crate::Error::Json("Unknown payload format".into())),
    }
}

/// Parse JSON using sonic-rs (SIMD-accelerated)
///
/// sonic-rs uses SIMD instructions for fast JSON parsing. The result is directly
/// deserialized to `serde_json::Value` for compatibility with downstream processing.
#[inline]
fn parse_json(payload: &[u8]) -> Result<serde_json::Value> {
    // Use sonic-rs's SIMD-accelerated parsing directly to serde_json::Value
    // This is efficient because sonic_rs::from_slice can deserialize into any
    // type implementing serde::Deserialize, including serde_json::Value
    sonic_rs::from_slice(payload).map_err(|e| crate::Error::Json(format!("JSON parse error: {e}")))
}

/// Parse `MessagePack` using rmp-serde
#[inline]
fn parse_msgpack(payload: &[u8]) -> Result<serde_json::Value> {
    rmp_serde::from_slice(payload)
        .map_err(|e| crate::Error::Json(format!("MessagePack parse error: {e}")))
}

/// On-demand field extraction from JSON without full DOM parse.
///
/// Uses sonic-rs's SIMD-accelerated path navigation to extract a single field.
/// This is 4-8x faster than full DOM parsing for 1-2 field extraction.
///
/// # Arguments
/// * `payload` - Raw JSON bytes
/// * `field` - Top-level field name to extract
///
/// # Returns
/// The field value as a string, or None if not found or not a string.
#[inline]
pub fn extract_field_json(payload: &[u8], field: &str) -> Option<String> {
    // Use on-demand get with single-element path slice
    // This navigates directly to the field without building a full DOM tree
    let lazy: LazyValue = get_from_slice(payload, &[field]).ok()?;

    // Extract string value from lazy wrapper
    lazy.as_str().map(std::string::ToString::to_string)
}

/// On-demand nested field extraction using dot notation (e.g., "`tags.event_category`")
///
/// Uses sonic-rs's SIMD-accelerated path navigation without building full DOM.
/// This is 4-7x faster than full DOM parsing for deeply nested field extraction.
///
/// # Arguments
/// * `payload` - Raw JSON bytes
/// * `path` - Dot-separated path (e.g., "tags.event.category")
///
/// # Returns
/// The field value as a string, or None if not found or not a string.
#[inline]
pub fn extract_nested_field_json(payload: &[u8], path: &str) -> Option<String> {
    // Build path from dot notation - sonic-rs accepts any IntoIterator<Item: Index>
    let parts: Vec<&str> = path.split('.').collect();

    if parts.is_empty() {
        return None;
    }

    // Use sonic-rs on-demand get with the path slice
    // This navigates directly to the nested field without building a full DOM tree
    let lazy: LazyValue = get_from_slice(payload, &parts).ok()?;

    lazy.as_str().map(std::string::ToString::to_string)
}

/// Zero-copy field extraction from JSON - returns Cow for true zero-copy.
///
/// For non-escaped strings (the common case), returns `Cow::Borrowed` pointing
/// directly into the payload bytes. For escaped strings, returns `Cow::Owned`
/// with the unescaped value.
///
/// This is 4-8x faster than full DOM parsing for 1-2 field extraction.
///
/// # Arguments
/// * `payload` - Raw JSON bytes
/// * `field` - Top-level field name to extract
///
/// # Returns
/// `Cow<str>` - Borrowed for non-escaped, Owned for escaped strings.
/// None if field not found or not a string.
#[inline]
pub fn extract_field_json_cow<'a>(
    payload: &'a [u8],
    field: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    use std::borrow::Cow;

    let lazy: LazyValue = get_from_slice(payload, &[field]).ok()?;

    if !lazy.is_str() {
        return None;
    }

    // Get raw JSON text as Cow with proper lifetime
    let raw_cow = lazy.as_raw_cow();

    // Strip quotes from the raw JSON string (e.g., "\"value\"" -> "value")
    match raw_cow {
        Cow::Borrowed(s) if s.len() >= 2 => {
            // Check if the string contains escape sequences
            let inner = &s[1..s.len() - 1];
            if inner.contains('\\') {
                // Has escapes - need to parse. Use as_str() which handles unescaping.
                lazy.as_str().map(|s| Cow::Owned(s.to_string()))
            } else {
                // No escapes - true zero-copy borrow from payload
                Some(Cow::Borrowed(inner))
            }
        }
        Cow::Owned(s) if s.len() >= 2 => {
            // Owned case (from FastStr) - strip quotes and check escapes
            let inner = &s[1..s.len() - 1];
            if inner.contains('\\') {
                lazy.as_str().map(|s| Cow::Owned(s.to_string()))
            } else {
                Some(Cow::Owned(inner.to_string()))
            }
        }
        _ => None,
    }
}

/// Zero-copy nested field extraction using dot notation.
///
/// For non-escaped strings, returns `Cow::Borrowed` pointing into payload.
/// For escaped strings, returns `Cow::Owned` with unescaped value.
///
/// # Arguments
/// * `payload` - Raw JSON bytes
/// * `path` - Dot-separated path (e.g., "tags.event.category")
///
/// # Returns
/// `Cow<str>` - Borrowed for non-escaped, Owned for escaped strings.
#[inline]
pub fn extract_nested_field_json_cow<'a>(
    payload: &'a [u8],
    path: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    use std::borrow::Cow;

    let parts: Vec<&str> = path.split('.').collect();

    if parts.is_empty() {
        return None;
    }

    let lazy: LazyValue = get_from_slice(payload, &parts).ok()?;

    if !lazy.is_str() {
        return None;
    }

    let raw_cow = lazy.as_raw_cow();

    match raw_cow {
        Cow::Borrowed(s) if s.len() >= 2 => {
            let inner = &s[1..s.len() - 1];
            if inner.contains('\\') {
                lazy.as_str().map(|s| Cow::Owned(s.to_string()))
            } else {
                Some(Cow::Borrowed(inner))
            }
        }
        Cow::Owned(s) if s.len() >= 2 => {
            let inner = &s[1..s.len() - 1];
            if inner.contains('\\') {
                lazy.as_str().map(|s| Cow::Owned(s.to_string()))
            } else {
                Some(Cow::Owned(inner.to_string()))
            }
        }
        _ => None,
    }
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
        assert_eq!(
            extract_field_json(payload, "event_category"),
            Some("api".to_string())
        );
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

    #[test]
    fn test_extract_field_json_cow() {
        use std::borrow::Cow;

        let payload = br#"{"event_category": "api", "user": "test"}"#;

        let result = extract_field_json_cow(payload, "event_category");
        assert_eq!(result.as_deref(), Some("api"));
        // Verify it's actually borrowed (zero-copy)
        assert!(matches!(result, Some(Cow::Borrowed(_))));

        let result = extract_field_json_cow(payload, "user");
        assert_eq!(result.as_deref(), Some("test"));
        assert!(matches!(result, Some(Cow::Borrowed(_))));

        assert_eq!(extract_field_json_cow(payload, "missing"), None);
    }

    #[test]
    fn test_extract_nested_field_json_cow() {
        use std::borrow::Cow;

        let payload = br#"{"tags": {"event_category": "admin", "level": "info"}}"#;

        let result = extract_nested_field_json_cow(payload, "tags.event_category");
        assert_eq!(result.as_deref(), Some("admin"));
        assert!(matches!(result, Some(Cow::Borrowed(_))));

        let result = extract_nested_field_json_cow(payload, "tags.level");
        assert_eq!(result.as_deref(), Some("info"));
        assert!(matches!(result, Some(Cow::Borrowed(_))));

        assert_eq!(extract_nested_field_json_cow(payload, "tags.missing"), None);
    }

    #[test]
    fn test_zero_copy_matches_allocating() {
        let payload = br#"{"org_id": "acme", "tags": {"env": "prod"}}"#;

        // Verify zero-copy and allocating versions return same content
        assert_eq!(
            extract_field_json_cow(payload, "org_id").as_deref(),
            extract_field_json(payload, "org_id").as_deref()
        );
        assert_eq!(
            extract_nested_field_json_cow(payload, "tags.env").as_deref(),
            extract_nested_field_json(payload, "tags.env").as_deref()
        );
    }

    #[test]
    fn test_escaped_strings_handled() {
        use std::borrow::Cow;

        // String with escape sequences - should be Owned
        let payload = br#"{"msg": "hello\nworld", "plain": "simple"}"#;

        let result = extract_field_json_cow(payload, "msg");
        assert_eq!(result.as_deref(), Some("hello\nworld"));
        // Escaped string should be Owned (allocation required for unescaping)
        assert!(matches!(result, Some(Cow::Owned(_))));

        let result = extract_field_json_cow(payload, "plain");
        assert_eq!(result.as_deref(), Some("simple"));
        // Non-escaped should be Borrowed (zero-copy)
        assert!(matches!(result, Some(Cow::Borrowed(_))));
    }

    #[test]
    fn test_parse_invalid_json_returns_json_error() {
        let payload = b"{not valid";
        let err = parse_payload(payload).unwrap_err();
        match err {
            crate::Error::Json(_) => {}
            other => panic!("Expected Json error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_msgpack_invalid_bytes_errors() {
        // Truncated MessagePack — will fail to parse
        let payload = &[0x81u8, 0xa3]; // fixmap(1) + fixstr(3) + no content
        let result = parse_payload(payload);
        // May succeed if detector doesn't recognize as MessagePack — then fails as JSON
        // Or fails as invalid MessagePack — either way, Err
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_payload_errors() {
        let result = parse_payload(b"");
        assert!(
            result.is_err(),
            "empty payload should fail format detection"
        );
    }

    #[test]
    fn test_extract_field_json_non_string_field_returns_none() {
        // The field exists but is not a string
        let payload = br#"{"num": 42, "arr": [1, 2], "obj": {}}"#;
        assert_eq!(extract_field_json(payload, "num"), None);
        assert_eq!(extract_field_json(payload, "arr"), None);
        assert_eq!(extract_field_json(payload, "obj"), None);
    }

    #[test]
    fn test_extract_field_json_cow_non_string_returns_none() {
        // Non-string values return None
        let payload = br#"{"num": 42, "bool_field": true, "null_field": null}"#;
        assert!(extract_field_json_cow(payload, "num").is_none());
        assert!(extract_field_json_cow(payload, "bool_field").is_none());
        assert!(extract_field_json_cow(payload, "null_field").is_none());
    }

    #[test]
    fn test_extract_field_json_cow_missing_returns_none() {
        let payload = br#"{"a": "x"}"#;
        assert!(extract_field_json_cow(payload, "missing").is_none());
    }

    #[test]
    fn test_extract_nested_field_json_cow_missing_returns_none() {
        let payload = br#"{"a": {"b": "x"}}"#;
        assert!(extract_nested_field_json_cow(payload, "a.missing").is_none());
        assert!(extract_nested_field_json_cow(payload, "x.y").is_none());
    }

    #[test]
    fn test_extract_nested_field_json_escapes() {
        use std::borrow::Cow;
        let payload = br#"{"outer": {"msg": "tab\there", "plain": "normal"}}"#;

        let result = extract_nested_field_json_cow(payload, "outer.msg");
        assert_eq!(result.as_deref(), Some("tab\there"));
        assert!(matches!(result, Some(Cow::Owned(_))));

        let result = extract_nested_field_json_cow(payload, "outer.plain");
        assert_eq!(result.as_deref(), Some("normal"));
    }

    #[test]
    fn test_extract_field_json_empty_string_value() {
        // Empty string "" — raw is "\"\"" (2 chars), .len() >= 2 but inner is ""
        let payload = br#"{"empty": ""}"#;
        let result = extract_field_json_cow(payload, "empty");
        // Cow::Borrowed("") inner
        assert_eq!(result.as_deref(), Some(""));
    }

    #[test]
    fn test_extract_deeply_nested_path() {
        let payload = br#"{"a": {"b": {"c": {"d": "deep"}}}}"#;
        let result = extract_nested_field_json(payload, "a.b.c.d");
        assert_eq!(result, Some("deep".to_string()));
    }

    #[test]
    fn test_extract_deeply_nested_path_cow() {
        use std::borrow::Cow;
        let payload = br#"{"a": {"b": {"c": {"d": "deep"}}}}"#;
        let result = extract_nested_field_json_cow(payload, "a.b.c.d");
        assert_eq!(result.as_deref(), Some("deep"));
        // Non-escaped → borrowed
        assert!(matches!(result, Some(Cow::Borrowed(_))));
    }

    #[test]
    fn test_extract_field_with_unicode_value() {
        let payload = r#"{"city": "Zürich"}"#.as_bytes();
        let result = extract_field_json(payload, "city");
        assert_eq!(result, Some("Zürich".to_string()));
    }

    #[test]
    fn test_parse_msgpack_valid_nested() {
        let original = serde_json::json!({
            "outer": {
                "inner": {
                    "deep": 42
                }
            }
        });
        let payload = rmp_serde::to_vec(&original).unwrap();
        let value = parse_payload(&payload).unwrap();
        assert_eq!(value["outer"]["inner"]["deep"], 42);
    }

    #[test]
    fn test_parse_json_array_at_top_level() {
        // Top-level array
        let payload = br"[1, 2, 3]";
        let value = parse_payload(payload).unwrap();
        assert!(value.is_array());
        assert_eq!(value.as_array().unwrap().len(), 3);
    }
}
