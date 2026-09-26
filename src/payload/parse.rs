// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! JSON payload parsing.
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
use crate::payload::LeadingBytes;
use crate::payload::depth::{MAX_BATCH_DEPTH, json_depth_within};

/// Parse a JSON payload into a `serde_json::Value`.
///
/// # Errors
///
/// [`crate::Error::NotJson`] when the payload does not open a JSON object or
/// array, [`crate::Error::Json`] when it does but does not parse.
#[inline]
pub fn parse_payload(payload: &[u8]) -> Result<serde_json::Value> {
    if !opens_json_document(payload) {
        return Err(crate::Error::NotJson {
            leading: LeadingBytes::of(payload),
        });
    }
    parse_json(payload)
}

/// True when the first non-whitespace byte opens a JSON object or array.
///
/// The gate every record passes before a parse, so bytes in any other
/// encoding are refused by name rather than reported as a parse error.
#[inline]
#[must_use]
pub fn opens_json_document(payload: &[u8]) -> bool {
    payload
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| matches!(b, b'{' | b'['))
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

/// True when the first non-whitespace byte opens a JSON array.
///
/// One byte on the hot path, so a message that is not batched pays nothing.
#[inline]
pub fn opens_json_array(payload: &[u8]) -> bool {
    payload
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'[')
}

/// Split a batch of records carried as a top-level JSON array into the raw
/// bytes of each element.
///
/// Every stage below this takes one message to be one record, so an array
/// reaching the encoder is handed whole to a per-row JSON column (#128). Only
/// a non-empty array of objects is a batch of records: a scalar array, an
/// empty one, or a body that does not parse is left untouched, so the format
/// check and the DLQ still see exactly what arrived.
pub fn split_json_array(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
    if !opens_json_array(payload) {
        return None;
    }
    // The element iterator stops at the closing bracket without reading the
    // tail, so trailing bytes are rejected here rather than fanned out.
    if payload.iter().rfind(|b| !b.is_ascii_whitespace()) != Some(&b']') {
        return None;
    }
    // Too deep to split stays whole, and the depth check after the split dead-letters it.
    if !json_depth_within(payload, MAX_BATCH_DEPTH) {
        return None;
    }

    let mut elements: Vec<Vec<u8>> = Vec::new();
    for item in sonic_rs::to_array_iter(payload) {
        let raw = item.ok()?;
        let bytes = raw.as_raw_str().as_bytes();
        if bytes.first() != Some(&b'{') {
            return None;
        }
        elements.push(bytes.to_vec());
    }

    if elements.is_empty() {
        return None;
    }
    Some(elements)
}

/// True when the payload carries a record boundary -- a closing brace meeting
/// the next record's opening brace with only whitespace between.
///
/// Inside one JSON value a closing brace is always followed by a comma, a
/// bracket or the end, so the shape occurs only between concatenated records or
/// inside a string; [`split_ndjson`] decides which.
#[must_use]
pub fn has_ndjson_boundary(payload: &[u8]) -> bool {
    // A raw newline is illegal inside a JSON string, so a compact single record
    // carries none and stops here without the scan below.
    if memchr::memchr(b'\n', payload).is_none() {
        return false;
    }

    let mut closed = false;
    for &b in payload {
        match b {
            b'}' => closed = true,
            b'{' if closed => return true,
            b if b.is_ascii_whitespace() => {}
            _ => closed = false,
        }
    }
    false
}

/// Split a batch of records carried as newline-separated JSON objects into the
/// raw bytes of each record.
///
/// dfe-transform-elastic batches its output this way and every stage below the
/// split takes one message to be one record, so a single-value parser meets the
/// second object and the whole message is lost (#184). Only a body that is two
/// or more whole JSON objects and nothing else is a batch: one object, a
/// truncated tail, or an element that is not an object is left untouched so the
/// format check and the DLQ see exactly what arrived.
pub fn split_ndjson(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
    if !has_ndjson_boundary(payload) {
        return None;
    }

    let mut elements: Vec<Vec<u8>> = Vec::new();
    let mut consumed = 0usize;
    // IgnoredAny skips each record without building a DOM; the raw bytes come
    // from the stream's offset, so nothing is parsed twice.
    let mut stream =
        serde_json::Deserializer::from_slice(payload).into_iter::<serde::de::IgnoredAny>();
    while let Some(item) = stream.next() {
        item.ok()?;
        let end = stream.byte_offset();
        let record = payload.get(consumed..end)?.trim_ascii();
        if record.first() != Some(&b'{') {
            return None;
        }
        elements.push(record.to_vec());
        consumed = end;
    }

    // Bytes the stream stopped short of are a truncated record, not a batch.
    if payload[consumed..].iter().any(|b| !b.is_ascii_whitespace()) {
        return None;
    }
    (elements.len() > 1).then_some(elements)
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
        let payload = b"not valid json";
        assert!(parse_payload(payload).is_err());
    }

    #[test]
    fn only_an_object_or_array_opens_a_json_document() {
        assert!(opens_json_document(br#"{"a": 1}"#));
        assert!(opens_json_document(b"[1]"));
        assert!(
            opens_json_document(b"  \n\t{\"a\": 1}"),
            "leading whitespace does not hide the object"
        );
        for refused in [
            b"".as_slice(),
            b"   \n\t  ",
            b"42",
            br#""text""#,
            b"null",
            b"\x00\x00\x02",
        ] {
            assert!(
                !opens_json_document(refused),
                "{refused:?} must not pass the JSON gate"
            );
        }
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
    fn test_parse_binary_bytes_is_not_json() {
        let err = parse_payload(b"\x00\x00\x02\x00").unwrap_err();
        assert!(matches!(err, crate::Error::NotJson { .. }), "got {err:?}");
        assert!(
            err.to_string()
                .contains("payload is not JSON, leading bytes 00 00 02 00"),
            "got: {err}"
        );
    }

    #[test]
    fn test_parse_empty_payload_errors() {
        let err = parse_payload(b"").unwrap_err();
        assert!(
            matches!(err, crate::Error::NotJson { .. }),
            "empty payload should fail the JSON gate, got {err:?}"
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
    fn test_parse_json_array_at_top_level() {
        // Top-level array
        let payload = br"[1, 2, 3]";
        let value = parse_payload(payload).unwrap();
        assert!(value.is_array());
        assert_eq!(value.as_array().unwrap().len(), 3);
    }

    // ---- batched-array split (#128) ----

    #[test]
    fn a_batch_array_splits_into_one_payload_per_element() {
        let payload = br#"[{"a": 1}, {"b": [2, 3]}, {"c": {"d": "e"}}]"#;
        let elements = split_json_array(payload).expect("an array of objects is a batch");

        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], br#"{"a": 1}"#.to_vec());
        assert_eq!(elements[1], br#"{"b": [2, 3]}"#.to_vec());
        assert_eq!(elements[2], br#"{"c": {"d": "e"}}"#.to_vec());
        for element in &elements {
            let value = parse_payload(element).expect("each element parses on its own");
            assert!(value.is_object(), "each element is a record, not an array");
        }
    }

    #[test]
    fn leading_whitespace_and_one_element_still_split() {
        let payload = b"  \n\t[{\"a\": 1}]  \n";
        let elements = split_json_array(payload).expect("whitespace does not hide the array");
        assert_eq!(elements, vec![br#"{"a": 1}"#.to_vec()]);
    }

    #[test]
    fn an_object_payload_is_not_a_batch() {
        assert!(split_json_array(br#"{"a": 1}"#).is_none());
        assert!(!opens_json_array(br#"{"a": 1}"#));
    }

    #[test]
    fn a_scalar_array_is_left_for_the_existing_path() {
        // Not a batch of records: splitting it would turn one DLQ entry into
        // three, so it goes through untouched.
        assert!(split_json_array(br"[1, 2, 3]").is_none());
        assert!(split_json_array(br#"["a", "b"]"#).is_none());
        assert!(split_json_array(br#"[{"a": 1}, 2]"#).is_none());
    }

    #[test]
    fn an_empty_array_is_left_for_the_existing_path() {
        // Splitting it to nothing would drop the message with no DLQ entry.
        assert!(split_json_array(br"[]").is_none());
        assert!(split_json_array(br"  [ ]  ").is_none());
    }

    #[test]
    fn a_truncated_or_trailing_body_is_left_untouched() {
        assert!(
            split_json_array(br#"[{"a": 1}, {"b":"#).is_none(),
            "a truncated array must still reach the format check and the DLQ"
        );
        assert!(
            split_json_array(br#"[{"a": 1}] junk"#).is_none(),
            "the element iterator stops at the bracket, so the tail is checked"
        );
    }

    // ---- newline-separated records (#184) ----

    /// The shape dfe-transform-elastic put on `cisco-ios_load`: whole ECS
    /// records, one per line, in one Kafka message.
    const NDJSON_BATCH: &[u8] =
        br#"{"event":{"code":"IPACCESSLOGRP"},"source":{"ip":"192.168.100.197"}}
{"event":{"code":"IPACCESSLOGP"},"source":{"ip":"192.168.100.198"}}
{"event":{"code":"IPACCESSLOGDP"},"source":{"ip":"192.168.100.199"}}
"#;

    #[test]
    fn newline_separated_records_split_into_one_payload_each() {
        let elements = split_ndjson(NDJSON_BATCH).expect("three records is a batch");

        assert_eq!(
            elements.len(),
            3,
            "every record, not the first and not zero"
        );
        for element in &elements {
            let value = parse_payload(element).expect("each record parses on its own");
            assert!(
                value.is_object(),
                "each element is a record, not a fragment"
            );
        }
        assert_eq!(
            parse_payload(&elements[2]).expect("third record")["event"]["code"],
            "IPACCESSLOGDP",
            "the last record survives the split as itself"
        );
    }

    #[test]
    fn the_whole_batch_is_what_a_single_value_parser_rejects() {
        // The live symptom: sonic-rs reads the first object and calls the second
        // one trailing characters, so all three records are lost at once.
        let err = parse_payload(NDJSON_BATCH)
            .expect_err("a single-value parser cannot read a batch")
            .to_string();
        assert!(
            err.contains("trailing characters"),
            "the split is what stops this reaching the parser, got: {err}"
        );
    }

    #[test]
    fn separator_whitespace_is_not_carried_into_a_record() {
        let elements = split_ndjson(b"  {\"a\": 1}\r\n\r\n  {\"b\": 2}  \n").expect("a batch");
        assert_eq!(
            elements,
            vec![br#"{"a": 1}"#.to_vec(), br#"{"b": 2}"#.to_vec()],
            "_raw and _json capture the record, never the separators around it"
        );
    }

    #[test]
    fn a_single_record_is_not_a_batch() {
        assert!(split_ndjson(br#"{"a": 1}"#).is_none());
        assert!(
            split_ndjson(b"{\n  \"a\": 1\n}\n").is_none(),
            "a pretty-printed record is one record"
        );
        assert!(!has_ndjson_boundary(br#"{"a": 1}"#));
    }

    #[test]
    fn a_batched_array_stays_with_the_array_split() {
        // Between array elements a closing brace meets a comma, so the array
        // never reads as a record boundary and keeps its own counters.
        assert!(!has_ndjson_boundary(b"[{\"a\": 1},\n{\"b\": 2}]"));
        assert!(split_ndjson(b"[{\"a\": 1},\n{\"b\": 2}]").is_none());
    }

    #[test]
    fn a_truncated_or_non_object_batch_is_left_untouched() {
        assert!(
            split_ndjson(b"{\"a\": 1}\n{\"b\":").is_none(),
            "a truncated tail must still reach the format check and the DLQ"
        );
        assert!(
            split_ndjson(b"{\"a\": 1}\n{\"b\": 2}\njunk").is_none(),
            "a body the stream stops short of is not a batch of records"
        );
        assert!(
            split_ndjson(b"{\"a\": 1}\n[2]\n{\"b\": 3}").is_none(),
            "splitting a mixed body would turn one DLQ entry into three"
        );
    }

    #[test]
    fn a_brace_pair_inside_a_string_is_not_a_batch() {
        // The scan is string-unaware, so the stream parse is what decides.
        let payload = b"{\"msg\": \"}\n{\"}";
        assert!(has_ndjson_boundary(payload), "the cheap scan cannot tell");
        assert!(
            split_ndjson(payload).is_none(),
            "one value parsed means one record"
        );
    }
}
