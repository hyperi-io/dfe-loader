// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Common-header keys a record must carry at most once.
//!
//! JSON readers disagree on which of two repeated keys wins, and ClickHouse
//! refuses the row under `full` capture. A record naming its tenant or source
//! twice is refused before any stage reads it, so no reader gets to pick.

use std::sync::LazyLock;

use memchr::memmem::Finder;

/// The header keys a record may carry once, as they decode.
const HEADER_KEYS: [&str; 2] = ["_org_id", "_source"];

/// Each header key as it appears quoted in the raw bytes.
static QUOTED: LazyLock<[Finder<'static>; 2]> =
    LazyLock::new(|| [Finder::new(b"\"_org_id\""), Finder::new(b"\"_source\"")]);

/// The header key the record's top-level object carries more than once, once
/// escapes are decoded.
///
/// A record with no `\` byte and at most one quoted spelling of each key is
/// answered without scanning it.
#[must_use]
pub fn duplicated_header_key(payload: &[u8]) -> Option<&'static str> {
    let escaped = memchr::memchr(b'\\', payload).is_some();
    if !escaped && QUOTED.iter().all(|f| f.find_iter(payload).nth(1).is_none()) {
        return None;
    }
    first_repeated_key(payload)
}

/// The reason a record refused for a repeated header key carries into the DLQ.
#[must_use]
pub fn duplicate_header_reason(key: &str) -> String {
    format!("payload carries the {key} header key more than once")
}

/// Walk the keys of the top-level object, skipping every value unread.
fn first_repeated_key(payload: &[u8]) -> Option<&'static str> {
    let start = payload.iter().position(|b| !b.is_ascii_whitespace())?;
    if payload[start] != b'{' {
        return None;
    }
    let mut seen = [false; HEADER_KEYS.len()];
    let mut depth = 0usize;
    let mut expect_key = false;
    let mut i = start;
    while i < payload.len() {
        match payload[i] {
            b'{' | b'[' => {
                depth += 1;
                expect_key = depth == 1;
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return None;
                }
            }
            b',' if depth == 1 => expect_key = true,
            b'"' => {
                let end = string_end(payload, i + 1)?;
                if depth == 1 && expect_key {
                    if let Some(k) = header_index(&payload[i..=end]) {
                        if seen[k] {
                            return Some(HEADER_KEYS[k]);
                        }
                        seen[k] = true;
                    }
                    expect_key = false;
                }
                i = end;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The index of the quote closing a string whose body starts at `from`.
fn string_end(payload: &[u8], mut from: usize) -> Option<usize> {
    loop {
        let at = from + memchr::memchr2(b'"', b'\\', payload.get(from..)?)?;
        if payload[at] == b'"' {
            return Some(at);
        }
        from = at + 2;
    }
}

/// Which header key a quoted key decodes to, if any.
fn header_index(quoted: &[u8]) -> Option<usize> {
    let body = &quoted[1..quoted.len() - 1];
    if memchr::memchr(b'\\', body).is_none() {
        return HEADER_KEYS.iter().position(|k| k.as_bytes() == body);
    }
    let decoded: String = serde_json::from_slice(quoted).ok()?;
    HEADER_KEYS.iter().position(|k| *k == decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_tenant_or_source_is_found() {
        for (payload, key) in [
            (br#"{"_org_id":"a","_org_id":"b"}"#.as_slice(), "_org_id"),
            (br#"{"_org_id":"a","\u005forg_id":"b"}"#, "_org_id"),
            (br#"{"\u005f\u006frg_id":"a","_org_id":"b"}"#, "_org_id"),
            (br#"{"_source":"x","n":1,"_source":"y"}"#, "_source"),
            (br#" {"_source":"x","\u005fsource":null}"#, "_source"),
        ] {
            assert_eq!(
                duplicated_header_key(payload),
                Some(key),
                "{}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn one_of_each_is_not_a_duplicate() {
        for payload in [
            br#"{"_org_id":"a","_source":"x"}"#.as_slice(),
            br#"{"\u005forg_id":"a"}"#,
            br#"{"msg":"C:\\temp\\x","_org_id":"a"}"#,
            br#"{"_org_id":"a","note":"\"_org_id\" again"}"#,
            br#"{"field":"_org_id","_org_id":"a"}"#,
            br#"{"a":{"_org_id":1},"b":{"_org_id":2},"_org_id":3}"#,
            br#"{"a":[{"_source":1},{"_source":2}],"_source":"x"}"#,
            br#"["_org_id","_org_id"]"#,
            br#"{"_org_id":"a"}{"_org_id":"b"}"#,
            br#"{"_org_id":"unterminated"#,
            b"",
        ] {
            assert_eq!(
                duplicated_header_key(payload),
                None,
                "{}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn an_escape_in_a_value_does_not_hide_a_repeated_key() {
        let payload = br#"{"_org_id":"a\"b","msg":"\\","_org_id":"c"}"#;
        assert_eq!(duplicated_header_key(payload), Some("_org_id"));
    }

    #[test]
    fn the_reason_names_the_key() {
        assert_eq!(
            duplicate_header_reason("_org_id"),
            "payload carries the _org_id header key more than once"
        );
    }
}
