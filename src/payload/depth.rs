// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Nesting-depth pre-check for untrusted JSON.
//!
//! sonic-rs validates a lazy value by recursing once per nesting level with no
//! depth limit, so a record nested a few thousand levels deep exhausts a 2 MiB
//! worker stack and aborts the process, and at-least-once delivery hands the
//! same record back after the restart. Every record is measured here,
//! iteratively, before a lazy sonic-rs call reads it.

use crate::payload::opens_json_document;

/// Deepest nesting a record may reach, the bound scalo's parse path uses.
pub const MAX_PARSE_DEPTH: usize = 64;

/// Deepest nesting a batch may reach: its records plus the array around them.
pub const MAX_BATCH_DEPTH: usize = MAX_PARSE_DEPTH + 1;

// SHORTCUT: app-local copy of scalo's json_depth_within, until scalo exposes it
/// `true` if the JSON payload nests no deeper than `max`.
///
/// One forward pass counting `{` and `[` outside strings, honouring `\`
/// escapes. Not a validator: on malformed input the parser stops at the first
/// bad token, which is no deeper than this pass has already counted.
#[must_use]
pub fn json_depth_within(payload: &[u8], max: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

/// `true` for a JSON record nested deeper than [`MAX_PARSE_DEPTH`].
///
/// A payload that does not open a JSON object or array is left to the JSON
/// gate, which refuses it by name before anything parses it.
#[must_use]
pub fn nests_too_deep(payload: &[u8]) -> bool {
    opens_json_document(payload) && !json_depth_within(payload, MAX_PARSE_DEPTH)
}

/// The reason a record refused for its depth carries into the DLQ.
#[must_use]
pub fn too_deep_reason() -> String {
    format!("payload nesting exceeds the maximum parse depth of {MAX_PARSE_DEPTH}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(open: &str, close: &str, depth: usize) -> Vec<u8> {
        let mut payload = open.repeat(depth).into_bytes();
        payload.extend_from_slice(b"1");
        payload.extend_from_slice(close.repeat(depth).as_bytes());
        payload
    }

    #[test]
    fn flat_and_shallow_pass() {
        assert!(json_depth_within(br"{}", MAX_PARSE_DEPTH));
        assert!(json_depth_within(
            br#"{"a":1,"b":[1,2,3]}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(
            br#"{"a":{"b":{"c":1}}}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(b"", MAX_PARSE_DEPTH));
    }

    #[test]
    fn exactly_at_the_bound_passes_and_one_over_fails() {
        assert!(json_depth_within(&nested("[", "]", 3), 3));
        assert!(!json_depth_within(&nested("[", "]", 4), 3));
        assert!(!nests_too_deep(&nested("{\"a\":", "}", MAX_PARSE_DEPTH)));
        assert!(nests_too_deep(&nested("{\"a\":", "}", MAX_PARSE_DEPTH + 1)));
    }

    #[test]
    fn sibling_containers_do_not_add_up() {
        let wide = format!("[{}]", vec!["[[1]]"; 1000].join(","));
        assert!(json_depth_within(wide.as_bytes(), 3));
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        assert!(json_depth_within(br#"{"k":"{{{{{{{{[[[[["}"#, 2));
    }

    #[test]
    fn an_escaped_quote_keeps_the_string_open() {
        assert!(json_depth_within(br#"{"k":"a\"{{{{{"}"#, 2));
    }

    #[test]
    fn an_escaped_backslash_closes_the_string() {
        // `\\` is one literal backslash, so the quote after it ends the string
        // and the brackets that follow are structure.
        assert!(!json_depth_within(br#"["\\"[[[1]]]]"#, 3));
    }

    #[test]
    fn pathological_depth_is_refused() {
        for depth in [5_000, 20_000, 100_000] {
            assert!(nests_too_deep(&nested("[", "]", depth)));
            assert!(nests_too_deep(&nested("{\"a\":", "}", depth)));
        }
    }

    #[test]
    fn the_reason_names_the_bound() {
        assert!(too_deep_reason().ends_with("maximum parse depth of 64"));
    }
}
