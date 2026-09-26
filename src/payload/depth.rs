// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Nesting-depth pre-check for untrusted JSON.
//!
//! sonic-rs validates a lazy value by recursing once per nesting level with no
//! depth limit, so a record nested a few thousand levels deep exhausts a 2 MiB
//! worker stack and aborts the process, and at-least-once delivery hands the
//! same record back after the restart. Every record is measured here,
//! iteratively, before a lazy sonic-rs call reads it.

use wide::u8x32;

use crate::payload::opens_json_document;

/// Deepest nesting a record may reach, the bound scalo's parse path uses.
pub const MAX_PARSE_DEPTH: usize = 64;

/// Deepest nesting a batch may reach: its records plus the array around them.
pub const MAX_BATCH_DEPTH: usize = MAX_PARSE_DEPTH + 1;

/// Bytes classified per step, one bit each in a `u64` mask.
const BLOCK: usize = 64;

/// Every odd bit position, which tells an odd run of backslashes from an even one.
const ODD_BITS: u64 = 0xAAAA_AAAA_AAAA_AAAA;

// SHORTCUT: app-local guard with the verdicts of scalo's json_depth_within, until scalo exposes it
/// `true` if the JSON payload nests no deeper than `max`.
///
/// One forward pass counting `{` and `[` outside strings, honouring `\`
/// escapes inside strings. Not a validator: on malformed input the parser
/// stops at the first bad token, which is no deeper than this pass has already
/// counted.
///
/// Classifies 64 bytes per step into bitmasks and walks only the brackets
/// outside strings. A backslash outside a string escapes nothing, which the
/// masks cannot express, so the rest of such a payload is read a byte at a time.
#[must_use]
pub fn json_depth_within(payload: &[u8], max: usize) -> bool {
    let (blocks, tail) = payload.as_chunks::<BLOCK>();
    let mut scan = Scan::default();
    for (i, block) in blocks.iter().enumerate() {
        match scan.block(Classes::of(block), max) {
            Step::Within => {}
            Step::TooDeep => return false,
            Step::Irregular => return scan.bytes(&payload[i * BLOCK..], max),
        }
    }
    if tail.is_empty() {
        return true;
    }
    // Re-read the last 64 bytes and shift out the ones already scanned.
    let classes = if let Some(last) = payload.last_chunk::<BLOCK>() {
        Classes::of(last).skip(BLOCK - tail.len())
    } else {
        // A zero byte is in no class, so the padding changes nothing.
        let mut padded = [0; BLOCK];
        padded[..tail.len()].copy_from_slice(tail);
        Classes::of(&padded)
    };
    match scan.block(classes, max) {
        Step::Within => true,
        Step::TooDeep => false,
        Step::Irregular => scan.bytes(tail, max),
    }
}

/// One bit per byte of a block for each byte class the scan reads.
#[derive(Clone, Copy)]
struct Classes {
    quotes: u64,
    backslashes: u64,
    opens: u64,
    closes: u64,
}

impl Classes {
    #[inline]
    fn of(block: &[u8; BLOCK]) -> Self {
        let quote = u8x32::splat(b'"');
        let backslash = u8x32::splat(b'\\');
        // `[` and `{` differ only in bit 5, as do `]` and `}`.
        let fold = u8x32::splat(0x20);
        let open = u8x32::splat(b'{');
        let close = u8x32::splat(b'}');
        let mut classes = Self {
            quotes: 0,
            backslashes: 0,
            opens: 0,
            closes: 0,
        };
        let (halves, _) = block.as_chunks::<32>();
        for (half, shift) in halves.iter().zip([0_u32, 32]) {
            let bytes = u8x32::new(*half);
            let folded = bytes | fold;
            classes.quotes |= u64::from(bytes.simd_eq(quote).to_bitmask()) << shift;
            classes.backslashes |= u64::from(bytes.simd_eq(backslash).to_bitmask()) << shift;
            classes.opens |= u64::from(folded.simd_eq(open).to_bitmask()) << shift;
            classes.closes |= u64::from(folded.simd_eq(close).to_bitmask()) << shift;
        }
        classes
    }

    /// Drops the first `seen` bytes, which an earlier block already scanned.
    #[inline]
    fn skip(self, seen: usize) -> Self {
        Self {
            quotes: self.quotes >> seen,
            backslashes: self.backslashes >> seen,
            opens: self.opens >> seen,
            closes: self.closes >> seen,
        }
    }
}

/// Scan state carried from one block to the next.
#[derive(Default)]
struct Scan {
    depth: usize,
    /// All ones while a string is open across a block boundary.
    in_string: u64,
    /// 1 when a backslash ending the last block escapes this block's first byte.
    escaped: u64,
}

/// What one block did to the scan.
enum Step {
    Within,
    TooDeep,
    /// A backslash outside a string: the byte scan takes over from this block.
    Irregular,
}

impl Scan {
    #[inline]
    fn block(&mut self, classes: Classes, max: usize) -> Step {
        let Classes {
            quotes,
            backslashes,
            opens,
            closes,
        } = classes;
        // A byte is escaped when an odd run of backslashes precedes it (simdjson's escape scanner).
        let (escaped, next_escaped) = if backslashes == 0 {
            (self.escaped, 0)
        } else {
            let potential = backslashes & !self.escaped;
            let codes = ((potential << 1) | ODD_BITS).wrapping_sub(potential) ^ ODD_BITS;
            (
                codes ^ (backslashes | self.escaped),
                (codes & backslashes) >> 63,
            )
        };
        // Set from each opening quote up to, not including, its closing quote.
        let strings = prefix_xor(quotes & !escaped) ^ self.in_string;
        if backslashes & !strings != 0 {
            return Step::Irregular;
        }
        if !self.walk(opens & !strings, closes & !strings, max) {
            return Step::TooDeep;
        }
        self.in_string = 0_u64.wrapping_sub(strings >> 63);
        self.escaped = next_escaped;
        Step::Within
    }

    /// Applies one block's brackets in order; `false` once the depth passes `max`.
    #[inline]
    fn walk(&mut self, opens: u64, closes: u64, max: usize) -> bool {
        let n_open = opens.count_ones() as usize;
        let n_close = closes.count_ones() as usize;
        // No close can reach zero and no open can pass `max`, so the counts alone are exact.
        if n_close <= self.depth && n_open <= max - self.depth {
            self.depth = self.depth + n_open - n_close;
            return true;
        }
        let mut marks = opens | closes;
        while marks != 0 {
            if (opens >> marks.trailing_zeros()) & 1 == 0 {
                self.depth = self.depth.saturating_sub(1);
            } else {
                self.depth += 1;
                if self.depth > max {
                    return false;
                }
            }
            marks &= marks - 1;
        }
        true
    }

    /// The byte-at-a-time scan, resumed from this state.
    fn bytes(&self, rest: &[u8], max: usize) -> bool {
        let mut depth = self.depth;
        let mut in_string = self.in_string != 0;
        let mut escaped = self.escaped != 0;
        for &b in rest {
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
}

/// Bit `i` of the result is the parity of the set bits at or below `i`.
#[inline]
fn prefix_xor(mut bits: u64) -> u64 {
    bits ^= bits << 1;
    bits ^= bits << 2;
    bits ^= bits << 4;
    bits ^= bits << 8;
    bits ^= bits << 16;
    bits ^= bits << 32;
    bits
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
    use proptest::prelude::*;

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

    /// The byte-at-a-time guard whose verdicts the block scan must reproduce.
    fn reference(payload: &[u8], max: usize) -> bool {
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

    fn agrees(payload: &[u8], max: usize) {
        assert_eq!(
            json_depth_within(payload, max),
            reference(payload, max),
            "max {max}, payload {:?}",
            String::from_utf8_lossy(payload)
        );
    }

    /// Every byte the scan reads specially, plus filler, so short strings hit every transition.
    const STRUCTURAL: &[u8] = b"{}[]\"\\x:, 1";

    /// Cases per differential property.
    const CASES: u32 = 4096;

    /// Cases for generated JSON, whose recursive generator costs far more per case than the scan.
    const JSON_CASES: u32 = 1024;

    /// JSON of random shape and depth, with brackets, quotes and backslashes inside strings.
    fn json_value() -> impl Strategy<Value = String> {
        let string = "[a-z{}\\[\\]\"\\\\]{0,12}".prop_map(|s| {
            let mut out = String::from("\"");
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        });
        let leaf = prop_oneof![Just("1".to_string()), Just("null".to_string()), string];
        leaf.prop_recursive(120, 4000, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6)
                    .prop_map(|items| format!("[{}]", items.join(","))),
                prop::collection::vec(("[a-z\\[{\"\\\\]{0,6}", inner), 0..6).prop_map(|fields| {
                    let body: Vec<String> = fields
                        .into_iter()
                        .map(|(key, value)| format!("{key:?}:{value}"))
                        .collect();
                    format!("{{{}}}", body.join(","))
                }),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(CASES))]

        #[test]
        fn block_scan_agrees_on_arbitrary_bytes(
            payload in prop::collection::vec(any::<u8>(), 0..700),
            max in 0_usize..80,
        ) {
            agrees(&payload, max);
        }

        #[test]
        fn block_scan_agrees_on_structural_bytes(
            payload in prop::collection::vec(prop::sample::select(STRUCTURAL), 0..700),
            max in 0_usize..40,
        ) {
            agrees(&payload, max);
        }

        #[test]
        fn block_scan_agrees_on_deep_and_mismatched_nesting(
            depth in 0_usize..400,
            offset in 0_usize..130,
            max in 0_usize..300,
            opens in prop::sample::select(vec!["[", "{\"a\":", "{"]),
            closes in prop::sample::select(vec!["]", "}"]),
        ) {
            let payload = format!("{}{}1{}", " ".repeat(offset), opens.repeat(depth), closes.repeat(depth));
            agrees(payload.as_bytes(), max);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(JSON_CASES))]

        #[test]
        fn block_scan_agrees_on_generated_json(json in json_value(), max in 0_usize..130) {
            agrees(json.as_bytes(), max);
        }
    }

    #[test]
    fn block_scan_agrees_on_every_short_string_across_a_block_boundary() {
        let alphabet = b"{]\"\\x[";
        for len in 0..=5_u32 {
            for n in 0..alphabet.len().pow(len) {
                let mut s = Vec::new();
                let mut k = n;
                for _ in 0..len {
                    s.push(alphabet[k % alphabet.len()]);
                    k /= alphabet.len();
                }
                for offset in [0, 59, 62, 63, 64, 123, 127] {
                    let mut payload = vec![b'x'; offset];
                    payload.extend_from_slice(&s);
                    for max in 0..4 {
                        agrees(&payload, max);
                    }
                }
            }
        }
    }

    #[test]
    fn a_backslash_outside_a_string_escapes_nothing() {
        // Outside a string the backslash is skipped, so the quote opens a string that hides the brackets.
        let payload = format!("[\\\"{}", "[".repeat(200));
        assert!(json_depth_within(payload.as_bytes(), 1));
        assert!(reference(payload.as_bytes(), 1));
    }

    #[test]
    fn a_close_with_nothing_open_does_not_go_below_zero() {
        // Extra closes saturate at zero, so they cannot bank headroom for later opens.
        let payload = format!("{}{}", "]".repeat(70), "[".repeat(4));
        assert!(!json_depth_within(payload.as_bytes(), 3));
        assert!(!reference(payload.as_bytes(), 3));
    }
}
