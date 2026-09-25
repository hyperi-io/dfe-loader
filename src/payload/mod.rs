// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Payload parsing. JSON is the only payload format.

pub mod parse;

pub use parse::{
    has_ndjson_boundary, opens_json_array, opens_json_document, parse_payload, split_json_array,
    split_ndjson,
};

/// The first bytes of a payload, for naming it in a rejection reason.
///
/// Holds at most [`LeadingBytes::MAX`]: framing magic is what identifies a
/// foreign record, and any more of the payload risks carrying its content
/// into logs and DLQ reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadingBytes {
    bytes: [u8; Self::MAX],
    len: usize,
}

impl LeadingBytes {
    /// The most bytes a rejection reason ever carries.
    pub const MAX: usize = 8;

    /// Take the first [`Self::MAX`] bytes of `payload`, or all of a shorter one.
    #[must_use]
    pub fn of(payload: &[u8]) -> Self {
        let len = payload.len().min(Self::MAX);
        let mut bytes = [0u8; Self::MAX];
        bytes[..len].copy_from_slice(&payload[..len]);
        Self { bytes, len }
    }

    /// The captured bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl std::fmt::Display for LeadingBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.len == 0 {
            return f.write_str("empty payload");
        }
        f.write_str("leading bytes")?;
        for b in self.as_slice() {
            write!(f, " {b:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leading_bytes_stop_at_eight() {
        let payload = [0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x8c, 0xde, 0xad];
        let lead = LeadingBytes::of(&payload);
        assert_eq!(lead.as_slice(), &payload[..LeadingBytes::MAX]);
        assert_eq!(lead.to_string(), "leading bytes 00 00 02 00 00 00 01 8c");
    }

    #[test]
    fn leading_bytes_of_a_short_payload_show_all_of_it() {
        let lead = LeadingBytes::of(b"\x7f\x45");
        assert_eq!(lead.to_string(), "leading bytes 7f 45");
    }

    #[test]
    fn leading_bytes_of_an_empty_payload_say_so() {
        assert_eq!(LeadingBytes::of(b"").to_string(), "empty payload");
    }
}
