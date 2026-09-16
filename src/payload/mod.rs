// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Payload format detection and parsing
//!
//! Auto-detects JSON vs `MessagePack` from raw bytes.
//! Supports forced modes: Auto (default), `ForceJson`, `ForceMessagePack`.

pub mod parse;

// Re-export from scalo with local type alias for backward compatibility
pub use parse::{opens_json_array, parse_payload, split_json_array};
pub use scalo::transport::{
    DetectedFormat as PayloadFormat, FormatDetector, FormatMode, detect_format,
};
