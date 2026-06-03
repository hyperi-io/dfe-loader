// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Payload format detection and parsing
//!
//! Auto-detects JSON vs `MessagePack` from raw bytes.
//! Supports forced modes: Auto (default), `ForceJson`, `ForceMessagePack`.

pub mod parse;

// Re-export from hyperi-rustlib with local type alias for backward compatibility
pub use hyperi_rustlib::transport::{
    DetectedFormat as PayloadFormat, FormatDetector, FormatMode, detect_format,
};
pub use parse::parse_payload;
