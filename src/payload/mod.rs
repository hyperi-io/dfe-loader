// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Payload format detection and parsing
//!
//! Auto-detects JSON vs MessagePack from raw bytes.
//! Supports forced modes: Auto (default), ForceJson, ForceMessagePack.

pub mod parse;

// Re-export from hyperi-rustlib with local type alias for backward compatibility
pub use hyperi_rustlib::transport::{
    detect_format, DetectedFormat as PayloadFormat, FormatDetector, FormatMode,
};
pub use parse::parse_payload;
