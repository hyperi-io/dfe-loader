//! Payload format detection and parsing
//!
//! Auto-detects JSON vs MessagePack from raw bytes.
//! Supports forced modes: Auto (default), ForceJson, ForceMessagePack.

pub mod parse;

// Re-export from hs-rustlib with local type alias for backward compatibility
pub use hs_rustlib::transport::{
    detect_format, DetectedFormat as PayloadFormat, FormatDetector, FormatMode,
};
pub use parse::parse_payload;
