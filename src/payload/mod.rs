//! Payload format detection and parsing
//!
//! Auto-detects JSON vs MessagePack from raw bytes.
//! Supports forced modes: Auto (default), ForceJson, ForceMessagePack.

pub mod detect;
pub mod parse;

pub use detect::{FormatDetector, FormatMode, PayloadFormat, detect_format};
pub use parse::parse_payload;
