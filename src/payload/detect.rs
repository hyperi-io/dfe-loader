//! Payload format detection
//!
//! Detects JSON vs MessagePack from the first byte(s).
//! Format is detected once at startup and cached.
//!
//! Modes:
//! - Auto (default): Detect format from first message, lock it
//! - ForceJson: Only accept JSON, reject MessagePack to DLQ
//! - ForceMessagePack: Only accept MessagePack, reject JSON to DLQ

use std::sync::atomic::{AtomicU8, Ordering};

/// Supported payload formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadFormat {
    Unknown = 0,
    Json = 1,
    MessagePack = 2,
}

impl From<u8> for PayloadFormat {
    fn from(v: u8) -> Self {
        match v {
            1 => PayloadFormat::Json,
            2 => PayloadFormat::MessagePack,
            _ => PayloadFormat::Unknown,
        }
    }
}

/// Format detection mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FormatMode {
    /// Auto-detect format from first message (default)
    #[default]
    Auto,
    /// Force JSON only - reject MessagePack to DLQ
    ForceJson,
    /// Force MessagePack only - reject JSON to DLQ
    ForceMessagePack,
}

impl FormatMode {
    /// Parse from string (for config)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "auto" => Some(FormatMode::Auto),
            "json" => Some(FormatMode::ForceJson),
            "messagepack" | "msgpack" => Some(FormatMode::ForceMessagePack),
            _ => None,
        }
    }
}

/// Global format detector with auto-detection on first message.
/// Once detected, format is locked and mismatches go to DLQ.
/// Can be forced to a specific format at construction.
pub struct FormatDetector {
    detected_format: AtomicU8,
    mismatch_count: AtomicU8,
    mode: FormatMode,
}

impl FormatDetector {
    /// Threshold of consecutive mismatches before considering format reset (Auto mode only)
    const MISMATCH_THRESHOLD: u8 = 10;

    /// Create a new detector in Auto mode
    pub const fn new() -> Self {
        Self {
            detected_format: AtomicU8::new(PayloadFormat::Unknown as u8),
            mismatch_count: AtomicU8::new(0),
            mode: FormatMode::Auto,
        }
    }

    /// Create a detector with a specific mode
    pub fn with_mode(mode: FormatMode) -> Self {
        let initial_format = match mode {
            FormatMode::Auto => PayloadFormat::Unknown,
            FormatMode::ForceJson => PayloadFormat::Json,
            FormatMode::ForceMessagePack => PayloadFormat::MessagePack,
        };
        Self {
            detected_format: AtomicU8::new(initial_format as u8),
            mismatch_count: AtomicU8::new(0),
            mode,
        }
    }

    /// Get the current mode
    pub fn mode(&self) -> FormatMode {
        self.mode
    }

    /// Get the currently detected format
    pub fn format(&self) -> PayloadFormat {
        PayloadFormat::from(self.detected_format.load(Ordering::Relaxed))
    }

    /// Check if format matches expected, tracking mismatches.
    /// Returns Ok(format) if message should be processed, Err if it should go to DLQ.
    #[inline]
    pub fn check_and_detect(&self, payload: &[u8]) -> Result<PayloadFormat, PayloadFormat> {
        let detected = detect_format_bytes(payload);

        // Handle forced modes - no auto-detection, no reset
        match self.mode {
            FormatMode::ForceJson => {
                return match detected {
                    Some(PayloadFormat::Json) => Ok(PayloadFormat::Json),
                    _ => Err(PayloadFormat::Json), // Expected JSON, got something else -> DLQ
                };
            }
            FormatMode::ForceMessagePack => {
                return match detected {
                    Some(PayloadFormat::MessagePack) => Ok(PayloadFormat::MessagePack),
                    _ => Err(PayloadFormat::MessagePack), // Expected MsgPack, got something else -> DLQ
                };
            }
            FormatMode::Auto => {} // Continue with auto-detection logic
        }

        // Auto mode logic
        let current = self.format();

        match (current, detected) {
            // First message - set the format
            (PayloadFormat::Unknown, Some(fmt)) => {
                self.detected_format.store(fmt as u8, Ordering::Relaxed);
                self.mismatch_count.store(0, Ordering::Relaxed);
                Ok(fmt)
            }

            // Unknown format in payload - DLQ
            (_, None) => Err(PayloadFormat::Unknown),

            // Format matches - process
            (expected, Some(actual)) if expected == actual => {
                self.mismatch_count.store(0, Ordering::Relaxed);
                Ok(actual)
            }

            // Format mismatch - check if we should reset
            (expected, Some(actual)) => {
                let count = self.mismatch_count.fetch_add(1, Ordering::Relaxed);
                if count >= Self::MISMATCH_THRESHOLD {
                    // Too many mismatches - assume format changed, reset
                    self.detected_format.store(actual as u8, Ordering::Relaxed);
                    self.mismatch_count.store(0, Ordering::Relaxed);
                    tracing::warn!(
                        old = ?expected,
                        new = ?actual,
                        "Format changed after {} mismatches, resetting",
                        count
                    );
                    Ok(actual)
                } else {
                    // Mismatch - send to DLQ
                    Err(expected)
                }
            }
        }
    }

    /// Force reset to unknown (for testing or manual override, Auto mode only)
    pub fn reset(&self) {
        if self.mode == FormatMode::Auto {
            self.detected_format.store(PayloadFormat::Unknown as u8, Ordering::Relaxed);
            self.mismatch_count.store(0, Ordering::Relaxed);
        }
    }
}

impl Default for FormatDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Detect payload format from raw bytes (internal).
#[inline]
fn detect_format_bytes(payload: &[u8]) -> Option<PayloadFormat> {
    // Skip leading whitespace for JSON detection
    let first = payload.iter().find(|&&b| !b.is_ascii_whitespace())?;

    match *first {
        // JSON object or array
        b'{' | b'[' => Some(PayloadFormat::Json),

        // MessagePack fixmap (0x80-0x8F)
        0x80..=0x8F => Some(PayloadFormat::MessagePack),

        // MessagePack map16 (0xDE) or map32 (0xDF)
        0xDE | 0xDF => Some(PayloadFormat::MessagePack),

        // MessagePack fixarray (0x90-0x9F)
        0x90..=0x9F => Some(PayloadFormat::MessagePack),

        // MessagePack array16 (0xDC) or array32 (0xDD)
        0xDC | 0xDD => Some(PayloadFormat::MessagePack),

        // Unknown format
        _ => None,
    }
}

/// Legacy function for direct detection
#[inline]
pub fn detect_format(payload: &[u8]) -> Option<PayloadFormat> {
    detect_format_bytes(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_json_object() {
        assert_eq!(detect_format(b"{\"key\": \"value\"}"), Some(PayloadFormat::Json));
    }

    #[test]
    fn test_detect_json_array() {
        assert_eq!(detect_format(b"[1, 2, 3]"), Some(PayloadFormat::Json));
    }

    #[test]
    fn test_detect_json_with_whitespace() {
        assert_eq!(detect_format(b"  \n\t{\"key\": 1}"), Some(PayloadFormat::Json));
    }

    #[test]
    fn test_detect_msgpack_fixmap() {
        assert_eq!(detect_format(&[0x81, 0xA3, b'k', b'e', b'y']), Some(PayloadFormat::MessagePack));
    }

    #[test]
    fn test_detect_msgpack_map16() {
        assert_eq!(detect_format(&[0xDE, 0x00, 0x01]), Some(PayloadFormat::MessagePack));
    }

    #[test]
    fn test_detect_empty() {
        assert_eq!(detect_format(b""), None);
    }

    #[test]
    fn test_detect_whitespace_only() {
        assert_eq!(detect_format(b"   \n\t  "), None);
    }

    #[test]
    fn test_detect_unknown() {
        assert_eq!(detect_format(b"hello"), None);
    }

    #[test]
    fn test_format_detector_auto_detect() {
        let detector = FormatDetector::new();
        assert_eq!(detector.format(), PayloadFormat::Unknown);

        // First JSON message sets format
        let result = detector.check_and_detect(b"{\"key\": 1}");
        assert_eq!(result, Ok(PayloadFormat::Json));
        assert_eq!(detector.format(), PayloadFormat::Json);

        // Subsequent JSON messages pass
        assert_eq!(detector.check_and_detect(b"{\"key\": 2}"), Ok(PayloadFormat::Json));

        // MessagePack mismatch goes to DLQ
        assert_eq!(detector.check_and_detect(&[0x81, 0xA1, b'k']), Err(PayloadFormat::Json));
    }

    #[test]
    fn test_format_detector_mismatch_reset() {
        let detector = FormatDetector::new();

        // Set to JSON
        detector.check_and_detect(b"{\"key\": 1}").unwrap();

        // Send 11 MessagePack messages (> threshold of 10)
        for _ in 0..11 {
            let _ = detector.check_and_detect(&[0x81, 0xA1, b'k']);
        }

        // Format should have switched to MessagePack
        assert_eq!(detector.format(), PayloadFormat::MessagePack);
    }

    #[test]
    fn test_force_json_mode() {
        let detector = FormatDetector::with_mode(FormatMode::ForceJson);
        assert_eq!(detector.mode(), FormatMode::ForceJson);
        assert_eq!(detector.format(), PayloadFormat::Json);

        // JSON passes
        assert_eq!(detector.check_and_detect(b"{\"key\": 1}"), Ok(PayloadFormat::Json));

        // MessagePack fails immediately (no mismatch counting)
        assert_eq!(detector.check_and_detect(&[0x81, 0xA1, b'k']), Err(PayloadFormat::Json));

        // Unknown format also fails
        assert_eq!(detector.check_and_detect(b"hello"), Err(PayloadFormat::Json));

        // Format stays locked
        assert_eq!(detector.format(), PayloadFormat::Json);
    }

    #[test]
    fn test_force_msgpack_mode() {
        let detector = FormatDetector::with_mode(FormatMode::ForceMessagePack);
        assert_eq!(detector.mode(), FormatMode::ForceMessagePack);
        assert_eq!(detector.format(), PayloadFormat::MessagePack);

        // MessagePack passes
        assert_eq!(detector.check_and_detect(&[0x81, 0xA1, b'k']), Ok(PayloadFormat::MessagePack));

        // JSON fails immediately
        assert_eq!(detector.check_and_detect(b"{\"key\": 1}"), Err(PayloadFormat::MessagePack));

        // Format stays locked
        assert_eq!(detector.format(), PayloadFormat::MessagePack);
    }

    #[test]
    fn test_force_mode_no_reset() {
        let detector = FormatDetector::with_mode(FormatMode::ForceJson);

        // Send many MessagePack messages - should NOT reset
        for _ in 0..20 {
            let _ = detector.check_and_detect(&[0x81, 0xA1, b'k']);
        }

        // Format should still be JSON (no auto-reset in force mode)
        assert_eq!(detector.format(), PayloadFormat::Json);
    }

    #[test]
    fn test_format_mode_from_str() {
        assert_eq!(FormatMode::from_str("auto"), Some(FormatMode::Auto));
        assert_eq!(FormatMode::from_str("AUTO"), Some(FormatMode::Auto));
        assert_eq!(FormatMode::from_str("json"), Some(FormatMode::ForceJson));
        assert_eq!(FormatMode::from_str("JSON"), Some(FormatMode::ForceJson));
        assert_eq!(FormatMode::from_str("messagepack"), Some(FormatMode::ForceMessagePack));
        assert_eq!(FormatMode::from_str("msgpack"), Some(FormatMode::ForceMessagePack));
        assert_eq!(FormatMode::from_str("invalid"), None);
    }
}
