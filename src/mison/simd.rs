// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! SIMD-accelerated structural character detection
//!
//! Uses AVX2 (256-bit) or SSE4.2 (128-bit) on x86_64, NEON (128-bit) on aarch64,
//! to find structural characters. Runtime detection is used on x86_64 to select
//! the best available instruction set.
//!
//! Based on Mison Section 4.2.1: Building Structural Character Bitmaps

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Word size for bitmap operations (64 bits = 8 bytes per word)
pub const WORD_SIZE: usize = 64;

// ============================================================================
// x86_64 SIMD capability detection
// ============================================================================

#[cfg(target_arch = "x86_64")]
static SIMD_CAPABILITY: std::sync::OnceLock<SimdCapability> = std::sync::OnceLock::new();

#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy, Debug, PartialEq)]
enum SimdCapability {
    Avx2,
    Sse42,
    Scalar,
}

#[cfg(target_arch = "x86_64")]
fn detect_simd_capability() -> SimdCapability {
    if is_x86_feature_detected!("avx2") {
        SimdCapability::Avx2
    } else if is_x86_feature_detected!("sse4.2") {
        SimdCapability::Sse42
    } else {
        SimdCapability::Scalar
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn get_simd_capability() -> SimdCapability {
    *SIMD_CAPABILITY.get_or_init(detect_simd_capability)
}

// ============================================================================
// aarch64 NEON detection (always available on aarch64)
// ============================================================================

#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy, Debug, PartialEq)]
enum SimdCapability {
    Neon,
    Scalar,
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn get_simd_capability() -> SimdCapability {
    // NEON is mandatory on aarch64, always available
    SimdCapability::Neon
}

// ============================================================================
// Public capability query
// ============================================================================

/// Get a human-readable description of the SIMD capability in use
///
/// Returns "AVX2", "SSE4.2", "NEON", or "Scalar"
pub fn simd_capability_name() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        match get_simd_capability() {
            SimdCapability::Avx2 => "AVX2",
            SimdCapability::Sse42 => "SSE4.2",
            SimdCapability::Scalar => "Scalar",
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        match get_simd_capability() {
            SimdCapability::Neon => "NEON",
            SimdCapability::Scalar => "Scalar",
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "Scalar"
    }
}

/// Structural character masks for a chunk of JSON
#[derive(Debug, Clone, Default)]
pub struct CharacterBitmaps {
    /// Positions of quote characters (")
    pub quote: u64,
    /// Positions of backslash characters (\)
    pub backslash: u64,
    /// Positions of colon characters (:)
    pub colon: u64,
    /// Positions of comma characters (,)
    pub comma: u64,
    /// Positions of left brace characters ({)
    pub lbrace: u64,
    /// Positions of right brace characters (})
    pub rbrace: u64,
    /// Positions of left bracket characters ([)
    pub lbracket: u64,
    /// Positions of right bracket characters (])
    pub rbracket: u64,
}

/// Build character bitmaps for a 64-byte chunk using AVX2
///
/// Returns a bitmap where each bit corresponds to a character position.
/// Bit N is set if the character at position N matches the target.
///
/// SAFETY: Caller must ensure AVX2 is available (use `is_x86_feature_detected!("avx2")`)
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn build_character_bitmaps_avx2(chunk: &[u8; 64]) -> CharacterBitmaps {
    // Load two 32-byte halves
    let data0 = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
    let data1 = _mm256_loadu_si256(chunk.as_ptr().add(32) as *const __m256i);

    // Create comparison vectors for each structural character
    let quote_vec = _mm256_set1_epi8(b'"' as i8);
    let backslash_vec = _mm256_set1_epi8(b'\\' as i8);
    let colon_vec = _mm256_set1_epi8(b':' as i8);
    let comma_vec = _mm256_set1_epi8(b',' as i8);
    let lbrace_vec = _mm256_set1_epi8(b'{' as i8);
    let rbrace_vec = _mm256_set1_epi8(b'}' as i8);
    let lbracket_vec = _mm256_set1_epi8(b'[' as i8);
    let rbracket_vec = _mm256_set1_epi8(b']' as i8);

    // Compare and extract bitmasks for first 32 bytes
    let quote0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, quote_vec)) as u32;
    let backslash0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, backslash_vec)) as u32;
    let colon0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, colon_vec)) as u32;
    let comma0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, comma_vec)) as u32;
    let lbrace0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, lbrace_vec)) as u32;
    let rbrace0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, rbrace_vec)) as u32;
    let lbracket0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, lbracket_vec)) as u32;
    let rbracket0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data0, rbracket_vec)) as u32;

    // Compare and extract bitmasks for second 32 bytes
    let quote1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, quote_vec)) as u32;
    let backslash1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, backslash_vec)) as u32;
    let colon1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, colon_vec)) as u32;
    let comma1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, comma_vec)) as u32;
    let lbrace1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, lbrace_vec)) as u32;
    let rbrace1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, rbrace_vec)) as u32;
    let lbracket1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, lbracket_vec)) as u32;
    let rbracket1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data1, rbracket_vec)) as u32;

    // Combine into 64-bit bitmaps
    CharacterBitmaps {
        quote: (quote0 as u64) | ((quote1 as u64) << 32),
        backslash: (backslash0 as u64) | ((backslash1 as u64) << 32),
        colon: (colon0 as u64) | ((colon1 as u64) << 32),
        comma: (comma0 as u64) | ((comma1 as u64) << 32),
        lbrace: (lbrace0 as u64) | ((lbrace1 as u64) << 32),
        rbrace: (rbrace0 as u64) | ((rbrace1 as u64) << 32),
        lbracket: (lbracket0 as u64) | ((lbracket1 as u64) << 32),
        rbracket: (rbracket0 as u64) | ((rbracket1 as u64) << 32),
    }
}

/// Build character bitmaps for a 64-byte chunk using SSE4.2
///
/// SAFETY: Caller must ensure SSE4.2 is available
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
#[inline]
unsafe fn build_character_bitmaps_sse42(chunk: &[u8; 64]) -> CharacterBitmaps {
    // Process in 16-byte chunks (4 chunks for 64 bytes)
    let mut result = CharacterBitmaps::default();

    for i in 0..4 {
        let offset = i * 16;
        let data = _mm_loadu_si128(chunk.as_ptr().add(offset) as *const __m128i);

        let quote_vec = _mm_set1_epi8(b'"' as i8);
        let backslash_vec = _mm_set1_epi8(b'\\' as i8);
        let colon_vec = _mm_set1_epi8(b':' as i8);
        let comma_vec = _mm_set1_epi8(b',' as i8);
        let lbrace_vec = _mm_set1_epi8(b'{' as i8);
        let rbrace_vec = _mm_set1_epi8(b'}' as i8);
        let lbracket_vec = _mm_set1_epi8(b'[' as i8);
        let rbracket_vec = _mm_set1_epi8(b']' as i8);

        let quote_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, quote_vec)) as u64;
        let backslash_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, backslash_vec)) as u64;
        let colon_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, colon_vec)) as u64;
        let comma_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, comma_vec)) as u64;
        let lbrace_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, lbrace_vec)) as u64;
        let rbrace_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, rbrace_vec)) as u64;
        let lbracket_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, lbracket_vec)) as u64;
        let rbracket_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(data, rbracket_vec)) as u64;

        let shift = offset;
        result.quote |= quote_mask << shift;
        result.backslash |= backslash_mask << shift;
        result.colon |= colon_mask << shift;
        result.comma |= comma_mask << shift;
        result.lbrace |= lbrace_mask << shift;
        result.rbrace |= rbrace_mask << shift;
        result.lbracket |= lbracket_mask << shift;
        result.rbracket |= rbracket_mask << shift;
    }

    result
}

// ============================================================================
// aarch64 NEON implementation
// ============================================================================

/// Build character bitmaps for a 64-byte chunk using NEON (aarch64)
///
/// NEON is mandatory on aarch64, so no runtime detection needed.
#[cfg(target_arch = "aarch64")]
#[inline]
fn build_character_bitmaps_neon(chunk: &[u8; 64]) -> CharacterBitmaps {
    // NEON processes 16 bytes at a time (4 chunks for 64 bytes)
    let mut result = CharacterBitmaps::default();

    // NEON doesn't have a direct movemask equivalent, so we use a reduction pattern
    // Process in 16-byte chunks
    for chunk_idx in 0..4 {
        let offset = chunk_idx * 16;
        let chunk_slice = &chunk[offset..offset + 16];

        // Load 16 bytes
        // SAFETY: chunk_slice is exactly 16 bytes
        let data = unsafe { vld1q_u8(chunk_slice.as_ptr()) };

        // Create comparison vectors
        let quote_vec = unsafe { vdupq_n_u8(b'"') };
        let backslash_vec = unsafe { vdupq_n_u8(b'\\') };
        let colon_vec = unsafe { vdupq_n_u8(b':') };
        let comma_vec = unsafe { vdupq_n_u8(b',') };
        let lbrace_vec = unsafe { vdupq_n_u8(b'{') };
        let rbrace_vec = unsafe { vdupq_n_u8(b'}') };
        let lbracket_vec = unsafe { vdupq_n_u8(b'[') };
        let rbracket_vec = unsafe { vdupq_n_u8(b']') };

        // Compare - result is 0xFF for match, 0x00 for no match
        let quote_cmp = unsafe { vceqq_u8(data, quote_vec) };
        let backslash_cmp = unsafe { vceqq_u8(data, backslash_vec) };
        let colon_cmp = unsafe { vceqq_u8(data, colon_vec) };
        let comma_cmp = unsafe { vceqq_u8(data, comma_vec) };
        let lbrace_cmp = unsafe { vceqq_u8(data, lbrace_vec) };
        let rbrace_cmp = unsafe { vceqq_u8(data, rbrace_vec) };
        let lbracket_cmp = unsafe { vceqq_u8(data, lbracket_vec) };
        let rbracket_cmp = unsafe { vceqq_u8(data, rbracket_vec) };

        // Convert to bitmask - NEON doesn't have movemask, so we extract bit-by-bit
        // This is slower than x86 movemask but still faster than pure scalar
        let quote_mask = neon_to_bitmask(quote_cmp);
        let backslash_mask = neon_to_bitmask(backslash_cmp);
        let colon_mask = neon_to_bitmask(colon_cmp);
        let comma_mask = neon_to_bitmask(comma_cmp);
        let lbrace_mask = neon_to_bitmask(lbrace_cmp);
        let rbrace_mask = neon_to_bitmask(rbrace_cmp);
        let lbracket_mask = neon_to_bitmask(lbracket_cmp);
        let rbracket_mask = neon_to_bitmask(rbracket_cmp);

        let shift = offset;
        result.quote |= (quote_mask as u64) << shift;
        result.backslash |= (backslash_mask as u64) << shift;
        result.colon |= (colon_mask as u64) << shift;
        result.comma |= (comma_mask as u64) << shift;
        result.lbrace |= (lbrace_mask as u64) << shift;
        result.rbrace |= (rbrace_mask as u64) << shift;
        result.lbracket |= (lbracket_mask as u64) << shift;
        result.rbracket |= (rbracket_mask as u64) << shift;
    }

    result
}

/// Convert NEON comparison result to a 16-bit bitmask
///
/// NEON vceqq_u8 produces 0xFF for matches and 0x00 for misses.
/// We need to extract the high bit of each byte into a 16-bit mask.
#[cfg(target_arch = "aarch64")]
#[inline]
fn neon_to_bitmask(cmp: uint8x16_t) -> u16 {
    // Use NEON's shrn instruction to narrow and shift
    // Then extract the packed result
    unsafe {
        // Shift each byte right by 7 to get just the high bit (0x01 or 0x00)
        let shifted = vshrq_n_u8::<7>(cmp);

        // Pack pairs of bytes: take bit 0 of each byte
        // Use the vsli approach to merge adjacent bits
        let powers: uint8x16_t =
            vld1q_u8([1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128].as_ptr());

        // Multiply each bit position by its power of 2
        let weighted = vmulq_u8(shifted, powers);

        // Sum the low 8 bytes and high 8 bytes separately
        let low_half = vget_low_u8(weighted);
        let high_half = vget_high_u8(weighted);

        // Horizontal add to get the final 8-bit values
        let low_sum = vaddv_u8(low_half) as u16;
        let high_sum = vaddv_u8(high_half) as u16;

        low_sum | (high_sum << 8)
    }
}

// ============================================================================
// Scalar fallback
// ============================================================================

/// Fallback scalar implementation for building character bitmaps
#[inline]
fn build_character_bitmaps_scalar(chunk: &[u8]) -> CharacterBitmaps {
    let mut result = CharacterBitmaps::default();

    for (i, &byte) in chunk.iter().enumerate().take(64) {
        let bit = 1u64 << i;
        match byte {
            b'"' => result.quote |= bit,
            b'\\' => result.backslash |= bit,
            b':' => result.colon |= bit,
            b',' => result.comma |= bit,
            b'{' => result.lbrace |= bit,
            b'}' => result.rbrace |= bit,
            b'[' => result.lbracket |= bit,
            b']' => result.rbracket |= bit,
            _ => {}
        }
    }

    result
}

/// Build character bitmaps using best available SIMD
///
/// Automatically selects AVX2, SSE4.2, NEON, or scalar based on architecture
/// and runtime detection (for x86_64).
#[inline]
pub fn build_character_bitmaps(data: &[u8], offset: usize) -> CharacterBitmaps {
    // Ensure we have a full 64-byte chunk
    if offset + 64 <= data.len() {
        let chunk: &[u8; 64] = data[offset..offset + 64].try_into().unwrap();

        #[cfg(target_arch = "x86_64")]
        {
            match get_simd_capability() {
                SimdCapability::Avx2 => {
                    // SAFETY: we checked AVX2 is available via runtime detection
                    return unsafe { build_character_bitmaps_avx2(chunk) };
                }
                SimdCapability::Sse42 => {
                    // SAFETY: we checked SSE4.2 is available via runtime detection
                    return unsafe { build_character_bitmaps_sse42(chunk) };
                }
                SimdCapability::Scalar => {
                    return build_character_bitmaps_scalar(chunk);
                }
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON is mandatory on aarch64, always use it
            return build_character_bitmaps_neon(chunk);
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            return build_character_bitmaps_scalar(chunk);
        }
    }

    // Fallback for partial chunks
    let end = std::cmp::min(offset + 64, data.len());
    build_character_bitmaps_scalar(&data[offset..end])
}

/// Bitwise manipulation: Remove rightmost 1-bit
/// R(x) = x & (x - 1)
#[inline(always)]
pub const fn remove_rightmost_one(x: u64) -> u64 {
    x & x.wrapping_sub(1)
}

/// Bitwise manipulation: Extract rightmost 1-bit
/// E(x) = x & -x (isolates the lowest set bit)
#[inline(always)]
pub const fn extract_rightmost_one(x: u64) -> u64 {
    x & x.wrapping_neg()
}

/// Bitwise manipulation: Smear rightmost 1-bit to the right
/// S(x) = x ^ (x - 1) (all bits from rightmost 1 to LSB are set)
#[inline(always)]
pub const fn smear_rightmost_one(x: u64) -> u64 {
    x ^ x.wrapping_sub(1)
}

/// Count trailing zeros (position of rightmost 1-bit)
#[inline(always)]
pub const fn trailing_zeros(x: u64) -> u32 {
    x.trailing_zeros()
}

/// Population count (number of 1-bits)
#[inline(always)]
pub const fn popcount(x: u64) -> u32 {
    x.count_ones()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_detection() {
        let name = simd_capability_name();
        println!("Detected SIMD capability: {}", name);
        // Should be one of the known values
        assert!(
            name == "AVX2" || name == "SSE4.2" || name == "NEON" || name == "Scalar",
            "Unknown SIMD capability: {}",
            name
        );
    }

    #[test]
    fn test_bitwise_remove_rightmost() {
        assert_eq!(remove_rightmost_one(0b11101000), 0b11100000);
        assert_eq!(remove_rightmost_one(0b00000001), 0b00000000);
        assert_eq!(remove_rightmost_one(0b10101010), 0b10101000);
    }

    #[test]
    fn test_bitwise_extract_rightmost() {
        assert_eq!(extract_rightmost_one(0b11101000), 0b00001000);
        assert_eq!(extract_rightmost_one(0b00000001), 0b00000001);
        assert_eq!(extract_rightmost_one(0b10101010), 0b00000010);
    }

    #[test]
    fn test_bitwise_smear_rightmost() {
        assert_eq!(smear_rightmost_one(0b11101000), 0b00001111);
        assert_eq!(smear_rightmost_one(0b00000001), 0b00000001);
        assert_eq!(smear_rightmost_one(0b01000000), 0b01111111);
    }

    #[test]
    fn test_scalar_character_detection() {
        let json = br#"{"id":"test","value":123}"#;
        let mut padded = [0u8; 64];
        padded[..json.len()].copy_from_slice(json);

        let bitmaps = build_character_bitmaps_scalar(&padded);

        // Check that we found the structural characters
        assert!(bitmaps.lbrace != 0, "Should find left brace");
        assert!(bitmaps.rbrace != 0, "Should find right brace");
        assert!(bitmaps.colon != 0, "Should find colons");
        assert!(bitmaps.quote != 0, "Should find quotes");
        assert!(bitmaps.comma != 0, "Should find comma");

        // Verify specific positions
        // {"id":"test","value":123}
        // 0123456789...
        // { = 0, " = 1, i = 2, d = 3, " = 4, : = 5
        assert_eq!(bitmaps.lbrace & 1, 1, "Left brace at position 0");
        assert_eq!((bitmaps.colon >> 5) & 1, 1, "First colon at position 5");
    }

    #[test]
    fn test_popcount() {
        assert_eq!(popcount(0b11101000), 4);
        assert_eq!(popcount(0), 0);
        assert_eq!(popcount(u64::MAX), 64);
    }
}
