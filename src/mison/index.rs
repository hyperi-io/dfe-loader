// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Structural Index for JSON documents
//!
//! Implements Mison's structural index: leveled bitmaps that map logical field
//! positions to physical byte offsets without full DOM parsing.
//!
//! Based on Mison Section 4: STRUCTURAL INDEX

use super::simd::{
    build_character_bitmaps, extract_rightmost_one, popcount, remove_rightmost_one,
    smear_rightmost_one, WORD_SIZE,
};

/// Maximum nesting depth supported
pub const MAX_DEPTH: usize = 16;

/// Leveled bitmaps for structural characters at each nesting level
#[derive(Debug, Clone)]
pub struct LeveledBitmaps {
    /// Colon positions at each level (object field separators)
    pub colon: Vec<Vec<u64>>,
    /// Comma positions at each level (array element separators)
    pub comma: Vec<Vec<u64>>,
    /// Number of 64-bit words in the bitmaps
    pub word_count: usize,
}

impl LeveledBitmaps {
    /// Create empty leveled bitmaps for a given data length
    pub fn new(data_len: usize, max_level: usize) -> Self {
        let word_count = (data_len + WORD_SIZE - 1) / WORD_SIZE;
        let max_level = max_level.min(MAX_DEPTH);

        Self {
            colon: (0..max_level).map(|_| vec![0u64; word_count]).collect(),
            comma: (0..max_level).map(|_| vec![0u64; word_count]).collect(),
            word_count,
        }
    }

    /// Get colon bitmap for a specific level
    #[inline]
    pub fn colon_at_level(&self, level: usize) -> Option<&[u64]> {
        self.colon.get(level).map(|v| v.as_slice())
    }

    /// Get comma bitmap for a specific level
    #[inline]
    pub fn comma_at_level(&self, level: usize) -> Option<&[u64]> {
        self.comma.get(level).map(|v| v.as_slice())
    }
}

/// Structural index for a JSON document
///
/// Maps logical field positions (e.g., "3rd field at level 1") to physical
/// byte offsets without parsing the full document.
#[derive(Debug)]
pub struct StructuralIndex {
    /// Leveled bitmaps for colons and commas
    pub bitmaps: LeveledBitmaps,
    /// Raw structural quote bitmap (for string boundary detection)
    pub structural_quotes: Vec<u64>,
    /// String mask bitmap (1 = inside string, 0 = outside)
    pub string_mask: Vec<u64>,
    /// Maximum depth encountered in the document
    pub max_depth: usize,
    /// Total document length
    pub doc_len: usize,
}

impl StructuralIndex {
    /// Build a structural index for a JSON document
    ///
    /// This is the main entry point. Processes the document in 64-byte chunks,
    /// building leveled bitmaps for structural characters.
    ///
    /// Based on Mison Algorithm: Steps 1-4
    pub fn build(data: &[u8]) -> Self {
        Self::build_with_max_level(data, MAX_DEPTH)
    }

    /// Build structural index up to a specific nesting level
    ///
    /// Use this when you know the maximum depth of fields you need.
    /// Saves memory and computation for shallow queries.
    pub fn build_with_max_level(data: &[u8], max_level: usize) -> Self {
        let word_count = (data.len() + WORD_SIZE - 1) / WORD_SIZE;
        let max_level = max_level.min(MAX_DEPTH);

        // Step 1: Build character bitmaps for entire document
        let mut quote_bitmaps = vec![0u64; word_count];
        let mut backslash_bitmaps = vec![0u64; word_count];
        let mut colon_bitmaps = vec![0u64; word_count];
        let mut comma_bitmaps = vec![0u64; word_count];
        let mut lbrace_bitmaps = vec![0u64; word_count];
        let mut rbrace_bitmaps = vec![0u64; word_count];
        let mut lbracket_bitmaps = vec![0u64; word_count];
        let mut rbracket_bitmaps = vec![0u64; word_count];

        for word_idx in 0..word_count {
            let offset = word_idx * WORD_SIZE;
            let chars = build_character_bitmaps(data, offset);

            quote_bitmaps[word_idx] = chars.quote;
            backslash_bitmaps[word_idx] = chars.backslash;
            colon_bitmaps[word_idx] = chars.colon;
            comma_bitmaps[word_idx] = chars.comma;
            lbrace_bitmaps[word_idx] = chars.lbrace;
            rbrace_bitmaps[word_idx] = chars.rbrace;
            lbracket_bitmaps[word_idx] = chars.lbracket;
            rbracket_bitmaps[word_idx] = chars.rbracket;
        }

        // Step 2: Build structural quote bitmap (exclude escaped quotes)
        let structural_quotes = Self::build_structural_quotes(&quote_bitmaps, &backslash_bitmaps);

        // Step 3: Build string mask bitmap
        let string_mask = Self::build_string_mask(&structural_quotes);

        // Apply string mask to get structural-only characters
        let structural_colons: Vec<u64> = colon_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&c, &m)| c & !m)
            .collect();

        let structural_commas: Vec<u64> = comma_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&c, &m)| c & !m)
            .collect();

        let structural_lbraces: Vec<u64> = lbrace_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&b, &m)| b & !m)
            .collect();

        let structural_rbraces: Vec<u64> = rbrace_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&b, &m)| b & !m)
            .collect();

        let structural_lbrackets: Vec<u64> = lbracket_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&b, &m)| b & !m)
            .collect();

        let structural_rbrackets: Vec<u64> = rbracket_bitmaps
            .iter()
            .zip(string_mask.iter())
            .map(|(&b, &m)| b & !m)
            .collect();

        // Step 4: Build leveled colon and comma bitmaps
        let (leveled_colons, leveled_commas, max_depth) = Self::build_leveled_bitmaps(
            &structural_colons,
            &structural_commas,
            &structural_lbraces,
            &structural_rbraces,
            &structural_lbrackets,
            &structural_rbrackets,
            max_level,
        );

        Self {
            bitmaps: LeveledBitmaps {
                colon: leveled_colons,
                comma: leveled_commas,
                word_count,
            },
            structural_quotes,
            string_mask,
            max_depth,
            doc_len: data.len(),
        }
    }

    /// Step 2: Build structural quote bitmap
    ///
    /// A structural quote is not preceded by an odd number of backslashes.
    /// This handles escaped quotes like \"
    fn build_structural_quotes(quotes: &[u64], backslashes: &[u64]) -> Vec<u64> {
        let mut result = Vec::with_capacity(quotes.len());

        for word_idx in 0..quotes.len() {
            let quote_word = quotes[word_idx];
            let backslash_word = backslashes[word_idx];

            // Find \" sequences
            let potential_escaped = backslash_word & (quote_word >> 1);

            // For each potential escape, count consecutive backslashes
            let mut escaped_mask = 0u64;
            let mut check = potential_escaped;

            while check != 0 {
                let bit_pos = check.trailing_zeros() as usize;
                let quote_pos = bit_pos + 1;

                // Count consecutive backslashes before the quote
                let mut backslash_count = 0;
                let mut scan_pos = bit_pos;

                // Look backwards for consecutive backslashes
                while scan_pos > 0 {
                    let prev_pos = scan_pos - 1;
                    if (backslash_word >> prev_pos) & 1 == 1 {
                        backslash_count += 1;
                        scan_pos = prev_pos;
                    } else {
                        break;
                    }
                }
                backslash_count += 1; // Include the one we found

                // Odd number of backslashes means the quote is escaped
                if backslash_count % 2 == 1 && quote_pos < 64 {
                    escaped_mask |= 1u64 << quote_pos;
                }

                check = remove_rightmost_one(check);
            }

            result.push(quote_word & !escaped_mask);
        }

        result
    }

    /// Step 3: Build string mask bitmap
    ///
    /// Marks all positions inside JSON strings (between structural quotes).
    /// Based on Mison Algorithm 1: BuildStringMask
    fn build_string_mask(structural_quotes: &[u64]) -> Vec<u64> {
        let mut result = Vec::with_capacity(structural_quotes.len());
        let mut quote_count = 0u64;

        for &quote_word in structural_quotes {
            let mut m_quote = quote_word;
            let mut m_string = 0u64;

            // Iterate over each quote in the word
            while m_quote != 0 {
                // Extract and smear the rightmost 1 to create mask
                let m = smear_rightmost_one(m_quote);
                // XOR extends the string mask to include this quote boundary
                m_string ^= m;
                // Remove the processed quote
                m_quote = remove_rightmost_one(m_quote);
                quote_count += 1;
            }

            // If we've seen an odd number of quotes, we're inside a string
            // Flip the mask to indicate inside-string positions
            if quote_count % 2 == 1 {
                m_string = !m_string;
            }

            result.push(m_string);
        }

        result
    }

    /// Step 4: Build leveled colon and comma bitmaps
    ///
    /// Separates colons and commas by their nesting level.
    /// Each level's bitmap contains ONLY colons/commas at that specific depth.
    /// Based on Mison Algorithm 2: BuildLeveledColonBitmap
    ///
    /// OPTIMIZED: Uses popcount-based iteration - O(structural chars) not O(64*words)
    fn build_leveled_bitmaps(
        colons: &[u64],
        commas: &[u64],
        lbraces: &[u64],
        rbraces: &[u64],
        lbrackets: &[u64],
        rbrackets: &[u64],
        max_level: usize,
    ) -> (Vec<Vec<u64>>, Vec<Vec<u64>>, usize) {
        let word_count = colons.len();

        // Initialize leveled bitmaps - start empty
        let mut leveled_colons: Vec<Vec<u64>> =
            (0..max_level).map(|_| vec![0u64; word_count]).collect();
        let mut leveled_commas: Vec<Vec<u64>> =
            (0..max_level).map(|_| vec![0u64; word_count]).collect();

        let mut current_level = 0usize;
        let mut max_depth_seen = 0usize;

        // Process each word
        for word_idx in 0..word_count {
            let colon_word = colons[word_idx];
            let comma_word = commas[word_idx];
            let left_delims = lbraces[word_idx] | lbrackets[word_idx];
            let right_delims = rbraces[word_idx] | rbrackets[word_idx];

            // All structural characters that affect nesting
            let delims = left_delims | right_delims;

            // Fast path: no delimiters in this word - assign all colons/commas at current level
            if delims == 0 {
                if current_level > 0 && current_level <= max_level {
                    leveled_colons[current_level - 1][word_idx] = colon_word;
                    leveled_commas[current_level - 1][word_idx] = comma_word;
                }
                continue;
            }

            // Slow path: has delimiters - must process in order
            // Use popcount iteration: only visit set bits
            let mut remaining_delims = delims;
            let mut remaining_colons = colon_word;
            let mut remaining_commas = comma_word;
            let mut processed_mask = 0u64;

            while remaining_delims != 0 {
                // Get next delimiter position
                let delim_bit = extract_rightmost_one(remaining_delims);

                // Create mask for everything before this delimiter
                let before_mask = delim_bit.wrapping_sub(1) & !processed_mask;

                // Assign colons/commas before this delimiter to current level
                if current_level > 0 && current_level <= max_level {
                    leveled_colons[current_level - 1][word_idx] |= remaining_colons & before_mask;
                    leveled_commas[current_level - 1][word_idx] |= remaining_commas & before_mask;
                }

                // Clear processed bits
                remaining_colons &= !before_mask;
                remaining_commas &= !before_mask;
                processed_mask |= before_mask | delim_bit;

                // Update level based on delimiter type
                if (left_delims & delim_bit) != 0 {
                    current_level += 1;
                    max_depth_seen = max_depth_seen.max(current_level);
                } else {
                    current_level = current_level.saturating_sub(1);
                }

                remaining_delims = remove_rightmost_one(remaining_delims);
            }

            // Handle any remaining colons/commas after last delimiter
            let after_mask = !processed_mask;
            if current_level > 0 && current_level <= max_level {
                leveled_colons[current_level - 1][word_idx] |= remaining_colons & after_mask;
                leveled_commas[current_level - 1][word_idx] |= remaining_commas & after_mask;
            }
        }

        (leveled_colons, leveled_commas, max_depth_seen)
    }

    /// Get positions of colons at a specific nesting level within a byte range
    ///
    /// This is the key lookup operation: given a field index, find its byte position.
    /// Based on Mison Algorithm 3: GenerateColonPositions
    pub fn colon_positions(&self, level: usize, start: usize, end: usize) -> Vec<usize> {
        self.positions_in_range(&self.bitmaps.colon, level, start, end)
    }

    /// Get positions of commas at a specific nesting level within a byte range
    pub fn comma_positions(&self, level: usize, start: usize, end: usize) -> Vec<usize> {
        self.positions_in_range(&self.bitmaps.comma, level, start, end)
    }

    /// Generic position extraction from leveled bitmaps
    fn positions_in_range(
        &self,
        bitmaps: &[Vec<u64>],
        level: usize,
        start: usize,
        end: usize,
    ) -> Vec<usize> {
        let Some(level_bitmap) = bitmaps.get(level) else {
            return Vec::new();
        };

        let start_word = start / WORD_SIZE;
        let end_word = (end + WORD_SIZE - 1) / WORD_SIZE;

        let mut positions = Vec::new();

        for word_idx in start_word..end_word.min(level_bitmap.len()) {
            let mut word = level_bitmap[word_idx];

            while word != 0 {
                let bit = extract_rightmost_one(word);
                let offset = word_idx * WORD_SIZE + popcount(bit.wrapping_sub(1)) as usize;

                if offset >= start && offset < end {
                    positions.push(offset);
                }

                word = remove_rightmost_one(word);
            }
        }

        positions
    }

    /// Find the N-th colon at a specific level within a range
    ///
    /// Returns the byte offset of the colon, or None if not found.
    /// This is used for speculative field access.
    #[inline]
    pub fn nth_colon(&self, level: usize, n: usize, start: usize, end: usize) -> Option<usize> {
        let positions = self.colon_positions(level, start, end);
        positions.get(n).copied()
    }

    /// Find the byte range of an object/array starting at a given position
    ///
    /// Scans for matching brace/bracket to determine the extent.
    pub fn find_extent(&self, data: &[u8], start: usize) -> Option<usize> {
        if start >= data.len() {
            return None;
        }

        let opener = data[start];
        let closer = match opener {
            b'{' => b'}',
            b'[' => b']',
            _ => return None,
        };

        let mut depth = 1i32;
        let mut in_string = false;
        let mut prev_backslash = false;

        for (i, &byte) in data[start + 1..].iter().enumerate() {
            if in_string {
                if byte == b'"' && !prev_backslash {
                    in_string = false;
                }
                prev_backslash = byte == b'\\' && !prev_backslash;
            } else {
                match byte {
                    b'"' => in_string = true,
                    b if b == opener => depth += 1,
                    b if b == closer => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(start + 1 + i + 1);
                        }
                    }
                    _ => {}
                }
            }
        }

        None
    }
}

/// Iterator over colon positions at a specific level
pub struct ColonIterator<'a> {
    index: &'a StructuralIndex,
    level: usize,
    start: usize,
    end: usize,
    current_word: usize,
    current_bits: u64,
}

impl<'a> ColonIterator<'a> {
    pub fn new(index: &'a StructuralIndex, level: usize, start: usize, end: usize) -> Self {
        let start_word = start / WORD_SIZE;
        let current_bits = index
            .bitmaps
            .colon
            .get(level)
            .and_then(|l| l.get(start_word))
            .copied()
            .unwrap_or(0);

        Self {
            index,
            level,
            start,
            end,
            current_word: start_word,
            current_bits,
        }
    }
}

impl<'a> Iterator for ColonIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_bits != 0 {
                let bit = extract_rightmost_one(self.current_bits);
                let offset = self.current_word * WORD_SIZE + popcount(bit.wrapping_sub(1)) as usize;
                self.current_bits = remove_rightmost_one(self.current_bits);

                if offset >= self.start && offset < self.end {
                    return Some(offset);
                }
            } else {
                self.current_word += 1;
                if self.current_word * WORD_SIZE >= self.end {
                    return None;
                }

                self.current_bits = self
                    .index
                    .bitmaps
                    .colon
                    .get(self.level)
                    .and_then(|l| l.get(self.current_word))
                    .copied()
                    .unwrap_or(0);

                if self.current_bits == 0 && self.current_word * WORD_SIZE >= self.end {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_object() {
        let json = br#"{"id":"test","value":123}"#;
        let index = StructuralIndex::build(json);

        // Should have colons at level 0
        let colons = index.colon_positions(0, 0, json.len());
        assert_eq!(colons.len(), 2, "Should find 2 colons at level 0");
    }

    #[test]
    fn test_nested_object() {
        let json = br#"{"user":{"id":123,"name":"test"},"active":true}"#;
        let index = StructuralIndex::build(json);

        // Level 0 colons: user, active
        let level0 = index.colon_positions(0, 0, json.len());
        assert_eq!(level0.len(), 2, "Should find 2 colons at level 0");

        // Level 1 colons: id, name (inside user object)
        let level1 = index.colon_positions(1, 0, json.len());
        assert_eq!(level1.len(), 2, "Should find 2 colons at level 1");
    }

    #[test]
    fn test_string_with_colon() {
        let json = br#"{"url":"http://example.com:8080"}"#;
        let index = StructuralIndex::build(json);

        // Only 1 structural colon (the one after "url")
        // The colons in the URL are inside a string
        let colons = index.colon_positions(0, 0, json.len());
        assert_eq!(colons.len(), 1, "Should find only 1 structural colon");
    }

    #[test]
    fn test_escaped_quote() {
        let json = br#"{"id":"test\"value"}"#;
        let index = StructuralIndex::build(json);

        // Should correctly handle escaped quote
        let colons = index.colon_positions(0, 0, json.len());
        assert_eq!(colons.len(), 1, "Should find 1 colon");
    }

    #[test]
    fn test_array_with_objects() {
        // JSON: {"items":[{"a":1},{"b":2}]}
        // Nesting: { = level 1, [ = level 2, inner { = level 3
        // So "items" colon is at level 0 (inside outer object)
        // And "a", "b" colons are at level 2 (inside inner objects which are in array)
        let json = br#"{"items":[{"a":1},{"b":2}]}"#;
        let index = StructuralIndex::build(json);

        // Level 0: items (inside outer object at depth 1)
        let level0 = index.colon_positions(0, 0, json.len());
        assert_eq!(level0.len(), 1, "Should find 1 colon at level 0");

        // Level 2: a, b (inside objects which are inside array)
        // Structure: {outer} -> [array] -> {inner}
        // Depth:     1          2           3
        // Level index: 0        1           2
        let level2 = index.colon_positions(2, 0, json.len());
        assert_eq!(level2.len(), 2, "Should find 2 colons at level 2");
    }

    #[test]
    fn test_nth_colon() {
        let json = br#"{"a":1,"b":2,"c":3}"#;
        let index = StructuralIndex::build(json);

        let first = index.nth_colon(0, 0, 0, json.len());
        let second = index.nth_colon(0, 1, 0, json.len());
        let third = index.nth_colon(0, 2, 0, json.len());
        let fourth = index.nth_colon(0, 3, 0, json.len());

        assert!(first.is_some());
        assert!(second.is_some());
        assert!(third.is_some());
        assert!(fourth.is_none());
    }

    #[test]
    fn test_find_extent() {
        let json = br#"{"user":{"id":123},"active":true}"#;
        let index = StructuralIndex::build(json);

        // Find extent of inner object starting at position of {
        let inner_start = json.iter().position(|&b| b == b'{').unwrap();
        let extent = index.find_extent(json, inner_start);
        assert!(extent.is_some());
    }
}
