// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Schema-guided field extraction
//!
//! Extracts fields directly from JSON bytes to typed values using the structural index.
//! No intermediate serde_json::Value allocation - values go straight to Arrow builders.
//!
//! Based on Mison Section 5: QUERY EXECUTION

use super::index::StructuralIndex;
use super::pattern::PatternTree;

/// Error during field extraction
#[derive(Debug, Clone)]
pub enum ExtractError {
    /// Field not found at expected position
    FieldNotFound(String),
    /// Invalid JSON value format
    InvalidValue(String),
    /// Nesting too deep
    MaxDepthExceeded,
    /// Unexpected end of data
    UnexpectedEof,
}

/// Extracted value from JSON without allocation
#[derive(Debug, Clone)]
pub enum ExtractedValue<'a> {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (i64)
    Int(i64),
    /// Floating point value (f64)
    Float(f64),
    /// String value (borrowed slice, excluding quotes)
    String(&'a [u8]),
    /// Object value (borrowed slice including braces)
    Object(&'a [u8]),
    /// Array value (borrowed slice including brackets)
    Array(&'a [u8]),
}

impl<'a> ExtractedValue<'a> {
    /// Check if value is null
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ExtractedValue::Null)
    }

    /// Try to get as boolean
    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ExtractedValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as i64
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ExtractedValue::Int(i) => Some(*i),
            ExtractedValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to get as f64
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ExtractedValue::Float(f) => Some(*f),
            ExtractedValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as string bytes
    #[inline]
    pub fn as_str_bytes(&self) -> Option<&'a [u8]> {
        match self {
            ExtractedValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as UTF-8 string
    #[inline]
    pub fn as_str(&self) -> Option<&'a str> {
        self.as_str_bytes()
            .and_then(|b| std::str::from_utf8(b).ok())
    }
}

/// Schema for field extraction
#[derive(Debug, Clone)]
pub struct FieldSchema {
    /// Field name (or path like "user.id")
    pub name: String,
    /// Path components for nested access
    pub path: Vec<String>,
    /// Expected type (for optimization hints)
    pub expected_type: FieldType,
    /// Whether field is required (affects error handling)
    pub required: bool,
}

/// Expected field type for optimization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Bool,
    Int,
    Float,
    String,
    Object,
    Array,
    Any,
}

impl FieldSchema {
    /// Create a simple top-level field schema
    pub fn new(name: &str, expected_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            path: vec![name.to_string()],
            expected_type,
            required: false,
        }
    }

    /// Create a nested field schema (e.g., "user.id")
    pub fn nested(path: &str, expected_type: FieldType) -> Self {
        let parts: Vec<String> = path.split('.').map(String::from).collect();
        Self {
            name: path.to_string(),
            path: parts,
            expected_type,
            required: false,
        }
    }

    /// Mark field as required
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

/// Schema-guided field extractor
///
/// Uses structural index + pattern tree to extract fields directly from JSON bytes.
pub struct SchemaExtractor {
    /// Fields to extract
    fields: Vec<FieldSchema>,
    /// Pattern tree for speculative access
    pattern_tree: PatternTree,
}

impl SchemaExtractor {
    /// Create a new extractor for the given fields
    pub fn new(fields: Vec<FieldSchema>) -> Self {
        let pattern_tree = PatternTree::new();
        Self {
            fields,
            pattern_tree,
        }
    }

    /// Number of fields this extractor is configured to extract
    #[inline]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Create from ClickHouse column schema
    pub fn from_columns(columns: &[(String, String)]) -> Self {
        let fields: Vec<FieldSchema> = columns
            .iter()
            .map(|(name, type_str)| {
                let field_type = Self::clickhouse_type_to_field_type(type_str);
                if name.contains('.') {
                    FieldSchema::nested(name, field_type)
                } else {
                    FieldSchema::new(name, field_type)
                }
            })
            .collect();

        Self::new(fields)
    }

    /// Map ClickHouse type to FieldType
    fn clickhouse_type_to_field_type(type_str: &str) -> FieldType {
        let type_lower = type_str.to_lowercase();
        if type_lower.starts_with("bool") {
            FieldType::Bool
        } else if type_lower.starts_with("int")
            || type_lower.starts_with("uint")
            || type_lower.contains("int8")
            || type_lower.contains("int16")
            || type_lower.contains("int32")
            || type_lower.contains("int64")
        {
            FieldType::Int
        } else if type_lower.starts_with("float")
            || type_lower.starts_with("decimal")
            || type_lower.contains("float32")
            || type_lower.contains("float64")
        {
            FieldType::Float
        } else if type_lower.starts_with("string")
            || type_lower.starts_with("fixedstring")
            || type_lower.starts_with("uuid")
            || type_lower.starts_with("datetime")
            || type_lower.starts_with("date")
        {
            FieldType::String
        } else if type_lower.starts_with("array") {
            FieldType::Array
        } else if type_lower.starts_with("tuple")
            || type_lower.starts_with("map")
            || type_lower.starts_with("nested")
            || type_lower.starts_with("json")
        {
            FieldType::Object
        } else {
            FieldType::Any
        }
    }

    /// Extract all fields from a JSON document
    ///
    /// Returns a vector of extracted values in the same order as the field schemas.
    pub fn extract_all<'a>(
        &mut self,
        index: &StructuralIndex,
        data: &'a [u8],
    ) -> Vec<Result<ExtractedValue<'a>, ExtractError>> {
        let mut results = Vec::with_capacity(self.fields.len());
        for i in 0..self.fields.len() {
            let field = &self.fields[i].clone();
            results.push(self.extract_field(index, data, field));
        }
        results
    }

    /// Single-pass batch extraction of all top-level fields
    ///
    /// OPTIMIZED: Iterates through colons ONCE, matching all target fields in parallel.
    /// Complexity: O(colons + fields) instead of O(colons * fields)
    ///
    /// Only works for top-level fields (path.len() == 1). Nested fields fall back
    /// to sequential extraction.
    pub fn extract_all_batch<'a>(
        &self,
        index: &StructuralIndex,
        data: &'a [u8],
    ) -> Vec<Result<ExtractedValue<'a>, ExtractError>> {
        use rustc_hash::FxHashMap;

        // Separate top-level vs nested fields
        let mut top_level_indices: Vec<usize> = Vec::new();
        let mut nested_indices: Vec<usize> = Vec::new();

        // Build lookup map: field_name -> index in results
        let mut field_lookup: FxHashMap<&[u8], usize> = FxHashMap::default();

        for (i, field) in self.fields.iter().enumerate() {
            if field.path.len() == 1 {
                top_level_indices.push(i);
                field_lookup.insert(field.path[0].as_bytes(), i);
            } else {
                nested_indices.push(i);
            }
        }

        let mut results: Vec<Option<Result<ExtractedValue<'a>, ExtractError>>> =
            vec![None; self.fields.len()];
        let mut found_count = 0;
        let target_count = top_level_indices.len();

        // Single pass through all level-0 colons
        let colons = index.colon_positions(0, 0, data.len());

        for &colon_pos in &colons {
            if found_count == target_count {
                break; // All top-level fields found
            }

            if colon_pos == 0 || colon_pos >= data.len() {
                continue;
            }

            // Find key by scanning backwards
            let mut key_end = colon_pos - 1;
            while key_end > 0 && data[key_end].is_ascii_whitespace() {
                key_end -= 1;
            }

            if data[key_end] != b'"' {
                continue;
            }

            // Find key start (opening quote)
            let mut key_start = key_end - 1;
            while key_start > 0 && data[key_start] != b'"' {
                key_start -= 1;
            }

            let key = &data[key_start + 1..key_end];

            // Check if this is one of our target fields
            if let Some(&result_idx) = field_lookup.get(key) {
                if results[result_idx].is_some() {
                    continue; // Already found this field
                }

                // Find value start
                let mut value_start = colon_pos + 1;
                while value_start < data.len() && data[value_start].is_ascii_whitespace() {
                    value_start += 1;
                }

                if value_start >= data.len() {
                    results[result_idx] = Some(Err(ExtractError::UnexpectedEof));
                    found_count += 1;
                    continue;
                }

                // Find value end
                let value_end = match self.find_value_end(data, index, value_start, data.len()) {
                    Some(end) => end,
                    None => {
                        results[result_idx] = Some(Err(ExtractError::UnexpectedEof));
                        found_count += 1;
                        continue;
                    }
                };

                // Extract the value
                results[result_idx] = Some(self.extract_value(data, value_start, value_end));
                found_count += 1;
            }
        }

        // Mark unfound top-level fields as not found
        for &idx in &top_level_indices {
            if results[idx].is_none() {
                results[idx] = Some(Err(ExtractError::FieldNotFound(
                    self.fields[idx].name.clone(),
                )));
            }
        }

        // Handle nested fields with sequential extraction (need to navigate levels)
        // For nested fields, we could optimize further but it's more complex
        for &idx in &nested_indices {
            // Clone needed to avoid borrow issues
            let field = self.fields[idx].clone();
            results[idx] = Some(self.extract_nested_field(index, data, &field));
        }

        // Convert Option<Result> to Result
        results
            .into_iter()
            .map(|opt| opt.unwrap_or(Err(ExtractError::FieldNotFound("unknown".to_string()))))
            .collect()
    }

    /// Extract a nested field (field with path.len() > 1)
    fn extract_nested_field<'a>(
        &self,
        index: &StructuralIndex,
        data: &'a [u8],
        field: &FieldSchema,
    ) -> Result<ExtractedValue<'a>, ExtractError> {
        if field.path.is_empty() {
            return Err(ExtractError::FieldNotFound(field.name.clone()));
        }

        // Navigate through each path component
        let mut start = 0;
        let mut end = data.len();
        let mut level = 0;

        for (path_idx, path_component) in field.path.iter().enumerate() {
            let is_last = path_idx == field.path.len() - 1;

            // Sequential search at this level
            let (field_start, field_end) =
                self.find_field_at_level(index, data, level, start, end, path_component)?;

            if is_last {
                return self.extract_value(data, field_start, field_end);
            } else {
                // Navigate into nested object
                start = field_start;
                end = field_end;
                level += 1;
            }
        }

        Err(ExtractError::FieldNotFound(field.name.clone()))
    }

    /// Find a field at a specific level (helper for nested extraction)
    fn find_field_at_level(
        &self,
        index: &StructuralIndex,
        data: &[u8],
        level: usize,
        start: usize,
        end: usize,
        field_name: &str,
    ) -> Result<(usize, usize), ExtractError> {
        let colons = index.colon_positions(level, start, end);

        for &colon_pos in &colons {
            // Find key before colon
            let Some(key_start) = self.find_key_start(data, colon_pos) else {
                continue;
            };

            if key_start + 1 >= colon_pos {
                continue;
            }

            let key_end = self.find_quote_before(data, colon_pos)?;
            if key_end <= key_start + 1 {
                continue;
            }

            let key_bytes = &data[key_start + 1..key_end];

            if key_bytes == field_name.as_bytes() {
                let value_start = self.skip_whitespace(data, colon_pos + 1);
                let value_end = self
                    .find_value_end(data, index, value_start, end)
                    .ok_or_else(|| ExtractError::UnexpectedEof)?;

                return Ok((value_start, value_end));
            }
        }

        Err(ExtractError::FieldNotFound(field_name.to_string()))
    }

    /// Extract a single field from a JSON document
    pub fn extract_field<'a>(
        &mut self,
        index: &StructuralIndex,
        data: &'a [u8],
        field: &FieldSchema,
    ) -> Result<ExtractedValue<'a>, ExtractError> {
        if field.path.is_empty() {
            return Err(ExtractError::FieldNotFound(field.name.clone()));
        }

        // Start from document root
        let mut start = 0;
        let mut end = data.len();
        let mut level = 0;

        // Navigate through each path component
        for (path_idx, path_component) in field.path.iter().enumerate() {
            let is_last = path_idx == field.path.len() - 1;

            // Try speculative access first (pattern tree)
            let value_range = if let Some(hint) =
                self.pattern_tree.get_hint(&field.path[..=path_idx])
            {
                self.try_speculative_access(index, data, level, start, end, path_component, hint)
            } else {
                None
            };

            // Fall back to sequential search if speculation fails
            let (field_start, field_end) = match value_range {
                Some(range) => range,
                None => {
                    self.find_field_sequential(index, data, level, start, end, path_component)?
                }
            };

            if is_last {
                // Extract the value
                return self.extract_value(data, field_start, field_end);
            } else {
                // Navigate into nested object
                start = field_start;
                end = field_end;
                level += 1;
            }
        }

        Err(ExtractError::FieldNotFound(field.name.clone()))
    }

    /// Try speculative field access using pattern hint
    fn try_speculative_access(
        &self,
        index: &StructuralIndex,
        data: &[u8],
        level: usize,
        start: usize,
        end: usize,
        field_name: &str,
        hint_index: usize,
    ) -> Option<(usize, usize)> {
        // Get the N-th colon at this level
        let colon_pos = index.nth_colon(level, hint_index, start, end)?;

        // Verify this is the right field by checking the key
        let key_start = self.find_key_start(data, colon_pos)?;
        let key_end = colon_pos;

        // Extract and compare key (excluding quotes)
        let key_bytes = &data[key_start + 1..key_end - 1];
        if key_bytes == field_name.as_bytes() {
            // Speculation succeeded! Find value range
            let value_start = self.skip_whitespace(data, colon_pos + 1);
            let value_end = self.find_value_end(data, index, value_start, end)?;
            Some((value_start, value_end))
        } else {
            // Speculation failed, will fall back to sequential
            None
        }
    }

    /// Find field by sequential search through colons at this level
    fn find_field_sequential(
        &mut self,
        index: &StructuralIndex,
        data: &[u8],
        level: usize,
        start: usize,
        end: usize,
        field_name: &str,
    ) -> Result<(usize, usize), ExtractError> {
        let colons = index.colon_positions(level, start, end);

        for (field_idx, &colon_pos) in colons.iter().enumerate() {
            // Find key before colon
            let Some(key_start) = self.find_key_start(data, colon_pos) else {
                continue;
            };

            // Extract key (between quotes)
            if key_start + 1 >= colon_pos {
                continue;
            }

            // Find end quote
            let key_end = self.find_quote_before(data, colon_pos)?;
            if key_end <= key_start + 1 {
                continue;
            }

            let key_bytes = &data[key_start + 1..key_end];

            if key_bytes == field_name.as_bytes() {
                // Found it! Update pattern tree for future speculation
                self.pattern_tree
                    .record_access(&[field_name.to_string()], field_idx);

                // Find value range
                let value_start = self.skip_whitespace(data, colon_pos + 1);
                let value_end = self
                    .find_value_end(data, index, value_start, end)
                    .ok_or_else(|| ExtractError::UnexpectedEof)?;

                return Ok((value_start, value_end));
            }
        }

        Err(ExtractError::FieldNotFound(field_name.to_string()))
    }

    /// Find the starting quote of a key before a colon
    fn find_key_start(&self, data: &[u8], colon_pos: usize) -> Option<usize> {
        // Scan backwards for the opening quote
        let mut pos = colon_pos.checked_sub(1)?;

        // Skip whitespace after key
        while pos > 0 && data[pos].is_ascii_whitespace() {
            pos -= 1;
        }

        // Should be at closing quote
        if data[pos] != b'"' {
            return None;
        }
        let _closing_quote = pos;

        // Find opening quote (skip string content)
        pos = pos.checked_sub(1)?;
        while pos > 0 {
            if data[pos] == b'"' {
                // Check if escaped
                let mut backslash_count = 0;
                let mut check_pos = pos;
                while check_pos > 0 && data[check_pos - 1] == b'\\' {
                    backslash_count += 1;
                    check_pos -= 1;
                }
                if backslash_count % 2 == 0 {
                    return Some(pos);
                }
            }
            pos -= 1;
        }

        // Check position 0
        if data[0] == b'"' {
            Some(0)
        } else {
            None
        }
    }

    /// Find closing quote before colon
    fn find_quote_before(&self, data: &[u8], colon_pos: usize) -> Result<usize, ExtractError> {
        let mut pos = colon_pos.saturating_sub(1);

        // Skip whitespace
        while pos > 0 && data[pos].is_ascii_whitespace() {
            pos -= 1;
        }

        if data[pos] == b'"' {
            Ok(pos)
        } else {
            Err(ExtractError::InvalidValue(
                "Expected closing quote".to_string(),
            ))
        }
    }

    /// Skip whitespace characters
    #[inline]
    fn skip_whitespace(&self, data: &[u8], start: usize) -> usize {
        let mut pos = start;
        while pos < data.len() && data[pos].is_ascii_whitespace() {
            pos += 1;
        }
        pos
    }

    /// Find the end of a JSON value
    fn find_value_end(
        &self,
        data: &[u8],
        index: &StructuralIndex,
        start: usize,
        _container_end: usize,
    ) -> Option<usize> {
        if start >= data.len() {
            return None;
        }

        match data[start] {
            b'"' => {
                // String: find closing quote
                let mut pos = start + 1;
                while pos < data.len() {
                    if data[pos] == b'"' {
                        // Check if escaped
                        let mut backslash_count = 0;
                        let mut check_pos = pos;
                        while check_pos > start + 1 && data[check_pos - 1] == b'\\' {
                            backslash_count += 1;
                            check_pos -= 1;
                        }
                        if backslash_count % 2 == 0 {
                            return Some(pos + 1);
                        }
                    }
                    pos += 1;
                }
                None
            }
            b'{' | b'[' => {
                // Object or array: use structural index to find extent
                index.find_extent(data, start)
            }
            b't' => {
                // true
                if start + 4 <= data.len() && &data[start..start + 4] == b"true" {
                    Some(start + 4)
                } else {
                    None
                }
            }
            b'f' => {
                // false
                if start + 5 <= data.len() && &data[start..start + 5] == b"false" {
                    Some(start + 5)
                } else {
                    None
                }
            }
            b'n' => {
                // null
                if start + 4 <= data.len() && &data[start..start + 4] == b"null" {
                    Some(start + 4)
                } else {
                    None
                }
            }
            b'-' | b'0'..=b'9' => {
                // Number: scan until non-numeric character
                let mut pos = start;
                while pos < data.len() {
                    match data[pos] {
                        b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9' => pos += 1,
                        _ => break,
                    }
                }
                Some(pos)
            }
            _ => None,
        }
    }

    /// Extract a typed value from the given byte range
    fn extract_value<'a>(
        &self,
        data: &'a [u8],
        start: usize,
        end: usize,
    ) -> Result<ExtractedValue<'a>, ExtractError> {
        if start >= end || start >= data.len() {
            return Err(ExtractError::UnexpectedEof);
        }

        match data[start] {
            b'"' => {
                // String value (exclude quotes)
                if end > start + 1 {
                    Ok(ExtractedValue::String(&data[start + 1..end - 1]))
                } else {
                    Ok(ExtractedValue::String(&[]))
                }
            }
            b'{' => Ok(ExtractedValue::Object(&data[start..end])),
            b'[' => Ok(ExtractedValue::Array(&data[start..end])),
            b't' => Ok(ExtractedValue::Bool(true)),
            b'f' => Ok(ExtractedValue::Bool(false)),
            b'n' => Ok(ExtractedValue::Null),
            b'-' | b'0'..=b'9' => {
                // Parse number
                let num_str = std::str::from_utf8(&data[start..end]).map_err(|_| {
                    ExtractError::InvalidValue("Invalid UTF-8 in number".to_string())
                })?;

                if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
                    let f: f64 = num_str.parse().map_err(|_| {
                        ExtractError::InvalidValue(format!("Invalid float: {}", num_str))
                    })?;
                    Ok(ExtractedValue::Float(f))
                } else {
                    let i: i64 = num_str.parse().map_err(|_| {
                        ExtractError::InvalidValue(format!("Invalid integer: {}", num_str))
                    })?;
                    Ok(ExtractedValue::Int(i))
                }
            }
            _ => Err(ExtractError::InvalidValue(format!(
                "Unknown value type at position {}",
                start
            ))),
        }
    }
}

/// Single-field extractor for common hot-path cases
///
/// Optimized for extracting just one or two fields (like db.table routing).
pub struct FieldExtractor;

impl FieldExtractor {
    /// Extract a top-level string field from JSON bytes
    ///
    /// This is the fastest path for simple field extraction.
    /// Returns None if field not found or not a string.
    #[inline]
    pub fn extract_string<'a>(
        index: &StructuralIndex,
        data: &'a [u8],
        field_name: &str,
    ) -> Option<&'a str> {
        let colons = index.colon_positions(0, 0, data.len());

        for &colon_pos in &colons {
            // Quick bounds check
            if colon_pos == 0 || colon_pos >= data.len() {
                continue;
            }

            // Find key by scanning backwards
            let mut key_end = colon_pos - 1;
            while key_end > 0 && data[key_end].is_ascii_whitespace() {
                key_end -= 1;
            }

            if data[key_end] != b'"' {
                continue;
            }

            // Find key start
            let mut key_start = key_end - 1;
            while key_start > 0 && data[key_start] != b'"' {
                key_start -= 1;
            }

            // Compare key
            let key = &data[key_start + 1..key_end];
            if key != field_name.as_bytes() {
                continue;
            }

            // Found it! Extract string value
            let mut value_start = colon_pos + 1;
            while value_start < data.len() && data[value_start].is_ascii_whitespace() {
                value_start += 1;
            }

            if value_start >= data.len() || data[value_start] != b'"' {
                return None; // Not a string value
            }

            // Find closing quote
            let mut value_end = value_start + 1;
            while value_end < data.len() {
                if data[value_end] == b'"' {
                    // Check for escape
                    let mut bs = 0;
                    let mut check = value_end;
                    while check > value_start + 1 && data[check - 1] == b'\\' {
                        bs += 1;
                        check -= 1;
                    }
                    if bs % 2 == 0 {
                        break;
                    }
                }
                value_end += 1;
            }

            // Return string content (without quotes)
            return std::str::from_utf8(&data[value_start + 1..value_end]).ok();
        }

        None
    }

    /// Extract a nested string field (e.g., "tags.event.org_id")
    pub fn extract_nested_string<'a>(
        index: &StructuralIndex,
        data: &'a [u8],
        path: &[&str],
    ) -> Option<&'a str> {
        if path.is_empty() {
            return None;
        }

        if path.len() == 1 {
            return Self::extract_string(index, data, path[0]);
        }

        // For nested paths, we need to navigate level by level
        let mut start = 0;
        let mut end = data.len();

        for (level, &field_name) in path.iter().enumerate() {
            let is_last = level == path.len() - 1;
            let colons = index.colon_positions(level, start, end);

            let mut found = false;
            for &colon_pos in &colons {
                if colon_pos <= start || colon_pos >= end {
                    continue;
                }

                // Find and compare key
                let mut key_end = colon_pos - 1;
                while key_end > start && data[key_end].is_ascii_whitespace() {
                    key_end -= 1;
                }

                if data[key_end] != b'"' {
                    continue;
                }

                let mut key_start = key_end - 1;
                while key_start > start && data[key_start] != b'"' {
                    key_start -= 1;
                }

                let key = &data[key_start + 1..key_end];
                if key != field_name.as_bytes() {
                    continue;
                }

                // Found field
                let mut value_start = colon_pos + 1;
                while value_start < end && data[value_start].is_ascii_whitespace() {
                    value_start += 1;
                }

                if is_last {
                    // Extract string value
                    if data[value_start] != b'"' {
                        return None;
                    }

                    let mut value_end = value_start + 1;
                    while value_end < end && data[value_end] != b'"' {
                        value_end += 1;
                    }

                    return std::str::from_utf8(&data[value_start + 1..value_end]).ok();
                } else {
                    // Navigate into nested object
                    if data[value_start] != b'{' {
                        return None;
                    }

                    // Find object extent
                    let value_end = index.find_extent(data, value_start)?;
                    start = value_start;
                    end = value_end;
                    found = true;
                    break;
                }
            }

            if !found && !is_last {
                return None;
            }
        }

        None
    }

    /// Extract multiple fields at once (batch extraction)
    ///
    /// More efficient than calling extract_string multiple times.
    pub fn extract_strings<'a>(
        index: &StructuralIndex,
        data: &'a [u8],
        field_names: &[&str],
    ) -> Vec<Option<&'a str>> {
        let mut results: Vec<Option<&'a str>> = vec![None; field_names.len()];
        let mut found_count = 0;

        let colons = index.colon_positions(0, 0, data.len());

        for &colon_pos in &colons {
            if found_count == field_names.len() {
                break; // All fields found
            }

            if colon_pos == 0 || colon_pos >= data.len() {
                continue;
            }

            // Find key
            let mut key_end = colon_pos - 1;
            while key_end > 0 && data[key_end].is_ascii_whitespace() {
                key_end -= 1;
            }

            if data[key_end] != b'"' {
                continue;
            }

            let mut key_start = key_end - 1;
            while key_start > 0 && data[key_start] != b'"' {
                key_start -= 1;
            }

            let key = &data[key_start + 1..key_end];

            // Check if this is one of our target fields
            for (i, &field_name) in field_names.iter().enumerate() {
                if results[i].is_some() {
                    continue; // Already found
                }

                if key == field_name.as_bytes() {
                    // Extract string value
                    let mut value_start = colon_pos + 1;
                    while value_start < data.len() && data[value_start].is_ascii_whitespace() {
                        value_start += 1;
                    }

                    if value_start < data.len() && data[value_start] == b'"' {
                        let mut value_end = value_start + 1;
                        while value_end < data.len() && data[value_end] != b'"' {
                            value_end += 1;
                        }

                        if let Ok(s) = std::str::from_utf8(&data[value_start + 1..value_end]) {
                            results[i] = Some(s);
                            found_count += 1;
                        }
                    }
                    break;
                }
            }
        }

        results
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_string() {
        let json = br#"{"id":"test123","value":42}"#;
        let index = StructuralIndex::build(json);

        let id = FieldExtractor::extract_string(&index, json, "id");
        assert_eq!(id, Some("test123"));
    }

    #[test]
    fn test_extract_missing_field() {
        let json = br#"{"id":"test123"}"#;
        let index = StructuralIndex::build(json);

        let value = FieldExtractor::extract_string(&index, json, "missing");
        assert_eq!(value, None);
    }

    #[test]
    fn test_extract_non_string_field() {
        let json = br#"{"id":"test","count":42}"#;
        let index = StructuralIndex::build(json);

        // count is a number, not a string
        let count = FieldExtractor::extract_string(&index, json, "count");
        assert_eq!(count, None);
    }

    #[test]
    fn test_extract_nested_string() {
        let json = br#"{"user":{"id":"u123","name":"Alice"}}"#;
        let index = StructuralIndex::build(json);

        let id = FieldExtractor::extract_nested_string(&index, json, &["user", "id"]);
        assert_eq!(id, Some("u123"));

        let name = FieldExtractor::extract_nested_string(&index, json, &["user", "name"]);
        assert_eq!(name, Some("Alice"));
    }

    #[test]
    fn test_extract_multiple_fields() {
        let json = br#"{"org_id":"acme","event_category":"auth","timestamp":"2024-01-01"}"#;
        let index = StructuralIndex::build(json);

        let results = FieldExtractor::extract_strings(&index, json, &["org_id", "event_category"]);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some("acme"));
        assert_eq!(results[1], Some("auth"));
    }

    #[test]
    fn test_schema_extractor_simple() {
        let json = br#"{"id":"test123","value":42,"active":true}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![
            FieldSchema::new("id", FieldType::String),
            FieldSchema::new("value", FieldType::Int),
            FieldSchema::new("active", FieldType::Bool),
        ];

        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        assert_eq!(results.len(), 3);

        match &results[0] {
            Ok(ExtractedValue::String(s)) => assert_eq!(*s, b"test123"),
            other => panic!("Expected String, got {:?}", other),
        }

        match &results[1] {
            Ok(ExtractedValue::Int(i)) => assert_eq!(*i, 42),
            other => panic!("Expected Int, got {:?}", other),
        }

        match &results[2] {
            Ok(ExtractedValue::Bool(b)) => assert!(*b),
            other => panic!("Expected Bool, got {:?}", other),
        }
    }

    #[test]
    fn test_schema_extractor_nested() {
        let json = br#"{"user":{"id":"u123"},"count":5}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![
            FieldSchema::nested("user.id", FieldType::String),
            FieldSchema::new("count", FieldType::Int),
        ];

        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        assert_eq!(results.len(), 2);

        match &results[0] {
            Ok(ExtractedValue::String(s)) => assert_eq!(*s, b"u123"),
            other => panic!("Expected String, got {:?}", other),
        }

        match &results[1] {
            Ok(ExtractedValue::Int(i)) => assert_eq!(*i, 5),
            other => panic!("Expected Int, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_with_spaces() {
        let json = br#"{ "id" : "test" , "value" : 123 }"#;
        let index = StructuralIndex::build(json);

        let id = FieldExtractor::extract_string(&index, json, "id");
        assert_eq!(id, Some("test"));
    }

    #[test]
    fn test_extract_null_value() {
        let json = br#"{"id":null,"value":"test"}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![FieldSchema::new("id", FieldType::String)];
        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        match &results[0] {
            Ok(ExtractedValue::Null) => {}
            other => panic!("Expected Null, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_object_value() {
        let json = br#"{"user":{"id":123,"name":"test"}}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![FieldSchema::new("user", FieldType::Object)];
        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        match &results[0] {
            Ok(ExtractedValue::Object(obj)) => {
                assert!(obj.starts_with(b"{"));
                assert!(obj.ends_with(b"}"));
            }
            other => panic!("Expected Object, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_array_value() {
        let json = br#"{"items":[1,2,3]}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![FieldSchema::new("items", FieldType::Array)];
        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        match &results[0] {
            Ok(ExtractedValue::Array(arr)) => {
                assert_eq!(*arr, b"[1,2,3]");
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_float() {
        let json = br#"{"pi":3.14159,"e":2.71828e0}"#;
        let index = StructuralIndex::build(json);

        let fields = vec![
            FieldSchema::new("pi", FieldType::Float),
            FieldSchema::new("e", FieldType::Float),
        ];
        let mut extractor = SchemaExtractor::new(fields);
        let results = extractor.extract_all(&index, json);

        match &results[0] {
            Ok(ExtractedValue::Float(f)) => assert!((f - 3.14159).abs() < 0.0001),
            other => panic!("Expected Float, got {:?}", other),
        }

        match &results[1] {
            Ok(ExtractedValue::Float(f)) => assert!((f - 2.71828).abs() < 0.0001),
            other => panic!("Expected Float, got {:?}", other),
        }
    }
}
