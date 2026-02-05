// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Mison-style JSON processing
//!
//! High-performance JSON field extraction using structural indices and speculation.
//! Based on the Mison paper: "A Fast JSON Parser for Data Analytics" (VLDB 2017)
//!
//! ## Key Concepts
//!
//! 1. **Structural Index**: Bitmap indices on structural characters ({, }, :, ,, [, ])
//!    that enable O(1) field location lookup without full DOM parsing.
//!
//! 2. **Leveled Bitmaps**: Separate bitmaps per nesting level, making nested objects
//!    independent of each other during parsing.
//!
//! 3. **Pattern Tree**: Learned field orderings that enable speculative jumps to
//!    expected field positions with verification.
//!
//! 4. **Schema-Guided Extraction**: Direct extraction to Arrow column builders
//!    without intermediate serde_json::Value allocation.
//!
//! ## Performance
//!
//! Traditional pipeline: Parse → DOM → Route → Flatten → Transform → Arrow (~1.5-2.5µs)
//! Mison pipeline: Structural Index → Schema Extract → Arrow (~300-400ns)
//!
//! ## Schema-Guided Approach
//!
//! The key insight is that we only need to extract fields that exist in the destination
//! ClickHouse schema. By introspecting the schema and creating a targeted extractor:
//!
//! - We skip parsing fields that won't be stored
//! - No flattening needed - extract nested paths directly (e.g., "user.id")
//! - No intermediate DOM allocation
//! - Direct byte-to-Arrow column building
//!
//! ## Usage
//!
//! ```ignore
//! // Get schema from ClickHouse (cached per table)
//! let schema = schema_cache.get("db.events").await?;
//!
//! // Create Mison processor for this schema
//! let mut processor = MisonBatchProcessor::new(&schema.columns);
//!
//! // Process batch of JSON messages directly to Arrow
//! let batch = processor.process_batch(&json_messages)?;
//!
//! // Insert to ClickHouse
//! inserter.insert_batch("db.events", batch).await?;
//! ```

pub mod arrow;
pub mod extract;
pub mod index;
pub mod pattern;
pub mod simd;

pub use arrow::{BuildError, MisonArrowBuilder, MisonBatchProcessor};
pub use extract::{ExtractError, ExtractedValue, FieldExtractor, FieldSchema, SchemaExtractor};
pub use index::{LeveledBitmaps, StructuralIndex};
pub use pattern::{FieldPattern, PatternTree, TablePatternRegistry};
pub use simd::simd_capability_name;
