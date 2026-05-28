// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! JSON transformation pipeline

pub mod coerce;
pub mod computed;
pub mod extractor;
pub mod field_mapping;
pub mod flatten;
pub mod mapping_builder;
pub mod project;
pub mod remap_loader;
pub mod timestamp;
pub mod transformer;

pub use coerce::{Coercer, CoercionMode};
pub use computed::{ComputedColumnCache, parse_computed_directive};
pub use extractor::HeaderExtractor;
pub use field_mapping::{
    FieldMappingRule, MappingAction, RuleOrigin, TableFieldMapping, parse_renamed_directive,
};
pub use flatten::{BatchFlattenStats, BatchFlattener, flatten, flatten_value, flatten_value_owned};
pub use mapping_builder::{FieldMappingCache, MappingBuilder};
pub use project::{ProjectedData, Projector, project};
pub use remap_loader::BuiltinPreset;
pub use timestamp::{
    MAX_DATETIME64_MS, MAX_DATETIME64_NANO_MS, MIN_DATETIME64_MS, TimestampResult,
    TimestampValidator, clamp_timestamp_ms, clamp_timestamp_ms_nano, is_valid_datetime64_ms,
    validate_timestamp,
};
pub use transformer::{TransformResult, Transformer};
