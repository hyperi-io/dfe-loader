// SPDX-License-Identifier: FSL-1.1-ALv2
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
pub use computed::{parse_computed_directive, ComputedColumnCache};
pub use extractor::HeaderExtractor;
pub use field_mapping::{
    parse_renamed_directive, FieldMappingRule, MappingAction, RuleOrigin, TableFieldMapping,
};
pub use flatten::{flatten, flatten_value, flatten_value_owned, BatchFlattenStats, BatchFlattener};
pub use mapping_builder::{FieldMappingCache, MappingBuilder};
pub use project::{project, ProjectedData, Projector};
pub use remap_loader::BuiltinPreset;
pub use timestamp::{
    clamp_timestamp_ms, clamp_timestamp_ms_nano, is_valid_datetime64_ms, validate_timestamp,
    TimestampResult, TimestampValidator, MAX_DATETIME64_MS, MAX_DATETIME64_NANO_MS,
    MIN_DATETIME64_MS,
};
pub use transformer::{TransformResult, Transformer};
