//! JSON transformation pipeline

pub mod arrow;
pub mod coerce;
pub mod flatten;
pub mod project;
pub mod timestamp;
pub mod transformer;

pub use arrow::{
    json_batch_to_arrow, json_bytes_to_arrow_simd, json_to_arrow_batch,
    infer_schema_from_json_bytes, ArrowBatchBuilder, SimdBatchBuilder,
};
pub use coerce::Coercer;
pub use flatten::{flatten, flatten_value, flatten_value_owned, BatchFlattener, BatchFlattenStats};
pub use project::{project, ProjectedData, Projector};
pub use timestamp::{
    clamp_timestamp_ms, clamp_timestamp_ms_nano, is_valid_datetime64_ms,
    validate_timestamp, TimestampResult, TimestampValidator,
    MIN_DATETIME64_MS, MAX_DATETIME64_MS, MAX_DATETIME64_NANO_MS,
};
pub use transformer::{TransformResult, Transformer};
