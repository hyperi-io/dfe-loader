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
pub use timestamp::{validate_timestamp, TimestampResult, TimestampValidator};
pub use transformer::{TransformResult, Transformer};
