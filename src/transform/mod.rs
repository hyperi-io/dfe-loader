//! JSON transformation pipeline

pub mod arrow;
pub mod coerce;
pub mod flatten;
pub mod project;
pub mod timestamp;
pub mod transformer;

pub use arrow::{json_batch_to_arrow, json_to_arrow_batch, ArrowBatchBuilder};
pub use coerce::Coercer;
pub use flatten::{flatten, flatten_value};
pub use timestamp::{validate_timestamp, TimestampResult, TimestampValidator};
pub use transformer::{TransformResult, Transformer};
