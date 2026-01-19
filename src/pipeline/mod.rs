//! Pipeline orchestration

pub mod auto_init;
pub mod orchestrator;

pub use auto_init::AutoInitializer;
pub use orchestrator::{Orchestrator, PipelineStats};
