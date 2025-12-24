//! Message routing based on event_category

pub mod mapping;
pub mod router;

pub use mapping::CategoryMapping;
pub use router::{RouteResult, Router};
