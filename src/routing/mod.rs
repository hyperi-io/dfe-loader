// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Message routing based on event_category

pub mod mapping;
pub mod router;

pub use mapping::CategoryMapping;
pub use router::{RouteResult, Router};
