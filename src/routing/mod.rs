// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Message routing to destination db.table based on _source and `org_id` fields

pub mod mapping;
pub mod router;

pub use mapping::SourceMapping;
pub use router::{RouteResult, Router};
