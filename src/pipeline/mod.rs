// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pipeline orchestration

pub mod auto_init;
pub mod orchestrator;

pub use auto_init::AutoInitializer;
pub use orchestrator::{Orchestrator, PipelineStats};
