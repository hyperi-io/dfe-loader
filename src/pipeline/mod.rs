// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Pipeline orchestration with parallel message processing.
//!
//! Architecture:
//! - `processor::MessageProcessor` — pure, parallel-safe computation (rayon)
//! - `coordinator::BatchCoordinator` — sequential state mutation (buffer, DLQ)
//! - [`orchestrator::Orchestrator`] — thin event loop coordinator

pub mod capture;
pub mod coordinator;
pub mod enrichment;
pub mod orchestrator;
pub(crate) mod pending_schema;
pub mod processor;
pub mod types;
pub(crate) mod unsettled;

pub use orchestrator::{Orchestrator, PipelineStats};
