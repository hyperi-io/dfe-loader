// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics and health server

pub mod prometheus;
pub mod server;

pub use self::prometheus::Metrics;
pub use self::server::{run_server, HealthStatus, ServerState};
