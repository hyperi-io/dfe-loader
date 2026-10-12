// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The loader's `tracing` output, routed through the test harness's capture.
//!
//! The integration tests run the orchestrator in the test process, so its logs
//! belong to the test that started it. The test writer prints them through the
//! harness, which shows them only for a test that fails.

use std::sync::Once;

use tracing_subscriber::EnvFilter;

/// Filter when `RUST_LOG` is unset: the loader at debug, scalo (which owns the
/// gRPC listener and the sink stack) at info, everything else at warn.
const DEFAULT_FILTER: &str = "warn,dfe_loader=debug,scalo=info";

/// Install the capturing subscriber, once per test process. Safe to call from
/// every test: later calls do nothing.
pub fn init() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
        // A subscriber already installed by the test itself wins.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .with_ansi(false)
            .try_init();
    });
}
