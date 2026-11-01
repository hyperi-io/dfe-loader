// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The `ClickHouse` a test reaches for is one it was told about, never a
//! default on `localhost` that a dfe-docker stack may be holding.

use crate::common::ClickHouseTestConfig;

#[test]
fn an_unnamed_clickhouse_is_unconfigured_and_never_probed() {
    let ch = ClickHouseTestConfig::unconfigured();
    assert!(!ch.is_configured(), "no host was named");
    assert!(!ch.is_reachable(), "an unnamed host must not be probed");
}
