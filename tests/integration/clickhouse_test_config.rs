// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The `ClickHouse` a test reaches for is one it was told about, never a
//! default on `localhost` that a dfe-docker stack may be holding.

use std::collections::HashMap;

use crate::common::ClickHouseTestConfig;

fn config(vars: &[(&str, &str)]) -> ClickHouseTestConfig {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    ClickHouseTestConfig::remote_from(|name| vars.get(name).cloned())
}

#[test]
fn remote_config_is_configured_only_when_a_host_is_named() {
    let cases: &[(&str, &[(&str, &str)], bool)] = &[
        ("nothing set", &[], false),
        (
            "ports without a host",
            &[("CLICKHOUSE_HTTP_PORT", "8123")],
            false,
        ),
        ("empty host", &[("CLICKHOUSE_HOST", "")], false),
        ("host named", &[("CLICKHOUSE_HOST", "ch.example")], true),
    ];
    for (label, vars, configured) in cases {
        let ch = config(vars);
        assert_eq!(ch.is_configured(), *configured, "{label}");
        if !configured {
            assert!(
                !ch.is_reachable(),
                "{label}: an unnamed host must not be probed"
            );
        }
    }
}

#[test]
fn remote_config_reads_ports_and_falls_back_on_bad_values() {
    let cases: &[(&str, &[(&str, &str)], u16, u16)] = &[
        ("defaults", &[("CLICKHOUSE_HOST", "h")], 8123, 9000),
        (
            "set",
            &[
                ("CLICKHOUSE_HOST", "h"),
                ("CLICKHOUSE_HTTP_PORT", "18123"),
                ("CLICKHOUSE_NATIVE_PORT", "19000"),
            ],
            18123,
            19000,
        ),
        (
            "not numbers",
            &[
                ("CLICKHOUSE_HOST", "h"),
                ("CLICKHOUSE_HTTP_PORT", "http"),
                ("CLICKHOUSE_NATIVE_PORT", "-1"),
            ],
            8123,
            9000,
        ),
    ];
    for (label, vars, http, native) in cases {
        let ch = config(vars);
        assert_eq!((ch.http_port, ch.native_port), (*http, *native), "{label}");
    }
}
