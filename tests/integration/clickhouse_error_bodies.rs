// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The server error bodies the classifier tests read, produced live.
//!
//! The classifier's table test (`src/clickhouse/error.rs`) runs in core on the
//! bodies saved in `tests/fixtures/clickhouse/errors/`. This full-tier test
//! makes the shared `ClickHouse` raise each one again and checks the server
//! still answers with the same code and error name, so a server upgrade that
//! changes either fails here rather than leaving the fixtures stale. With
//! `DFE_LOADER_CAPTURE_FIXTURES` set it writes the live bodies over the
//! fixtures instead: `scripts/capture-clickhouse-fixtures.py` runs it that way.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`.

#![cfg(feature = "testcontainers")]

use std::path::PathBuf;

use crate::common::containers::{CLICKHOUSE_HTTP_PORT, shared_clickhouse};
use crate::common::unique_table_name;

/// One fixture: its file stem, the setup statements, and the request whose
/// error body it holds, as (query, body).
struct Capture {
    fixture: &'static str,
    setup: Vec<String>,
    request: (String, String),
}

fn captures() -> Vec<Capture> {
    let json = unique_table_name("fixture_json");
    let parts = unique_table_name("fixture_parts");
    let partitions = unique_table_name("fixture_partitions");
    let async_insert = "SETTINGS async_insert = 1, wait_for_async_insert = 1";
    vec![
        Capture {
            fixture: "incorrect_data_117",
            setup: vec![format!(
                "CREATE TABLE default.{json} (_json JSON) ENGINE = MergeTree ORDER BY tuple()"
            )],
            request: (
                format!("INSERT INTO default.{json} {async_insert} FORMAT JSONEachRow"),
                r#"{"_json": "not an object"}"#.into(),
            ),
        },
        Capture {
            fixture: "unknown_table_60",
            setup: Vec::new(),
            request: (
                "INSERT INTO default.events FORMAT JSONEachRow".into(),
                "{}".into(),
            ),
        },
        Capture {
            fixture: "unknown_user_192",
            setup: Vec::new(),
            request: (
                "ALTER USER loader_writer SETTINGS max_threads = 1".into(),
                String::new(),
            ),
        },
        Capture {
            fixture: "memory_limit_exceeded_241",
            setup: Vec::new(),
            request: (
                "SELECT groupArray(number) FROM numbers(10000000) \
                 SETTINGS max_memory_usage = 10000000 FORMAT Null"
                    .into(),
                String::new(),
            ),
        },
        Capture {
            fixture: "too_many_parts_252",
            setup: vec![
                format!(
                    "CREATE TABLE default.{parts} (x UInt64) ENGINE = MergeTree ORDER BY x \
                     SETTINGS parts_to_throw_insert = 1"
                ),
                format!("SYSTEM STOP MERGES default.{parts}"),
                format!("INSERT INTO default.{parts} VALUES (1)"),
            ],
            request: (
                format!("INSERT INTO default.{parts} VALUES (2)"),
                String::new(),
            ),
        },
        Capture {
            fixture: "too_many_partitions_252",
            setup: vec![format!(
                "CREATE TABLE default.{partitions} (x UInt64) ENGINE = MergeTree \
                 PARTITION BY x ORDER BY x"
            )],
            request: (
                format!(
                    "INSERT INTO default.{partitions} SETTINGS async_insert = 1, \
                     wait_for_async_insert = 1, max_partitions_per_insert_block = 1 \
                     FORMAT JSONEachRow"
                ),
                "{\"x\": 1}\n{\"x\": 2}\n".into(),
            ),
        },
    ]
}

/// `Code: N` and the `(ERROR_NAME)` before the version, the parts of a body
/// the classifier reads and that do not change with the row or the build.
fn code_and_name(body: &str) -> (Option<&str>, Option<&str>) {
    let code = body
        .strip_prefix("Code: ")
        .and_then(|rest| rest.split('.').next());
    let name = body
        .rsplit_once(" (version ")
        .and_then(|(head, _)| head.rsplit_once('('))
        .map(|(_, name)| name.trim_end_matches(')'));
    (code, name)
}

fn fixture_path(stem: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/clickhouse/errors")
        .join(format!("{stem}.txt"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_still_answers_as_the_fixtures_say() {
    let shared = shared_clickhouse().await;
    let url = format!("http://{}", shared.addr(CLICKHOUSE_HTTP_PORT));
    let password = crate::common::containers::clickhouse_password();
    let http = reqwest::Client::new();
    let capture = std::env::var_os("DFE_LOADER_CAPTURE_FIXTURES").is_some();

    let mut mismatches = Vec::new();
    for case in captures() {
        for statement in &case.setup {
            let response = http
                .post(&url)
                .basic_auth("default", Some(&password))
                .body(statement.clone())
                .send()
                .await
                .expect("send a setup statement");
            assert!(
                response.status().is_success(),
                "{}: setup `{statement}` failed: {}",
                case.fixture,
                response.text().await.unwrap_or_default()
            );
        }
        // A statement with no data goes in the body: a POST needs a length.
        let (query, body) = &case.request;
        let request = if body.is_empty() {
            http.post(&url)
                .basic_auth("default", Some(&password))
                .body(query.clone())
        } else {
            http.post(&url)
                .basic_auth("default", Some(&password))
                .query(&[("query", query)])
                .body(body.clone())
        };
        let response = request.send().await.expect("send the failing request");
        let status = response.status();
        let live = response.text().await.expect("read the error body");
        let live = live.trim_end().to_string();
        assert!(
            !status.is_success(),
            "{}: `{query}` succeeded, so it no longer produces the error",
            case.fixture
        );

        let path = fixture_path(case.fixture);
        if capture {
            std::fs::write(&path, &live).expect("write the fixture");
            eprintln!("captured {}", path.display());
            continue;
        }
        let saved = std::fs::read_to_string(&path).expect("read the fixture");
        if code_and_name(&live) != code_and_name(&saved) {
            mismatches.push(format!(
                "{}: live {:?}, fixture {:?}\n  live body: {live}",
                case.fixture,
                code_and_name(&live),
                code_and_name(&saved)
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "the server's errors moved away from the fixtures; re-capture with \
         scripts/capture-clickhouse-fixtures.py and review the classifier:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn a_body_is_read_for_its_code_and_name() {
    for (body, want) in [
        (
            "Code: 60. DB::Exception: Table x does not exist. (UNKNOWN_TABLE) (version 26.3.42.3 (official build))",
            (Some("60"), Some("UNKNOWN_TABLE")),
        ),
        ("not a server body", (None, None)),
        ("Code: 1. no version here (NAME)", (Some("1"), None)),
    ] {
        assert_eq!(code_and_name(body), want, "{body}");
    }
}
