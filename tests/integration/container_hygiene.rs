// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The naming and cleanup convention for containers this suite starts.
//!
//! Several runs share a developer machine, so a container has to say what it is,
//! which suite started it, and whose run owns it -- otherwise `docker ps` shows a
//! wall of random hex and nobody can tell what is safe to remove. These tests
//! pin the scheme so it cannot drift back to testcontainers' defaults.

use crate::common::{TEST_SUITE_LABEL, container_name};
use crate::test_name;

/// A per-test instance carries the test name between the suite and the service,
/// so two tests owning their own container do not collide.
#[test]
fn per_test_container_name_includes_the_test() {
    let name = container_name(Some("inserter_basic"), "clickhouse");
    assert_eq!(
        name,
        "dfe-loader-test-integration-inserter-basic-clickhouse"
    );
}

/// The binary-scoped form, for a container started once for a whole test binary.
#[test]
fn binary_scoped_container_name_omits_the_test() {
    let name = container_name(None, "clickhouse");
    assert_eq!(name, "dfe-loader-test-integration-clickhouse");
}

/// Two tests asking for the same service must get DIFFERENT names.
///
/// nextest runs each test in its own process, so the 15 tests calling `spin_up`
/// start 15 ClickHouse containers -- they do not share one. On a single name the
/// first create wins and the other 14 fail with "name is already in use", and
/// those tests then skip or panic. Either way the coverage is gone.
#[test]
fn two_tests_wanting_one_service_do_not_collide() {
    assert_ne!(
        container_name(Some("first_test"), "clickhouse"),
        container_name(Some("second_test"), "clickhouse"),
    );
}

/// `test_name!()` must report the test it expands in, not the helper it is
/// passed to and not the module.
///
/// This is what makes the 15 `spin_up(test_name!())` call sites distinct without
/// 15 hand-written literals that drift as tests get renamed. If the macro ever
/// resolved to something constant, every container would collide again -- so the
/// property is asserted rather than assumed.
#[test]
fn test_name_reports_the_calling_test() {
    assert_eq!(test_name!(), "test_name_reports_the_calling_test");
}

/// The same, from an async test: an `async fn` body compiles to a generated
/// future, so the raw path carries `::{{closure}}` and has to be trimmed. Every
/// caller of `test_name!` in this suite is an async test, so this is the case
/// that actually matters.
#[tokio::test]
async fn test_name_reports_the_calling_test_when_async() {
    assert_eq!(
        test_name!(),
        "test_name_reports_the_calling_test_when_async"
    );
}

/// Docker only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`. The paths `test_name!`
/// produces, and any Rust test path, carry colons -- which would be rejected at
/// create time as what looks like a Docker fault, so they are normalised here.
#[test]
fn container_name_is_a_legal_docker_name() {
    let name = container_name(Some("clickhouse_inserter_e2e::test_basic"), "ClickHouse");
    assert_eq!(
        name,
        "dfe-loader-test-integration-clickhouse-inserter-e2e--test-basic-clickhouse"
    );

    let legal = |s: &str| {
        let mut chars = s.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    };
    assert!(legal(&name), "{name} is not a legal Docker container name");
    assert!(legal(&container_name(None, "kafka")));
}

/// Every name is prefixed so one `docker ps` filter finds everything this repo's
/// suite started, and nothing belonging to another repo.
#[test]
fn names_share_one_greppable_prefix() {
    for name in [
        container_name(None, "kafka"),
        container_name(None, "clickhouse"),
        container_name(Some("some_test"), "clickhouse"),
    ] {
        assert!(
            name.starts_with("dfe-loader-test-integration-"),
            "{name} does not carry the suite prefix"
        );
    }
}

/// The label is what makes a bulk sweep possible when a run was killed and the
/// names are not known: `docker rm -f $(docker ps -aq --filter label=...)`.
#[test]
fn suite_label_identifies_this_repo() {
    assert_eq!(TEST_SUITE_LABEL.0, "io.hyperi.test.suite");
    assert_eq!(TEST_SUITE_LABEL.1, "dfe-loader-integration");
}

/// A REAL container carries the name and labels, and is gone afterwards.
///
/// The tests above only exercise `container_name`, which proves the helper
/// returns the right string -- not that anything calls it. A convention that is
/// never wired to a container is decoration, so this asks Docker what actually
/// got created, and then that it was removed.
#[cfg(feature = "testcontainers")]
#[tokio::test(flavor = "multi_thread")]
async fn a_started_container_carries_the_name_and_label_then_goes_away() {
    if !crate::common::has_docker() {
        eprintln!("skipping: no Docker daemon");
        return;
    }

    let expected = container_name(Some(test_name!()), "clickhouse");

    let inspect = |field: &str| {
        std::process::Command::new("docker")
            .args(["inspect", &expected, "--format", field])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };

    {
        let _infra =
            crate::common::containers::TestInfrastructure::new(test_name!(), true, false).await;

        let name =
            inspect("{{.Name}}").expect("docker must know the container by its expected name");
        assert_eq!(
            name.trim_start_matches('/'),
            expected,
            "the container is not named per the convention"
        );

        let suite = inspect(&format!(
            "{{{{index .Config.Labels \"{}\"}}}}",
            TEST_SUITE_LABEL.0
        ))
        .expect("docker inspect must report labels");
        assert_eq!(suite, TEST_SUITE_LABEL.1, "suite label missing or wrong");

        let owner = inspect(r#"{{index .Config.Labels "io.hyperi.test.owner-pid"}}"#)
            .expect("docker inspect must report labels");
        assert_eq!(
            owner,
            std::process::id().to_string(),
            "owner-pid label must name THIS test process, or it cannot answer whose run left it"
        );
    }

    // Dropped above. Removal is asynchronous, so give it a moment before
    // asserting -- a bare check here would be racy and fail for the wrong reason.
    for _ in 0..50 {
        if inspect("{{.Name}}").is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{expected} still exists after the holder was dropped -- the suite leaks containers");
}
