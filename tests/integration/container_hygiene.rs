// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The naming and cleanup convention for containers this suite starts.
//!
//! Several runs share a developer machine, so a container has to say what it is,
//! which suite started it, and whose run owns it -- otherwise `docker ps` shows a
//! wall of random hex and nobody can tell what is safe to remove. These tests
//! pin the scheme so it cannot drift back to testcontainers' defaults.

use crate::common::{TEST_SUITE_LABEL, container_name, containers_prefix};
use crate::test_name;

/// The suite part of every name: `dfe-loader-test-integration` by default.
fn base() -> String {
    format!("{}-integration", containers_prefix())
}

/// A per-test instance carries the test name between the suite and the service,
/// so two tests owning their own container do not collide.
#[test]
fn per_test_container_name_includes_the_test() {
    let name = container_name(Some("inserter_basic"), "clickhouse");
    assert_eq!(
        name,
        format!(
            "{}-inserter-basic-clickhouse-p{}",
            base(),
            std::process::id()
        )
    );
}

/// The binary-scoped form, for a container started once for a whole test binary.
#[test]
fn binary_scoped_container_name_omits_the_test() {
    let name = container_name(None, "clickhouse");
    assert_eq!(
        name,
        format!("{}-clickhouse-p{}", base(), std::process::id())
    );
}

/// Two tests asking for the same service must get DIFFERENT names.
///
/// nextest runs each test in its own process, so two tests each owning a
/// container start two. On a single name the first create wins and the other
/// fails with "name is already in use", and its coverage is gone.
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
/// This is what keeps every `exclusive(test_name!(), ..)` call site distinct
/// without hand-written literals that drift as tests get renamed. If the macro
/// ever resolved to something constant, those containers would collide.
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
        format!(
            "{}-clickhouse-inserter-e2e--test-basic-clickhouse-p{}",
            base(),
            std::process::id()
        )
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
            name.starts_with(&format!("{}-", base())),
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
            crate::common::containers::TestInfrastructure::exclusive(test_name!(), true, false)
                .await;

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

/// Value of `field` from `docker inspect` on `name`, or `None` once it is gone.
#[cfg(feature = "testcontainers")]
fn inspect(name: &str, field: &str) -> Option<String> {
    std::process::Command::new("docker")
        .args(["inspect", name, "--format", field])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

/// The run's shared ClickHouse is named for the run and labelled with the run
/// and the process whose exit removes it, which under nextest is nextest.
#[cfg(feature = "testcontainers")]
#[tokio::test(flavor = "multi_thread")]
async fn the_shared_container_is_named_and_labelled_for_the_run() {
    use crate::common::shared_service::{run_id, run_owner_pid};

    let shared = crate::common::containers::shared_clickhouse().await;
    assert_eq!(
        shared.name,
        format!("{}-clickhouse-r{}", base(), run_id()),
        "the shared container is not named for the run"
    );
    let label = |key: &str| {
        inspect(
            &shared.name,
            &format!("{{{{index .Config.Labels \"{key}\"}}}}"),
        )
    };
    assert_eq!(
        label(TEST_SUITE_LABEL.0).as_deref(),
        Some(TEST_SUITE_LABEL.1)
    );
    assert_eq!(label("io.hyperi.test.run"), Some(run_id()));
    assert_eq!(
        label("io.hyperi.test.owner-host"),
        Some(crate::common::shared_service::host_id())
    );
    let bound = crate::common::shared_service::bound_addresses(&shared.name);
    assert!(!bound.is_empty(), "docker reports no published port");
    assert!(
        bound.iter().all(|addr| addr == "127.0.0.1"),
        "a shared port is published beyond loopback: {bound:?}"
    );
    let port = shared.port(crate::common::containers::CLICKHOUSE_HTTP_PORT);
    let anonymous =
        crate::common::shared_service::http_get(&shared.host, port, "/?query=SELECT%201");
    assert!(
        anonymous.as_ref().is_some_and(|(status, _)| *status != 200),
        "the shared ClickHouse answered a query with no password: {anonymous:?}"
    );
    let kafka = crate::common::containers::shared_kafka().await;
    let bound = crate::common::shared_service::bound_addresses(&kafka.name);
    assert!(
        !bound.is_empty() && bound.iter().all(|addr| addr == "127.0.0.1"),
        "a shared Kafka port is published beyond loopback: {bound:?}"
    );
    assert_eq!(
        label("io.hyperi.test.owner-pid"),
        Some(run_owner_pid().to_string())
    );
    if std::env::var_os("NEXTEST_RUN_ID").is_some() {
        let owner = std::fs::read_to_string(format!("/proc/{}/cmdline", run_owner_pid()))
            .unwrap_or_default();
        assert!(
            owner.contains("nextest"),
            "the owner pid {} is not the nextest process: {owner:?}",
            run_owner_pid()
        );
    }
}

/// The watcher leaves a shared container alone while its owner lives, and
/// removes it once the owner exits, however the owner exits.
#[cfg(feature = "testcontainers")]
#[tokio::test(flavor = "multi_thread")]
async fn the_watcher_removes_the_container_once_its_owner_exits() {
    crate::common::require_docker_in_ci();
    if !crate::common::has_docker() {
        eprintln!("skipping: no Docker daemon");
        return;
    }
    let name = format!("{}-watcher-p{}", base(), std::process::id());
    let image = format!(
        "clickhouse/clickhouse-server:{}@{}",
        crate::common::containers::CLICKHOUSE_TAG,
        crate::common::containers::CLICKHOUSE_DIGEST
    );
    let created = std::process::Command::new("docker")
        .args(["create", "--name", &name, "--label"])
        .arg(format!("{}={}", TEST_SUITE_LABEL.0, TEST_SUITE_LABEL.1))
        .arg(&image)
        .output()
        .expect("run docker create");
    assert!(
        created.status.success(),
        "create {name}: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    let mut owner = std::process::Command::new("sleep")
        .arg("600")
        .spawn()
        .expect("start a stand-in owner");
    crate::common::shared_service::spawn_watcher(owner.id(), Some(&name), None);

    // Long enough for a watcher that ignored its owner to have acted.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let alive_while_owned = inspect(&name, "{{.Name}}").is_some();

    owner.kill().expect("end the stand-in owner");
    owner.wait().expect("reap the stand-in owner");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while inspect(&name, "{{.Name}}").is_some() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let gone = inspect(&name, "{{.Name}}").is_none();
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", &name])
        .output();
    assert!(
        alive_while_owned,
        "the watcher removed {name} while its owner was alive"
    );
    assert!(
        gone,
        "{name} survived its owner by 30 s: the watcher did not remove it"
    );
}

/// A sweep on one daemon leaves another owner's live container alone and
/// removes one whose lease has lapsed, whichever machine runs it.
#[cfg(feature = "testcontainers")]
#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_leaves_another_owners_live_container_and_takes_a_lapsed_one() {
    crate::common::require_docker_in_ci();
    if !crate::common::has_docker() {
        eprintln!("skipping: no Docker daemon");
        return;
    }
    let image = format!(
        "clickhouse/clickhouse-server:{}@{}",
        crate::common::containers::CLICKHOUSE_TAG,
        crate::common::containers::CLICKHOUSE_DIGEST
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    // Owned elsewhere: no live process here can vouch for either.
    let foreign = |name: &str, lease_until: u64| {
        let created = std::process::Command::new("docker")
            .args(["create", "--name", name])
            .args([
                "--label",
                &format!("{}={}", TEST_SUITE_LABEL.0, TEST_SUITE_LABEL.1),
            ])
            .args(["--label", "io.hyperi.test.owner-host=another-host/0000"])
            .args(["--label", "io.hyperi.test.owner-pidns=pid:[1]"])
            .args(["--label", "io.hyperi.test.owner-pid=1"])
            .args(["--label", "io.hyperi.test.owner-start=1"])
            .args([
                "--label",
                &format!("io.hyperi.test.lease-until={lease_until}"),
            ])
            .arg(&image)
            .output()
            .expect("run docker create");
        assert!(
            created.status.success(),
            "create {name}: {}",
            String::from_utf8_lossy(&created.stderr)
        );
    };
    let pid = std::process::id();
    let live = format!("{}-sweep-live-p{pid}", base());
    let lapsed = format!("{}-sweep-lapsed-p{pid}", base());
    foreign(&live, now + 3600);
    foreign(&lapsed, now.saturating_sub(60));

    crate::common::shared_service::reap_dead_owners(&crate::common::containers::SUITE);

    let live_kept = inspect(&live, "{{.Name}}").is_some();
    let lapsed_kept = inspect(&lapsed, "{{.Name}}").is_some();
    for name in [&live, &lapsed] {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", name])
            .output();
    }
    assert!(
        live_kept,
        "the sweep removed another owner's container inside its lease"
    );
    assert!(
        !lapsed_kept,
        "the sweep left a container whose lease had lapsed"
    );
}
