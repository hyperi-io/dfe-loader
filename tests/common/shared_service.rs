// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! One container per backing service per test run, shared by every test process.
//!
//! nextest runs each test in a process of its own, so whichever process asks
//! first creates the container under a run-scoped name and the rest join it by
//! that name. The creator also starts a detached watcher that removes the
//! container once the run's nextest process exits, whether it passed, failed
//! or was interrupted. Nothing here names a repo, image or port: the caller
//! passes a [`Suite`] and a [`ServiceSpec`].
//!
//! The pet rung lives here too: [`lookup`] reads `HYPERI_TEST_*` settings from
//! the process environment, then the project `.env`, then the file named by
//! `HYPERI_TEST_ENV_FILE`, and [`pets_allowed`] says when a run may use them.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Interval between readiness probes and state checks.
const POLL: Duration = Duration::from_millis(250);

/// The repo whose suite starts the containers, for names and labels.
#[derive(Debug, Clone, Copy)]
pub struct Suite {
    /// Repo name, e.g. the crate name.
    pub repo: &'static str,
    /// Directory holding the project `.env` (`CARGO_MANIFEST_DIR`).
    pub manifest_dir: &'static str,
}

impl Suite {
    /// Value of the `io.hyperi.test.suite` label.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}-integration", self.repo)
    }

    /// Container name prefix: `HYPERI_TEST_CONTAINER_PREFIX`, else `<repo>-test`.
    #[must_use]
    pub fn prefix(&self) -> String {
        lookup(self, "HYPERI_TEST_CONTAINER_PREFIX")
            .unwrap_or_else(|| format!("{}-test", self.repo))
    }
}

/// How a started container shows it is ready to serve.
#[derive(Debug, Clone)]
pub enum Ready {
    /// `GET path` on the container port answers 200 with this body, trimmed.
    Http {
        port: u16,
        path: &'static str,
        body: &'static str,
    },
    /// The container log carries this line.
    Log(&'static str),
}

/// One backing service the suite shares across its test processes.
pub struct ServiceSpec {
    /// Service name, used in the container name and the service label.
    pub service: &'static str,
    /// Image reference, pinned by tag and digest.
    pub image: String,
    /// Container ports to publish, each to a free host port.
    pub ports: Vec<u16>,
    /// Environment for the container.
    pub env: Vec<(String, String)>,
    /// Entrypoint override, when the image's own cannot be used.
    pub entrypoint: Option<String>,
    /// Arguments after the image.
    pub args: Vec<String>,
    /// Files copied in before start, built from the endpoint host name. A path
    /// ending in `/` creates that directory, ahead of the files that go in it.
    pub files: fn(&str) -> Vec<(String, Vec<u8>)>,
    /// Runs once the ports are known and before the readiness wait.
    pub after_start: fn(&SharedContainer),
    /// The readiness signal.
    pub ready: Ready,
    /// Bound on create, start and readiness together.
    pub startup_timeout: Duration,
}

/// A running shared container and where its ports are published.
#[derive(Debug, Clone)]
pub struct SharedContainer {
    /// Container name.
    pub name: String,
    /// Host the published ports are reachable on.
    pub host: String,
    ports: Vec<(u16, u16)>,
}

impl SharedContainer {
    /// Host port that `container_port` is published on.
    ///
    /// # Panics
    ///
    /// Panics if the spec did not publish `container_port`.
    #[must_use]
    pub fn port(&self, container_port: u16) -> u16 {
        self.ports
            .iter()
            .find_map(|&(c, h)| (c == container_port).then_some(h))
            .unwrap_or_else(|| panic!("{} does not publish port {container_port}", self.name))
    }

    /// `host:port` for `container_port`.
    #[must_use]
    pub fn addr(&self, container_port: u16) -> String {
        format!("{}:{}", self.host, self.port(container_port))
    }

    /// Bytes of a file inside the container.
    ///
    /// # Panics
    ///
    /// Panics when the file cannot be read.
    #[must_use]
    pub fn read_file(&self, path: &str) -> Vec<u8> {
        let out = docker(&["exec", &self.name, "cat", path]);
        assert!(
            out.status.success(),
            "read {path} from {}: {}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    /// Run a command inside the container and return its stdout.
    ///
    /// # Panics
    ///
    /// Panics when the command exits non-zero.
    #[must_use]
    pub fn exec(&self, argv: &[&str]) -> String {
        let mut args = vec!["exec", self.name.as_str()];
        args.extend_from_slice(argv);
        let out = docker(&args);
        assert!(
            out.status.success(),
            "{argv:?} in {}: {}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

/// The shared container for `spec` in this run, created if no process has yet.
///
/// Blocking: call it from a plain thread or `spawn_blocking`, never directly on
/// an async runtime thread.
///
/// # Panics
///
/// Panics when Docker is unreachable, the container cannot be created, or it
/// is not ready within `spec.startup_timeout`.
#[must_use]
pub fn acquire(suite: &Suite, spec: &ServiceSpec) -> SharedContainer {
    let name = format!(
        "{}-integration-{}-r{}",
        suite.prefix(),
        spec.service,
        run_id()
    );
    let deadline = Instant::now() + spec.startup_timeout;
    let host = docker_host();
    loop {
        match state(&name) {
            State::Running => {
                let container = published(&name, &host, &spec.ports, deadline);
                wait_ready(&container, &spec.ready, deadline);
                return container;
            }
            State::Absent => {
                if create(suite, spec, &name, &host) {
                    return start_owned(spec, &name, &host, deadline);
                }
            }
            // A peer created it and is still copying files in or starting it.
            State::Other(status) => assert!(
                Instant::now() < deadline,
                "shared {} container {name} stuck in state {status}",
                spec.service
            ),
        }
        std::thread::sleep(POLL);
    }
}

/// Copy the files in, start the container, hand it to a watcher and log it.
fn start_owned(spec: &ServiceSpec, name: &str, host: &str, deadline: Instant) -> SharedContainer {
    spawn_watcher(run_owner_pid(), name);
    let dir = tempfile::tempdir().expect("staging directory for container files");
    for (index, (target, bytes)) in (spec.files)(host).into_iter().enumerate() {
        let staged = dir.path().join(index.to_string());
        if target.ends_with('/') {
            use std::os::unix::fs::PermissionsExt;
            std::fs::create_dir(&staged).expect("stage a container directory");
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
                .expect("make the staged directory readable");
        } else {
            std::fs::write(&staged, bytes).expect("stage a container file");
        }
        let target = target.trim_end_matches('/');
        let copied = docker(&["cp", &path_str(&staged), &format!("{name}:{target}")]);
        assert!(
            copied.status.success(),
            "copy {target} into {name}: {}",
            String::from_utf8_lossy(&copied.stderr)
        );
    }
    let started = docker(&["start", name]);
    assert!(
        started.status.success(),
        "start {name}: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    let container = published(name, host, &spec.ports, deadline);
    (spec.after_start)(&container);
    wait_ready(&container, &spec.ready, deadline);
    record(&format!(
        "started {} container {name} on {host} for run {} (owner pid {}): 1 {} container this run",
        spec.service,
        run_id(),
        run_owner_pid(),
        spec.service
    ));
    container
}

/// Create the container. False when another process created it first.
fn create(suite: &Suite, spec: &ServiceSpec, name: &str, host: &str) -> bool {
    reap_dead_owners(suite);
    // A local daemon publishes on loopback alone, a remote one on every interface.
    let bind = if host == "localhost" {
        "127.0.0.1::"
    } else {
        ""
    };
    let mut args: Vec<String> = vec!["create".into(), "--name".into(), name.into()];
    for (key, value) in labels(suite, spec.service) {
        args.extend(["--label".into(), format!("{key}={value}")]);
    }
    for port in &spec.ports {
        args.extend(["--publish".into(), format!("{bind}{port}")]);
    }
    for (key, value) in &spec.env {
        args.extend(["--env".into(), format!("{key}={value}")]);
    }
    if let Some(entrypoint) = &spec.entrypoint {
        args.extend(["--entrypoint".into(), entrypoint.clone()]);
    }
    args.push(spec.image.clone());
    args.extend(spec.args.iter().cloned());
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = docker(&refs);
    if out.status.success() {
        return true;
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already in use") || stderr.contains("Conflict"),
        "create {name}: {stderr}"
    );
    false
}

/// Labels on every container: suite, repo, service, run and owning process.
#[must_use]
pub fn labels(suite: &Suite, service: &str) -> Vec<(String, String)> {
    vec![
        ("io.hyperi.test.suite".into(), suite.label()),
        ("io.hyperi.test.repo".into(), suite.repo.into()),
        ("io.hyperi.test.service".into(), service.into()),
        ("io.hyperi.test.run".into(), run_id()),
        (
            "io.hyperi.test.owner-pid".into(),
            run_owner_pid().to_string(),
        ),
    ]
}

/// Remove this suite's containers whose owning process has gone.
pub fn reap_dead_owners(suite: &Suite) {
    let filter = format!("label=io.hyperi.test.suite={}", suite.label());
    let format = "{{.Names}} {{.Label \"io.hyperi.test.owner-pid\"}}";
    let out = docker(&["ps", "--all", "--filter", &filter, "--format", format]);
    for name in dead_owner_names(&String::from_utf8_lossy(&out.stdout), process_alive) {
        let _ = docker(&["rm", "--force", "--volumes", &name]);
    }
}

/// Names from `"<name> <owner-pid>"` lines whose owner is no longer alive.
pub fn dead_owner_names(listing: &str, alive: impl Fn(u32) -> bool) -> Vec<String> {
    listing
        .lines()
        .filter_map(|line| {
            let (name, pid) = line.split_once(' ')?;
            let pid: u32 = pid.trim().parse().ok()?;
            (!alive(pid)).then(|| name.to_string())
        })
        .collect()
}

/// Whether a process with this id exists, via `kill -0`.
fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Start a detached process that removes container `name` once `owner` exits.
///
/// The shell backgrounds the watcher and exits, so the watcher is reparented
/// rather than left for this process to reap. It sits in a process group of
/// its own so a Ctrl-C to the run does not reach it, and holds none of the
/// test's pipes, which nextest would report as a leak.
///
/// # Panics
///
/// Panics when `sh` cannot be run.
pub fn spawn_watcher(owner: u32, name: &str) {
    use std::os::unix::process::CommandExt;
    let script = r#"(
while kill -0 "$1" 2>/dev/null; do sleep 1; done
docker rm --force --volumes "$2" >/dev/null 2>&1
printf '%s removed %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$2" >> "$3"
) </dev/null >/dev/null 2>&1 &"#;
    let started = Command::new("sh")
        .args(["-c", script, "shared-service-watcher"])
        .arg(owner.to_string())
        .arg(name)
        .arg(log_path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .status()
        .expect("start the shared-container watcher");
    assert!(
        started.success(),
        "the shared-container watcher did not start: {started}"
    );
}

enum State {
    Absent,
    Running,
    Other(String),
}

fn state(name: &str) -> State {
    let out = docker(&["inspect", "--format", "{{.State.Status}}", name]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        assert!(
            stderr.to_ascii_lowercase().contains("no such"),
            "Docker is needed for the shared test containers: {stderr}"
        );
        return State::Absent;
    }
    match String::from_utf8_lossy(&out.stdout).trim() {
        "running" => State::Running,
        other => State::Other(other.to_string()),
    }
}

/// The published host ports, waiting for Docker to report every one.
fn published(name: &str, host: &str, ports: &[u16], deadline: Instant) -> SharedContainer {
    let mut mapped = Vec::with_capacity(ports.len());
    for &port in ports {
        loop {
            let out = docker(&["port", name, &format!("{port}/tcp")]);
            let text = String::from_utf8_lossy(&out.stdout);
            if let Some(host_port) = text
                .lines()
                .find_map(|l| l.rsplit(':').next()?.trim().parse().ok())
            {
                mapped.push((port, host_port));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{name} never published port {port}"
            );
            std::thread::sleep(POLL);
        }
    }
    SharedContainer {
        name: name.to_string(),
        host: host.to_string(),
        ports: mapped,
    }
}

fn wait_ready(container: &SharedContainer, ready: &Ready, deadline: Instant) {
    loop {
        let up = match ready {
            Ready::Http { port, path, body } => {
                http_get(&container.host, container.port(*port), path)
                    .is_some_and(|(status, text)| status == 200 && text.trim() == *body)
            }
            Ready::Log(line) => {
                let out = docker(&["logs", &container.name]);
                String::from_utf8_lossy(&out.stdout).contains(line)
                    || String::from_utf8_lossy(&out.stderr).contains(line)
            }
        };
        if up {
            return;
        }
        if Instant::now() >= deadline {
            let logs = docker(&["logs", "--tail", "40", &container.name]);
            panic!(
                "{} not ready by its deadline. Last log lines:\n{}{}",
                container.name,
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr)
            );
        }
        std::thread::sleep(POLL);
    }
}

/// `GET path` over plain HTTP/1.0: the status code and the body.
#[must_use]
pub fn http_get(host: &str, port: u16, path: &str) -> Option<(u16, String)> {
    let addr = (host, port).to_socket_addrs().ok()?.next()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n").ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    let status: u16 = head.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, body.to_string()))
}

/// Host the daemon publishes ports on: `localhost` for a local socket, else the
/// host named by `DOCKER_HOST` or the current docker context.
#[must_use]
pub fn docker_host() -> String {
    let endpoint = std::env::var("DOCKER_HOST")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| {
            let out = docker(&[
                "context",
                "inspect",
                "--format",
                "{{.Endpoints.docker.Host}}",
            ]);
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        });
    host_of_endpoint(&endpoint)
}

/// The host part of a Docker endpoint URL.
#[must_use]
pub fn host_of_endpoint(endpoint: &str) -> String {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return "localhost".into();
    };
    if !matches!(scheme, "tcp" | "ssh" | "http" | "https") {
        return "localhost".into();
    }
    let authority = rest.split('/').next().unwrap_or(rest);
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = host_port.rsplit_once(':').map_or(host_port, |(h, _)| h);
    host.to_string()
}

/// Short id of this test run: nextest's run id, else this process.
#[must_use]
pub fn run_id() -> String {
    std::env::var("NEXTEST_RUN_ID").map_or_else(
        |_| format!("p{}", std::process::id()),
        |id| {
            id.chars()
                .filter(char::is_ascii_alphanumeric)
                .take(8)
                .collect()
        },
    )
}

/// The process whose exit ends the run: nextest when it is the runner.
#[must_use]
pub fn run_owner_pid() -> u32 {
    if std::env::var_os("NEXTEST_RUN_ID").is_some() {
        std::os::unix::process::parent_id()
    } else {
        std::process::id()
    }
}

/// The run log every creator and watcher appends to.
#[must_use]
pub fn log_path() -> PathBuf {
    let dir = option_env!("CARGO_TARGET_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from);
    dir.join(format!("shared-services-{}.log", run_id()))
}

/// Write `line` to stderr and to the run log.
pub fn record(line: &str) {
    eprintln!("shared-service: {line}");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path())
    {
        let _ = writeln!(file, "{line}");
    }
}

/// A `HYPERI_TEST_*` setting: the process environment, then the project
/// `.env`, then the file named by `HYPERI_TEST_ENV_FILE`. Empty counts as unset.
#[must_use]
pub fn lookup(suite: &Suite, key: &str) -> Option<String> {
    let project = Path::new(suite.manifest_dir).join(".env");
    let in_project = |name: &str| file_value(&project, name);
    std::env::var(key)
        .ok()
        .or_else(|| in_project(key))
        .or_else(|| {
            let user = std::env::var("HYPERI_TEST_ENV_FILE")
                .ok()
                .or_else(|| in_project("HYPERI_TEST_ENV_FILE"))?;
            file_value(Path::new(&user), key)
        })
        .filter(|v| !v.is_empty())
}

/// `key` from a dotenv-format file, without touching the process environment.
fn file_value(path: &Path, key: &str) -> Option<String> {
    dotenvy::from_path_iter(path)
        .ok()?
        .filter_map(Result::ok)
        .find_map(|(k, v)| (k == key).then_some(v))
}

/// Whether this run may use a pet: never in CI, never under the full tier.
#[must_use]
pub fn pets_allowed() -> bool {
    std::env::var_os("CI").is_none()
        && !std::env::var("HYPERCI_TEST_TIER").is_ok_and(|t| t.eq_ignore_ascii_case("full"))
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("run docker {args:?}: {e}"))
}

fn path_str(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::host_of_endpoint;

    #[test]
    fn endpoint_hosts() {
        for (endpoint, want) in [
            ("unix:///var/run/docker.sock", "localhost"),
            ("", "localhost"),
            ("npipe:////./pipe/docker_engine", "localhost"),
            ("tcp://10.0.0.5:2376", "10.0.0.5"),
            ("ssh://derek@build.example:22", "build.example"),
            ("ssh://build.example", "build.example"),
        ] {
            assert_eq!(host_of_endpoint(endpoint), want, "{endpoint}");
        }
    }
}
