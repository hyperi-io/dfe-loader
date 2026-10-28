// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! One single-node KRaft Apache Kafka broker with three secured listeners, and
//! the certificates for it.
//!
//! The broker is Apache Kafka's own image, driven by the docker CLI so the
//! certificates and `server.properties` are in place before it starts. It runs
//! on the host network, so the addresses it advertises are the ones the loader
//! dials.
//!
//! | Listener   | Transport | Client must present                     |
//! |------------|-----------|-----------------------------------------|
//! | `SSL`      | TLS       | nothing                                 |
//! | `SASL_SSL` | TLS       | SCRAM-SHA-512 credentials               |
//! | `SSL_MTLS` | TLS       | a certificate signed by the test CA     |
//!
//! The certificates come from `openssl` in a pinned container, so the host needs
//! no `openssl` and the keys come from a different implementation from the one
//! under test (librdkafka links OpenSSL, the broker is a JVM).

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;

/// Makes the throwaway certificates.
// renovate: datasource=docker depName=alpine/openssl
const OPENSSL_IMAGE: &str =
    "alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2";

/// The JVM image, as the rest of the suite pins it: `apache/kafka-native` before 4.4.0 segfaults
/// in `getpwuid` on ~2% of starts.
// renovate: datasource=docker depName=apache/kafka
const KAFKA_IMAGE: &str =
    "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

/// Directory inside the broker container the test's files are copied to.
const BROKER_DIR: &str = "/dfe-test";

/// A JVM broker takes 5-12 s to become ready, a good deal longer on a busy runner.
const START_DEADLINE: Duration = Duration::from_secs(240);

/// How long an image may take to pull.
const PULL_DEADLINE: Duration = Duration::from_secs(300);

/// The SCRAM-SHA-512 user the broker is formatted with.
pub const SCRAM_USER: &str = "dfe";

/// Its password.
pub const SCRAM_PASSWORD: &str = "dfe-secret";

/// Fixed cluster id: KRaft storage must be formatted with one.
const CLUSTER_ID: &str = "MkU3OEVBNTcwNTJENDM2Qk";

/// Certificates for the broker and its clients.
///
/// `ca.crt` signs `server` (valid for `localhost` and `127.0.0.1`) and `client`.
/// `rogue-ca.crt` is an unrelated CA that signs `rogue-client`.
const PKI_SCRIPT: &str = r#"set -e
mkdir /pki && cd /pki
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"; }
ca() {
  key "$1.key"
  openssl req -x509 -new -key "$1.key" -subj "/CN=$1" -days 2 \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$1.crt"
}
leaf() {
  key "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$1" -out "$1.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA "$2.crt" -CAkey "$2.key" -CAcreateserial -days 2 -extfile "$1.ext" -out "$1.crt"
}
ca ca
ca rogue-ca
leaf server ca "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf client ca "extendedKeyUsage=clientAuth"
leaf rogue-client rogue-ca "extendedKeyUsage=clientAuth"
cat server.key server.crt > server.pem
rm -f *.csr *.ext *.srl
chmod 644 *
tar -cf - .
"#;

async fn docker(args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("docker could not run: {e}"))
}

/// The generated certificates, in a directory removed on drop.
pub struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    /// Generate the certificates in a container.
    ///
    /// # Panics
    ///
    /// When Docker or the openssl image fails: a TLS test that cannot make its
    /// certificates has nothing to test.
    pub async fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("temp dir: {e}"));
        let out = docker(&[
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            OPENSSL_IMAGE,
            "-c",
            PKI_SCRIPT,
        ])
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            out.status.success(),
            "the openssl container could not make certificates: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tar = dir.path().join("pki.tar");
        std::fs::write(&tar, &out.stdout).unwrap_or_else(|e| panic!("write pki.tar: {e}"));
        let status = std::process::Command::new("tar")
            .arg("-xf")
            .arg(&tar)
            .arg("-C")
            .arg(dir.path())
            .status()
            .unwrap_or_else(|e| panic!("tar could not run: {e}"));
        assert!(status.success(), "tar could not unpack the certificates");
        std::fs::remove_file(&tar).unwrap_or_else(|e| panic!("remove pki.tar: {e}"));
        Self { dir }
    }

    /// Path of a generated file on the host, such as `ca.crt` or `client.key`.
    #[must_use]
    pub fn file(&self, name: &str) -> String {
        self.dir.path().join(name).to_string_lossy().into_owned()
    }

    fn dir(&self) -> &Path {
        self.dir.path()
    }
}

/// A running broker, removed when dropped.
pub struct TlsBroker {
    name: String,
    /// The certificates the broker and the test's clients use.
    pub pki: Pki,
    /// `SSL` listener: TLS, no client certificate asked for.
    pub tls: String,
    /// `SASL_SSL` listener: TLS plus SCRAM-SHA-512.
    pub sasl_ssl: String,
    /// `SSL_MTLS` listener: TLS, a client certificate required.
    pub mtls: String,
    scratch: tempfile::TempDir,
}

fn server_properties(ports: [u16; 4]) -> String {
    let [controller, tls, sasl_ssl, mtls] = ports;
    format!(
        "process.roles=broker,controller\n\
         node.id=1\n\
         controller.quorum.voters=1@127.0.0.1:{controller}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://127.0.0.1:{controller},SSL://127.0.0.1:{tls},\
         SASL_SSL://127.0.0.1:{sasl_ssl},SSL_MTLS://127.0.0.1:{mtls}\n\
         advertised.listeners=SSL://127.0.0.1:{tls},SASL_SSL://127.0.0.1:{sasl_ssl},\
         SSL_MTLS://127.0.0.1:{mtls}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL,SSL_MTLS:SSL\n\
         inter.broker.listener.name=SSL\n\
         ssl.keystore.type=PEM\n\
         ssl.keystore.location={BROKER_DIR}/server.pem\n\
         ssl.truststore.type=PEM\n\
         ssl.truststore.location={BROKER_DIR}/ca.crt\n\
         ssl.client.auth=none\n\
         listener.name.ssl_mtls.ssl.client.auth=required\n\
         sasl.enabled.mechanisms=SCRAM-SHA-512\n\
         listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
         org.apache.kafka.common.security.scram.ScramLoginModule required;\n\
         offsets.topic.replication.factor=1\n\
         transaction.state.log.replication.factor=1\n\
         transaction.state.log.min.isr=1\n\
         group.initial.rebalance.delay.ms=0\n\
         num.partitions=1\n\
         log.dirs=/tmp/kraft-logs\n"
    )
}

/// Formats the KRaft storage with the SCRAM user, then runs the broker.
fn start_script() -> String {
    format!(
        "set -e\n\
         cd /opt/kafka\n\
         bin/kafka-storage.sh format -t {CLUSTER_ID} -c {BROKER_DIR}/server.properties \
         --add-scram 'SCRAM-SHA-512=[name={SCRAM_USER},password={SCRAM_PASSWORD}]' --ignore-formatted\n\
         exec bin/kafka-server-start.sh {BROKER_DIR}/server.properties\n"
    )
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("write {}: {e}", path.display()))
}

impl TlsBroker {
    /// Generate the certificates, start the broker and wait until it serves.
    ///
    /// `ports` are the controller, `SSL`, `SASL_SSL` and `SSL_MTLS` listener
    /// ports on `127.0.0.1`. `test` names the owning test in the container name.
    ///
    /// # Errors
    ///
    /// The docker CLI's complaint when an image will not pull, the files will
    /// not copy in, or the broker does not come up.
    pub async fn start(test: &str, ports: [u16; 4]) -> Result<Self, String> {
        let pki = Pki::generate().await;
        let [_, tls, sasl_ssl, mtls] = ports;

        let scratch = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
        for name in ["ca.crt", "server.pem"] {
            std::fs::copy(pki.dir().join(name), scratch.path().join(name))
                .map_err(|e| format!("stage {name}: {e}"))?;
        }
        write(
            &scratch.path().join("server.properties"),
            &server_properties(ports),
        )?;
        write(&scratch.path().join("start.sh"), &start_script())?;
        // The broker runs as its image's user, whose uid need not match this process's.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(scratch.path(), std::fs::Permissions::from_mode(0o755))
                .map_err(|e| format!("chmod scratch dir: {e}"))?;
        }

        let name = crate::common::container_name(Some(test), "kafka");
        crate::common::reap_stale(&name);
        let mut create: Vec<String> = [
            "create",
            "--name",
            &name,
            "--network",
            "host",
            "--env",
            "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
        for (key, value) in crate::common::test_labels("kafka") {
            create.extend(["--label".to_string(), format!("{key}={value}")]);
        }
        create.extend(
            ["--entrypoint", "sh", KAFKA_IMAGE]
                .iter()
                .map(|s| (*s).to_string()),
        );
        create.push(format!("{BROKER_DIR}/start.sh"));
        let refs: Vec<&str> = create.iter().map(String::as_str).collect();
        let created = tokio::time::timeout(PULL_DEADLINE, docker(&refs))
            .await
            .map_err(|_| format!("{KAFKA_IMAGE} did not pull within {PULL_DEADLINE:?}"))??;
        if !created.status.success() {
            return Err(format!(
                "docker create {KAFKA_IMAGE} failed: {}",
                String::from_utf8_lossy(&created.stderr)
            ));
        }
        // From here the Drop impl removes the container on any early return.
        let broker = Self {
            name,
            pki,
            tls: format!("127.0.0.1:{tls}"),
            sasl_ssl: format!("127.0.0.1:{sasl_ssl}"),
            mtls: format!("127.0.0.1:{mtls}"),
            scratch,
        };

        let src = format!("{}/.", broker.scratch.path().display());
        let dest = format!("{}:{BROKER_DIR}", broker.name);
        let copied = docker(&["cp", &src, &dest]).await?;
        if !copied.status.success() {
            return Err(format!(
                "docker cp into {} failed: {}",
                broker.name,
                String::from_utf8_lossy(&copied.stderr)
            ));
        }
        let started = docker(&["start", &broker.name]).await?;
        if !started.status.success() {
            return Err(format!(
                "docker start {} failed: {}",
                broker.name,
                String::from_utf8_lossy(&started.stderr)
            ));
        }
        broker
            .wait_for_log("Kafka Server started", START_DEADLINE)
            .await?;
        Ok(broker)
    }

    /// Everything the broker has written to stdout and stderr.
    pub async fn logs(&self) -> String {
        match docker(&["logs", &self.name]).await {
            Ok(out) => {
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                text
            }
            Err(e) => e,
        }
    }

    async fn running(&self) -> bool {
        docker(&["inspect", "--format", "{{.State.Running}}", &self.name])
            .await
            .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "true")
    }

    async fn wait_for_log(&self, needle: &str, within: Duration) -> Result<(), String> {
        let deadline = Instant::now() + within;
        loop {
            let logs = self.logs().await;
            if logs.contains(needle) {
                return Ok(());
            }
            let running = self.running().await;
            if !running || Instant::now() > deadline {
                return Err(format!(
                    "{} never logged {needle:?} (running: {running}). Log:\n{logs}",
                    self.name
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

impl Drop for TlsBroker {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", "--volumes", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
