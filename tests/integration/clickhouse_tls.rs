// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! HTTPS and native TLS to `ClickHouse` are verified against the system trust store.
//!
//! A private CA reaches the loader's client the only way the config allows,
//! through the trust store (`SSL_CERT_FILE` here), so the same server is
//! accepted when its CA is there and refused when it is not, when another CA
//! is, and when the host name is not the one the certificate names.
//!
//! Runs against the run's shared `ClickHouse`, whose certificate names
//! `localhost` and the Docker host and chains to a CA made for the run.
//! Gated behind `#[cfg(feature = "testcontainers")]`. Needs process-per-test
//! (nextest): it sets `SSL_CERT_FILE` for the whole process.

#![cfg(feature = "testcontainers")]

use std::net::ToSocketAddrs;
use std::path::Path;

use dfe_loader::clickhouse::client_http::build_client;
use dfe_loader::clickhouse::{ChClient, ClickHouseConfig, ClickHouseQueryClient, Transport};

use crate::common::containers::{
    CLICKHOUSE_HTTP_PORT, CLICKHOUSE_HTTPS_PORT, CLICKHOUSE_NATIVE_TLS_PORT, private_ca,
    shared_clickhouse, shared_clickhouse_ca, tls_name,
};

fn config(transport: Transport, host: &str, port: u16, tls: bool) -> ClickHouseConfig {
    ClickHouseConfig {
        hosts: vec![format!("{host}:{port}")],
        transport,
        database: "default".to_string(),
        password: crate::common::containers::clickhouse_password(),
        tls,
        ..Default::default()
    }
}

/// Point the system trust store at `bundle`, or back at the platform's own.
fn trust(bundle: Option<&Path>) {
    // SAFETY: test-only; nextest runs this test in a process of its own.
    unsafe {
        std::env::remove_var("SSL_CERT_DIR");
        match bundle {
            Some(path) => std::env::set_var("SSL_CERT_FILE", path),
            None => std::env::remove_var("SSL_CERT_FILE"),
        }
    }
}

/// Every message in `err`'s source chain, so a TLS refusal is visible past
/// the transport's own wording.
fn chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut next = err.source();
    while let Some(e) = next {
        out.push_str(" <- ");
        out.push_str(&e.to_string());
        next = e.source();
    }
    out
}

/// `SELECT 1` through the loader's client, with the trust store as it now stands.
async fn select_one(config: &ClickHouseConfig) -> Result<(), String> {
    match build_client(config).map_err(|e| e.to_string())? {
        ChClient::Http(client) => client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|e| chain(&e)),
        ChClient::Native(client) => client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|e| chain(&e)),
    }
}

/// HTTPS on its port, and the native protocol over TLS on its own, each
/// checked against the same trust-store cases.
#[tokio::test(flavor = "multi_thread")]
async fn tls_is_verified_against_the_system_trust_store_on_both_protocols() {
    let shared = shared_clickhouse().await;
    let ca_pem = shared_clickhouse_ca().await;
    let other_ca = private_ca("unrelated CA");
    // The certificate names this host, and not its address.
    let named = tls_name(&shared.host).to_string();
    let unnamed = (shared.host.as_str(), 0)
        .to_socket_addrs()
        .expect("resolve the Docker host")
        .find(std::net::SocketAddr::is_ipv4)
        .expect("an IPv4 address for the Docker host")
        .ip()
        .to_string();
    let http_port = shared.port(CLICKHOUSE_HTTP_PORT);
    let https_port = shared.port(CLICKHOUSE_HTTPS_PORT);
    let native_tls_port = shared.port(CLICKHOUSE_NATIVE_TLS_PORT);

    let dir = tempfile::tempdir().expect("CA bundle directory");
    let ca_bundle = dir.path().join("private-ca.pem");
    let other_bundle = dir.path().join("other-ca.pem");
    std::fs::write(&ca_bundle, &ca_pem).expect("write CA bundle");
    std::fs::write(&other_bundle, other_ca.pem()).expect("write other CA bundle");

    trust(Some(&ca_bundle));
    let plain_http = select_one(&config(Transport::Http, &named, http_port, false)).await;
    assert!(
        plain_http.is_ok(),
        "plain HTTP with TLS off failed: {plain_http:?}"
    );

    for (transport, port) in [
        (Transport::Http, https_port),
        (Transport::Native, native_tls_port),
    ] {
        trust(Some(&ca_bundle));
        let with_its_ca = select_one(&config(transport, &named, port, true)).await;
        let query_client = ClickHouseQueryClient::new(&config(transport, &named, port, true))
            .expect("query client")
            .health_check()
            .await;
        let wrong_name = select_one(&config(transport, &unnamed, port, true)).await;

        trust(Some(&other_bundle));
        let wrong_ca = select_one(&config(transport, &named, port, true)).await;

        trust(None);
        let no_ca = select_one(&config(transport, &named, port, true)).await;

        assert!(
            with_its_ca.is_ok(),
            "{transport}: refused with the server's CA in the trust store: {with_its_ca:?}"
        );
        assert!(
            query_client.is_ok(),
            "{transport}: the query client was refused with the server's CA in the trust \
             store: {query_client:?}"
        );
        for (case, result) in [
            ("a host name the certificate does not name", &wrong_name),
            ("only an unrelated CA in the trust store", &wrong_ca),
            ("the platform trust store alone", &no_ca),
        ] {
            let err = result
                .as_ref()
                .expect_err(&format!("{transport}: TLS was accepted with {case}"));
            // A platform store with no usable CA fails at the client build instead.
            assert!(
                err.contains("invalid peer certificate")
                    || err.contains("no CA certificate could be read")
                    || err.contains("trust store is empty"),
                "{transport}: with {case} the request failed for a reason other than \
                 certificate verification: {err}"
            );
        }
    }
}

/// Native TLS fails over from a host that is down to one that is up, and
/// verifies the second against its own name, not the first's address, which
/// the certificate does not carry.
#[tokio::test(flavor = "multi_thread")]
async fn native_tls_fails_over_to_a_second_host_verified_against_its_own_name() {
    let shared = shared_clickhouse().await;
    let ca_pem = shared_clickhouse_ca().await;
    let named = tls_name(&shared.host).to_string();
    let native_tls_port = shared.port(CLICKHOUSE_NATIVE_TLS_PORT);
    let down = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free local port")
        .port();

    let dir = tempfile::tempdir().expect("CA bundle directory");
    let ca_bundle = dir.path().join("private-ca.pem");
    std::fs::write(&ca_bundle, &ca_pem).expect("write CA bundle");
    trust(Some(&ca_bundle));

    let mut failover = config(Transport::Native, "127.0.0.1", down, true);
    failover.hosts.push(format!("{named}:{native_tls_port}"));
    let landed = select_one(&failover).await;

    assert!(
        landed.is_ok(),
        "native TLS did not land on the second host: {landed:?}"
    );
}
